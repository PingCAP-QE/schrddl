package dbconfig

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func setup(t *testing.T) string {
	t.Helper()
	for _, key := range []string{"SCHRDDL_DB_USER", "SCHRDDL_DB_PASSWORD_FILE", "SCHRDDL_ALLOW_INSECURE", "SCHRDDL_TLS_CA", "SCHRDDL_TLS_CERT", "SCHRDDL_TLS_KEY"} {
		t.Setenv(key, "")
	}
	t.Setenv("SCHRDDL_DB_USER", "workload")
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("synthetic:@/password\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCHRDDL_DB_PASSWORD_FILE", path)
	return path
}

func TestCredentialsAndTLSFailClosed(t *testing.T) {
	path := setup(t)
	cfg, err := FromEnv("[::1]:4001", "test")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "workload" || cfg.Passwd != "synthetic:@/password" || cfg.TLS == nil || cfg.TLS.InsecureSkipVerify || cfg.AllowFallbackToPlaintext || cfg.TLS.ServerName != "::1" || cfg.TLS.MinVersion < tls.VersionTLS12 {
		t.Fatal("unsafe configuration")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := FromEnv("localhost:4000", "test"); err == nil || strings.Contains(err.Error(), "synthetic") {
		t.Fatal("accepted public password file or leaked secret")
	}
	t.Setenv("SCHRDDL_DB_PASSWORD_FILE", "")
	if _, err := FromEnv("localhost:4000", "test"); err == nil {
		t.Fatal("accepted missing credentials")
	}
	t.Setenv("SCHRDDL_ALLOW_INSECURE", "true")
	cfg, err = FromEnv("localhost:4000", "test")
	if err != nil || cfg.TLS != nil {
		t.Fatal("explicit local insecure configuration failed")
	}
	t.Setenv("SCHRDDL_DB_USER", "")
	if _, err := FromEnv("localhost:4000", "test"); err == nil {
		t.Fatal("silently defaulted user")
	}
}

func TestStorageURIRejectsCredentialsWithoutEchoingThem(t *testing.T) {
	for _, raw := range []string{"s3://bucket/prefix?access-key=synthetic-secret", "s3://user:synthetic-secret@bucket", "s3://bucket?credentials=synthetic-secret", "s3://bucket?endpoint=https://synthetic-secret", "s3://bucket?%zz", "file:///tmp/a"} {
		if err := ValidateStorageURI(raw); err == nil || strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatalf("unsafe URI handling")
		}
	}
	if err := ValidateStorageURI("s3://bucket/prefix?region=us-east-1"); err != nil {
		t.Fatal(err)
	}
}

func TestPasswordFileReload(t *testing.T) {
	path := setup(t)
	if err := os.WriteFile(path, []byte("rotated-password\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := FromEnv("localhost:4000", "test")
	if err != nil || cfg.Passwd != "rotated-password" {
		t.Fatal("rotated secret was not read")
	}
}

func greeting(withTLS bool) []byte {
	caps := uint32(0x00088201)
	if withTLS {
		caps |= 0x800
	}
	p := append([]byte{10}, []byte("5.7.25\x00")...)
	p = append(p, 1, 0, 0, 0)
	p = append(p, []byte("12345678\x00")...)
	p = append(p, byte(caps), byte(caps>>8), 33, 2, 0, byte(caps>>16), byte(caps>>24), 21)
	p = append(p, make([]byte, 10)...)
	p = append(p, []byte("123456789012\x00mysql_native_password\x00")...)
	return append([]byte{byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16), 0}, p...)
}

func TestConnectionDoesNotAuthenticateWithoutVerifiedTLS(t *testing.T) {
	setup(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pair, err := tls.X509KeyPair(pemCert, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pemCert, 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"no-tls", "untrusted", "wrong-host", "trusted"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			auth := make(chan bool, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					auth <- false
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				conn.Write(greeting(mode != "no-tls"))
				header := make([]byte, 4)
				if _, err := io.ReadFull(conn, header); err != nil {
					auth <- false
					return
				}
				size := int(binary.LittleEndian.Uint32(append(header[:3], 0)))
				payload := make([]byte, size)
				if _, err := io.ReadFull(conn, payload); err != nil {
					auth <- false
					return
				}
				if mode == "no-tls" {
					auth <- true
					return
				}
				secure := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})
				if err := secure.Handshake(); err != nil {
					auth <- false
					return
				}
				_, err = io.ReadFull(secure, header)
				auth <- err == nil
			}()
			t.Setenv("SCHRDDL_TLS_CA", "")
			if mode == "wrong-host" || mode == "trusted" {
				t.Setenv("SCHRDDL_TLS_CA", ca)
			}
			cfg, err := FromEnv(listener.Addr().String(), "test")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "trusted" {
				cfg.TLS.ServerName = "localhost"
			}
			connector, err := mysql.NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			db := sql.OpenDB(connector)
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = db.PingContext(ctx)
			if received := <-auth; received != (mode == "trusted") {
				t.Fatalf("authentication sent=%v, mode=%s", received, mode)
			}
		})
	}
}
