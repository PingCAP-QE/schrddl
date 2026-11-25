package dbconfig

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testCA signs server certificates for the loopback address.
type testCA struct {
	key *ecdsa.PrivateKey
	der []byte
	pem []byte
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{key: key, der: der, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// serverCertificate returns a leaf certificate for 127.0.0.1 signed by the CA.
func (ca *testCA) serverCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := x509.ParseCertificate(ca.der)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, parent, key.Public(), ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// writeCAFile writes content to ca.pem with exactly the requested mode; the
// process umask would otherwise take bits away from os.WriteFile.
func writeCAFile(t *testing.T, content []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

// silentTarget is a TCP listener that answers nothing. The MySQL driver would
// wait for its greeting forever, so every attempt is recorded and closed at
// once instead.
type silentTarget struct {
	listener net.Listener
	attempts chan struct{}
}

func newSilentTarget(t *testing.T) *silentTarget {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	target := &silentTarget{listener: listener, attempts: make(chan struct{}, 8)}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			target.attempts <- struct{}{}
			conn.Close()
		}
	}()
	return target
}

func TestTrustAnchorPinIsOptionalButEnforcedWhenSet(t *testing.T) {
	setup(t)
	const address = "tidb.example:4000"
	// Without a trust anchor the system roots are used and nothing needs pinning.
	cfg, err := FromEnv(address, "test")
	if err != nil || cfg.TLS.RootCAs != nil {
		t.Fatalf("configuration without SCHRDDL_TLS_CA = %v, %v; want the system roots", cfg, err)
	}

	trusted := newTestCA(t, "disposable-test-ca")
	digest := sha256Hex(trusted.pem)
	t.Setenv("SCHRDDL_TLS_CA_SHA256", digest)
	if _, err := FromEnv(address, "test"); err == nil {
		t.Fatal("accepted a pin without a trust anchor")
	}

	t.Setenv("SCHRDDL_TLS_CA", writeCAFile(t, trusted.pem, 0644))
	// The pin is optional: on its own, the file still has to pass the
	// ownership and permission checks.
	t.Setenv("SCHRDDL_TLS_CA_SHA256", "")
	if cfg, err = FromEnv(address, "test"); err != nil || cfg.TLS.RootCAs == nil {
		t.Fatalf("unpinned trust anchor = %v, %v; want it loaded", cfg, err)
	}
	for name, pin := range map[string]string{
		"truncated pin": digest[:63],
		"non-hex pin":   strings.Repeat("g", 64),
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SCHRDDL_TLS_CA_SHA256", pin)
			if _, err := FromEnv(address, "test"); err == nil || !strings.Contains(err.Error(), "SCHRDDL_TLS_CA_SHA256") {
				t.Fatalf("trust anchor without a valid pin = %v, want it refused by name of the pin", err)
			}
		})
	}
	// The pin is accepted in any letter case, with surrounding whitespace.
	t.Setenv("SCHRDDL_TLS_CA_SHA256", " "+strings.ToUpper(digest)+"\n")
	if cfg, err = FromEnv(address, "test"); err != nil || cfg.TLS.RootCAs == nil {
		t.Fatalf("pinned trust anchor = %v, %v; want it loaded", cfg, err)
	}
}

func TestTrustAnchorRejectsSubstitutedAndUnprotectedFiles(t *testing.T) {
	setup(t)
	const address = "tidb.example:4000"
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	junk := []byte("not a certificate at all")
	// A valid bundle that is only too large: PEM ignores text outside blocks.
	oversized := append(bytes.Repeat([]byte("#"), maxTrustAnchor), trusted.pem...)
	for _, tc := range []struct {
		name    string
		content []byte
		pinned  []byte
		mode    os.FileMode
		reason  string
	}{
		// The rogue-CA case: the replacement is every bit as protected as
		// the pinned file, so only the pin can tell them apart.
		{"rogue CA substituted for the pinned one", rogue.pem, trusted.pem, 0644, "SCHRDDL_TLS_CA_SHA256"},
		{"group-writable CA", trusted.pem, trusted.pem, 0664, "not writable by group or others"},
		{"world-writable CA", trusted.pem, trusted.pem, 0666, "not writable by group or others"},
		{"oversized CA", oversized, oversized, 0644, "at most 1 MiB"},
		{"file that is not PEM", junk, junk, 0644, "invalid TLS CA file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SCHRDDL_TLS_CA", writeCAFile(t, tc.content, tc.mode))
			t.Setenv("SCHRDDL_TLS_CA_SHA256", sha256Hex(tc.pinned))
			_, err := FromEnv(address, "test")
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("trust anchor = %v, want it refused with %q", err, tc.reason)
			}
		})
	}
	t.Setenv("SCHRDDL_TLS_CA", t.TempDir())
	t.Setenv("SCHRDDL_TLS_CA_SHA256", sha256Hex(trusted.pem))
	if _, err := FromEnv(address, "test"); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory as the trust anchor = %v, want it refused", err)
	}
}

func TestSymlinkedTrustAnchorIsCheckedAtTheFileItLeadsTo(t *testing.T) {
	setup(t)
	const address = "tidb.example:4000"
	trusted := newTestCA(t, "disposable-test-ca")
	t.Setenv("SCHRDDL_TLS_CA_SHA256", sha256Hex(trusted.pem))
	dir := t.TempDir()
	// The layout of a secret volume: the name is a link into a versioned directory.
	versioned := filepath.Join(dir, "..2026_11_25")
	if err := os.Mkdir(versioned, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(versioned, "ca.pem"), trusted.pem, 0644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "ca.pem")
	if err := os.Symlink(filepath.Join("..2026_11_25", "ca.pem"), link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCHRDDL_TLS_CA", link)
	if cfg, err := FromEnv(address, "test"); err != nil || cfg.TLS.RootCAs == nil {
		t.Fatalf("a secret-volume trust anchor = %v, %v; want it loaded", cfg, err)
	}

	// The link is then repointed at a file other accounts can rewrite.
	weak := filepath.Join(versioned, "weak.pem")
	if err := os.WriteFile(weak, trusted.pem, 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(weak, 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..2026_11_25", "weak.pem"), link); err != nil {
		t.Fatal(err)
	}
	if _, err := FromEnv(address, "test"); err == nil {
		t.Fatal("accepted a symlink to a world-writable trust anchor")
	}
}

// handshakeWithConfiguredTrust dials a TLS server whose certificate is signed
// by ca, using the trust anchor the environment selects for that address.
func handshakeWithConfiguredTrust(t *testing.T, ca *testCA) error {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{ca.serverCertificate(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		if conn, err := listener.Accept(); err == nil {
			_ = conn.(*tls.Conn).Handshake()
			conn.Close()
		}
	}()
	cfg, err := FromEnv(listener.Addr().String(), "test")
	if err != nil {
		<-served
		t.Fatal(err)
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", listener.Addr().String(), cfg.TLS)
	if conn != nil {
		conn.Close()
	}
	<-served
	return err
}

func TestTrustAnchorDecidesWhichServersTheConnectionTrusts(t *testing.T) {
	setup(t)
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	t.Setenv("SCHRDDL_TLS_CA", writeCAFile(t, trusted.pem, 0644))
	t.Setenv("SCHRDDL_TLS_CA_SHA256", sha256Hex(trusted.pem))
	if err := handshakeWithConfiguredTrust(t, trusted); err != nil {
		t.Fatalf("rejected a server signed by the pinned CA: %v", err)
	}
	if err := handshakeWithConfiguredTrust(t, rogue); err == nil {
		t.Fatal("trusted a server signed by a CA outside the pinned trust anchor")
	}
}

func TestPinnedTrustAnchorPreventsConnectionAfterSubstitution(t *testing.T) {
	setup(t)
	target := newSilentTarget(t)
	ping := func() error {
		database, err := Open(target.listener.Addr().String(), "test")
		if err != nil {
			return err
		}
		defer database.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return database.PingContext(ctx)
	}
	waitForAttempt := func() bool {
		select {
		case <-target.attempts:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}

	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	caFile := writeCAFile(t, trusted.pem, 0644)
	t.Setenv("SCHRDDL_TLS_CA", caFile)
	t.Setenv("SCHRDDL_TLS_CA_SHA256", sha256Hex(trusted.pem))
	// With the pinned file in place the target is reached. The driver retries
	// a dropped connection, so every attempt of that ping is drained.
	_ = ping()
	if !waitForAttempt() {
		t.Fatal("the intact trust anchor did not lead to a connection")
	}
	for waitForAttempt() {
	}

	// The trust anchor is replaced afterwards. The replacement keeps the
	// same ownership and permissions, so only the pin can tell it apart.
	if err := os.WriteFile(caFile, rogue.pem, 0644); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(caFile); err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("the scenario needs a protected replacement, got %v, %v", info, err)
	}
	if err := ping(); err == nil {
		t.Fatal("connected through a substituted trust anchor")
	}
	if waitForAttempt() {
		t.Fatal("reached the target before validating the trust anchor")
	}
}
