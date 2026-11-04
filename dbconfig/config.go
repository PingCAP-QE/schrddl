// Package dbconfig supplies the same credentials and TLS policy to every DB client.
package dbconfig

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ReadSecret reads a mounted secret without exposing its contents in errors.
func ReadSecret(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("cannot open secret file")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("secret file must be a regular owner-only file (0600)")
	}
	// Bound reads, including accidentally mounted large files.
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil || len(data) == 0 || len(data) > 64*1024 {
		return "", fmt.Errorf("invalid secret file")
	}
	value := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if value == "" {
		return "", fmt.Errorf("secret file is empty")
	}
	return value, nil
}

func FromEnv(address, database string) (*mysql.Config, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return nil, fmt.Errorf("database address must be host:port")
	}
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr, cfg.DBName = "tcp", address, database
	cfg.User = os.Getenv("SCHRDDL_DB_USER")
	if cfg.User == "" {
		return nil, fmt.Errorf("SCHRDDL_DB_USER must be set explicitly")
	}
	cfg.Timeout = 10 * time.Second
	insecure := os.Getenv("SCHRDDL_ALLOW_INSECURE")
	if insecure != "" && insecure != "true" && insecure != "false" {
		return nil, fmt.Errorf("SCHRDDL_ALLOW_INSECURE must be true or false")
	}
	if path := os.Getenv("SCHRDDL_DB_PASSWORD_FILE"); path != "" {
		cfg.Passwd, err = ReadSecret(path)
		if err != nil {
			return nil, err
		}
	} else if insecure != "true" {
		return nil, fmt.Errorf("SCHRDDL_DB_PASSWORD_FILE is required")
	}
	if insecure == "true" {
		return cfg, nil
	}
	cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	if path := os.Getenv("SCHRDDL_TLS_CA"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("cannot read TLS CA file")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("invalid TLS CA file")
		}
		cfg.TLS.RootCAs = pool
	}
	cert, key := os.Getenv("SCHRDDL_TLS_CERT"), os.Getenv("SCHRDDL_TLS_KEY")
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("TLS client certificate and key must be configured together")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("cannot load TLS client certificate and key")
		}
		cfg.TLS.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func Open(address, database string) (*sql.DB, error) {
	cfg, err := FromEnv(address, database)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid database connection configuration")
	}
	return sql.OpenDB(connector), nil
}

// ValidateStorageURI prevents credentials from being persisted in a readable TiDB
// global. Use workload identity / instance roles on the TiDB storage workers.
func ValidateStorageURI(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "s3" && u.Scheme != "gs" && u.Scheme != "gcs" && u.Scheme != "azblob") || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("invalid global sort URI; use a credential-free cloud storage URI")
	}
	// Allow only non-secret routing options; new options require an explicit review.
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return fmt.Errorf("invalid global sort URI query")
	}
	for name := range query {
		if name != "region" && name != "force-path-style" && name != "storage-class" {
			return fmt.Errorf("global sort URI options must not contain credentials; use workload identity")
		}
	}
	return nil
}
