package dbconfig

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestStatusAddressDerivation(t *testing.T) {
	t.Setenv("SCHRDDL_STATUS_ADDR", "")
	for _, tc := range []struct{ address, want string }{
		{"127.0.0.1:4000", "127.0.0.1:10080"},
		{"tidb.example:4400", "tidb.example:10480"},
	} {
		if got, err := StatusAddress(tc.address); err != nil || got != tc.want {
			t.Fatalf("StatusAddress(%q) = %q, %v; want %q", tc.address, got, err, tc.want)
		}
	}
	// An override may only select another port on the SQL host.
	for _, tc := range []struct{ address, override, want string }{
		{"127.0.0.1:4000", "127.0.0.1:10081", "127.0.0.1:10081"},
		{"tidb.example:4000", "TIDB.Example.:10080", "TIDB.Example.:10080"},
		{"[::1]:4000", "[0:0:0:0:0:0:0:1]:10080", "[0:0:0:0:0:0:0:1]:10080"},
	} {
		t.Setenv("SCHRDDL_STATUS_ADDR", tc.override)
		if got, err := StatusAddress(tc.address); err != nil || got != tc.want {
			t.Fatalf("StatusAddress(%q) with override %q = %q, %v; want %q", tc.address, tc.override, got, err, tc.want)
		}
	}
	// A status address on any other host would receive the credentials
	// intended for the SQL host.
	for _, tc := range []struct{ address, override string }{
		{"127.0.0.1:4000", "10.0.0.1:10080"},
		{"127.0.0.1:4000", "localhost:10080"},
		{"localhost:4000", "127.0.0.1:10080"},
		{"tidb.example:4000", "status.tidb.example:10080"},
		{"tidb.example:4000", "tidb.example.evil:10080"},
		{"tidb.example:4000", "tidb.example@evil.example:10080"},
		{"tidb.example:4000", "tidb.example:10080@evil.example:80"},
		{"[::1]:4000", "[::2]:10080"},
	} {
		t.Setenv("SCHRDDL_STATUS_ADDR", tc.override)
		if _, err := StatusAddress(tc.address); err == nil {
			t.Fatalf("accepted SCHRDDL_STATUS_ADDR %q for database %q", tc.override, tc.address)
		}
	}
	for _, bad := range []string{"127.0.0.1:0", "127.0.0.1:70000", "127.0.0.1/a:10080", "10080", "127.0.0.1:"} {
		t.Setenv("SCHRDDL_STATUS_ADDR", bad)
		if _, err := StatusAddress("127.0.0.1:4000"); err == nil {
			t.Fatalf("accepted SCHRDDL_STATUS_ADDR %q", bad)
		}
	}
	t.Setenv("SCHRDDL_STATUS_ADDR", "")
	if _, err := StatusAddress("tidb.example:65535"); err == nil {
		t.Fatal("accepted an underivable status port")
	}
	if _, err := StatusAddress("tidb.example"); err == nil {
		t.Fatal("accepted an address without a port")
	}
}

func tlsStatusServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	caFile := filepath.Join(t.TempDir(), "status-ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caFile, pemBytes, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCHRDDL_TLS_CA", caFile)
	return server
}

func statusOverride(t *testing.T, server *httptest.Server) {
	t.Helper()
	t.Setenv("SCHRDDL_STATUS_ADDR", server.Listener.Addr().String())
}

func requireAuth(expected string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != expected {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// recordAuthorization reports what a status endpoint received before serving it.
func recordAuthorization(seen chan<- string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Authorization")
		next(w, r)
	}
}

func basicAuth(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}

func TestStatusRequestCarriesCredentialsOnlyOverVerifiedTLS(t *testing.T) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd = "workload", "synthetic:@/password"
	verified := &tls.Config{MinVersion: tls.VersionTLS12}
	unverified := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // fixture for the rule under test
	for _, tc := range []struct {
		name       string
		scheme     string
		connTLS    *tls.Config
		authorized bool
	}{
		{"verified TLS", "https", verified, true},
		{"insecure opt-in", "http", nil, false},
		{"plaintext with TLS configured", "http", verified, false},
		{"https without TLS", "https", nil, false},
		{"unverified TLS", "https", unverified, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg.TLS = tc.connTLS
			request, err := newStatusRequest(cfg, tc.scheme, "127.0.0.1:10080", "/schema/test/t1")
			if err != nil {
				t.Fatal(err)
			}
			authorization := request.Header.Get("Authorization")
			if tc.authorized {
				if authorization != basicAuth("workload", "synthetic:@/password") {
					t.Fatalf("verified TLS request carries %q, want the workload credentials", authorization)
				}
			} else if authorization != "" {
				t.Fatalf("credentials were attached to a %s request: %q", tc.scheme, authorization)
			}
		})
	}
}

func TestGetStatusJSONAuthenticatesAndVerifiesTLS(t *testing.T) {
	setup(t)
	expectedAuth := basicAuth("workload", "synthetic:@/password")
	server := tlsStatusServer(t, requireAuth(expectedAuth, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schema/test/t1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":65535,"name":{"O":"t1","L":"t1"}}`)
	}))
	statusOverride(t, server)

	body, err := GetStatusJSON("127.0.0.1:4000", StatusPath("schema", "test", "t1"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"O":"t1"`)) {
		t.Fatalf("unexpected payload %s", body)
	}

	cases := []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{
			name: "unauthenticated",
			path: "/schema/test/t1",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
			},
		},
		{
			name: "missing",
			path: "/schema/test/missing",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
		},
		{
			name: "wrong content type",
			path: "/schema/test/t1",
			handler: requireAuth(expectedAuth, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				fmt.Fprint(w, `<html>trust me</html>`)
			}),
		},
		{
			name: "invalid json",
			path: "/schema/test/t1",
			handler: requireAuth(expectedAuth, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `not json`)
			}),
		},
		{
			name: "oversized",
			path: "/schema/test/t1",
			handler: requireAuth(expectedAuth, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write(bytes.Repeat([]byte("a"), maxStatusResponse+2))
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := tlsStatusServer(t, tc.handler)
			statusOverride(t, server)
			if _, err := GetStatusJSON("127.0.0.1:4000", tc.path); err == nil {
				t.Fatalf("accepted %s response", tc.name)
			}
		})
	}
}

func TestGetStatusJSONDoesNotFollowRedirects(t *testing.T) {
	setup(t)
	var followed atomic.Bool
	server := tlsStatusServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			followed.Store(true)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"redirected":true}`)
			return
		}
		http.Redirect(w, r, "/redirected", http.StatusFound)
	})
	statusOverride(t, server)
	if _, err := GetStatusJSON("127.0.0.1:4000", "/schema/test/t1"); err == nil {
		t.Fatal("accepted a redirect response as schema metadata")
	}
	if followed.Load() {
		t.Fatal("status API redirect was followed")
	}
}

func TestGetStatusJSONRefusesPlaintextInSecureMode(t *testing.T) {
	setup(t)
	// A cleartext endpoint must never be contacted in secure mode: the
	// client attempts verified TLS and fails instead of downgrading.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("cleartext status endpoint was contacted in secure mode")
	}))
	t.Cleanup(server.Close)
	statusOverride(t, server)
	if _, err := GetStatusJSON("127.0.0.1:4000", "/schema/test/t1"); err == nil {
		t.Fatal("secure mode contacted a cleartext status endpoint")
	}
}

func TestGetStatusJSONNeverSendsCredentialsOverPlaintext(t *testing.T) {
	setup(t)
	t.Setenv("SCHRDDL_ALLOW_INSECURE", "true")
	seen := make(chan string, 8)
	status := httptest.NewServer(recordAuthorization(seen, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":65535,"name":{"O":"t1","L":"t1"}}`)
	}))
	t.Cleanup(status.Close)
	statusOverride(t, status)

	// With the password configured for the SQL sessions, and then omitted as
	// the insecure opt-in permits, the plaintext request carries neither.
	for _, passwordFile := range []string{"configured", ""} {
		if passwordFile == "" {
			t.Setenv("SCHRDDL_DB_PASSWORD_FILE", "")
		}
		body, err := GetStatusJSON("127.0.0.1:4000", StatusPath("schema", "test", "t1"))
		if err != nil {
			t.Fatalf("plaintext fetch without credentials failed: %v", err)
		}
		if !bytes.Contains(body, []byte(`"O":"t1"`)) {
			t.Fatalf("unexpected payload %s", body)
		}
		if authorization := <-seen; authorization != "" {
			t.Fatalf("credentials were sent over cleartext HTTP: %q", authorization)
		}
	}

	// A status API that demands authentication is reported, never satisfied
	// by sending the password over plaintext.
	demanding := httptest.NewServer(recordAuthorization(seen, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(demanding.Close)
	statusOverride(t, demanding)
	if _, err := GetStatusJSON("127.0.0.1:4000", "/schema/test/t1"); err == nil {
		t.Fatal("plaintext status API that requires authentication was accepted")
	}
	if authorization := <-seen; authorization != "" {
		t.Fatalf("credentials were sent to a plaintext status API demanding authentication: %q", authorization)
	}
}

func TestGetStatusJSONRejectsStatusAddressOnAnotherHost(t *testing.T) {
	setup(t)
	server := tlsStatusServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("status API request was sent to a host other than the database host")
	})
	// The same listener, named by a host other than the SQL address.
	t.Setenv("SCHRDDL_STATUS_ADDR", strings.Replace(server.URL, "https://127.0.0.1", "localhost", 1))
	_, err := GetStatusJSON("127.0.0.1:4000", "/schema/test/t1")
	if err == nil || !strings.Contains(err.Error(), "must use the database host") {
		t.Fatalf("status address on another host was not rejected: %v", err)
	}
}
