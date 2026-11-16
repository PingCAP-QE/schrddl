package dbconfig

import (
	"bytes"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
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
	t.Setenv("SCHRDDL_STATUS_ADDR", "10.0.0.1:10080")
	if got, err := StatusAddress("127.0.0.1:4000"); err != nil || got != "10.0.0.1:10080" {
		t.Fatalf("override ignored: %q, %v", got, err)
	}
	for _, bad := range []string{"10.0.0.1:0", "10.0.0.1:70000", "10.0.0.1/a:10080", "10080", "10.0.0.1:"} {
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

func TestGetStatusJSONAuthenticatesAndVerifiesTLS(t *testing.T) {
	setup(t)
	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("workload:synthetic:@/password"))
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

func TestGetStatusJSONRefusesPlaintextUnlessInsecureOptIn(t *testing.T) {
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

	t.Setenv("SCHRDDL_ALLOW_INSECURE", "true")
	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("workload:synthetic:@/password"))
	insecure := httptest.NewServer(requireAuth(expectedAuth, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	}))
	t.Cleanup(insecure.Close)
	statusOverride(t, insecure)
	if _, err := GetStatusJSON("127.0.0.1:4000", "/schema/test/t1"); err != nil {
		t.Fatalf("insecure mode fetch failed: %v", err)
	}
}
