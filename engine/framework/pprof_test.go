package framework

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPprofConfinesAddressAndRequiresAuthentication(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	token := strings.Repeat("a", 32)
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{":6060", "0.0.0.0:6060", "[::]:6060", "192.0.2.1:6060", "localhost:6060"} {
		if _, err := newPprofServer(addr, file); err == nil {
			t.Fatalf("accepted non-loopback binding %q", addr)
		}
	}
	if _, err := newPprofServer("127.0.0.1:6060", ""); err == nil {
		t.Fatal("accepted missing token")
	}
	server, err := newPprofServer("127.0.0.1:6060", file)
	if err != nil {
		t.Fatal(err)
	}
	if server.ReadHeaderTimeout == 0 || server.ReadTimeout == 0 || server.WriteTimeout == 0 || server.IdleTimeout == 0 {
		t.Fatal("missing HTTP timeouts")
	}
	for _, tc := range []struct {
		path, authorization string
		status              int
	}{
		{"/debug/pprof/", "", http.StatusUnauthorized},
		{"/debug/pprof/", "Bearer wrong", http.StatusUnauthorized},
		{"/debug/pprof/", "Bearer " + token, http.StatusOK},
		{"/debug/pprof/cmdline", "Bearer " + token, http.StatusNotFound},
		{"/debug/pprof/cmdline/", "Bearer " + token, http.StatusNotFound},
	} {
		request := httptest.NewRequest(http.MethodGet, tc.path, nil)
		request.Header.Set("Authorization", tc.authorization)
		response := httptest.NewRecorder()
		server.Handler.ServeHTTP(response, request)
		if response.Code != tc.status {
			t.Fatalf("%s: got %d, expected %d", tc.path, response.Code, tc.status)
		}
	}
	if err := os.Chmod(file, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := newPprofServer("127.0.0.1:6060", file); err == nil {
		t.Fatal("accepted shared token file")
	}
}
