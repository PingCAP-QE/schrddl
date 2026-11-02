package framework

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"
)

func newPprofServer(address, tokenFile string) (*http.Server, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, fmt.Errorf("pprof requires an explicit loopback IP and port")
	}
	info, err := os.Stat(tokenFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("pprof requires a token file readable only by its owner")
	}
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read pprof token file")
	}
	token := strings.TrimSpace(string(data))
	if len(token) < 32 {
		return nil, fmt.Errorf("pprof token must contain at least 32 characters")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	// Command-line arguments can contain database passwords and DSNs. Never
	// expose them, including to authenticated profiling clients.
	mux.HandleFunc("/debug/pprof/cmdline", http.NotFound)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
	return &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 120 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}, nil
}

func startPprof(address, tokenFile string) (*http.Server, error) {
	server, err := newPprofServer(address, tokenFile)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, err
	}
	go server.Serve(listener)
	return server, nil
}
