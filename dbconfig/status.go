// Status API access shares the SQL connections' credentials and TLS policy.
package dbconfig

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxStatusResponse bounds status API payloads so a hostile or broken
// endpoint cannot exhaust process memory.
const maxStatusResponse = 64 << 20 // 64 MiB

// statusPortOffset maps the SQL port to the status API port (4000 -> 10080).
const statusPortOffset = 6080

// StatusAddress returns the TiDB status API address for a SQL address. The
// status port is derived from the SQL port (plus 6080) unless
// SCHRDDL_STATUS_ADDR provides an explicit host:port override.
func StatusAddress(address string) (string, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return "", fmt.Errorf("database address must be host:port")
	}
	if override := os.Getenv("SCHRDDL_STATUS_ADDR"); override != "" {
		overrideHost, overridePort, err := net.SplitHostPort(override)
		if err != nil || overrideHost == "" || strings.ContainsAny(override, "/?#") {
			return "", fmt.Errorf("SCHRDDL_STATUS_ADDR must be an explicit host:port")
		}
		if port, err := strconv.Atoi(overridePort); err != nil || port <= 0 || port > 65535 {
			return "", fmt.Errorf("SCHRDDL_STATUS_ADDR must be an explicit host:port")
		}
		return net.JoinHostPort(overrideHost, overridePort), nil
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port+statusPortOffset > 65535 {
		return "", fmt.Errorf("cannot derive a status port from %q; set SCHRDDL_STATUS_ADDR", address)
	}
	return net.JoinHostPort(host, strconv.Itoa(port+statusPortOffset)), nil
}

// GetStatusJSON fetches path from the TiDB status API and returns its body
// only after the response passes validation. The request carries the same
// workload credentials (basic authentication) and TLS policy (verified
// TLS 1.2+, plaintext only with SCHRDDL_ALLOW_INSECURE=true) as SQL
// connections; cleartext HTTP and shell helpers such as curl are never used.
func GetStatusJSON(address, path string) ([]byte, error) {
	cfg, err := FromEnv(address, "")
	if err != nil {
		return nil, err
	}
	statusAddr, err := StatusAddress(address)
	if err != nil {
		return nil, err
	}
	scheme := "https"
	transport := &http.Transport{
		TLSClientConfig: cfg.TLS,
		Proxy:           nil,
	}
	if cfg.TLS == nil {
		// Only reachable with SCHRDDL_ALLOW_INSECURE=true, which is meant
		// for isolated disposable test clusters.
		scheme = "http"
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	request, err := http.NewRequest(http.MethodGet, scheme+"://"+statusAddr+path, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid status API request")
	}
	request.SetBasicAuth(cfg.User, cfg.Passwd)
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("status API request to %s failed", statusAddr)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status API returned %s", response.Status)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "" && !strings.Contains(contentType, "json") {
		return nil, fmt.Errorf("status API returned unexpected content type %q", contentType)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxStatusResponse+1))
	if err != nil || len(body) > maxStatusResponse {
		return nil, fmt.Errorf("invalid status API response body")
	}
	// Reject the payload before callers trust it as schema metadata.
	if !json.Valid(body) {
		return nil, fmt.Errorf("status API response is not valid JSON")
	}
	return body, nil
}

// StatusPath escapes database and table names into a status API path.
func StatusPath(elements ...string) string {
	escaped := make([]string, 0, len(elements))
	for _, element := range elements {
		escaped = append(escaped, url.PathEscape(element))
	}
	return "/" + strings.Join(escaped, "/")
}
