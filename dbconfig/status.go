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

	"github.com/go-sql-driver/mysql"
)

// maxStatusResponse bounds status API payloads so a hostile or broken
// endpoint cannot exhaust process memory.
const maxStatusResponse = 64 << 20 // 64 MiB

// statusPortOffset maps the SQL port to the status API port (4000 -> 10080).
const statusPortOffset = 6080

// sameHost reports whether two hosts name the same endpoint, comparing names
// and IP literals the way TLS hostname verification does.
func sameHost(a, b string) bool {
	a, b = strings.TrimSuffix(a, "."), strings.TrimSuffix(b, ".")
	if ipA, ipB := net.ParseIP(a), net.ParseIP(b); ipA != nil && ipB != nil {
		return ipA.Equal(ipB)
	}
	return strings.EqualFold(a, b)
}

// StatusAddress returns the TiDB status API address for a SQL address. The
// status port is derived from the SQL port (plus 6080) unless
// SCHRDDL_STATUS_ADDR provides an explicit host:port override. An override
// may only select another port: its host must be the SQL host, the one
// endpoint already trusted with the workload credentials.
func StatusAddress(address string) (string, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return "", fmt.Errorf("database address must be host:port")
	}
	if override := os.Getenv("SCHRDDL_STATUS_ADDR"); override != "" {
		overrideHost, overridePort, err := net.SplitHostPort(override)
		if err != nil || overrideHost == "" || strings.ContainsAny(override, "/?#@") {
			return "", fmt.Errorf("SCHRDDL_STATUS_ADDR must be an explicit host:port")
		}
		if !sameHost(overrideHost, host) {
			return "", fmt.Errorf("SCHRDDL_STATUS_ADDR must use the database host %q", host)
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
// only after the response passes validation. Under the default TLS policy the
// request is protected by verified TLS 1.2+ and carries the workload
// credentials as basic authentication. SCHRDDL_ALLOW_INSECURE=true permits
// plaintext HTTP for isolated disposable clusters; such requests carry no
// credentials, because HTTP Basic would transmit the database password
// verbatim. Shell helpers such as curl are never used.
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
		scheme = "http"
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		// Never follow a redirect: credentials must only reach the host
		// that was validated for this request.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	request, err := newStatusRequest(cfg, scheme, statusAddr, path)
	if err != nil {
		return nil, err
	}
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

// newStatusRequest builds a status API GET. The workload credentials are
// attached only to a request protected by verified TLS: HTTP Basic would
// transmit the database password verbatim, which SCHRDDL_ALLOW_INSECURE does
// not permit.
func newStatusRequest(cfg *mysql.Config, scheme, address, path string) (*http.Request, error) {
	request, err := http.NewRequest(http.MethodGet, scheme+"://"+address+path, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid status API request")
	}
	if scheme == "https" && cfg.TLS != nil && !cfg.TLS.InsecureSkipVerify {
		request.SetBasicAuth(cfg.User, cfg.Passwd)
	}
	request.Header.Set("Accept", "application/json")
	return request, nil
}

// StatusPath escapes database and table names into a status API path.
func StatusPath(elements ...string) string {
	escaped := make([]string, 0, len(elements))
	for _, element := range elements {
		escaped = append(escaped, url.PathEscape(element))
	}
	return "/" + strings.Join(escaped, "/")
}
