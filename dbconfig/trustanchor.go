package dbconfig

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

// maxTrustAnchor bounds the CA bundle so a hostile or misconfigured file
// cannot exhaust memory.
const maxTrustAnchor = 1 << 20 // 1 MiB

// trustPool returns the certificate pool of the trust anchor. The operator
// may pin the bundle with SCHRDDL_TLS_CA_SHA256, the SHA-256 of the PEM file:
// a substituted CA is then rejected even when the replacement has safe
// ownership and permissions.
func trustPool(path, pinnedSHA256 string) (*x509.CertPool, error) {
	pinnedSHA256 = strings.ToLower(strings.TrimSpace(pinnedSHA256))
	if pinnedSHA256 != "" {
		if _, err := hex.DecodeString(pinnedSHA256); err != nil || len(pinnedSHA256) != sha256.Size*2 {
			return nil, fmt.Errorf("SCHRDDL_TLS_CA_SHA256 must be the SHA-256 of the PEM file")
		}
	}
	data, err := readTrustAnchor(path, pinnedSHA256)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("invalid TLS CA file")
	}
	return pool, nil
}

// readTrustAnchor loads the CA bundle that every database connection and
// status API request trusts with its credentials. The file must be a regular
// file owned by root or the process user and not writable by group or others,
// so no other account can plant a rogue CA. When a digest is pinned, the bytes
// must match it as well. The file is read through the descriptor that was
// checked, so the validated bytes are exactly the trusted ones. Symlinks are
// followed on purpose, as secret volumes mount them: the opened file is what
// is checked, not the path.
func readTrustAnchor(path, pinnedSHA256 string) ([]byte, error) {
	// O_NONBLOCK keeps a FIFO planted at the path from blocking the open.
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("cannot open TLS CA file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot inspect TLS CA file")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("cannot inspect TLS CA file")
	}
	if !info.Mode().IsRegular() || info.Size() > maxTrustAnchor {
		return nil, fmt.Errorf("TLS CA file must be a regular file, at most 1 MiB")
	}
	if (owner.Uid != 0 && owner.Uid != uint32(os.Geteuid())) || info.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("TLS CA file must be owned by root or the process user and not writable by group or others")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxTrustAnchor+1))
	if err != nil || len(data) > maxTrustAnchor {
		return nil, fmt.Errorf("cannot read TLS CA file")
	}
	if pinnedSHA256 == "" {
		return data, nil
	}
	digest := sha256.Sum256(data)
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(digest[:])), []byte(pinnedSHA256)) != 1 {
		return nil, fmt.Errorf("TLS CA file does not match SCHRDDL_TLS_CA_SHA256")
	}
	return data, nil
}
