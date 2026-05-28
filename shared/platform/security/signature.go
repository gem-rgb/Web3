package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// RequestSigner creates and validates HMAC signatures for internal requests.
type RequestSigner struct {
	Secret      []byte
	AllowedSkew time.Duration
}

// Sign returns a detached signature for the request payload.
func (s RequestSigner) Sign(method, path string, body []byte, timestamp time.Time) string {
	canonical := canonicalRequest(method, path, body, timestamp.UTC())
	mac := hmac.New(sha256.New, s.Secret)
	_, _ = mac.Write([]byte(canonical))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Verify validates a detached signature.
func (s RequestSigner) Verify(method, path string, body []byte, timestamp time.Time, signature string) error {
	if len(s.Secret) == 0 {
		return errors.New("missing request signing secret")
	}
	expected := s.Sign(method, path, body, timestamp)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return errors.New("invalid request signature")
	}
	if s.AllowedSkew <= 0 {
		s.AllowedSkew = time.Minute
	}
	if time.Since(timestamp.UTC()) > s.AllowedSkew || time.Until(timestamp.UTC()) > s.AllowedSkew {
		return errors.New("request timestamp outside allowed skew")
	}
	return nil
}

// AuthorizationHeaders returns the headers used for signed requests.
func (s RequestSigner) AuthorizationHeaders(method, path string, body []byte, timestamp time.Time) map[string]string {
	return map[string]string{
		"X-RMS-Signature":     s.Sign(method, path, body, timestamp),
		"X-RMS-Signature-Ts":   fmt.Sprintf("%d", timestamp.UTC().Unix()),
		"X-RMS-Signature-Alg":  "hs256",
		"X-RMS-Signature-Vers": "v1",
	}
}

func canonicalRequest(method, path string, body []byte, timestamp time.Time) string {
	return strings.ToUpper(strings.TrimSpace(method)) + "\n" +
		strings.TrimSpace(path) + "\n" +
		string(body) + "\n" +
		fmt.Sprintf("%d", timestamp.Unix())
}

