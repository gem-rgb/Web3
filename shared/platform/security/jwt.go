package security

import (
	"bytes"
	"crypto/rand"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type jwtHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid,omitempty"`
}

// JWTClaims represents the canonical claims used by the RMS platform.
type JWTClaims struct {
	Issuer    string            `json:"iss,omitempty"`
	Subject   string            `json:"sub,omitempty"`
	Audience  string            `json:"aud,omitempty"`
	TenantID  string            `json:"tenant_id,omitempty"`
	ClientID  string            `json:"client_id,omitempty"`
	Scopes    []string          `json:"scopes,omitempty"`
	Roles     []string          `json:"roles,omitempty"`
	SessionID string            `json:"sid,omitempty"`
	ID        string            `json:"jti,omitempty"`
	IssuedAt  int64             `json:"iat,omitempty"`
	NotBefore int64             `json:"nbf,omitempty"`
	ExpiresAt int64             `json:"exp,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// JWTSigner signs platform JWTs using HS256 for phase 1.
type JWTSigner struct {
	Secret   []byte
	Issuer   string
	Audience string
	TTL      time.Duration
	Clock    func() time.Time
	KeyID    string
}

// NewJWTSigner returns a signer with default clock semantics.
func NewJWTSigner(secret []byte, issuer, audience string, ttl time.Duration) *JWTSigner {
	return &JWTSigner{
		Secret:   append([]byte(nil), secret...),
		Issuer:   issuer,
		Audience: audience,
		TTL:      ttl,
		Clock:    time.Now,
		KeyID:    "rms-phase1",
	}
}

// Sign returns a compact JWT string.
func (s *JWTSigner) Sign(claims JWTClaims) (string, error) {
	if len(s.Secret) == 0 {
		return "", errors.New("missing signing secret")
	}
	now := s.now()
	if claims.Issuer == "" {
		claims.Issuer = s.Issuer
	}
	if claims.Audience == "" {
		claims.Audience = s.Audience
	}
	if claims.IssuedAt == 0 {
		claims.IssuedAt = now.Unix()
	}
	if claims.NotBefore == 0 {
		claims.NotBefore = now.Add(-time.Minute).Unix()
	}
	if claims.ExpiresAt == 0 {
		ttl := s.TTL
		if ttl <= 0 {
			ttl = 15 * time.Minute
		}
		claims.ExpiresAt = now.Add(ttl).Unix()
	}
	if claims.ID == "" {
		claims.ID = randomID()
	}
	header := jwtHeader{Algorithm: "HS256", Type: "JWT", KeyID: s.KeyID}
	headerPart, payloadPart, err := encodeJWT(header, claims)
	if err != nil {
		return "", err
	}
	signature := signHMAC(headerPart + "." + payloadPart, s.Secret)
	return headerPart + "." + payloadPart + "." + signature, nil
}

// JWTVerifier validates HS256 JWTs produced by JWTSigner.
type JWTVerifier struct {
	Secret      []byte
	Issuer      string
	Audience    string
	Clock       func() time.Time
	AllowedSkew time.Duration
}

// NewJWTVerifier creates a verifier with default clock semantics.
func NewJWTVerifier(secret []byte, issuer, audience string) *JWTVerifier {
	return &JWTVerifier{
		Secret:      append([]byte(nil), secret...),
		Issuer:      issuer,
		Audience:    audience,
		Clock:       time.Now,
		AllowedSkew: time.Minute,
	}
}

// Verify parses and validates a compact JWT string.
func (v *JWTVerifier) Verify(token string) (*JWTClaims, error) {
	if len(v.Secret) == 0 {
		return nil, errors.New("missing verification secret")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid token format")
	}
	unsigned := parts[0] + "." + parts[1]
	if !hmac.Equal([]byte(signHMAC(unsigned, v.Secret)), []byte(parts[2])) {
		return nil, errors.New("invalid token signature")
	}

	var header jwtHeader
	if err := decodePart(parts[0], &header); err != nil {
		return nil, err
	}
	if header.Algorithm != "HS256" {
		return nil, fmt.Errorf("unsupported algorithm %s", header.Algorithm)
	}

	var claims JWTClaims
	if err := decodePart(parts[1], &claims); err != nil {
		return nil, err
	}
	now := v.now().Unix()
	if claims.ExpiresAt != 0 && now > claims.ExpiresAt+int64(v.AllowedSkew.Seconds()) {
		return nil, errors.New("token expired")
	}
	if claims.NotBefore != 0 && now+int64(v.AllowedSkew.Seconds()) < claims.NotBefore {
		return nil, errors.New("token not yet valid")
	}
	if v.Issuer != "" && claims.Issuer != "" && claims.Issuer != v.Issuer {
		return nil, errors.New("issuer mismatch")
	}
	if v.Audience != "" && claims.Audience != "" && claims.Audience != v.Audience {
		return nil, errors.New("audience mismatch")
	}
	return &claims, nil
}

// HasRole reports whether the claims contain a required role.
func HasRole(claims *JWTClaims, allowed ...string) bool {
	if claims == nil || len(allowed) == 0 {
		return false
	}
	roleSet := map[string]struct{}{}
	for _, role := range claims.Roles {
		roleSet[strings.ToLower(strings.TrimSpace(role))] = struct{}{}
	}
	for _, required := range allowed {
		if _, ok := roleSet[strings.ToLower(strings.TrimSpace(required))]; ok {
			return true
		}
	}
	return false
}

// ParseBearer extracts the token from an Authorization header.
func ParseBearer(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
		return strings.TrimSpace(raw[7:])
	}
	return raw
}

func (s *JWTSigner) now() time.Time {
	if s != nil && s.Clock != nil {
		return s.Clock().UTC()
	}
	return time.Now().UTC()
}

func (v *JWTVerifier) now() time.Time {
	if v != nil && v.Clock != nil {
		return v.Clock().UTC()
	}
	return time.Now().UTC()
}

func encodeJWT(header jwtHeader, claims JWTClaims) (string, string, error) {
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", "", err
	}
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		return "", "", err
	}
	return base64.RawURLEncoding.EncodeToString(headerJSON), base64.RawURLEncoding.EncodeToString(payloadJSON), nil
}

func decodePart(raw string, dst any) error {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

func signHMAC(data string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(data))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func randomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
