// Package jwt issues and verifies the merchant JWTs (SRS 4.1.10): ES256, ECDSA P-256. The private key is
// behind the Signer interface; FileSigner reads it from a mounted secret file. The JWKS is built from the
// public keys of the signer.
package jwt

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// PublicKey is a verification key and its key id (the kid header and the JWKS entry).
type PublicKey struct {
	KID string
	Key *ecdsa.PublicKey
}

// Signer signs JWTs with an ECDSA P-256 key it never exposes.
type Signer interface {
	// Sign signs the JWS signing input with ES256. It returns the key id and the signature in the JWS form
	// (R || S, 32 bytes each).
	Sign(ctx context.Context, signingInput []byte) (kid string, sig []byte, err error)
	// PublicKeys returns the keys a valid token may be signed with, the current one first.
	PublicKeys(ctx context.Context) ([]PublicKey, error)
}

// ErrKey is returned when the signing key cannot be read or is not an ECDSA P-256 private key.
// The error never carries the key material.
var ErrKey = errors.New("jwt: signing key")

type fileKey struct {
	priv *ecdsa.PrivateKey
	pub  PublicKey
}

// FileSigner signs with the private key in a mounted PEM file (PKCS#8 or SEC 1). The file is re-read on
// every call, so a rotated secret takes effect without a restart. The previous key stays in PublicKeys for
// Retain after the rotation, so tokens it signed keep verifying until they expire.
type FileSigner struct {
	path   string
	retain time.Duration
	now    func() time.Time

	mu        sync.Mutex
	raw       []byte
	cur       *fileKey
	prev      *fileKey
	prevUntil time.Time
}

// NewFileSigner loads the key once to fail fast on a missing or wrong file. retain is normally the token TTL.
func NewFileSigner(path string, retain time.Duration) (*FileSigner, error) {
	s := &FileSigner{path: path, retain: retain, now: time.Now}
	if _, err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// load returns the current key, re-parsing the file when its content changed. When the file cannot be
// read but a key was loaded before, the cached key is used: kubelet replaces secret files atomically,
// so a failed read is transient.
func (s *FileSigner) load() (*fileKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if s.cur != nil {
			return s.cur, nil
		}
		return nil, fmt.Errorf("%w: read %s: %w", ErrKey, s.path, err)
	}
	if s.cur != nil && bytes.Equal(raw, s.raw) {
		return s.cur, nil
	}
	priv, err := ParsePrivateKey(raw)
	if err != nil {
		if s.cur != nil {
			return s.cur, nil
		}
		return nil, err
	}
	k := &fileKey{priv: priv, pub: PublicKey{KID: Thumbprint(&priv.PublicKey), Key: &priv.PublicKey}}
	if s.cur != nil && s.cur.pub.KID != k.pub.KID {
		s.prev, s.prevUntil = s.cur, s.now().Add(s.retain)
	}
	s.cur, s.raw = k, raw
	return k, nil
}

// Sign implements Signer.
func (s *FileSigner) Sign(_ context.Context, signingInput []byte) (string, []byte, error) {
	k, err := s.load()
	if err != nil {
		return "", nil, err
	}
	digest := sha256.Sum256(signingInput)
	r, sv, err := ecdsa.Sign(rand.Reader, k.priv, digest[:])
	if err != nil {
		return "", nil, fmt.Errorf("jwt: sign: %w", err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	sv.FillBytes(sig[32:])
	return k.pub.KID, sig, nil
}

// PublicKeys implements Signer.
func (s *FileSigner) PublicKeys(_ context.Context) ([]PublicKey, error) {
	k, err := s.load()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := []PublicKey{k.pub}
	if s.prev != nil && s.now().Before(s.prevUntil) {
		keys = append(keys, s.prev.pub)
	}
	return keys, nil
}

// ParsePrivateKey parses an ECDSA P-256 private key from PEM ("PRIVATE KEY" or "EC PRIVATE KEY").
func ParsePrivateKey(pemBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("%w: no PEM block", ErrKey)
	}
	var key any
	var err error
	switch block.Type {
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	default:
		return nil, fmt.Errorf("%w: unsupported PEM block %q", ErrKey, block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: parse: %w", ErrKey, err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() {
		return nil, fmt.Errorf("%w: not an ECDSA P-256 key", ErrKey)
	}
	return ec, nil
}

// coordinates returns X and Y of a P-256 public key, 32 bytes each.
func coordinates(pub *ecdsa.PublicKey) (x, y []byte, err error) {
	b, err := pub.Bytes()
	if err != nil || len(b) != 65 {
		return nil, nil, fmt.Errorf("%w: encode public key", ErrKey)
	}
	return b[1:33], b[33:], nil
}

// Thumbprint is the RFC 7638 JWK thumbprint of the key, used as kid: it changes with the key and is
// the same on every replica.
func Thumbprint(pub *ecdsa.PublicKey) string {
	x, y, err := coordinates(pub)
	if err != nil {
		return ""
	}
	enc := base64.RawURLEncoding
	canonical := `{"crv":"P-256","kty":"EC","x":"` + enc.EncodeToString(x) + `","y":"` + enc.EncodeToString(y) + `"}`
	sum := sha256.Sum256([]byte(canonical))
	return enc.EncodeToString(sum[:])
}

// JWK is a public key in the JWKS (contracts: Jwk).
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// ToJWK converts a public key to its JWKS entry.
func ToJWK(k PublicKey) (JWK, error) {
	x, y, err := coordinates(k.Key)
	if err != nil {
		return JWK{}, err
	}
	enc := base64.RawURLEncoding
	return JWK{Kty: "EC", Crv: "P-256", Alg: Alg, Use: "sig", Kid: k.KID, X: enc.EncodeToString(x), Y: enc.EncodeToString(y)}, nil
}
