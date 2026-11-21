//go:build integration

package api

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/services/invoice-service/internal/jwks"
)

// merchantService mimics merchant-service as the contracts and its code define it (internal/jwt there cannot be
// imported from here): the ES256 key is read from a PKCS#8 PEM file, kid is the RFC 7638 thumbprint, the JWKS
// entry has kty, crv, alg, use, kid, x, y, and the token header is {alg, typ, kid}.
type merchantService struct {
	t    *testing.T
	file string
	key  *ecdsa.PrivateKey
	kid  string
}

func newMerchantService(t *testing.T) *merchantService {
	t.Helper()
	m := &merchantService{t: t, file: filepath.Join(t.TempDir(), "jwt-signing.pem")}
	m.rotate()
	return m
}

// rotate writes a new key to the file, as a replaced secret does.
func (m *merchantService) rotate() {
	m.t.Helper()
	key := mustKey()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		m.t.Fatal(err)
	}
	if err := os.WriteFile(m.file, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		m.t.Fatal(err)
	}
	m.load()
}

func (m *merchantService) load() {
	m.t.Helper()
	raw, err := os.ReadFile(m.file)
	if err != nil {
		m.t.Fatal(err)
	}
	block, _ := pem.Decode(raw)
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		m.t.Fatal(err)
	}
	m.key = k.(*ecdsa.PrivateKey)
	b, _ := m.key.PublicKey.Bytes()
	enc := base64.RawURLEncoding
	sum := sha256.Sum256([]byte(`{"crv":"P-256","kty":"EC","x":"` + enc.EncodeToString(b[1:33]) + `","y":"` + enc.EncodeToString(b[33:]) + `"}`))
	m.kid = enc.EncodeToString(sum[:])
}

func (m *merchantService) jwks(w http.ResponseWriter, _ *http.Request) {
	b, _ := m.key.PublicKey.Bytes()
	enc := base64.RawURLEncoding
	w.Header().Set("Cache-Control", "public, max-age=300")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
		"kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig", "kid": m.kid,
		"x": enc.EncodeToString(b[1:33]), "y": enc.EncodeToString(b[33:]),
	}}})
}

func (m *merchantService) issue(merchant uuid.UUID) string {
	return signToken(m.t, m.key, m.kid, map[string]any{
		"iss": issuer, "sub": merchant.String(), "iat": t0.Unix(), "exp": t0.Add(15 * time.Minute).Unix(), "jti": uuid.NewString(),
	})
}

// A token issued by merchant-service with a key from a file is accepted by invoice-service that loads the
// JWKS over HTTP; after the key rotates, the unknown kid makes the service reload the JWKS.
func TestMerchantServiceTokenAccepted(t *testing.T) {
	m := newMerchantService(t)
	srv := httptest.NewServer(http.HandlerFunc(m.jwks))
	t.Cleanup(srv.Close)

	now := t0
	keys := jwks.New(srv.URL, jwks.Config{MinInterval: time.Second, Now: func() time.Time { return now }})
	e := newEnvKeys(t, keys)
	get := func(tok string) int {
		return e.do(t, http.MethodGet, "/api/v1/invoices", tok, nil, nil).status
	}
	if got := get(m.issue(e.merchant)); got != http.StatusOK {
		t.Fatalf("token of merchant-service: %d", got)
	}
	// Another instance of the key (not the one in the JWKS) is refused.
	if got := get(signToken(t, mustKey(), m.kid, map[string]any{
		"iss": issuer, "sub": e.merchant.String(), "exp": t0.Add(time.Minute).Unix(),
	})); got != http.StatusUnauthorized {
		t.Errorf("foreign key: %d", got)
	}
	now = now.Add(2 * time.Second)
	m.rotate()
	if got := get(m.issue(e.merchant)); got != http.StatusOK {
		t.Errorf("token after rotation: %d", got)
	}
}
