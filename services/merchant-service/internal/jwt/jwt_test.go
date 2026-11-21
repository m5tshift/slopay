package jwt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var ctx = context.Background()

// The keys are generated per test run: no key material is committed.
func newKey(t *testing.T, curve elliptic.Curve) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func pkcs8PEM(t *testing.T, k any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func sec1PEM(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func writeKey(t *testing.T, path string, pemBytes []byte) {
	t.Helper()
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
}

// newSigner writes a fresh P-256 key and returns a signer over it.
func newSigner(t *testing.T) (*FileSigner, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private.pem")
	writeKey(t, path, pkcs8PEM(t, newKey(t, elliptic.P256())))
	s, err := NewFileSigner(path, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestParsePrivateKey(t *testing.T) {
	p256 := newKey(t, elliptic.P256())
	if _, err := ParsePrivateKey(pkcs8PEM(t, p256)); err != nil {
		t.Errorf("PKCS#8: %v", err)
	}
	if _, err := ParsePrivateKey(sec1PEM(t, p256)); err != nil {
		t.Errorf("SEC 1: %v", err)
	}
	for name, in := range map[string][]byte{
		"not PEM":      []byte("garbage"),
		"wrong type":   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1}}),
		"broken DER":   pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte{1, 2, 3}}),
		"P-384":        sec1PEM(t, newKey(t, elliptic.P384())),
		"P-384 PKCS#8": pkcs8PEM(t, newKey(t, elliptic.P384())),
	} {
		if _, err := ParsePrivateKey(in); !errors.Is(err, ErrKey) {
			t.Errorf("%s: err = %v, want ErrKey", name, err)
		}
	}
}

func TestNewFileSignerErrors(t *testing.T) {
	if _, err := NewFileSigner(filepath.Join(t.TempDir(), "missing.pem"), time.Minute); !errors.Is(err, ErrKey) {
		t.Errorf("missing file: %v", err)
	}
	path := filepath.Join(t.TempDir(), "bad.pem")
	writeKey(t, path, []byte("not a key"))
	if _, err := NewFileSigner(path, time.Minute); !errors.Is(err, ErrKey) {
		t.Errorf("bad file: %v", err)
	}
}

func TestIssueVerify(t *testing.T) {
	s, _ := newSigner(t)
	iss := NewIssuer(s, "weigate", 15*time.Minute)
	tok, err := iss.Issue(ctx, "merchant-1")
	if err != nil {
		t.Fatal(err)
	}
	c, err := iss.Verify(ctx, tok)
	if err != nil {
		t.Fatal(err)
	}
	if c.Subject != "merchant-1" || c.Issuer != "weigate" || c.ExpiresAt-c.IssuedAt != 900 || c.ID == "" {
		t.Errorf("claims = %+v", c)
	}
	var hdr header
	if err := decodePart(strings.Split(tok, ".")[0], &hdr); err != nil {
		t.Fatal(err)
	}
	keys, _ := s.PublicKeys(ctx)
	if hdr.Alg != "ES256" || hdr.Typ != "JWT" || hdr.Kid != keys[0].KID {
		t.Errorf("header = %+v", hdr)
	}
}

func encode(v any) string {
	b, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestVerifyRejects(t *testing.T) {
	s, _ := newSigner(t)
	iss := NewIssuer(s, "weigate", 15*time.Minute)
	good, err := iss.Issue(ctx, "m")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(good, ".")
	keys, _ := s.PublicKeys(ctx)
	kid := keys[0].KID

	// sign builds a correctly signed token from arbitrary header and claims.
	sign := func(h header, c Claims) string {
		input := encode(h) + "." + encode(c)
		_, sig, err := s.Sign(ctx, []byte(input))
		if err != nil {
			t.Fatal(err)
		}
		return input + "." + base64.RawURLEncoding.EncodeToString(sig)
	}
	now := time.Now().Unix()
	valid := Claims{Issuer: "weigate", Subject: "m", IssuedAt: now, ExpiresAt: now + 60}

	other, _ := newSigner(t)
	foreign, _ := NewIssuer(other, "weigate", time.Minute).Issue(ctx, "m")

	for name, tok := range map[string]string{
		"empty":           "",
		"two parts":       parts[0] + "." + parts[1],
		"bad header":      "!!." + parts[1] + "." + parts[2],
		"alg none":        encode(header{Alg: "none", Kid: kid}) + "." + parts[1] + ".",
		"alg HS256":       encode(header{Alg: "HS256", Kid: kid}) + "." + parts[1] + "." + parts[2],
		"unknown kid":     sign(header{Alg: Alg, Kid: "other"}, valid),
		"short signature": parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString([]byte("short")),
		"bad signature":   parts[0] + "." + encode(Claims{Issuer: "weigate", Subject: "x", ExpiresAt: now + 60}) + "." + parts[2],
		"foreign key":     foreign,
		"bad claims":      signRaw(t, s, kid, "notjson"),
		"wrong issuer":    sign(header{Alg: Alg, Kid: kid}, Claims{Issuer: "evil", Subject: "m", ExpiresAt: now + 60}),
		"no subject":      sign(header{Alg: Alg, Kid: kid}, Claims{Issuer: "weigate", ExpiresAt: now + 60}),
		"expired":         sign(header{Alg: Alg, Kid: kid}, Claims{Issuer: "weigate", Subject: "m", ExpiresAt: now - 1}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := iss.Verify(ctx, tok)
			if !errors.Is(err, ErrInvalidToken) {
				t.Errorf("err = %v, want ErrInvalidToken", err)
			}
			if tok != "" && err != nil && strings.Contains(err.Error(), tok) {
				t.Error("error carries the token")
			}
		})
	}
}

// signRaw signs a payload that is not valid claims JSON.
func signRaw(t *testing.T, s *FileSigner, kid, payload string) string {
	t.Helper()
	input := encode(header{Alg: Alg, Kid: kid}) + "." + base64.RawURLEncoding.EncodeToString([]byte(payload))
	_, sig, err := s.Sign(ctx, []byte(input))
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestExpiryUsesClock(t *testing.T) {
	s, _ := newSigner(t)
	iss := NewIssuer(s, "weigate", 15*time.Minute)
	start := time.Now()
	iss.now = func() time.Time { return start }
	tok, err := iss.Issue(ctx, "m")
	if err != nil {
		t.Fatal(err)
	}
	iss.now = func() time.Time { return start.Add(15*time.Minute - time.Second) }
	if _, err := iss.Verify(ctx, tok); err != nil {
		t.Errorf("before expiry: %v", err)
	}
	iss.now = func() time.Time { return start.Add(15 * time.Minute) }
	if _, err := iss.Verify(ctx, tok); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("at expiry: %v", err)
	}
}

func TestRotationKeepsPreviousKey(t *testing.T) {
	s, path := newSigner(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	iss := NewIssuer(s, "weigate", 15*time.Minute)
	old, err := iss.Issue(ctx, "m")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := s.PublicKeys(ctx)

	writeKey(t, path, sec1PEM(t, newKey(t, elliptic.P256())))
	keys, err := s.PublicKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].KID == before[0].KID || keys[1].KID != before[0].KID {
		t.Fatalf("after rotation: %v, before %v", kids(keys), kids(before))
	}
	fresh, err := iss.Issue(ctx, "m")
	if err != nil {
		t.Fatal(err)
	}
	for name, tok := range map[string]string{"old": old, "fresh": fresh} {
		if _, err := iss.Verify(ctx, tok); err != nil {
			t.Errorf("%s token: %v", name, err)
		}
	}

	// After the retention the old key leaves the JWKS.
	now = now.Add(15 * time.Minute)
	keys, _ = s.PublicKeys(ctx)
	if len(keys) != 1 || keys[0].KID == before[0].KID {
		t.Errorf("after retention: %v", kids(keys))
	}

	// A broken or missing file does not stop signing with the loaded key.
	writeKey(t, path, []byte("broken"))
	if _, err := iss.Issue(ctx, "m"); err != nil {
		t.Errorf("broken file: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := iss.Issue(ctx, "m"); err != nil {
		t.Errorf("missing file: %v", err)
	}
}

func kids(keys []PublicKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.KID
	}
	return out
}

func TestJWK(t *testing.T) {
	s, _ := newSigner(t)
	keys, _ := s.PublicKeys(ctx)
	j, err := ToJWK(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if j.Kty != "EC" || j.Crv != "P-256" || j.Alg != "ES256" || j.Use != "sig" || j.Kid != keys[0].KID {
		t.Errorf("jwk = %+v", j)
	}
	x, _ := base64.RawURLEncoding.DecodeString(j.X)
	y, _ := base64.RawURLEncoding.DecodeString(j.Y)
	if len(x) != 32 || len(y) != 32 {
		t.Fatalf("coordinates: %d, %d bytes", len(x), len(y))
	}
	// The JWK reconstructs the same key.
	pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, x...), y...))
	if err != nil || !pub.Equal(keys[0].Key) {
		t.Errorf("reconstructed key differs: %v", err)
	}
	// kid is the RFC 7638 thumbprint: 32 bytes of SHA-256, stable for the key.
	if raw, _ := base64.RawURLEncoding.DecodeString(j.Kid); len(raw) != 32 || Thumbprint(keys[0].Key) != j.Kid {
		t.Errorf("kid = %q", j.Kid)
	}
}

// failingSigner exercises the error paths of Issue.
type failingSigner struct {
	keys    []PublicKey
	keysErr error
	signKid string
	signErr error
}

func (f failingSigner) Sign(context.Context, []byte) (string, []byte, error) {
	return f.signKid, make([]byte, 64), f.signErr
}
func (f failingSigner) PublicKeys(context.Context) ([]PublicKey, error) { return f.keys, f.keysErr }

func TestIssueSignerErrors(t *testing.T) {
	boom := errors.New("boom")
	k := PublicKey{KID: "a", Key: &newKey(t, elliptic.P256()).PublicKey}
	for name, s := range map[string]failingSigner{
		"keys error":  {keysErr: boom},
		"no keys":     {},
		"sign error":  {keys: []PublicKey{k}, signErr: boom},
		"kid changed": {keys: []PublicKey{k}, signKid: "b"},
	} {
		if _, err := NewIssuer(s, "i", time.Minute).Issue(ctx, "m"); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := NewIssuer(failingSigner{keysErr: boom}, "i", time.Minute).Verify(ctx, encode(header{Alg: Alg})+".e30.sig"); !errors.Is(err, boom) {
		t.Errorf("verify keys error: %v", err)
	}
}
