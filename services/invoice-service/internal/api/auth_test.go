package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/services/invoice-service/internal/jwks"
)

const testKID = "kid-current"

var (
	testKey = mustKey()
	// testKeys is the JWKS of the tests: only testKey, under testKID.
	testKeys = staticKeys{testKID: &testKey.PublicKey}
)

func mustKey() *ecdsa.PrivateKey {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return k
}

type staticKeys map[string]*ecdsa.PublicKey

func (s staticKeys) Key(_ context.Context, kid string) (*ecdsa.PublicKey, error) {
	if k, ok := s[kid]; ok {
		return k, nil
	}
	return nil, jwks.ErrUnknownKid
}

type downKeys struct{}

func (downKeys) Key(context.Context, string) (*ecdsa.PublicKey, error) {
	return nil, jwks.ErrUnavailable
}

// signToken signs claims the way merchant-service does: ES256, R || S, header with kid.
func signToken(t *testing.T, key *ecdsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	return signWithHeader(t, key, map[string]string{"alg": "ES256", "typ": "JWT", "kid": kid}, claims)
}

func signWithHeader(t *testing.T, key *ecdsa.PrivateKey, hdr map[string]string, claims map[string]any) string {
	t.Helper()
	input := segment(t, hdr) + "." + segment(t, claims)
	digest := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestMerchantFromToken(t *testing.T) {
	ctx := context.Background()
	merchant := uuid.New()
	claims := func() map[string]any {
		return map[string]any{"sub": merchant.String(), "iss": issuer, "exp": t0.Add(time.Minute).Unix()}
	}
	parts := func(tok string) []string { return strings.Split(tok, ".") }
	valid := signToken(t, testKey, testKID, claims())
	otherClaims := claims()
	otherClaims["sub"] = uuid.NewString()
	forged := parts(valid)[0] + "." + segment(t, otherClaims) + "." + parts(valid)[2]
	expired := claims()
	expired["exp"] = t0.Unix()
	// HS256 with the public key bytes as the secret: the classic algorithm confusion.
	pubBytes, err := testKey.PublicKey.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	hsInput := segment(t, map[string]string{"alg": "HS256", "typ": "JWT", "kid": testKID}) + "." + segment(t, claims())
	mac := hmac.New(sha256.New, pubBytes)
	mac.Write([]byte(hsInput))
	hs256 := hsInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	noneInput := segment(t, map[string]string{"alg": "none", "kid": testKID}) + "." + segment(t, claims())

	cases := []struct {
		name  string
		token string
		keys  KeyResolver
		want  error // nil: accepted
	}{
		{"valid signature", valid, testKeys, nil},
		{"foreign key", signToken(t, mustKey(), testKID, claims()), testKeys, errBadSignature},
		{"forged payload", forged, testKeys, errBadSignature},
		{"unknown kid", signToken(t, testKey, "kid-other", claims()), testKeys, jwks.ErrUnknownKid},
		{"no kid", signWithHeader(t, testKey, map[string]string{"alg": "ES256"}, claims()), testKeys, errNoKid},
		{"alg none", noneInput + ".", testKeys, errBadAlgorithm},
		{"alg none with signature", noneInput + "." + parts(valid)[2], testKeys, errBadAlgorithm},
		{"alg None", segment(t, map[string]string{"alg": "None", "kid": testKID}) + "." + segment(t, claims()) + ".", testKeys, errBadAlgorithm},
		{"alg HS256", hs256, testKeys, errBadAlgorithm},
		{"alg ES384", signWithHeader(t, testKey, map[string]string{"alg": "ES384", "kid": testKID}, claims()), testKeys, errBadAlgorithm},
		{"alg RS256", signWithHeader(t, testKey, map[string]string{"alg": "RS256", "kid": testKID}, claims()), testKeys, errBadAlgorithm},
		{"alg lower case", signWithHeader(t, testKey, map[string]string{"alg": "es256", "kid": testKID}, claims()), testKeys, errBadAlgorithm},
		{"expired", signToken(t, testKey, testKID, expired), testKeys, errExpiredToken},
		{"jwks unavailable", valid, downKeys{}, jwks.ErrUnavailable},
		{"signature of wrong length", parts(valid)[0] + "." + parts(valid)[1] + ".AAAA", testKeys, errBadSignature},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := merchantFromToken(ctx, tc.token, issuer, t0, tc.keys)
			if tc.want == nil {
				if err != nil || id != merchant {
					t.Fatalf("%v %v", id, err)
				}
				return
			}
			if !errors.Is(err, tc.want) || id != uuid.Nil {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

// A request whose key set is down gets 401, as for every failed authentication, and the cause is logged.
func TestAuthenticationJWKSDown(t *testing.T) {
	e := newEnv(t)
	g := &gate{keys: downKeys{}, issuer: issuer, now: func() time.Time { return t0 }, log: slogTo(e.logs)}
	_, ok := g.authenticate(newAuthRequest(e.token))
	if ok {
		t.Fatal("accepted without keys")
	}
	if !strings.Contains(e.logs.String(), "key set unavailable") || !strings.Contains(e.logs.String(), `"WARN"`) {
		t.Errorf("logs: %s", e.logs)
	}
	if strings.Contains(e.logs.String(), e.token) {
		t.Error("token logged")
	}
}

func TestRegisterNeedsKeys(t *testing.T) {
	if err := Register(http.NewServeMux(), Options{}); err == nil {
		t.Error("Register without Keys")
	}
}

func slogTo(w interface{ Write([]byte) (int, error) }) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func newAuthRequest(token string) *http.Request {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/invoices", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}
