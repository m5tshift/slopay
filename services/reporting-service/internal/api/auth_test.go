package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/services/reporting-service/internal/jwks"
)

const (
	testKID = "kid-current"
	issuer  = "weigate-merchant-service"
)

var (
	t0      = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
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

func segment(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// signWithHeader signs claims the way merchant-service does: ES256, R || S.
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

func signToken(t *testing.T, key *ecdsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	return signWithHeader(t, key, map[string]string{"alg": "ES256", "typ": "JWT", "kid": kid}, claims)
}

func TestMerchantFromToken(t *testing.T) {
	ctx := context.Background()
	merchant := uuid.New()
	claims := func() map[string]any {
		return map[string]any{"sub": merchant.String(), "iss": issuer, "exp": t0.Add(time.Minute).Unix()}
	}
	valid := signToken(t, testKey, testKID, claims())
	parts := strings.Split(valid, ".")
	otherClaims := claims()
	otherClaims["sub"] = uuid.NewString()
	expired := claims()
	expired["exp"] = t0.Unix()
	noExp := claims()
	delete(noExp, "exp")
	foreignIssuer := claims()
	foreignIssuer["iss"] = "someone-else"
	notUUID := claims()
	notUUID["sub"] = "merchant-1"
	noneInput := segment(t, map[string]string{"alg": "none", "kid": testKID}) + "." + segment(t, claims())

	cases := []struct {
		name  string
		token string
		keys  KeyResolver
		want  error // nil: accepted
	}{
		{"valid signature", valid, testKeys, nil},
		{"not a JWT", "abc", testKeys, errMalformedToken},
		{"header is not base64", "!!." + parts[1] + "." + parts[2], testKeys, errMalformedToken},
		{"foreign key", signToken(t, mustKey(), testKID, claims()), testKeys, errBadSignature},
		{"forged payload", parts[0] + "." + segment(t, otherClaims) + "." + parts[2], testKeys, errBadSignature},
		{"unknown kid", signToken(t, testKey, "kid-other", claims()), testKeys, jwks.ErrUnknownKid},
		{"no kid", signWithHeader(t, testKey, map[string]string{"alg": "ES256"}, claims()), testKeys, errNoKid},
		{"alg none", noneInput + ".", testKeys, errBadAlgorithm},
		{"alg HS256", signWithHeader(t, testKey, map[string]string{"alg": "HS256", "kid": testKID}, claims()), testKeys, errBadAlgorithm},
		{"expired", signToken(t, testKey, testKID, expired), testKeys, errExpiredToken},
		{"no exp", signToken(t, testKey, testKID, noExp), testKeys, errExpiredToken},
		{"wrong issuer", signToken(t, testKey, testKID, foreignIssuer), testKeys, errWrongIssuer},
		{"subject is not a UUID", signToken(t, testKey, testKID, notUUID), testKeys, errBadSubject},
		{"jwks unavailable", valid, downKeys{}, jwks.ErrUnavailable},
		{"signature of wrong length", parts[0] + "." + parts[1] + ".AAAA", testKeys, errBadSignature},
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

func TestEmptyIssuerAcceptsAny(t *testing.T) {
	c := map[string]any{"sub": uuid.NewString(), "iss": "x", "exp": t0.Add(time.Minute).Unix()}
	if _, err := merchantFromToken(context.Background(), signToken(t, testKey, testKID, c), "", t0, testKeys); err != nil {
		t.Fatal(err)
	}
}
