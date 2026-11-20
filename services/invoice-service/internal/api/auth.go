package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/services/invoice-service/internal/jwks"
)

// Merchant identification (contracts/openapi/invoice-service.yaml, "Authentication"): the merchant id is the
// sub claim of a JWT signed by merchant-service with ES256. The service verifies the signature itself against
// the JWKS of merchant-service (SRS 4.1.10), with or without the Istio gateway in front: the gateway check
// is the first line, this one the second. The only accepted algorithm is the one merchant-service issues,
// so "alg: none" and a switch to another algorithm are rejected before any key is looked up.

var (
	errMalformedToken = errors.New("malformed token")
	errBadAlgorithm   = errors.New("unsupported algorithm")
	errNoKid          = errors.New("token has no kid")
	errBadSignature   = errors.New("bad signature")
	errExpiredToken   = errors.New("token expired")
	errWrongIssuer    = errors.New("wrong issuer")
	errBadSubject     = errors.New("subject is not a merchant id")
)

// KeyResolver returns the verification key by key id. *jwks.Keys implements it.
type KeyResolver interface {
	Key(ctx context.Context, kid string) (*ecdsa.PublicKey, error)
}

type tokenHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type tokenClaims struct {
	Subject string `json:"sub"`
	Issuer  string `json:"iss"`
	Expiry  int64  `json:"exp"`
}

// merchantFromToken returns the merchant id of a JWT whose ES256 signature verifies against keys. issuer,
// when not empty, must equal the iss claim; exp is required. The signature is checked before the claims
// are trusted.
func merchantFromToken(ctx context.Context, token, issuer string, now time.Time, keys KeyResolver) (uuid.UUID, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return uuid.Nil, errMalformedToken
	}
	var h tokenHeader
	if err := decodeSegment(parts[0], &h); err != nil {
		return uuid.Nil, errMalformedToken
	}
	if h.Alg != jwks.Alg {
		return uuid.Nil, errBadAlgorithm
	}
	if h.Kid == "" {
		return uuid.Nil, errNoKid
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return uuid.Nil, errBadSignature
	}
	pub, err := keys.Key(ctx, h.Kid)
	if err != nil {
		return uuid.Nil, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(pub, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return uuid.Nil, errBadSignature
	}
	var c tokenClaims
	if err := decodeSegment(parts[1], &c); err != nil {
		return uuid.Nil, errMalformedToken
	}
	if c.Expiry == 0 || !now.Before(time.Unix(c.Expiry, 0)) {
		return uuid.Nil, errExpiredToken
	}
	if issuer != "" && c.Issuer != issuer {
		return uuid.Nil, errWrongIssuer
	}
	id, err := uuid.Parse(c.Subject)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, errBadSubject
	}
	return id, nil
}

func decodeSegment(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
