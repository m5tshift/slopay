package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Merchant identification (contracts/openapi/invoice-service.yaml, "Authentication"): the Istio
// RequestAuthentication of the gateway verifies the ES256 signature of the JWT against the JWKS of
// merchant-service, and the merchant id is the sub claim. The service does not verify the signature again:
// it would need a third synchronous internal call (the JWKS of merchant-service), which SRS 4.1.2 does not
// allow. It still refuses a token that is malformed, unsigned, expired or of another issuer, so that a
// request bypassing the gateway with a stale or foreign token is not accepted.

var (
	errMalformedToken = errors.New("malformed token")
	errUnsignedToken  = errors.New("unsigned token")
	errExpiredToken   = errors.New("token expired")
	errWrongIssuer    = errors.New("wrong issuer")
	errBadSubject     = errors.New("subject is not a merchant id")
)

type tokenHeader struct {
	Alg string `json:"alg"`
}

type tokenClaims struct {
	Subject string `json:"sub"`
	Issuer  string `json:"iss"`
	Expiry  int64  `json:"exp"`
}

// merchantFromToken returns the merchant id of a JWT verified by the gateway. issuer, when not empty, must
// equal the iss claim; exp is required.
func merchantFromToken(token, issuer string, now time.Time) (uuid.UUID, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return uuid.Nil, errMalformedToken
	}
	var h tokenHeader
	if err := decodeSegment(parts[0], &h); err != nil {
		return uuid.Nil, errMalformedToken
	}
	if h.Alg == "" || strings.EqualFold(h.Alg, "none") || parts[2] == "" {
		return uuid.Nil, errUnsignedToken
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
