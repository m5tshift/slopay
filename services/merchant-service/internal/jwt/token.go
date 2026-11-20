package jwt

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Alg is the only accepted signature algorithm.
const Alg = "ES256"

// ErrInvalidToken is returned for any token that must not be accepted. The reason is wrapped for logs;
// the token itself is never part of the error.
var ErrInvalidToken = errors.New("jwt: invalid token")

// Claims are the claims of a merchant token. Subject is the merchant id.
type Claims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	ID        string `json:"jti"`
}

type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// Issuer issues and verifies tokens.
type Issuer struct {
	signer Signer
	issuer string
	ttl    time.Duration
	now    func() time.Time
}

// NewIssuer returns an Issuer; issuer is the iss claim, ttl the token lifetime (SRS 4.1.10: 15 min).
func NewIssuer(s Signer, issuer string, ttl time.Duration) *Issuer {
	return &Issuer{signer: s, issuer: issuer, ttl: ttl, now: time.Now}
}

// TTL is the token lifetime.
func (i *Issuer) TTL() time.Duration { return i.ttl }

var b64 = base64.RawURLEncoding

// Issue returns a signed token for the subject.
func (i *Issuer) Issue(ctx context.Context, subject string) (string, error) {
	now := i.now()
	claims := Claims{Issuer: i.issuer, Subject: subject, IssuedAt: now.Unix(), ExpiresAt: now.Add(i.ttl).Unix(), ID: uuid.NewString()}
	keys, err := i.signer.PublicKeys(ctx)
	if err != nil {
		return "", err
	}
	if len(keys) == 0 {
		return "", fmt.Errorf("%w: no keys", ErrKey)
	}
	hdr, err := json.Marshal(header{Alg: Alg, Typ: "JWT", Kid: keys[0].KID})
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := b64.EncodeToString(hdr) + "." + b64.EncodeToString(body)
	kid, sig, err := i.signer.Sign(ctx, []byte(input))
	if err != nil {
		return "", err
	}
	if kid != keys[0].KID {
		// The key rotated between reading the kid and signing: the header would name the wrong key.
		return "", fmt.Errorf("%w: key changed while signing, retry", ErrKey)
	}
	return input + "." + b64.EncodeToString(sig), nil
}

// Verify checks the signature, algorithm, issuer and expiry and returns the claims.
func (i *Issuer) Verify(ctx context.Context, token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("%w: malformed", ErrInvalidToken)
	}
	var hdr header
	if err := decodePart(parts[0], &hdr); err != nil {
		return Claims{}, fmt.Errorf("%w: header", ErrInvalidToken)
	}
	if hdr.Alg != Alg {
		return Claims{}, fmt.Errorf("%w: algorithm %q", ErrInvalidToken, hdr.Alg)
	}
	keys, err := i.signer.PublicKeys(ctx)
	if err != nil {
		return Claims{}, err
	}
	var pub *ecdsa.PublicKey
	for _, k := range keys {
		if k.KID == hdr.Kid {
			pub = k.Key
			break
		}
	}
	if pub == nil {
		return Claims{}, fmt.Errorf("%w: unknown kid", ErrInvalidToken)
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return Claims{}, fmt.Errorf("%w: signature encoding", ErrInvalidToken)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	r, s := new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])
	if !ecdsa.Verify(pub, digest[:], r, s) {
		return Claims{}, fmt.Errorf("%w: signature", ErrInvalidToken)
	}
	var c Claims
	if err := decodePart(parts[1], &c); err != nil {
		return Claims{}, fmt.Errorf("%w: claims", ErrInvalidToken)
	}
	switch {
	case c.Issuer != i.issuer:
		return Claims{}, fmt.Errorf("%w: issuer", ErrInvalidToken)
	case c.Subject == "":
		return Claims{}, fmt.Errorf("%w: no subject", ErrInvalidToken)
	case i.now().Unix() >= c.ExpiresAt:
		return Claims{}, fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	return c, nil
}

func decodePart(s string, v any) error {
	raw, err := b64.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
