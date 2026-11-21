// Package jwks loads the public keys of merchant-service from its JWKS endpoint
// (contracts/openapi/merchant-service.yaml, GET /.well-known/jwks.json) and caches them.
//
// The cache is refreshed when it is older than the refresh interval and once more when a token names an
// unknown kid (a rotated key), but never more often than the minimum interval between attempts, so a flood
// of tokens with random kids cannot turn into a flood of requests to merchant-service. When the endpoint
// is down, the keys loaded earlier keep verifying tokens: the service does not depend on merchant-service
// for requests whose key it already knows.
package jwks

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Alg is the only signature algorithm merchant-service issues (SRS 4.1.10).
const Alg = "ES256"

// maxBody bounds the JWKS response; a key set of a few keys is under 1 KiB.
const maxBody = 64 << 10

var (
	// ErrUnknownKid means the JWKS does not contain the key id.
	ErrUnknownKid = errors.New("jwks: unknown kid")
	// ErrUnavailable means the key could not be found and the JWKS could not be loaded (or was loaded
	// too recently to be asked again).
	ErrUnavailable = errors.New("jwks: key set unavailable")
)

// Config of a key set. Zero values take the defaults.
type Config struct {
	// Client is the HTTP client (default: 2 s timeout).
	Client *http.Client
	// RefreshInterval is the age after which the cache is reloaded (default 5 min, the Cache-Control of
	// merchant-service).
	RefreshInterval time.Duration
	// MinInterval is the shortest time between two load attempts, successful or not (default 10 s).
	MinInterval time.Duration
	Logger      *slog.Logger
	// Now is the clock (default time.Now).
	Now func() time.Time
}

// Keys is a cached JWKS. It is safe for concurrent use.
type Keys struct {
	url     string
	client  *http.Client
	refresh time.Duration
	minGap  time.Duration
	log     *slog.Logger
	now     func() time.Time

	mu       sync.Mutex
	keys     map[string]*ecdsa.PublicKey
	loadedAt time.Time
	tried    time.Time
}

// New returns a key set that loads url on first use.
func New(url string, c Config) *Keys {
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 2 * time.Second}
	}
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = 5 * time.Minute
	}
	if c.MinInterval <= 0 {
		c.MinInterval = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Keys{url: url, client: c.Client, refresh: c.RefreshInterval, minGap: c.MinInterval, log: c.Logger, now: c.Now}
}

// Key returns the public key with the key id.
func (k *Keys) Key(ctx context.Context, kid string) (*ecdsa.PublicKey, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.now()
	key, known := k.keys[kid]
	stale := k.keys == nil || now.Sub(k.loadedAt) >= k.refresh
	if known && !stale {
		return key, nil
	}
	if !k.tried.IsZero() && now.Sub(k.tried) < k.minGap {
		return k.fallback(key, known)
	}
	k.tried = now
	keys, err := k.load(ctx)
	if err != nil {
		k.log.WarnContext(ctx, "jwks load failed", "error", err.Error())
		if known {
			return key, nil // a stale key still verifies the tokens it signed
		}
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	k.keys, k.loadedAt = keys, now
	if key, ok := keys[kid]; ok {
		return key, nil
	}
	return nil, ErrUnknownKid
}

// fallback answers without a request: the stale key if there is one, otherwise unknown kid (or unavailable
// when nothing was ever loaded).
func (k *Keys) fallback(key *ecdsa.PublicKey, known bool) (*ecdsa.PublicKey, error) {
	switch {
	case known:
		return key, nil
	case k.keys == nil:
		return nil, ErrUnavailable
	default:
		return nil, ErrUnknownKid
	}
}

type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (k *Keys) load(ctx context.Context) (map[string]*ecdsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, err
	}
	return Parse(body)
}

// Parse reads a JWKS. Keys that are not ES256 signing keys on P-256 are skipped; a set without any
// usable key is an error, so a broken response does not replace a good cache.
func Parse(data []byte) (map[string]*ecdsa.PublicKey, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("jwks: parse: %w", err)
	}
	out := make(map[string]*ecdsa.PublicKey, len(set.Keys))
	for _, j := range set.Keys {
		if j.Kid == "" || j.Kty != "EC" || j.Crv != "P-256" || (j.Alg != "" && j.Alg != Alg) || (j.Use != "" && j.Use != "sig") {
			continue
		}
		x, errX := base64.RawURLEncoding.DecodeString(j.X)
		y, errY := base64.RawURLEncoding.DecodeString(j.Y)
		if errX != nil || errY != nil || len(x) != 32 || len(y) != 32 {
			continue
		}
		pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, x...), y...))
		if err != nil {
			continue // not on the curve
		}
		out[j.Kid] = pub
	}
	if len(out) == 0 {
		return nil, errors.New("jwks: no usable keys")
	}
	return out, nil
}
