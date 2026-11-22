package jwt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Transit is the OpenBao Transit key operations TransitSigner needs (implemented by openbao.Client with
// the mount and key name bound by TransitBinding).
type Transit interface {
	// Sign returns the key version that signed and the JWS-form ES256 signature (R || S).
	Sign(ctx context.Context, input []byte) (version int, sig []byte, err error)
	// Keys returns the latest key version and the public key PEM of every available version.
	Keys(ctx context.Context) (latest int, pems map[int]string, err error)
}

// TransitSigner signs through OpenBao Transit: the private key never leaves OpenBao. The public key versions
// are cached for refresh and re-read when a signature comes from a version not seen yet (key rotation).
type TransitSigner struct {
	t       Transit
	refresh time.Duration
	now     func() time.Time

	mu      sync.Mutex
	keys    []PublicKey // current first
	byVer   map[int]string
	latest  int
	fetched time.Time
}

// NewTransitSigner loads the public keys once to fail fast on a missing key. refresh is how long the
// loaded public keys are used before they are re-read.
func NewTransitSigner(ctx context.Context, t Transit, refresh time.Duration) (*TransitSigner, error) {
	s := &TransitSigner{t: t, refresh: refresh, now: time.Now}
	if err := s.reload(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// reload re-reads the public keys; the caller holds no lock.
func (s *TransitSigner) reload(ctx context.Context) error {
	latest, pems, err := s.t.Keys(ctx)
	if err != nil {
		return err
	}
	versions := make([]int, 0, len(pems))
	for v := range pems {
		versions = append(versions, v)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(versions)))
	keys := make([]PublicKey, 0, len(versions))
	byVer := make(map[int]string, len(versions))
	for _, v := range versions {
		pub, err := parsePublicKey(pems[v])
		if err != nil {
			return err
		}
		pk := PublicKey{KID: Thumbprint(pub), Key: pub}
		byVer[v] = pk.KID
		if v == latest {
			keys = append([]PublicKey{pk}, keys...)
		} else {
			keys = append(keys, pk)
		}
	}
	s.mu.Lock()
	s.keys, s.byVer, s.latest, s.fetched = keys, byVer, latest, s.now()
	s.mu.Unlock()
	return nil
}

// fresh returns the cached state, re-reading it when it is older than refresh. A failed re-read keeps
// the cached keys: they stay valid, only a newer version would be missing.
func (s *TransitSigner) fresh(ctx context.Context) {
	s.mu.Lock()
	stale := s.now().Sub(s.fetched) >= s.refresh
	s.mu.Unlock()
	if stale {
		_ = s.reload(ctx)
	}
}

func (s *TransitSigner) kid(version int) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.byVer[version]
	return k, ok
}

// Sign implements Signer.
func (s *TransitSigner) Sign(ctx context.Context, signingInput []byte) (string, []byte, error) {
	s.fresh(ctx)
	version, sig, err := s.t.Sign(ctx, signingInput)
	if err != nil {
		return "", nil, err
	}
	kid, ok := s.kid(version)
	if !ok {
		// Rotated since the last read.
		if err := s.reload(ctx); err != nil {
			return "", nil, err
		}
		if kid, ok = s.kid(version); !ok {
			return "", nil, fmt.Errorf("%w: transit key version %d is not published", ErrKey, version)
		}
	}
	return kid, sig, nil
}

// PublicKeys implements Signer: every available Transit key version, the latest first.
func (s *TransitSigner) PublicKeys(ctx context.Context) ([]PublicKey, error) {
	s.fresh(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.keys) == 0 {
		return nil, fmt.Errorf("%w: no transit public keys", ErrKey)
	}
	return append([]PublicKey(nil), s.keys...), nil
}

func parsePublicKey(pemStr string) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("%w: no PEM block in transit public key", ErrKey)
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse transit public key", ErrKey)
	}
	ec, ok := k.(*ecdsa.PublicKey)
	if !ok || ec.Curve != elliptic.P256() {
		return nil, errors.Join(ErrKey, errors.New("transit key is not ECDSA P-256"))
	}
	return ec, nil
}
