package jwt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"
)

// fakeTransit mimics a Transit key: versions are ECDSA P-256 keys, signing uses the latest.
type fakeTransit struct {
	keys    map[int]*ecdsa.PrivateKey
	latest  int
	signVer int // when set, Sign reports this version instead of latest
	err     error
	reads   int
	signs   int
}

func newFakeTransit(t *testing.T) *fakeTransit {
	f := &fakeTransit{keys: map[int]*ecdsa.PrivateKey{}}
	f.rotate(t)
	return f
}

func (f *fakeTransit) rotate(t *testing.T) {
	t.Helper()
	f.latest++
	f.keys[f.latest] = newKey(t, elliptic.P256())
}

func (f *fakeTransit) Sign(_ context.Context, input []byte) (int, []byte, error) {
	f.signs++
	if f.err != nil {
		return 0, nil, f.err
	}
	v := f.latest
	if f.signVer != 0 {
		v = f.signVer
	}
	d := sha256.Sum256(input)
	r, s, err := ecdsa.Sign(rand.Reader, f.keys[f.latest], d[:])
	if err != nil {
		return 0, nil, err
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return v, sig, nil
}

func (f *fakeTransit) Keys(context.Context) (int, map[int]string, error) {
	f.reads++
	if f.err != nil {
		return 0, nil, f.err
	}
	out := map[int]string{}
	for v, k := range f.keys {
		der, _ := x509.MarshalPKIXPublicKey(&k.PublicKey)
		out[v] = string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	}
	return f.latest, out, nil
}

func newTransitSigner(t *testing.T, f *fakeTransit) (*TransitSigner, *time.Time) {
	t.Helper()
	s, err := NewTransitSigner(ctx, f, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.now = func() time.Time { return now }
	s.fetched = now
	return s, &now
}

func TestTransitSignerIssueVerify(t *testing.T) {
	f := newFakeTransit(t)
	s, _ := newTransitSigner(t, f)
	iss := NewIssuer(s, "weigate-merchant-service", 15*time.Minute)
	tok, err := iss.Issue(ctx, "7d2f3a4b-5c6e-4d7f-9a81-a2b3c4d5e6f7")
	if err != nil {
		t.Fatal(err)
	}
	c, err := iss.Verify(ctx, tok)
	if err != nil || c.Subject != "7d2f3a4b-5c6e-4d7f-9a81-a2b3c4d5e6f7" {
		t.Fatalf("verify: %+v %v", c, err)
	}
	keys, err := s.PublicKeys(ctx)
	if err != nil || len(keys) != 1 || keys[0].KID != Thumbprint(&f.keys[1].PublicKey) {
		t.Errorf("keys: %+v %v", keys, err)
	}
	if _, err := ToJWK(keys[0]); err != nil {
		t.Error(err)
	}
}

func TestTransitSignerRotation(t *testing.T) {
	f := newFakeTransit(t)
	s, now := newTransitSigner(t, f)
	iss := NewIssuer(s, "i", 15*time.Minute)
	old, err := iss.Issue(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	f.rotate(t)
	// Within the refresh interval the new version is discovered when a signature comes from it.
	fresh, err := iss.Issue(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := s.PublicKeys(ctx)
	if len(keys) != 2 || keys[0].KID != Thumbprint(&f.keys[2].PublicKey) || keys[1].KID != Thumbprint(&f.keys[1].PublicKey) {
		t.Fatalf("keys after rotation: %+v", keys)
	}
	for _, tok := range []string{old, fresh} {
		if _, err := iss.Verify(ctx, tok); err != nil {
			t.Errorf("verify: %v", err)
		}
	}
	// A new version is also picked up by the periodic refresh, without any signing.
	f.rotate(t)
	reads := f.reads
	if _, err := s.PublicKeys(ctx); err != nil || f.reads != reads {
		t.Errorf("read before refresh interval: %d %d %v", reads, f.reads, err)
	}
	*now = now.Add(time.Minute)
	keys, err = s.PublicKeys(ctx)
	if err != nil || len(keys) != 3 || f.reads != reads+1 {
		t.Errorf("after refresh interval: %d keys, %d reads, %v", len(keys), f.reads, err)
	}
}

func TestTransitSignerUnknownVersion(t *testing.T) {
	f := newFakeTransit(t)
	s, _ := newTransitSigner(t, f)
	f.signVer = 9
	if _, _, err := s.Sign(ctx, []byte("x")); !errors.Is(err, ErrKey) {
		t.Errorf("unknown version: %v", err)
	}
}

func TestTransitSignerFailures(t *testing.T) {
	f := newFakeTransit(t)
	s, now := newTransitSigner(t, f)
	boom := errors.New("sealed")
	f.err = boom
	if _, _, err := s.Sign(ctx, []byte("x")); !errors.Is(err, boom) {
		t.Errorf("sign: %v", err)
	}
	// A failed refresh keeps the cached public keys.
	*now = now.Add(time.Hour)
	if keys, err := s.PublicKeys(ctx); err != nil || len(keys) != 1 {
		t.Errorf("stale keys: %v %v", keys, err)
	}
	// A rotation seen during a failing key read cannot be resolved.
	f.err = nil
	f.rotate(t)
	f.signVer = 0
	f2 := &failKeys{fakeTransit: f}
	s.t = f2
	if _, _, err := s.Sign(ctx, []byte("x")); err == nil {
		t.Error("sign with unreadable keys of a new version: no error")
	}
	// No keys at construction.
	f3 := newFakeTransit(t)
	f3.err = boom
	if _, err := NewTransitSigner(ctx, f3, time.Minute); !errors.Is(err, boom) {
		t.Errorf("construct: %v", err)
	}
}

// failKeys signs but cannot list keys.
type failKeys struct{ *fakeTransit }

func (f *failKeys) Keys(context.Context) (int, map[int]string, error) {
	return 0, nil, errors.New("keys unavailable")
}

func TestTransitSignerBadPublicKeys(t *testing.T) {
	ec := newKey(t, elliptic.P256())
	der, _ := x509.MarshalPKIXPublicKey(&ec.PublicKey)
	good := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	p384 := newKey(t, elliptic.P384())
	der384, _ := x509.MarshalPKIXPublicKey(&p384.PublicKey)
	for name, pemStr := range map[string]string{
		"not pem":  "nope",
		"not pkix": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("x")})),
		"p384":     string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der384})),
	} {
		_, err := NewTransitSigner(ctx, staticKeys{1, map[int]string{1: pemStr}}, time.Minute)
		if !errors.Is(err, ErrKey) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := NewTransitSigner(ctx, staticKeys{1, map[int]string{1: good}}, time.Minute); err != nil {
		t.Errorf("good: %v", err)
	}
	s, _ := NewTransitSigner(ctx, staticKeys{1, map[int]string{}}, time.Minute)
	if s != nil {
		if _, err := s.PublicKeys(ctx); !errors.Is(err, ErrKey) {
			t.Errorf("empty: %v", err)
		}
	}
}

type staticKeys struct {
	latest int
	pems   map[int]string
}

func (staticKeys) Sign(context.Context, []byte) (int, []byte, error) { return 0, nil, errors.New("no") }
func (k staticKeys) Keys(context.Context) (int, map[int]string, error) {
	return k.latest, k.pems, nil
}
