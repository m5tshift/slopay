package jwks

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func entry(t *testing.T, kid string, k *ecdsa.PrivateKey) map[string]string {
	t.Helper()
	b, err := k.PublicKey.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	e := base64.RawURLEncoding
	return map[string]string{"kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig", "kid": kid,
		"x": e.EncodeToString(b[1:33]), "y": e.EncodeToString(b[33:])}
}

// server serves the key set in body (replaceable) with status; hits counts requests.
type server struct {
	*httptest.Server
	body   atomic.Value // []byte
	status atomic.Int32
	hits   atomic.Int32
}

func newServer(t *testing.T, keys ...map[string]string) *server {
	t.Helper()
	s := &server{}
	s.status.Store(http.StatusOK)
	s.set(t, keys...)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.hits.Add(1)
		w.WriteHeader(int(s.status.Load()))
		_, _ = w.Write(s.body.Load().([]byte))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) set(t *testing.T, keys ...map[string]string) {
	t.Helper()
	b, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	s.body.Store(b)
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newKeys(url string, c *clock) *Keys {
	return New(url, Config{RefreshInterval: 5 * time.Minute, MinInterval: 10 * time.Second, Now: c.now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
}

func TestCachesAndFindsKeys(t *testing.T) {
	k1, k2 := newKey(t), newKey(t)
	srv := newServer(t, entry(t, "a", k1), entry(t, "b", k2))
	c := &clock{time.Unix(1000, 0)}
	keys := newKeys(srv.URL, c)
	for _, kid := range []string{"a", "b", "a"} {
		if _, err := keys.Key(context.Background(), kid); err != nil {
			t.Fatalf("%s: %v", kid, err)
		}
	}
	got, _ := keys.Key(context.Background(), "a")
	if !got.Equal(&k1.PublicKey) {
		t.Error("wrong key")
	}
	if srv.hits.Load() != 1 {
		t.Errorf("hits %d, want 1: the set is cached", srv.hits.Load())
	}
	// After the refresh interval the set is loaded again.
	c.t = c.t.Add(5 * time.Minute)
	if _, err := keys.Key(context.Background(), "a"); err != nil || srv.hits.Load() != 2 {
		t.Errorf("hits %d err %v", srv.hits.Load(), err)
	}
}

func TestUnknownKidRefreshesOnceWithRateLimit(t *testing.T) {
	k1, k2 := newKey(t), newKey(t)
	srv := newServer(t, entry(t, "a", k1))
	c := &clock{time.Unix(1000, 0)}
	keys := newKeys(srv.URL, c)
	ctx := context.Background()
	if _, err := keys.Key(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	// A key appears after rotation: the unknown kid triggers one reload.
	srv.set(t, entry(t, "a", k1), entry(t, "b", k2))
	c.t = c.t.Add(11 * time.Second)
	if _, err := keys.Key(ctx, "b"); err != nil || srv.hits.Load() != 2 {
		t.Fatalf("rotated key: hits %d err %v", srv.hits.Load(), err)
	}
	// Unknown kids in a burst: one reload at most per MinInterval.
	c.t = c.t.Add(11 * time.Second)
	for range 20 {
		if _, err := keys.Key(ctx, "zzz"); !errors.Is(err, ErrUnknownKid) {
			t.Fatalf("got %v", err)
		}
	}
	if srv.hits.Load() != 3 {
		t.Errorf("hits %d, want 3", srv.hits.Load())
	}
	// Known keys are still served while unknown ones are throttled.
	if _, err := keys.Key(ctx, "a"); err != nil {
		t.Error(err)
	}
}

func TestUnavailable(t *testing.T) {
	srv := newServer(t, entry(t, "a", newKey(t)))
	srv.status.Store(http.StatusInternalServerError)
	c := &clock{time.Unix(1000, 0)}
	keys := newKeys(srv.URL, c)
	ctx := context.Background()
	if _, err := keys.Key(ctx, "a"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("down at start: %v", err)
	}
	// Within MinInterval no new request is made.
	if _, err := keys.Key(ctx, "a"); !errors.Is(err, ErrUnavailable) || srv.hits.Load() != 1 {
		t.Fatalf("throttled: %v hits %d", err, srv.hits.Load())
	}
	// The endpoint comes back: the next attempt after MinInterval loads the keys.
	srv.status.Store(http.StatusOK)
	c.t = c.t.Add(11 * time.Second)
	if _, err := keys.Key(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	// It goes down again: the stale key still verifies, an unknown kid is unavailable.
	srv.status.Store(http.StatusServiceUnavailable)
	c.t = c.t.Add(time.Hour)
	if _, err := keys.Key(ctx, "a"); err != nil {
		t.Errorf("stale key: %v", err)
	}
	c.t = c.t.Add(time.Minute)
	if _, err := keys.Key(ctx, "b"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("unknown kid while down: %v", err)
	}
}

func TestUnreachableAndGarbage(t *testing.T) {
	srv := newServer(t, entry(t, "a", newKey(t)))
	url := srv.URL
	srv.Close()
	c := &clock{time.Unix(1000, 0)}
	if _, err := newKeys(url, c).Key(context.Background(), "a"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("connection refused: %v", err)
	}
	g := newServer(t)
	g.body.Store([]byte("<html>"))
	if _, err := newKeys(g.URL, c).Key(context.Background(), "a"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("garbage: %v", err)
	}
}

func TestParseSkipsUnusableKeys(t *testing.T) {
	good := newKey(t)
	bad := entry(t, "bad-curve", good)
	bad["y"] = bad["x"] // not on the curve
	rsa := entry(t, "rsa", good)
	rsa["kty"] = "RSA"
	other := entry(t, "es384", good)
	other["alg"] = "ES384"
	enc := entry(t, "enc", good)
	enc["use"] = "enc"
	short := entry(t, "short", good)
	short["x"] = "AAAA"
	nokid := entry(t, "", good)
	data, _ := json.Marshal(map[string]any{"keys": []map[string]string{bad, rsa, other, enc, short, nokid, entry(t, "good", good)}})
	keys, err := Parse(data)
	if err != nil || len(keys) != 1 || keys["good"] == nil {
		t.Fatalf("%v %v", keys, err)
	}
	if _, err := Parse([]byte(`{"keys":[]}`)); err == nil {
		t.Error("empty set accepted")
	}
}
