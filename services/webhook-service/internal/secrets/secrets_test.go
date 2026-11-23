package secrets

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/storage"
)

const merchant = "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b"

func tokenFile(t *testing.T, token string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func newClient(t *testing.T, h http.HandlerFunc, token string) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(Config{Addr: srv.URL, TokenFile: tokenFile(t, token),
		Backoff: storage.Backoff{Initial: time.Millisecond, Max: time.Millisecond, Total: 5 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSecretByVersionAndCache(t *testing.T) {
	var calls atomic.Int32
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/secret/data/webhook/"+merchant || r.URL.Query().Get("version") != "2" ||
			r.Header.Get("X-Vault-Token") != "tok" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"data":{"secret":"whsec_two"},"metadata":{"version":2}}}`))
	}, "tok")
	for range 2 {
		s, err := c.Secret(context.Background(), merchant, 2)
		if err != nil || string(s) != "whsec_two" {
			t.Fatalf("secret %q, %v", s, err)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("openbao called %d times, want 1 (cached)", calls.Load())
	}
}

func TestSecretNotFound(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"404": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
		"deleted": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"data":{"data":null,"metadata":{"version":1}}}`))
		},
		"other version": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"data":{"data":{"secret":"x"},"metadata":{"version":3}}}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, h, "tok")
			if _, err := c.Secret(context.Background(), merchant, 1); !errors.Is(err, ErrNotFound) {
				t.Errorf("err = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestSecretUnavailable(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"sealed":  func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
		"garbage": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{")) },
	} {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, h, "tok")
			if _, err := c.Secret(context.Background(), merchant, 1); !errors.Is(err, ErrUnavailable) {
				t.Errorf("err = %v, want ErrUnavailable", err)
			}
		})
	}
}

// A rejected token is retried with the token file read again.
func TestSecretRereadsTokenAfterDenied(t *testing.T) {
	var file string
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "new" {
			if err := os.WriteFile(file, []byte("new"), 0o600); err != nil {
				t.Error(err)
			}
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"data":{"secret":"s"},"metadata":{"version":1}}}`))
	}, "old")
	file = c.cfg.TokenFile
	if s, err := c.Secret(context.Background(), merchant, 1); err != nil || string(s) != "s" {
		t.Fatalf("secret %q, %v", s, err)
	}
}

func TestSecretDeniedGivesUp(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }, "s3cr3t-value")
	c.cfg.Backoff.Total = 20 * time.Millisecond
	_, err := c.Secret(context.Background(), merchant, 1)
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "s3cr3t-value") {
		t.Errorf("err = %v", err)
	}
}

func TestSecretNoTokenFile(t *testing.T) {
	c, err := New(Config{Addr: "http://127.0.0.1:1", TokenFile: filepath.Join(t.TempDir(), "missing")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Secret(context.Background(), merchant, 1); !errors.Is(err, ErrUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{Addr: "openbao:8200", TokenFile: "t"}); err == nil {
		t.Error("expected an address error")
	}
	if _, err := New(Config{Addr: "http://openbao:8200"}); err == nil {
		t.Error("expected a token file error")
	}
}
