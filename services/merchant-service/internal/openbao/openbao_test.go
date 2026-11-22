package openbao

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func tokenFile(t *testing.T, tok string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(tok+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNewValidates(t *testing.T) {
	tf := tokenFile(t, "t")
	for name, cfg := range map[string]Config{
		"no addr":    {TokenFile: tf},
		"bad scheme": {Addr: "ftp://x", TokenFile: tf},
		"no host":    {Addr: "https://", TokenFile: tf},
		"no token":   {Addr: "https://x"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	c, err := New(Config{Addr: "https://x", TokenFile: tf, Mount: "/kv/"})
	if err != nil || c.cfg.Mount != "kv" {
		t.Errorf("mount: %v %+v", err, c)
	}
	c, _ = New(Config{Addr: "https://x", TokenFile: tf})
	if c.cfg.Mount != "secret" {
		t.Errorf("default mount %q", c.cfg.Mount)
	}
}

func TestPutWebhookSecret(t *testing.T) {
	id := uuid.New()
	var gotPath, gotTok, gotMethod string
	var gotBody map[string]map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotTok, gotMethod = r.URL.Path, r.Header.Get("X-Vault-Token"), r.Method
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		_, _ = w.Write([]byte(`{"data":{"version":7}}`))
	}))
	defer srv.Close()
	tf := tokenFile(t, "tok-1")
	c, err := New(Config{Addr: srv.URL + "/", TokenFile: tf, Mount: "kv"})
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.PutWebhookSecret(context.Background(), id, "whsec_x")
	if err != nil || v != 7 {
		t.Fatalf("put: %d %v", v, err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/kv/data/webhook/"+id.String() || gotTok != "tok-1" {
		t.Errorf("request: %s %s %q", gotMethod, gotPath, gotTok)
	}
	if gotBody["data"]["secret"] != "whsec_x" || len(gotBody["data"]) != 1 {
		t.Errorf("body: %v", gotBody)
	}
	// The token file is re-read on every request.
	if err := os.WriteFile(tf, []byte("tok-2"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutWebhookSecret(context.Background(), id, "whsec_y"); err != nil || gotTok != "tok-2" {
		t.Errorf("token reread: %v %q", err, gotTok)
	}
}

func TestPutWebhookSecretFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // sealed
	}))
	tf := tokenFile(t, "t")
	c, _ := New(Config{Addr: srv.URL, TokenFile: tf})
	_, err := c.PutWebhookSecret(context.Background(), uuid.New(), "whsec_SECRET")
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "whsec_SECRET") {
		t.Errorf("sealed: %v", err)
	}
	srv.Close()
	_, err = c.PutWebhookSecret(context.Background(), uuid.New(), "whsec_SECRET")
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "whsec_SECRET") {
		t.Errorf("closed: %v", err)
	}
	c2, _ := New(Config{Addr: srv.URL, TokenFile: filepath.Join(t.TempDir(), "none")})
	if _, err := c2.PutWebhookSecret(context.Background(), uuid.New(), "s"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("no token file: %v", err)
	}
}

func TestPutWebhookSecretBadResponse(t *testing.T) {
	for name, body := range map[string]string{"no version": `{"data":{}}`, "not json": `x`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		c, _ := New(Config{Addr: srv.URL, TokenFile: tokenFile(t, "t")})
		if _, err := c.PutWebhookSecret(context.Background(), uuid.New(), "s"); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: %v", name, err)
		}
		srv.Close()
	}
}
