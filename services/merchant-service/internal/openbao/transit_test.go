package openbao

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTransit(t *testing.T, h http.HandlerFunc) *TransitKey {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(Config{Addr: srv.URL, TokenFile: tokenFile(t, "tok")})
	if err != nil {
		t.Fatal(err)
	}
	return c.Transit("/transit/", "merchant-jwt")
}

func sigOf(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }

func TestTransitSign(t *testing.T) {
	var gotPath, gotTok string
	var body map[string]any
	k := newTransit(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotTok = r.URL.Path, r.Header.Get("X-Vault-Token")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		_, _ = w.Write([]byte(`{"data":{"signature":"vault:v3:` + sigOf(64) + `"}}`))
	})
	v, sig, err := k.Sign(context.Background(), []byte("abc"))
	if err != nil || v != 3 || len(sig) != 64 {
		t.Fatalf("sign: %d %d %v", v, len(sig), err)
	}
	if gotPath != "/v1/transit/sign/merchant-jwt" || gotTok != "tok" {
		t.Errorf("request: %s %q", gotPath, gotTok)
	}
	if body["input"] != base64.StdEncoding.EncodeToString([]byte("abc")) || body["hash_algorithm"] != "sha2-256" || body["marshaling_algorithm"] != "jws" {
		t.Errorf("body: %v", body)
	}
}

func TestTransitSignFailures(t *testing.T) {
	for name, resp := range map[string]string{
		"not json":     `x`,
		"no prefix":    `{"data":{"signature":"abc"}}`,
		"wrong scheme": `{"data":{"signature":"other:v1:` + sigOf(64) + `"}}`,
		"no v":         `{"data":{"signature":"vault:1:` + sigOf(64) + `"}}`,
		"bad version":  `{"data":{"signature":"vault:vx:` + sigOf(64) + `"}}`,
		"zero version": `{"data":{"signature":"vault:v0:` + sigOf(64) + `"}}`,
		"bad base64":   `{"data":{"signature":"vault:v1:!!"}}`,
		"short":        `{"data":{"signature":"vault:v1:` + sigOf(63) + `"}}`,
	} {
		k := newTransit(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(resp)) })
		if _, _, err := k.Sign(context.Background(), []byte("x")); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: %v", name, err)
		}
	}
	k := newTransit(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) })
	if _, _, err := k.Sign(context.Background(), []byte("x")); !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "403") {
		t.Errorf("forbidden: %v", err)
	}
}

func TestTransitKeys(t *testing.T) {
	var gotPath, gotMethod string
	k := newTransit(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		_, _ = w.Write([]byte(`{"data":{"type":"ecdsa-p256","latest_version":2,"keys":{"1":{"public_key":"P1"},"2":{"public_key":"P2"}}}}`))
	})
	latest, pems, err := k.Keys(context.Background())
	if err != nil || latest != 2 || pems[1] != "P1" || pems[2] != "P2" {
		t.Fatalf("keys: %d %v %v", latest, pems, err)
	}
	if gotMethod != http.MethodGet || gotPath != "/v1/transit/keys/merchant-jwt" {
		t.Errorf("request: %s %s", gotMethod, gotPath)
	}
}

func TestTransitKeysFailures(t *testing.T) {
	for name, resp := range map[string]string{
		"not json":    `x`,
		"wrong type":  `{"data":{"type":"rsa-2048","latest_version":1,"keys":{"1":{"public_key":"P"}}}}`,
		"bad version": `{"data":{"type":"ecdsa-p256","latest_version":1,"keys":{"x":{"public_key":"P"}}}}`,
		"no public":   `{"data":{"type":"ecdsa-p256","latest_version":1,"keys":{"1":{}}}}`,
		"no latest":   `{"data":{"type":"ecdsa-p256","latest_version":2,"keys":{"1":{"public_key":"P"}}}}`,
		"empty":       `{"data":{"type":"ecdsa-p256"}}`,
	} {
		k := newTransit(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(resp)) })
		if _, _, err := k.Keys(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
