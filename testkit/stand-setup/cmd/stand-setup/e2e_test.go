package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeService is an in-memory merchant-service speaking the contract.
type fakeService struct {
	mu        sync.Mutex
	byKey     map[string]map[string]any
	registers int
}

func (f *fakeService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var in map[string]any
	_ = json.NewDecoder(r.Body).Decode(&in)
	out := func(code int, v any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	cur := f.byKey[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer jwt:")]
	switch r.Method + " " + r.URL.Path {
	case "POST /api/v1/merchants":
		f.registers++
		key := "k" + in["name"].(string)
		f.byKey[key] = map[string]any{"merchantId": "id", "name": in["name"], "email": in["email"],
			"xpub": nil, "webhookUrl": nil, "toleranceBps": 100, "webhookSecretVersion": 1}
		out(201, map[string]any{"merchantId": "id", "apiKey": map[string]any{"key": key}})
	case "POST /api/v1/auth/token":
		if f.byKey[in["apiKey"].(string)] == nil {
			out(401, map[string]any{"code": "unauthorized", "message": "x"})
			return
		}
		out(200, map[string]any{"accessToken": "jwt:" + in["apiKey"].(string)})
	case "GET /api/v1/merchants/me":
		out(200, cur)
	case "PUT /api/v1/merchants/me/xpub":
		cur["xpub"] = in["xpub"]
		out(200, map[string]any{"xpub": in["xpub"], "firstAddress": "0x0"})
	case "PUT /api/v1/merchants/me/webhook":
		cur["webhookUrl"], cur["toleranceBps"] = in["webhookUrl"], in["toleranceBps"]
		out(200, cur)
	default:
		out(404, map[string]any{"code": "not_found", "message": "x"})
	}
}

func TestRunEndToEndAgainstFake(t *testing.T) {
	svc := &fakeService{byKey: map[string]map[string]any{}}
	srv := httptest.NewServer(svc)
	defer srv.Close()
	dir := t.TempDir()
	mn := filepath.Join(dir, "mn")
	_ = os.WriteFile(mn, []byte("test test test test test test test test test test test junk"), 0o600)
	keys := filepath.Join(dir, "keys.json")
	args := []string{"-base-url", srv.URL, "-mnemonic-file", mn, "-keys-file", keys,
		"-webhook-url", "https://mock.test/hook", "-merchants", "2", "-extra", "0"}
	// The fake returns a wrong first address, so the run must fail loudly
	// after registering the first merchant and saving its key.
	err := run(args)
	if err == nil || !strings.Contains(err.Error(), "first address") {
		t.Fatalf("err %v", err)
	}
	b, _ := os.ReadFile(keys)
	if !strings.Contains(string(b), "stand-merchant-001") || svc.registers != 1 {
		t.Fatalf("keys %s registers %d", b, svc.registers)
	}
}
