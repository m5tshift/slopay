package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/hdwallet"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/merchant-service/internal/jwt"
	"github.com/m5tshift/weigate/services/merchant-service/internal/merchant"
	"github.com/m5tshift/weigate/services/merchant-service/internal/xpubtest"
)

const contractFile = "../../../../contracts/openapi/merchant-service.yaml"

// contract is the OpenAPI document every response is validated against, and the operations seen by tests.
var contract struct {
	once   sync.Once
	doc    *openapi3.T
	router routers.Router
	err    error
	mu     sync.Mutex
	seen   map[string]bool
}

func contractRouter(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	contract.once.Do(func() {
		doc, err := openapi3.NewLoader().LoadFromFile(contractFile)
		if err != nil {
			contract.err = err
			return
		}
		if err := doc.Validate(context.Background()); err != nil {
			contract.err = err
			return
		}
		doc.Servers = openapi3.Servers{{URL: "/"}}
		contract.doc = doc
		contract.router, contract.err = legacy.NewRouter(doc)
		contract.seen = map[string]bool{}
	})
	if contract.err != nil {
		t.Fatal(contract.err)
	}
	return contract.doc, contract.router
}

type env struct {
	mux    *http.ServeMux
	svc    *merchant.Service
	store  *merchant.MemStore
	signer *jwt.FileSigner
	logs   *bytes.Buffer
}

func writeSigningKey(t *testing.T, path string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func newEnv(t *testing.T) *env {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "private.pem")
	writeSigningKey(t, keyFile)
	signer, err := jwt.NewFileSigner(keyFile, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{mux: http.NewServeMux(), store: merchant.NewMemStore(codec), signer: signer, logs: &bytes.Buffer{}}
	log := telemetry.NewLogger(e.logs, slog.LevelDebug)
	svc := merchant.NewService(e.store, merchant.Config{
		KeyRotationGrace: 24 * time.Hour, MaxActiveKeys: 2, DefaultToleranceBps: 100, MaxToleranceBps: 500,
	}, log)
	e.svc = svc
	err = Register(e.mux, Options{
		Service: svc, Issuer: jwt.NewIssuer(signer, "weigate-merchant-service", 15*time.Minute),
		Signer: signer, JWKSMaxAge: 5 * time.Minute, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

type resp struct {
	status int
	header http.Header
	body   []byte
}

func (r resp) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

// do sends a request and validates the response against the contract.
func (e *env) do(t *testing.T, method, path, token string, body any) resp {
	t.Helper()
	var raw []byte
	switch b := body.(type) {
	case nil:
	case string:
		raw = []byte(b)
	default:
		var err error
		if raw, err = json.Marshal(b); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(raw))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	r := resp{status: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
	validateResponse(t, method, path, raw, r)
	return r
}

func validateResponse(t *testing.T, method, path string, reqBody []byte, r resp) {
	t.Helper()
	_, router := contractRouter(t)
	req := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(reqBody))
	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s is not in the contract: %v", method, path, err)
	}
	contract.mu.Lock()
	contract.seen[route.Operation.OperationID] = true
	contract.mu.Unlock()
	if r.status == http.StatusNotImplemented && route.Operation.OperationID == "rotateWebhookSecret" {
		return // no secret store configured; the 501 is documented in AGENTS.md of the service
	}
	err = openapi3filter.ValidateResponse(context.Background(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 r.status,
		Header:                 r.header,
		Body:                   io.NopCloser(bytes.NewReader(r.body)),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	})
	if err != nil {
		t.Errorf("%s %s -> %d does not match the contract: %v\n%s", method, path, r.status, err, r.body)
	}
}

func (e *env) register(t *testing.T) RegisterMerchantResponse {
	t.Helper()
	r := e.do(t, http.MethodPost, "/api/v1/merchants", "", map[string]string{"name": "Example Shop", "email": "owner@merchant.example.com"})
	if r.status != http.StatusCreated {
		t.Fatalf("register: %d %s", r.status, r.body)
	}
	var out RegisterMerchantResponse
	r.decode(t, &out)
	return out
}

func (e *env) token(t *testing.T, apiKey string) string {
	t.Helper()
	r := e.do(t, http.MethodPost, "/api/v1/auth/token", "", map[string]string{"apiKey": apiKey})
	if r.status != http.StatusOK {
		t.Fatalf("token: %d %s", r.status, r.body)
	}
	var out TokenResponse
	r.decode(t, &out)
	if out.TokenType != "Bearer" || out.ExpiresIn != 900 {
		t.Errorf("token response %+v", out)
	}
	return out.AccessToken
}

func details(t *testing.T, r resp) map[string]string {
	t.Helper()
	if r.status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", r.status, r.body)
	}
	var v ValidationError
	r.decode(t, &v)
	out := map[string]string{}
	if v.Code != "validation_failed" || v.Details == nil {
		t.Fatalf("validation error %s", r.body)
	}
	for _, d := range *v.Details {
		out[d.Field] = d.Message
	}
	return out
}

func wantUnauthorized(t *testing.T, r resp) {
	t.Helper()
	if r.status != http.StatusUnauthorized || r.header.Get("WWW-Authenticate") != "Bearer" {
		t.Errorf("status %d, WWW-Authenticate %q, want 401 Bearer: %s", r.status, r.header.Get("WWW-Authenticate"), r.body)
	}
}

func TestRegisterAndToken(t *testing.T) {
	e := newEnv(t)
	reg := e.register(t)
	if reg.ApiKey.Status != "active" || !strings.HasPrefix(reg.ApiKey.Key, merchant.KeyPrefix) {
		t.Errorf("registration %+v", reg.ApiKey)
	}
	tok := e.token(t, reg.ApiKey.Key)

	r := e.do(t, http.MethodGet, "/api/v1/merchants/me", tok, nil)
	var m Merchant
	r.decode(t, &m)
	if r.status != http.StatusOK || m.MerchantId != reg.MerchantId || m.ToleranceBps != 100 || m.Xpub != nil || m.WebhookSecretVersion != 1 {
		t.Errorf("me: %d %s", r.status, r.body)
	}

	wantUnauthorized(t, e.do(t, http.MethodPost, "/api/v1/auth/token", "", map[string]string{"apiKey": "slp_live_wrong"}))
	other, _ := merchant.GenerateKey()
	wantUnauthorized(t, e.do(t, http.MethodPost, "/api/v1/auth/token", "", map[string]string{"apiKey": other}))
	if d := details(t, e.do(t, http.MethodPost, "/api/v1/auth/token", "", map[string]string{})); len(d) == 0 {
		t.Error("missing apiKey accepted")
	}

	// Keys and tokens never reach the logs.
	logs := e.logs.String()
	if strings.Contains(logs, reg.ApiKey.Key) || strings.Contains(logs, tok) || strings.Contains(logs, other) {
		t.Error("a key or a token is in the logs")
	}
}

func TestRegisterValidation(t *testing.T) {
	e := newEnv(t)
	for name, tc := range map[string]struct {
		body  any
		field string
	}{
		"no name":        {map[string]string{"email": "a@b.example"}, "name"},
		"empty name":     {map[string]string{"name": "", "email": "a@b.example"}, "name"},
		"long name":      {map[string]string{"name": strings.Repeat("a", 201), "email": "a@b.example"}, "name"},
		"blank name":     {map[string]string{"name": "  ", "email": "a@b.example"}, "name"},
		"bad email":      {map[string]string{"name": "Shop", "email": "nope"}, "email"},
		"extra property": {map[string]string{"name": "Shop", "email": "a@b.example", "role": "admin"}, "body"},
		"not json":       {"{", "body"},
		"wrong type":     {map[string]any{"name": 1, "email": "a@b.example"}, "name"},
		"empty body":     {"", "body"},
	} {
		t.Run(name, func(t *testing.T) {
			d := details(t, e.do(t, http.MethodPost, "/api/v1/merchants", "", tc.body))
			if _, ok := d[tc.field]; !ok {
				t.Errorf("details %v, want field %s", d, tc.field)
			}
		})
	}
	if len(e.store.StoredHashes()) != 0 {
		t.Error("an invalid registration was stored")
	}
}

func TestAuthRequired(t *testing.T) {
	e := newEnv(t)
	reg := e.register(t)
	tok := e.token(t, reg.ApiKey.Key)
	// A JWT for a merchant that does not exist, signed by the same key.
	ghost, err := jwt.NewIssuer(e.signer, "weigate-merchant-service", 15*time.Minute).Issue(context.Background(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	notUUID, err := jwt.NewIssuer(e.signer, "weigate-merchant-service", 15*time.Minute).Issue(context.Background(), "someone")
	if err != nil {
		t.Fatal(err)
	}
	ops := []struct{ method, path string }{
		{http.MethodGet, "/api/v1/merchants/me"},
		{http.MethodPut, "/api/v1/merchants/me/xpub"},
		{http.MethodPut, "/api/v1/merchants/me/webhook"},
		{http.MethodGet, "/api/v1/merchants/me/api-keys"},
		{http.MethodPost, "/api/v1/merchants/me/api-keys"},
		{http.MethodDelete, "/api/v1/merchants/me/api-keys/" + uuid.NewString()},
		{http.MethodDelete, "/api/v1/merchants/me/api-keys/not-a-uuid"},
		{http.MethodPost, "/api/v1/merchants/me/webhook-secret/rotate"},
	}
	for _, op := range ops {
		for name, token := range map[string]string{
			"no token": "", "garbage": "abc", "api key as token": reg.ApiKey.Key, "tampered": tok[:len(tok)-2] + "AA",
			"subject not uuid": notUUID,
		} {
			wantUnauthorized(t, e.do(t, op.method, op.path, token, nil))
			_ = name
		}
	}
	// Other schemes are refused.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/merchants/me", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("a:b")))
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("basic auth: %d", rec.Code)
	}
	// A valid JWT of a merchant that does not exist.
	for _, op := range ops[:6] {
		var body any
		switch op.path {
		case "/api/v1/merchants/me/xpub":
			body = map[string]string{"xpub": xpubtest.Xpub}
		case "/api/v1/merchants/me/webhook":
			body = map[string]string{"webhookUrl": "https://a.example"}
		}
		r := e.do(t, op.method, op.path, ghost, body)
		if op.path == "/api/v1/merchants/me/api-keys" && op.method == http.MethodGet {
			// Listing keys of an unknown merchant is an empty list.
			if r.status != http.StatusOK {
				t.Errorf("list for unknown merchant: %d", r.status)
			}
			continue
		}
		if strings.HasPrefix(op.path, "/api/v1/merchants/me/api-keys/") {
			if r.status != http.StatusUnauthorized {
				t.Errorf("%s %s: %d", op.method, op.path, r.status)
			}
			continue
		}
		wantUnauthorized(t, r)
	}
	if strings.Contains(e.logs.String(), tok) {
		t.Error("token logged")
	}
}

func TestJWKS(t *testing.T) {
	e := newEnv(t)
	reg := e.register(t)
	tok := e.token(t, reg.ApiKey.Key)
	r := e.do(t, http.MethodGet, "/.well-known/jwks.json", "", nil)
	if r.status != http.StatusOK || r.header.Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("jwks: %d %v", r.status, r.header)
	}
	var set Jwks
	r.decode(t, &set)
	hdr, _, _ := strings.Cut(tok, ".")
	raw, err := base64.RawURLEncoding.DecodeString(hdr)
	if err != nil {
		t.Fatal(err)
	}
	var h struct{ Alg, Kid string }
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	if len(set.Keys) != 1 || set.Keys[0].Kid != h.Kid || h.Alg != "ES256" {
		t.Errorf("jwks %s, token header %+v", r.body, h)
	}
	if strings.Contains(string(r.body), `"d"`) {
		t.Error("private part in the JWKS")
	}
}

func TestXpub(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, e.register(t).ApiKey.Key)
	r := e.do(t, http.MethodPut, "/api/v1/merchants/me/xpub", tok, map[string]string{"xpub": xpubtest.Xpub})
	var out XpubResponse
	r.decode(t, &out)
	want, _ := hdwallet.Derive(xpubtest.Xpub, 0)
	if r.status != http.StatusOK || out.FirstAddress != want || out.Xpub != xpubtest.Xpub {
		t.Errorf("xpub: %d %s, want first address %s", r.status, r.body, want)
	}
	if len(e.store.Events) != 1 {
		t.Errorf("events %d", len(e.store.Events))
	}
	for name, in := range map[string]string{
		"private key":  xpubtest.PrivateVersion(),
		"bad checksum": xpubtest.BadChecksum(),
		"too short":    "xpub123",
	} {
		t.Run(name, func(t *testing.T) {
			r := e.do(t, http.MethodPut, "/api/v1/merchants/me/xpub", tok, map[string]string{"xpub": in})
			if _, ok := details(t, r)["xpub"]; !ok {
				t.Errorf("details %s", r.body)
			}
			if bytes.Contains(r.body, []byte(in)) || strings.Contains(e.logs.String(), in) {
				t.Error("the submitted key is echoed")
			}
		})
	}
}

func TestWebhook(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, e.register(t).ApiKey.Key)
	r := e.do(t, http.MethodPut, "/api/v1/merchants/me/webhook", tok, map[string]any{"webhookUrl": "https://merchant.example.com/hooks", "toleranceBps": 250})
	var m Merchant
	r.decode(t, &m)
	if r.status != http.StatusOK || m.ToleranceBps != 250 || m.WebhookUrl == nil {
		t.Errorf("webhook: %d %s", r.status, r.body)
	}
	r = e.do(t, http.MethodPut, "/api/v1/merchants/me/webhook", tok, map[string]any{"webhookUrl": "https://merchant.example.com/hooks"})
	r.decode(t, &m)
	if r.status != http.StatusOK || m.ToleranceBps != 100 {
		t.Errorf("default tolerance: %d %s", r.status, r.body)
	}
	for name, tc := range map[string]struct {
		body  map[string]any
		field string
	}{
		"tolerance over 5 %": {map[string]any{"webhookUrl": "https://a.example", "toleranceBps": 501}, "toleranceBps"},
		"negative":           {map[string]any{"webhookUrl": "https://a.example", "toleranceBps": -1}, "toleranceBps"},
		"http":               {map[string]any{"webhookUrl": "http://a.example"}, "webhookUrl"},
		"no host":            {map[string]any{"webhookUrl": "https:///x"}, "webhookUrl"},
		"missing url":        {map[string]any{"toleranceBps": 1}, "webhookUrl"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := details(t, e.do(t, http.MethodPut, "/api/v1/merchants/me/webhook", tok, tc.body))[tc.field]; !ok {
				t.Errorf("want field %s", tc.field)
			}
		})
	}
}

func TestAPIKeys(t *testing.T) {
	e := newEnv(t)
	reg := e.register(t)
	tok := e.token(t, reg.ApiKey.Key)
	keysPath := "/api/v1/merchants/me/api-keys"

	r := e.do(t, http.MethodPost, keysPath, tok, nil)
	var second IssuedApiKey
	r.decode(t, &second)
	if r.status != http.StatusCreated || second.Status != "active" || second.Key == reg.ApiKey.Key {
		t.Fatalf("issue: %d %s", r.status, r.body)
	}
	r = e.do(t, http.MethodGet, keysPath, tok, nil)
	var list struct{ Items []ApiKey }
	r.decode(t, &list)
	if r.status != http.StatusOK || len(list.Items) != 2 || list.Items[0].Status != "expiring" || list.Items[0].ExpiresAt == nil {
		t.Errorf("list: %d %s", r.status, r.body)
	}
	if bytes.Contains(r.body, []byte(reg.ApiKey.Key)) || bytes.Contains(r.body, []byte(second.Key)) {
		t.Error("keys are listed in clear")
	}
	if r := e.do(t, http.MethodPost, keysPath, tok, nil); r.status != http.StatusConflict {
		t.Errorf("third key: %d", r.status)
	}
	if r := e.do(t, http.MethodDelete, keysPath+"/"+uuid.NewString(), tok, nil); r.status != http.StatusNotFound {
		t.Errorf("unknown key: %d", r.status)
	}
	if r := e.do(t, http.MethodDelete, keysPath+"/not-a-uuid", tok, nil); r.status != http.StatusNotFound {
		t.Errorf("malformed key id: %d", r.status)
	}
	if r := e.do(t, http.MethodDelete, keysPath+"/"+second.Id.String(), tok, nil); r.status != http.StatusNoContent {
		t.Errorf("revoke: %d %s", r.status, r.body)
	}
	wantUnauthorized(t, e.do(t, http.MethodPost, "/api/v1/auth/token", "", map[string]string{"apiKey": second.Key}))
	if r := e.do(t, http.MethodDelete, keysPath+"/"+reg.ApiKey.Id.String(), tok, nil); r.status != http.StatusConflict {
		t.Errorf("last key: %d", r.status)
	}
	// The old key works again without an expiry.
	e.token(t, reg.ApiKey.Key)
}

func TestRotateWebhookSecretNotImplemented(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, e.register(t).ApiKey.Key)
	r := e.do(t, http.MethodPost, "/api/v1/merchants/me/webhook-secret/rotate", tok, nil)
	if r.status != http.StatusNotImplemented {
		t.Errorf("rotate: %d", r.status)
	}
}

type fakeSecrets struct {
	got map[int]string
	err error
}

func (f *fakeSecrets) PutWebhookSecret(_ context.Context, _ uuid.UUID, version int, secret string) error {
	if f.err != nil {
		return f.err
	}
	f.got[version] = secret
	return nil
}

func TestRotateWebhookSecret(t *testing.T) {
	e := newEnv(t)
	fs := &fakeSecrets{got: map[int]string{}}
	e.svc.WithSecrets(fs)
	tok := e.token(t, e.register(t).ApiKey.Key)
	const path = "/api/v1/merchants/me/webhook-secret/rotate"
	var out struct {
		Version int    `json:"version"`
		Secret  string `json:"secret"`
	}
	r := e.do(t, http.MethodPost, path, tok, nil)
	if r.status != http.StatusOK {
		t.Fatalf("rotate: %d %s", r.status, r.body)
	}
	r.decode(t, &out)
	if out.Version != 2 || out.Secret == "" || fs.got[2] != out.Secret {
		t.Errorf("rotated: %+v stored %v", out, fs.got)
	}
	r = e.do(t, http.MethodGet, "/api/v1/merchants/me", tok, nil)
	var m struct {
		WebhookSecretVersion int `json:"webhookSecretVersion"`
	}
	r.decode(t, &m)
	if m.WebhookSecretVersion != 2 {
		t.Errorf("version = %d", m.WebhookSecretVersion)
	}
	if strings.Contains(e.logs.String(), out.Secret) {
		t.Error("secret in logs")
	}
	// A failing store changes nothing and leaks nothing.
	fs.err = errors.New("sealed")
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "sealed") {
		t.Errorf("store down: %d %s", rec.Code, rec.Body)
	}
	fs.err = nil
	r = e.do(t, http.MethodPost, path, tok, nil)
	r.decode(t, &out)
	if r.status != http.StatusOK || out.Version != 3 {
		t.Errorf("after failure: %d %+v", r.status, out)
	}
}

func TestInternalError(t *testing.T) {
	e := newEnv(t)
	reg := e.register(t)
	tok := e.token(t, reg.ApiKey.Key)
	e.svc.WithSecrets(&fakeSecrets{got: map[int]string{}})
	e.store.Err = errors.New("db down")
	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/api/v1/merchants", map[string]string{"name": "Shop", "email": "a@b.example"}},
		{http.MethodPost, "/api/v1/auth/token", map[string]string{"apiKey": reg.ApiKey.Key}},
		{http.MethodGet, "/api/v1/merchants/me", nil},
		{http.MethodPut, "/api/v1/merchants/me/xpub", map[string]string{"xpub": xpubtest.Xpub}},
		{http.MethodPut, "/api/v1/merchants/me/webhook", map[string]string{"webhookUrl": "https://a.example"}},
		{http.MethodGet, "/api/v1/merchants/me/api-keys", nil},
		{http.MethodPost, "/api/v1/merchants/me/api-keys", nil},
		{http.MethodDelete, "/api/v1/merchants/me/api-keys/" + uuid.NewString(), nil},
		{http.MethodPost, "/api/v1/merchants/me/webhook-secret/rotate", nil},
	} {
		req := httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, nil)
		if tc.body != nil {
			raw, _ := json.Marshal(tc.body)
			req = httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		e.mux.ServeHTTP(rec, req)
		var body Error
		if rec.Code != http.StatusInternalServerError || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body.Code != "internal" {
			t.Errorf("%s %s: %d %s", tc.method, tc.path, rec.Code, rec.Body)
		}
	}
	if strings.Contains(e.logs.String(), reg.ApiKey.Key) || strings.Contains(e.logs.String(), tok) {
		t.Error("secrets in error logs")
	}
}

type brokenSigner struct{}

func (brokenSigner) Sign(context.Context, []byte) (string, []byte, error) {
	return "", nil, errors.New("no key")
}

func (brokenSigner) PublicKeys(context.Context) ([]jwt.PublicKey, error) {
	return nil, errors.New("no key")
}

func TestSignerFailure(t *testing.T) {
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	store := merchant.NewMemStore(codec)
	svc := merchant.NewService(store, merchant.Config{KeyRotationGrace: time.Hour, MaxActiveKeys: 2, DefaultToleranceBps: 100, MaxToleranceBps: 500}, nil)
	mux := http.NewServeMux()
	if err := Register(mux, Options{Service: svc, Issuer: jwt.NewIssuer(brokenSigner{}, "x", 15*time.Minute), Signer: brokenSigner{}}); err != nil {
		t.Fatal(err)
	}
	_, key, err := svc.Register(context.Background(), "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/auth/token", `{"apiKey":"` + key.Key + `"}`},
		{http.MethodGet, "/.well-known/jwks.json", ""},
	} {
		req := httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s: %d", tc.path, rec.Code)
		}
	}
}

func TestBodyLimit(t *testing.T) {
	e := newEnv(t)
	big := `{"name":"` + strings.Repeat("a", maxBodyBytes) + `","email":"a@b.example"}`
	if r := e.do(t, http.MethodPost, "/api/v1/merchants", "", big); r.status != http.StatusBadRequest {
		t.Errorf("big body: %d", r.status)
	}
}

// TestContractCoverage runs last in this file and checks that every operation of the contract was exercised
// and its responses validated.
func TestContractCoverage(t *testing.T) {
	TestRegisterAndToken(t)
	TestJWKS(t)
	TestXpub(t)
	TestWebhook(t)
	TestAPIKeys(t)
	TestRotateWebhookSecretNotImplemented(t)
	TestRotateWebhookSecret(t)
	doc, _ := contractRouter(t)
	contract.mu.Lock()
	defer contract.mu.Unlock()
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			if !contract.seen[op.OperationID] {
				t.Errorf("%s %s (%s) is not covered", method, path, op.OperationID)
			}
		}
	}
}

func TestErrorHelpers(t *testing.T) {
	g := &gate{log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	rec := httptest.NewRecorder()
	g.requestError(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil), errors.New("decode"))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("request error: %d", rec.Code)
	}
	if errorReason(jwt.ErrInvalidToken) != "invalid token" || errorReason(errors.New("signer down")) != "verification failed" {
		t.Error("error reasons")
	}
}
