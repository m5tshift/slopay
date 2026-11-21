package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoicetest"
)

const contractFile = "../../../../contracts/openapi/invoice-service.yaml"

const issuer = "weigate-merchant-service"

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

var t0 = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

type env struct {
	mux      *http.ServeMux
	store    *invoice.MemStore
	rates    *invoicetest.Rates
	cache    *invoicetest.Cache
	logs     *bytes.Buffer
	merchant uuid.UUID
	token    string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvKeys(t, testKeys)
}

// newEnvKeys is newEnv with the given key resolver; the tokens of the env are signed by testKey.
func newEnvKeys(t *testing.T, keys KeyResolver) *env { return newEnvUSDT(t, keys, nil) }

// newEnvUSDT is newEnvKeys with the test USDT token configured (nil: not configured).
func newEnvUSDT(t *testing.T, keys KeyResolver, usdt *invoice.Asset) *env {
	t.Helper()
	e := &env{
		mux: http.NewServeMux(), store: invoice.NewMemStore(), rates: invoicetest.NewRates("3694.25", t0.Add(-3*time.Second)),
		cache: invoicetest.NewCache(), logs: &bytes.Buffer{}, merchant: uuid.New(),
	}
	log := telemetry.NewLogger(e.logs, slog.LevelDebug)
	svc, err := invoice.NewService(e.store, e.rates, e.cache, invoice.Config{
		InvoiceTTL: 15 * time.Minute, RateMaxAge: 60 * time.Second, Now: func() time.Time { return t0 }, USDT: usdt,
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := Register(e.mux, Options{Service: svc, JWTIssuer: issuer, Keys: keys, RetryAfter: 5 * time.Second, Logger: log,
		Now: func() time.Time { return t0 }}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.UpsertMerchant(context.Background(), invoice.Merchant{ID: e.merchant, Version: 1, Xpub: invoicetest.Xpub, ToleranceBps: 100}); err != nil {
		t.Fatal(err)
	}
	e.token = token(t, map[string]any{"sub": e.merchant.String(), "iss": issuer, "exp": t0.Add(15 * time.Minute).Unix()})
	return e
}

func segment(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// token builds a JWT signed by the test merchant-service key.
func token(t *testing.T, claims map[string]any) string {
	t.Helper()
	return signToken(t, testKey, testKID, claims)
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
func (e *env) do(t *testing.T, method, path, token string, header map[string]string, body any) resp {
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
	for k, v := range header {
		req.Header.Set(k, v)
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

func (e *env) create(t *testing.T, key string, body any) resp {
	t.Helper()
	return e.do(t, http.MethodPost, "/api/v1/invoices", e.token, map[string]string{"Idempotency-Key": key}, body)
}

func errorCode(t *testing.T, r resp) Error {
	t.Helper()
	var out Error
	r.decode(t, &out)
	return out
}

// FR-10: a USDT invoice answers with the ERC20 asset and the amount in token base units, and passes the
// response check against the contract.
func TestCreateInvoiceUSDT(t *testing.T) {
	usdt := invoice.Asset{Kind: invoice.KindERC20, Contract: "0x5fbdb2315678afecb367f032d93f642f64180aa3", Decimals: 6}
	e := newEnvUSDT(t, testKeys, &usdt)
	e.rates.Set(invoice.Quote{Price: "1.0003", QuotedAt: t0.Add(-time.Second)}, nil)
	r := e.create(t, "key-usdt", map[string]any{"amountUsdCents": 1000, "orderId": "order-usdt", "asset": "USDT"})
	if r.status != http.StatusCreated {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	var inv Invoice
	r.decode(t, &inv)
	a, err := inv.Asset.AsAsset1()
	if err != nil || a.Kind != ERC20 || a.Contract != usdt.Contract || a.Decimals != 6 {
		t.Errorf("asset %+v %v", a, err)
	}
	if inv.Amount != "9997001" || inv.Rate != "1.0003" {
		t.Errorf("amount %s rate %s", inv.Amount, inv.Rate)
	}
	// The payment page shows the same asset and amount.
	pr := e.do(t, http.MethodGet, "/pay/"+inv.InvoiceId.String(), "", nil, nil)
	var page PaymentPage
	pr.decode(t, &page)
	if pr.status != http.StatusOK || page.Amount != "9997001" {
		t.Errorf("page %d %s", pr.status, pr.body)
	}
}

func TestCreateInvoice(t *testing.T) {
	e := newEnv(t)
	body := map[string]any{"amountUsdCents": 1999, "orderId": "order-10042"}
	r := e.create(t, "key-1", body)
	if r.status != http.StatusCreated {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	var inv Invoice
	r.decode(t, &inv)
	if r.header.Get("Location") != "/api/v1/invoices/"+inv.InvoiceId.String() {
		t.Errorf("Location %q", r.header.Get("Location"))
	}
	// 19.99 USD at 3694.25 USD/ETH, rounded up in wei.
	if inv.Amount != "5411111863030386" || inv.Rate != "3694.25" || inv.Status != "NEW" || inv.Sequence != 1 ||
		inv.MerchantId != e.merchant || inv.ReceivedAmount != "0" || inv.Overpaid || inv.Confirmations != 0 ||
		!inv.ExpiresAt.Equal(t0.Add(15*time.Minute)) || !inv.CreatedAt.Equal(t0) {
		t.Errorf("invoice %+v", inv)
	}
	a, err := inv.Asset.AsAsset0()
	if err != nil || a.Kind != Asset0KindETH || a.Decimals != 18 {
		t.Errorf("asset %+v %v", a, err)
	}

	// FR-01: the same key and body is the same invoice, 200.
	again := e.create(t, "key-1", map[string]any{"amountUsdCents": 1999, "orderId": "order-10042", "asset": "ETH"})
	var inv2 Invoice
	again.decode(t, &inv2)
	if again.status != http.StatusOK || inv2.InvoiceId != inv.InvoiceId || inv2.Address != inv.Address {
		t.Errorf("repeat: %d %+v", again.status, inv2)
	}
	// Another body with the key is 422.
	reuse := e.create(t, "key-1", map[string]any{"amountUsdCents": 2000, "orderId": "order-10042"})
	if reuse.status != http.StatusUnprocessableEntity || errorCode(t, reuse).Code != codeKeyReuse {
		t.Errorf("reuse: %d %s", reuse.status, reuse.body)
	}
	if n := len(e.store.Events()); n != 1 {
		t.Errorf("%d invoice.created events, want 1", n)
	}
}

func TestCreateInvoiceErrors(t *testing.T) {
	e := newEnv(t)
	valid := map[string]any{"amountUsdCents": 100, "orderId": "o"}
	cases := map[string]struct {
		token  string
		header map[string]string
		body   any
		status int
		field  string
	}{
		"no token":            {"", map[string]string{"Idempotency-Key": "k"}, valid, 401, ""},
		"no key":              {e.token, nil, valid, 400, "Idempotency-Key"},
		"long key":            {e.token, map[string]string{"Idempotency-Key": strings.Repeat("k", 129)}, valid, 400, "Idempotency-Key"},
		"zero amount":         {e.token, map[string]string{"Idempotency-Key": "k"}, map[string]any{"amountUsdCents": 0, "orderId": "o"}, 400, "amountUsdCents"},
		"no order":            {e.token, map[string]string{"Idempotency-Key": "k"}, map[string]any{"amountUsdCents": 1}, 400, "orderId"},
		"extra field":         {e.token, map[string]string{"Idempotency-Key": "k"}, map[string]any{"amountUsdCents": 1, "orderId": "o", "webhookUrl": "x"}, 400, ""},
		"unknown asset":       {e.token, map[string]string{"Idempotency-Key": "k"}, map[string]any{"amountUsdCents": 1, "orderId": "o", "asset": "BTC"}, 400, "asset"},
		"usdt not configured": {e.token, map[string]string{"Idempotency-Key": "k"}, map[string]any{"amountUsdCents": 1, "orderId": "o", "asset": "USDT"}, 400, "asset"},
		"not json":            {e.token, map[string]string{"Idempotency-Key": "k"}, "{", 400, "body"},
		"no body":             {e.token, map[string]string{"Idempotency-Key": "k"}, nil, 400, "body"},
		"merchant not seen":   {token(t, map[string]any{"sub": uuid.NewString(), "iss": issuer, "exp": t0.Add(time.Hour).Unix()}), map[string]string{"Idempotency-Key": "k"}, valid, 409, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := e.do(t, http.MethodPost, "/api/v1/invoices", tc.token, tc.header, tc.body)
			if r.status != tc.status {
				t.Fatalf("%d %s", r.status, r.body)
			}
			switch tc.status {
			case 401:
				if r.header.Get("WWW-Authenticate") != "Bearer" {
					t.Error("no WWW-Authenticate")
				}
			case 409:
				if errorCode(t, r).Code != codeXpubNotSet {
					t.Errorf("%s", r.body)
				}
			case 400:
				var v ValidationError
				r.decode(t, &v)
				if v.Code != codeValidation || v.Details == nil || len(*v.Details) == 0 {
					t.Fatalf("%s", r.body)
				}
				if tc.field != "" && (*v.Details)[0].Field != tc.field {
					t.Errorf("field %s, want %s", (*v.Details)[0].Field, tc.field)
				}
			}
		})
	}
	if len(e.store.Events()) != 0 {
		t.Error("an invoice was created by an invalid request")
	}
}

func TestCreateInvoiceRateUnavailable(t *testing.T) {
	cases := map[string]struct {
		quote  invoice.Quote
		err    error
		reason ErrorReason
		retry  string
	}{
		"diverged with Retry-After": {err: &invoice.RateError{Reason: invoice.ReasonDiverged, RetryAfter: 7 * time.Second}, reason: ErrorReasonDiverged, retry: "7"},
		"unavailable":               {err: errors.New("connection refused"), reason: ErrorReasonUnavailable, retry: "5"},
		"stale":                     {quote: invoice.Quote{Price: "3000", QuotedAt: t0.Add(-61 * time.Second)}, reason: ErrorReasonStale, retry: "5"},
		"sub-second Retry-After":    {err: &invoice.RateError{Reason: invoice.ReasonStale, RetryAfter: 1500 * time.Millisecond}, reason: ErrorReasonStale, retry: "2"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.rates.Set(tc.quote, tc.err)
			r := e.create(t, "k", map[string]any{"amountUsdCents": 100, "orderId": "o"})
			if r.status != http.StatusServiceUnavailable || r.header.Get("Retry-After") != tc.retry {
				t.Fatalf("%d Retry-After %q %s", r.status, r.header.Get("Retry-After"), r.body)
			}
			body := errorCode(t, r)
			if body.Code != codeRateUnavailable || body.Reason == nil || *body.Reason != tc.reason {
				t.Errorf("%s", r.body)
			}
		})
	}
}

func TestGetInvoice(t *testing.T) {
	e := newEnv(t)
	r := e.create(t, "k", map[string]any{"amountUsdCents": 100, "orderId": "o"})
	var created Invoice
	r.decode(t, &created)
	path := "/api/v1/invoices/" + created.InvoiceId.String()

	got := e.do(t, http.MethodGet, path, e.token, nil, nil)
	var inv Invoice
	got.decode(t, &inv)
	if got.status != http.StatusOK || inv.InvoiceId != created.InvoiceId || inv.Address != created.Address {
		t.Errorf("get: %d %s", got.status, got.body)
	}
	other := token(t, map[string]any{"sub": uuid.NewString(), "iss": issuer, "exp": t0.Add(time.Hour).Unix()})
	for name, tc := range map[string]struct {
		path, token string
		status      int
	}{
		"someone else's": {path, other, 404},
		"unknown":        {"/api/v1/invoices/" + uuid.NewString(), e.token, 404},
		"malformed id":   {"/api/v1/invoices/not-a-uuid", e.token, 404},
		"malformed anon": {"/api/v1/invoices/not-a-uuid", "", 401},
		"no token":       {path, "", 401},
	} {
		r := e.do(t, http.MethodGet, tc.path, tc.token, nil, nil)
		if r.status != tc.status {
			t.Errorf("%s: %d %s", name, r.status, r.body)
		}
		if tc.status == 404 && errorCode(t, r).Code != codeNotFound {
			t.Errorf("%s: %s", name, r.body)
		}
	}
}

func TestListInvoices(t *testing.T) {
	e := newEnv(t)
	ids := map[uuid.UUID]bool{}
	for i := range 5 {
		r := e.create(t, fmt.Sprintf("k%d", i), map[string]any{"amountUsdCents": 100 + i, "orderId": fmt.Sprintf("order-%d", i%2)})
		var inv Invoice
		r.decode(t, &inv)
		ids[inv.InvoiceId] = true
	}
	var all []Invoice
	path := "/api/v1/invoices?limit=2"
	for range 5 {
		r := e.do(t, http.MethodGet, path, e.token, nil, nil)
		if r.status != http.StatusOK {
			t.Fatalf("%d %s", r.status, r.body)
		}
		var page InvoicePage
		r.decode(t, &page)
		all = append(all, page.Items...)
		if page.NextCursor == nil {
			break
		}
		path = "/api/v1/invoices?limit=2&cursor=" + *page.NextCursor
	}
	if len(all) != 5 {
		t.Fatalf("%d invoices", len(all))
	}
	for _, inv := range all {
		if !ids[inv.InvoiceId] {
			t.Errorf("unexpected %s", inv.InvoiceId)
		}
		delete(ids, inv.InvoiceId)
	}

	filtered := e.do(t, http.MethodGet, "/api/v1/invoices?orderId=order-1&status=NEW&status=PAID&createdFrom=2026-11-20T00:00:00Z&createdTo=2026-11-21T00:00:00Z", e.token, nil, nil)
	var page InvoicePage
	filtered.decode(t, &page)
	if filtered.status != http.StatusOK || len(page.Items) != 2 || page.NextCursor != nil {
		t.Errorf("filtered: %d %s", filtered.status, filtered.body)
	}
	empty := e.do(t, http.MethodGet, "/api/v1/invoices?status=PAID", e.token, nil, nil)
	if empty.status != http.StatusOK || !bytes.Contains(empty.body, []byte(`"items":[]`)) {
		t.Errorf("empty page: %s", empty.body)
	}

	for name, tc := range map[string]struct {
		query string
		field string
	}{
		"cursor":        {"cursor=garbage", "cursor"},
		"limit 0":       {"limit=0", "limit"},
		"limit 101":     {"limit=101", "limit"},
		"limit text":    {"limit=many", "limit"},
		"status":        {"status=LOST", "status"},
		"createdFrom":   {"createdFrom=yesterday", "createdFrom"},
		"long cursor":   {"cursor=" + strings.Repeat("a", 513), "cursor"},
		"empty orderId": {"orderId=", "orderId"},
	} {
		r := e.do(t, http.MethodGet, "/api/v1/invoices?"+tc.query, e.token, nil, nil)
		var v ValidationError
		r.decode(t, &v)
		if r.status != http.StatusBadRequest || v.Details == nil || (*v.Details)[0].Field != tc.field {
			t.Errorf("%s: %d %s", name, r.status, r.body)
		}
	}
	if r := e.do(t, http.MethodGet, "/api/v1/invoices", "", nil, nil); r.status != http.StatusUnauthorized {
		t.Errorf("no token: %d", r.status)
	}
	if r := e.do(t, http.MethodGet, "/api/v1/invoices?limit=many", "", nil, nil); r.status != http.StatusUnauthorized {
		t.Errorf("bad query without token: %d", r.status)
	}
}

func TestPaymentPage(t *testing.T) {
	e := newEnv(t)
	r := e.create(t, "k", map[string]any{"amountUsdCents": 100, "orderId": "order-7"})
	var inv Invoice
	r.decode(t, &inv)
	for _, path := range []string{"/pay/" + inv.InvoiceId.String(), "/pay/" + inv.InvoiceId.String() + "/status"} {
		// Public: no token.
		got := e.do(t, http.MethodGet, path, "", nil, nil)
		if got.status != http.StatusOK {
			t.Fatalf("%s: %d %s", path, got.status, got.body)
		}
		var p PaymentPage
		got.decode(t, &p)
		if p.InvoiceId != inv.InvoiceId || p.Address != inv.Address || p.Amount != inv.Amount || p.OrderId != "order-7" ||
			p.Status != "NEW" || !p.ExpiresAt.Equal(inv.ExpiresAt) {
			t.Errorf("%s: %+v", path, p)
		}
		// No merchant data except the order id.
		if bytes.Contains(got.body, []byte(e.merchant.String())) || bytes.Contains(got.body, []byte("rate")) {
			t.Errorf("%s leaks merchant data: %s", path, got.body)
		}
		for _, missing := range []string{"/pay/" + uuid.NewString(), "/pay/not-a-uuid"} {
			if strings.HasSuffix(path, "/status") {
				missing += "/status"
			}
			if r := e.do(t, http.MethodGet, missing, "", nil, nil); r.status != http.StatusNotFound || errorCode(t, r).Code != codeNotFound {
				t.Errorf("%s: %d %s", missing, r.status, r.body)
			}
		}
	}
	// The polling endpoint is served from the status cache.
	e.store.SetStatus(inv.InvoiceId, invoice.StatusDetected)
	got := e.do(t, http.MethodGet, "/pay/"+inv.InvoiceId.String()+"/status", "", nil, nil)
	if !bytes.Contains(got.body, []byte(`"status":"NEW"`)) {
		t.Errorf("status not from the cache: %s", got.body)
	}
	e.cache.Flush()
	got = e.do(t, http.MethodGet, "/pay/"+inv.InvoiceId.String()+"/status", "", nil, nil)
	if !bytes.Contains(got.body, []byte(`"status":"DETECTED"`)) {
		t.Errorf("miss not read from the store: %s", got.body)
	}
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t)
	valid := map[string]any{"sub": e.merchant.String(), "iss": issuer, "exp": t0.Add(time.Minute).Unix()}
	with := func(k string, v any) map[string]any {
		c := map[string]any{}
		for kk, vv := range valid {
			c[kk] = vv
		}
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
		return c
	}
	header := segment(t, map[string]string{"alg": "ES256", "kid": testKID})
	cases := map[string]string{
		"expired":        token(t, with("exp", t0.Unix())),
		"no exp":         token(t, with("exp", nil)),
		"other issuer":   token(t, with("iss", "someone")),
		"no issuer":      token(t, with("iss", nil)),
		"bad subject":    token(t, with("sub", "merchant-1")),
		"nil subject":    token(t, with("sub", uuid.Nil.String())),
		"alg none":       segment(t, map[string]string{"alg": "none"}) + "." + segment(t, valid) + ".",
		"no alg":         segment(t, map[string]string{"typ": "JWT"}) + "." + segment(t, valid) + ".c2ln",
		"no signature":   header + "." + segment(t, valid) + ".",
		"short sig":      header + "." + segment(t, valid) + ".c2ln",
		"two parts":      header + "." + segment(t, valid),
		"bad header":     "!!." + segment(t, valid) + ".c2ln",
		"bad claims":     header + ".!!.c2ln",
		"claims not obj": header + "." + base64.RawURLEncoding.EncodeToString([]byte(`[1]`)) + ".c2ln",
	}
	for name, tok := range cases {
		r := e.do(t, http.MethodGet, "/api/v1/invoices", tok, nil, nil)
		if r.status != http.StatusUnauthorized {
			t.Errorf("%s: %d", name, r.status)
		}
	}
	for name, h := range map[string]string{"basic": "Basic " + e.token, "empty bearer": "Bearer  ", "no scheme": e.token} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/invoices", nil)
		req.Header.Set("Authorization", h)
		rec := httptest.NewRecorder()
		e.mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
	if r := e.do(t, http.MethodGet, "/api/v1/invoices", token(t, valid), nil, nil); r.status != http.StatusOK {
		t.Errorf("valid token: %d", r.status)
	}
	// Tokens never reach the logs.
	if strings.Contains(e.logs.String(), e.token) || strings.Contains(e.logs.String(), segment(t, valid)) {
		t.Error("a token was logged")
	}

	// Without a configured issuer any iss is accepted.
	if _, err := merchantFromToken(context.Background(), token(t, with("iss", "other")), "", t0, testKeys); err != nil {
		t.Errorf("no issuer configured: %v", err)
	}
}

func TestErrorHelpers(t *testing.T) {
	g := &gate{log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	rec := httptest.NewRecorder()
	g.requestError(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil), errors.New("decode"))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("request error: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	g.internalError(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil), errors.New("db down"))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), codeInternal) {
		t.Errorf("internal error: %d %s", rec.Code, rec.Body)
	}
	if _, err := assetBody(invoice.Asset{Kind: "BTC"}); err == nil {
		t.Error("unknown asset kind")
	}
	a, err := assetBody(invoice.Asset{Kind: invoice.KindERC20, Contract: "0x" + strings.Repeat("a", 40), Decimals: 6})
	if err != nil {
		t.Fatal(err)
	}
	if erc, err := a.AsAsset1(); err != nil || erc.Decimals != 6 || erc.Kind != ERC20 {
		t.Errorf("erc20 %+v %v", erc, err)
	}
	if retrySeconds(0) != 1 || retrySeconds(1100*time.Millisecond) != 2 {
		t.Error("retrySeconds")
	}
	if paramName(errors.New("other")) != "query" {
		t.Error("paramName default")
	}
}

// brokenStore fails every read, for the 500 path of the handlers.
type brokenStore struct{ *invoice.MemStore }

var errDown = errors.New("postgres down")

func (brokenStore) FindByKey(context.Context, uuid.UUID, string) (invoice.Invoice, []byte, error) {
	return invoice.Invoice{}, nil, errDown
}
func (brokenStore) Get(context.Context, uuid.UUID, uuid.UUID) (invoice.Invoice, error) {
	return invoice.Invoice{}, errDown
}
func (brokenStore) GetPublic(context.Context, uuid.UUID) (invoice.Invoice, error) {
	return invoice.Invoice{}, errDown
}
func (brokenStore) List(context.Context, uuid.UUID, invoice.Filter) ([]invoice.Invoice, error) {
	return nil, errDown
}

func TestInternalErrors(t *testing.T) {
	svc, err := invoice.NewService(brokenStore{invoice.NewMemStore()}, invoicetest.NewRates("1", t0), nil,
		invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	logs := &bytes.Buffer{}
	if err := Register(mux, Options{Service: svc, JWTIssuer: issuer, Keys: testKeys, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
		Now: func() time.Time { return t0 }}); err != nil {
		t.Fatal(err)
	}
	tok := token(t, map[string]any{"sub": uuid.NewString(), "iss": issuer, "exp": t0.Add(time.Hour).Unix()})
	id := uuid.NewString()
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/invoices", `{"amountUsdCents":1,"orderId":"o"}`},
		{http.MethodGet, "/api/v1/invoices", ""},
		{http.MethodGet, "/api/v1/invoices/" + id, ""},
		{http.MethodGet, "/pay/" + id, ""},
		{http.MethodGet, "/pay/" + id + "/status", ""},
	} {
		req := httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Idempotency-Key", "k")
		if tc.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s %s: %d %s", tc.method, tc.path, rec.Code, rec.Body)
		}
	}
	if !strings.Contains(logs.String(), "postgres down") || strings.Contains(logs.String(), tok) {
		t.Errorf("logs: %s", logs)
	}
}

func TestContractCoverage(t *testing.T) {
	TestCreateInvoice(t)
	TestGetInvoice(t)
	TestListInvoices(t)
	TestPaymentPage(t)
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
