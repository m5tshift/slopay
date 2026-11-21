package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

const contractFile = "../../../../contracts/openapi/rate-service.yaml"

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

type rates map[rate.Pair]rate.Snapshot

func (r rates) Get(_ context.Context, pair rate.Pair) (rate.Snapshot, bool) {
	s, ok := r[pair]
	return s, ok
}

type env struct {
	mux   *http.ServeMux
	rates rates
	now   time.Time
	logs  *bytes.Buffer
}

func newEnv(t *testing.T, fault bool) *env {
	t.Helper()
	e := &env{mux: http.NewServeMux(), rates: rates{}, now: t0, logs: &bytes.Buffer{}}
	Register(e.mux, Options{
		Rates: e.rates, MaxAge: 60 * time.Second, RetryAfter: 5 * time.Second, FaultInjection: fault,
		Now: func() time.Time { return e.now }, Logger: slog.New(slog.NewJSONHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
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

func (e *env) do(t *testing.T, method, path, body string) resp {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	r := resp{status: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
	validateResponse(t, method, path, body, r)
	return r
}

func validateResponse(t *testing.T, method, path, reqBody string, r resp) {
	t.Helper()
	_, router := contractRouter(t)
	req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(reqBody))
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

func snapshot(price string, quotedAt time.Time, sources ...rate.Source) rate.Snapshot {
	return rate.Snapshot{Rate: rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice(price), QuotedAt: quotedAt, Sources: sources}, UpdatedAt: quotedAt}
}

func TestGetRate(t *testing.T) {
	e := newEnv(t, false)
	e.rates[rate.ETHUSD] = snapshot("3694.25", t0.Add(-3*time.Second), rate.Coinbase, rate.OKX)
	r := e.do(t, http.MethodGet, "/rates/ETH-USD", "")
	if r.status != http.StatusOK {
		t.Fatalf("%d %s", r.status, r.body)
	}
	var got Rate
	r.decode(t, &got)
	if got.Pair != ETHUSD || got.Price != "3694.25" || got.AgeSeconds != 3 || !got.QuotedAt.Equal(t0.Add(-3*time.Second)) ||
		len(got.Sources) != 2 || got.Sources[1] != Okx {
		t.Errorf("%+v", got)
	}
	// At exactly 60 s the rate is still served.
	e.now = t0.Add(57 * time.Second)
	r = e.do(t, http.MethodGet, "/rates/ETH-USD", "")
	r.decode(t, &got)
	if r.status != http.StatusOK || got.AgeSeconds != 60 {
		t.Errorf("at 60 s: %d %s", r.status, r.body)
	}
	// A source clock ahead of ours does not give a negative age.
	e.now = t0.Add(-10 * time.Second)
	r = e.do(t, http.MethodGet, "/rates/ETH-USD", "")
	r.decode(t, &got)
	if r.status != http.StatusOK || got.AgeSeconds != 0 {
		t.Errorf("clock skew: %d %s", r.status, r.body)
	}
}

func TestGetRateUSDT(t *testing.T) {
	e := newEnv(t, false)
	s := snapshot("0.9998", t0, rate.Coinbase)
	s.Rate.Pair = rate.USDTUSD
	e.rates[rate.USDTUSD] = s
	r := e.do(t, http.MethodGet, "/rates/USDT-USD", "")
	var got Rate
	r.decode(t, &got)
	if r.status != http.StatusOK || got.Pair != USDTUSD || got.Price != "0.9998" {
		t.Errorf("%d %s", r.status, r.body)
	}
}

func TestGetRateUnknownPair(t *testing.T) {
	e := newEnv(t, false)
	for _, pair := range []string{"ETH-USDT", "BTC-USD"} {
		r := e.do(t, http.MethodGet, "/rates/"+pair, "")
		var got Error
		r.decode(t, &got)
		if r.status != http.StatusNotFound || got.Code != codeUnknownPair {
			t.Errorf("%s: %d %s", pair, r.status, r.body)
		}
	}
}

func TestGetRateUnavailable(t *testing.T) {
	cases := []struct {
		name string
		snap *rate.Snapshot
		now  time.Time
		want ErrorReason
	}{
		{"no rate", nil, t0, Unavailable},
		{"stale", new(snapshot("3700", t0)), t0.Add(61 * time.Second), Stale},
		{"diverged", &rate.Snapshot{Rate: snapshot("3700", t0).Rate, Diverged: true, UpdatedAt: t0}, t0, Diverged},
		{"diverged without rate", &rate.Snapshot{Diverged: true, UpdatedAt: t0}, t0, Diverged},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, false)
			if tc.snap != nil {
				e.rates[rate.ETHUSD] = *tc.snap
			}
			e.now = tc.now
			r := e.do(t, http.MethodGet, "/rates/ETH-USD", "")
			var got Error
			r.decode(t, &got)
			if r.status != http.StatusServiceUnavailable || got.Code != codeRateUnavailable || got.Reason == nil || *got.Reason != tc.want {
				t.Fatalf("%d %s", r.status, r.body)
			}
			if r.header.Get("Retry-After") != "5" {
				t.Errorf("Retry-After %q", r.header.Get("Retry-After"))
			}
		})
	}
}

func TestRetryAfterIsWholeSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int{1500 * time.Millisecond: 2, 0: 1, 5 * time.Second: 5} {
		s := &Server{o: Options{RetryAfter: d}}
		if got := s.unavailable(Error{}).Headers.RetryAfter; got != want {
			t.Errorf("%s: %d, want %d", d, got, want)
		}
	}
}

func TestFaultInjectionDisabled(t *testing.T) {
	e := newEnv(t, false)
	for _, rq := range []struct{ method, body string }{{http.MethodGet, ""}, {http.MethodPut, `{"enabled":true}`}, {http.MethodPut, `nonsense`}} {
		r := e.do(t, rq.method, "/test/fault", rq.body)
		var got Error
		r.decode(t, &got)
		if r.status != http.StatusNotFound || got.Code != codeNotFound {
			t.Errorf("%s %s: %d %s", rq.method, rq.body, r.status, r.body)
		}
	}
}

func TestFaultInjection(t *testing.T) {
	e := newEnv(t, true)
	e.rates[rate.ETHUSD] = snapshot("3700", t0, rate.Coinbase)
	var f Fault
	r := e.do(t, http.MethodGet, "/test/fault", "")
	r.decode(t, &f)
	if r.status != http.StatusOK || f.Enabled {
		t.Fatalf("initial: %d %s", r.status, r.body)
	}

	for range 2 { // repeating the command changes nothing
		r = e.do(t, http.MethodPut, "/test/fault", `{"enabled":true}`)
		r.decode(t, &f)
		if r.status != http.StatusOK || !f.Enabled {
			t.Fatalf("on: %d %s", r.status, r.body)
		}
	}
	if strings.Count(e.logs.String(), "fault injection changed") != 1 {
		t.Errorf("logs: %s", e.logs.String())
	}
	for _, path := range []string{"/rates/ETH-USD", "/rates/USDT-USD"} {
		r = e.do(t, http.MethodGet, path, "")
		var got Error
		r.decode(t, &got)
		if r.status != http.StatusServiceUnavailable || got.Code != codeFaultInjected || got.Reason != nil || r.header.Get("Retry-After") == "" {
			t.Errorf("%s with fault: %d %s", path, r.status, r.body)
		}
	}
	r = e.do(t, http.MethodGet, "/test/fault", "")
	r.decode(t, &f)
	if !f.Enabled {
		t.Error("flag not reported")
	}

	r = e.do(t, http.MethodPut, "/test/fault", `{"enabled":false}`)
	if r.status != http.StatusOK {
		t.Fatalf("off: %d %s", r.status, r.body)
	}
	if r = e.do(t, http.MethodGet, "/rates/ETH-USD", ""); r.status != http.StatusOK {
		t.Errorf("after off: %d %s", r.status, r.body)
	}
}

func TestSetFaultInvalidBody(t *testing.T) {
	e := newEnv(t, true)
	for _, body := range []string{``, `{}`, `{"enabled":"yes"}`, `not json`, `{"enabled":true,"x":"` + strings.Repeat("a", 2<<10) + `"}`} {
		r := e.do(t, http.MethodPut, "/test/fault", body)
		var got Error
		r.decode(t, &got)
		if r.status != http.StatusBadRequest || got.Code != codeInvalidRequest {
			t.Errorf("%.20q: %d %s", body, r.status, r.body)
		}
	}
	r := e.do(t, http.MethodGet, "/test/fault", "")
	var f Fault
	r.decode(t, &f)
	if f.Enabled {
		t.Error("an invalid body changed the flag")
	}
}

// TestInternalError checks the answer to a bug of the server; 500 is not part of the contract.
func TestInternalError(t *testing.T) {
	var logs bytes.Buffer
	s := &Server{log: slog.New(slog.NewJSONHandler(&logs, nil))}
	rec := httptest.NewRecorder()
	s.internalError(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/rates/ETH-USD", nil), errors.New("boom"))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(logs.String(), "boom") {
		t.Errorf("%d %s", rec.Code, logs.String())
	}
}

func TestRegisterDefaults(t *testing.T) {
	s := Register(http.NewServeMux(), Options{Rates: rates{}})
	if s.o.Now == nil || s.log == nil {
		t.Error("defaults not set")
	}
}

// TestContractCoverage runs last in this file and checks that every operation of the contract was exercised
// and its responses validated.
func TestContractCoverage(t *testing.T) {
	TestGetRate(t)
	TestFaultInjection(t)
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
