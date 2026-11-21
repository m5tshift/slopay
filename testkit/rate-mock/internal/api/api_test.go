package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/m5tshift/weigate/testkit/rate-mock/internal/market"
)

const contractFile = "../../../../contracts/openapi/rate-mock.yaml"

// do sends a request and checks the response against the OpenAPI contract.
func do(t *testing.T, mux http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(contractFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s is not in the contract: %v", method, path, err)
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 rec.Code,
		Header:                 rec.Header(),
	}
	in.SetBodyBytes(rec.Body.Bytes())
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Fatalf("%s %s -> %d violates the contract: %v\n%s", method, path, rec.Code, err, rec.Body.String())
	}
	return rec
}

func newMux(t *testing.T) (*http.ServeMux, *market.Market) {
	t.Helper()
	m, err := market.New(market.Config{
		ETHUSD: 3694_250000, USDTUSD: market.Scale, StepBps: 2, BandBps: 10,
		Now:  func() time.Time { return time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC) },
		Intn: func(int) int { return 2 },
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Register(mux, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mux, m
}

func TestQuotes(t *testing.T) {
	mux, _ := newMux(t)
	for path, want := range map[string]string{
		"/sources/coinbase/quotes/ETH-USD":  `"price":"3694.250000"`,
		"/sources/coinbase/quotes/USDT-USD": `"price":"1.000000"`,
		"/sources/okx/quotes/ETH-USDT":      `"source":"okx"`,
	} {
		rec := do(t, mux, http.MethodGet, path, "")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) ||
			!strings.Contains(rec.Body.String(), `"quotedAt":"2026-11-28T12:00:00Z"`) {
			t.Errorf("%s: %d %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestQuoteNotFound(t *testing.T) {
	mux, _ := newMux(t)
	for _, path := range []string{"/sources/okx/quotes/ETH-USD", "/sources/coinbase/quotes/ETH-USDT"} {
		if rec := do(t, mux, http.MethodGet, path, ""); rec.Code != http.StatusNotFound {
			t.Errorf("%s: %d", path, rec.Code)
		}
	}
	// An unknown source is outside the contract enum, so the response is checked by status only.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/sources/kraken/quotes/ETH-USD", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown source: %d", rec.Code)
	}
}

func TestDisableEnable(t *testing.T) {
	mux, _ := newMux(t)
	rec := do(t, mux, http.MethodPost, "/control/sources/okx/disable", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"okx":{"enabled":false}`) {
		t.Fatalf("disable: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodGet, "/sources/okx/quotes/ETH-USDT", ""); rec.Code != http.StatusServiceUnavailable ||
		!strings.Contains(rec.Body.String(), "source_disabled") {
		t.Fatalf("quote of disabled source: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodGet, "/sources/coinbase/quotes/ETH-USD", ""); rec.Code != http.StatusOK {
		t.Fatalf("coinbase: %d", rec.Code)
	}
	rec = do(t, mux, http.MethodPost, "/control/sources/okx/enable", "")
	if !strings.Contains(rec.Body.String(), `"okx":{"enabled":true}`) {
		t.Fatalf("enable: %s", rec.Body.String())
	}
	if rec := do(t, mux, http.MethodGet, "/sources/okx/quotes/ETH-USDT", ""); rec.Code != http.StatusOK {
		t.Fatalf("quote after enable: %d", rec.Code)
	}
}

func TestUnknownSourceCommand(t *testing.T) {
	mux, _ := newMux(t)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/control/sources/kraken/disable", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("%d", rec.Code)
	}
}

func TestDivergenceAndReset(t *testing.T) {
	mux, m := newMux(t)
	rec := do(t, mux, http.MethodPut, "/control/divergence", `{"bps":150}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"divergenceBps":150`) {
		t.Fatalf("divergence: %d %s", rec.Code, rec.Body.String())
	}
	m.Tick()
	cb, _ := m.Quote(market.Coinbase, market.ETHUSD)
	okx, _ := m.Quote(market.OKX, market.ETHUSDT)
	if (okx.Price-cb.Price)*10000/cb.Price < 100 {
		t.Fatalf("sources do not diverge: %d vs %d", cb.Price, okx.Price)
	}
	if rec := do(t, mux, http.MethodGet, "/control", ""); !strings.Contains(rec.Body.String(), `"divergenceBps":150`) {
		t.Fatalf("state: %s", rec.Body.String())
	}
	do(t, mux, http.MethodPost, "/control/sources/coinbase/disable", "")
	rec = do(t, mux, http.MethodPost, "/control/reset", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"divergenceBps":0`) ||
		strings.Contains(rec.Body.String(), "false") {
		t.Fatalf("reset: %d %s", rec.Code, rec.Body.String())
	}
}

func TestDivergenceInvalid(t *testing.T) {
	mux, m := newMux(t)
	for _, body := range []string{``, `{}`, `{"bps":"x"}`, `{"bps":1,"x":2}`, `{"bps":5001}`, `{"bps":-5001}`, `not json`} {
		rec := do(t, mux, http.MethodPut, "/control/divergence", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%q: %d", body, rec.Code)
		}
	}
	if m.State().DivergenceBps != 0 {
		t.Fatal("invalid command changed the state")
	}
}

func TestOversizedBody(t *testing.T) {
	mux, _ := newMux(t)
	body := `{"bps":1,"pad":"` + string(bytes.Repeat([]byte("a"), 2<<10)) + `"}`
	if rec := do(t, mux, http.MethodPut, "/control/divergence", body); rec.Code != http.StatusBadRequest {
		t.Fatalf("%d", rec.Code)
	}
}
