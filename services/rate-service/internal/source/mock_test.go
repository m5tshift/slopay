package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

const rateMockContract = "../../../../contracts/openapi/rate-mock.yaml"

type canned struct {
	status int
	body   string
}

// fake serves canned answers by path and checks that each one matches the rate-mock contract, so the
// client is tested against what rate-mock is allowed to answer.
func fake(t *testing.T, answers map[string]canned) *httptest.Server {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(rateMockContract)
	if err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, ok := answers[r.URL.Path]
		if !ok {
			a = canned{http.StatusNotFound, `{"code":"not_found","message":"Source does not quote the pair"}`}
		}
		if a.status == 0 { // a broken answer outside the contract
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(a.body))
			return
		}
		req := httptest.NewRequestWithContext(context.Background(), r.Method, r.URL.Path, nil)
		route, params, err := router.FindRoute(req)
		if err != nil {
			t.Errorf("%s is not in the rate-mock contract: %v", r.URL.Path, err)
		} else {
			hdr := http.Header{"Content-Type": {"application/json"}}
			err = openapi3filter.ValidateResponse(context.Background(), &openapi3filter.ResponseValidationInput{
				RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
				Status:                 a.status, Header: hdr, Body: io.NopCloser(bytes.NewReader([]byte(a.body))),
				Options: &openapi3filter.Options{IncludeResponseStatus: true},
			})
			if err != nil {
				t.Errorf("canned %s %d does not match the rate-mock contract: %v", r.URL.Path, a.status, err)
			}
		}
		if r.Header.Get("traceparent") == "" {
			t.Error("no trace context in the request")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	}))
}

func TestMockQuote(t *testing.T) {
	srv := fake(t, map[string]canned{
		"/sources/coinbase/quotes/ETH-USD":  {200, `{"source":"coinbase","pair":"ETH-USD","price":"3694.250000","quotedAt":"2026-11-28T12:00:00Z"}`},
		"/sources/coinbase/quotes/USDT-USD": {200, `{"source":"coinbase","pair":"USDT-USD","price":"0.999800","quotedAt":"2026-11-28T12:00:01Z"}`},
		"/sources/okx/quotes/ETH-USDT":      {200, `{"source":"okx","pair":"ETH-USDT","price":"3695.000000","quotedAt":"2026-11-28T12:00:02Z"}`},
		"/sources/okx/quotes/ETH-USD":       {200, `{"source":"coinbase","pair":"ETH-USD","price":"3694.250000","quotedAt":"2026-11-28T12:00:00Z"}`},
		"/sources/coinbase/quotes/ETH-USDT": {503, `{"code":"source_disabled","message":"Source is disabled"}`},
		"/sources/okx/quotes/USDT-USD":      {0, `{"source":"okx",`},
	})
	defer srv.Close()
	// The trace context is injected by the global propagator, as in the service.
	m, err := NewMock(srv.URL+"/", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := withSpan(t)

	got, err := m.Quote(ctx, rate.Coinbase, rate.ETHUSD)
	if err != nil || got.Price.String() != "3694.25" || got.Source != rate.Coinbase || got.Pair != rate.ETHUSD ||
		!got.QuotedAt.Equal(time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("coinbase ETH-USD: %+v %v", got, err)
	}
	if got, err := m.Quote(ctx, rate.OKX, rate.ETHUSDT); err != nil || got.Price.String() != "3695" {
		t.Errorf("okx ETH-USDT: %+v %v", got, err)
	}
	if _, err := m.Quote(ctx, rate.Coinbase, rate.USDTUSD); err != nil {
		t.Errorf("coinbase USDT-USD: %v", err)
	}
	if _, err := m.Quote(ctx, rate.Coinbase, rate.ETHUSDT); err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Errorf("disabled source: %v", err)
	}
	if _, err := m.Quote(ctx, rate.OKX, rate.Pair("BTC-USD")); err == nil || !strings.Contains(err.Error(), "status 404") {
		t.Errorf("unknown pair: %v", err)
	}
	if _, err := m.Quote(ctx, rate.OKX, rate.ETHUSD); !errors.Is(err, ErrBadQuote) {
		t.Errorf("answer for another source: %v", err)
	}
	if _, err := m.Quote(ctx, rate.OKX, rate.USDTUSD); !errors.Is(err, ErrBadQuote) {
		t.Errorf("broken JSON: %v", err)
	}
}

func TestParse(t *testing.T) {
	at := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	if _, err := parse(quoteBody{Source: "okx", Pair: "ETH-USDT", Price: "0.000000", QuotedAt: at}, rate.OKX, rate.ETHUSDT); !errors.Is(err, ErrBadQuote) {
		t.Errorf("zero price: %v", err)
	}
	if _, err := parse(quoteBody{Source: "okx", Pair: "ETH-USDT", Price: "1.000000"}, rate.OKX, rate.ETHUSDT); !errors.Is(err, ErrBadQuote) {
		t.Errorf("no quotedAt: %v", err)
	}
}

func TestMockErrors(t *testing.T) {
	for _, bad := range []string{"", "ftp://x", "http://", "::"} {
		if _, err := NewMock(bad, time.Second); err == nil {
			t.Errorf("NewMock(%q) accepted", bad)
		}
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	m, _ := NewMock(slow.URL, 50*time.Millisecond)
	if _, err := m.Quote(context.Background(), rate.Coinbase, rate.ETHUSD); err == nil {
		t.Error("timeout is not an error")
	}
	m, _ = NewMock("http://127.0.0.1:1", time.Second)
	if _, err := m.Quote(context.Background(), rate.Coinbase, rate.ETHUSD); err == nil {
		t.Error("connection refused is not an error")
	}
	m.base = "http://bad host"
	if _, err := m.Quote(context.Background(), rate.Coinbase, rate.ETHUSD); err == nil {
		t.Error("bad URL is not an error")
	}
}
