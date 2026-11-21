package source

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

// Recorded answers of the public APIs (values are rounded test data).
const (
	coinbaseETH  = `{"trade_id":812345678,"price":"3694.25","size":"0.0152","time":"2026-11-21T10:00:00.123456Z","bid":"3694.24","ask":"3694.25","volume":"152345.123"}`
	coinbaseUSDT = `{"trade_id":99887766,"price":"0.99985000","size":"1500","time":"2026-11-21T10:00:01.5Z","bid":"0.9998","ask":"0.9999","volume":"98765432.1"}`
	okxETH       = `{"code":"0","msg":"","data":[{"instType":"SPOT","instId":"ETH-USDT","last":"3695.12","lastSz":"0.3","askPx":"3695.13","bidPx":"3695.12","ts":"1763719201234"}]}`
)

type exchangeServer struct {
	*httptest.Server
	paths []string
	heads []http.Header
}

func newExchanges(t *testing.T, answers map[string]canned) (*Exchanges, *exchangeServer) {
	t.Helper()
	es := &exchangeServer{}
	es.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		es.paths = append(es.paths, r.URL.RequestURI())
		es.heads = append(es.heads, r.Header.Clone())
		a, ok := answers[r.URL.RequestURI()]
		if !ok {
			a = canned{http.StatusNotFound, `{"message":"NotFound"}`}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	}))
	t.Cleanup(es.Close)
	e, err := NewExchanges(es.URL, es.URL+"/", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return e, es
}

func TestExchangesQuotes(t *testing.T) {
	e, es := newExchanges(t, map[string]canned{
		"/products/ETH-USD/ticker":              {200, coinbaseETH},
		"/products/USDT-USD/ticker":             {200, coinbaseUSDT},
		"/api/v5/market/ticker?instId=ETH-USDT": {200, okxETH},
	})
	ctx := withSpan(t)
	for _, tc := range []struct {
		src   rate.Source
		pair  rate.Pair
		price string
		at    time.Time
	}{
		{rate.Coinbase, rate.ETHUSD, "3694.25", time.Date(2026, 11, 21, 10, 0, 0, 123456000, time.UTC)},
		{rate.Coinbase, rate.USDTUSD, "0.99985", time.Date(2026, 11, 21, 10, 0, 1, 500000000, time.UTC)},
		{rate.OKX, rate.ETHUSDT, "3695.12", time.UnixMilli(1763719201234).UTC()},
	} {
		q, err := e.Quote(ctx, tc.src, tc.pair)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.src, tc.pair, err)
		}
		if q.Source != tc.src || q.Pair != tc.pair || q.Price.Cmp(rate.MustPrice(tc.price)) != 0 || !q.QuotedAt.Equal(tc.at) {
			t.Errorf("%s %s: got %+v", tc.src, tc.pair, q)
		}
	}
	if len(es.paths) != 3 {
		t.Fatalf("requests = %v", es.paths)
	}
	for _, h := range es.heads {
		if h.Get("Traceparent") == "" {
			t.Error("trace context is not propagated")
		}
	}
}

func TestExchangesErrors(t *testing.T) {
	const eth = "/products/ETH-USD/ticker"
	const okx = "/api/v5/market/ticker?instId=ETH-USDT"
	cases := map[string]struct {
		path    string
		src     rate.Source
		pair    rate.Pair
		answer  canned
		badBody bool
	}{
		"coinbase region block 403": {eth, rate.Coinbase, rate.ETHUSD, canned{403, `{"message":"Forbidden"}`}, false},
		"okx region block 451":      {okx, rate.OKX, rate.ETHUSDT, canned{451, `{}`}, false},
		"coinbase 429":              {eth, rate.Coinbase, rate.ETHUSD, canned{429, `{"message":"Too many"}`}, false},
		"okx 500":                   {okx, rate.OKX, rate.ETHUSDT, canned{500, ``}, false},
		"coinbase not json":         {eth, rate.Coinbase, rate.ETHUSD, canned{200, `<html>`}, true},
		"coinbase no price":         {eth, rate.Coinbase, rate.ETHUSD, canned{200, `{"time":"2026-11-21T10:00:00Z"}`}, true},
		"coinbase zero price":       {eth, rate.Coinbase, rate.ETHUSD, canned{200, `{"price":"0","time":"2026-11-21T10:00:00Z"}`}, true},
		"coinbase negative price":   {eth, rate.Coinbase, rate.ETHUSD, canned{200, `{"price":"-1","time":"2026-11-21T10:00:00Z"}`}, true},
		"coinbase no time":          {eth, rate.Coinbase, rate.ETHUSD, canned{200, `{"price":"3000"}`}, true},
		"okx not json":              {okx, rate.OKX, rate.ETHUSDT, canned{200, `nope`}, true},
		"okx error code":            {okx, rate.OKX, rate.ETHUSDT, canned{200, `{"code":"51001","msg":"Instrument ID does not exist","data":[]}`}, true},
		"okx empty data":            {okx, rate.OKX, rate.ETHUSDT, canned{200, `{"code":"0","msg":"","data":[]}`}, true},
		"okx other instrument":      {okx, rate.OKX, rate.ETHUSDT, canned{200, `{"code":"0","data":[{"instId":"BTC-USDT","last":"1","ts":"1763719201234"}]}`}, true},
		"okx bad ts":                {okx, rate.OKX, rate.ETHUSDT, canned{200, `{"code":"0","data":[{"instId":"ETH-USDT","last":"3000","ts":"x"}]}`}, true},
		"okx bad price":             {okx, rate.OKX, rate.ETHUSDT, canned{200, `{"code":"0","data":[{"instId":"ETH-USDT","last":"1e3","ts":"1763719201234"}]}`}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e, _ := newExchanges(t, map[string]canned{tc.path: tc.answer})
			_, err := e.Quote(context.Background(), tc.src, tc.pair)
			if err == nil {
				t.Fatal("want error")
			}
			if tc.badBody && !errors.Is(err, ErrBadQuote) {
				t.Errorf("error %v is not ErrBadQuote", err)
			}
		})
	}
}

func TestExchangesUnknownPairNotRequested(t *testing.T) {
	e, es := newExchanges(t, nil)
	for _, tc := range []struct {
		src  rate.Source
		pair rate.Pair
	}{{rate.Coinbase, rate.ETHUSDT}, {rate.OKX, rate.ETHUSD}, {rate.OKX, rate.USDTUSD}, {"kraken", rate.ETHUSD}} {
		if _, err := e.Quote(context.Background(), tc.src, tc.pair); !errors.Is(err, ErrBadQuote) {
			t.Errorf("%s %s: error %v", tc.src, tc.pair, err)
		}
	}
	if len(es.paths) != 0 {
		t.Fatalf("requested %v", es.paths)
	}
}

func TestExchangesTimeoutAndCancel(t *testing.T) {
	slow := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-slow:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(slow); srv.Close() })
	e, err := NewExchanges(srv.URL, srv.URL, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Quote(context.Background(), rate.Coinbase, rate.ETHUSD); err == nil {
		t.Fatal("want timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Quote(ctx, rate.OKX, rate.ETHUSDT); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestExchangesIgnoreProxyEnvironment(t *testing.T) {
	var proxied bool
	proxy := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { proxied = true }))
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("http_proxy", proxy.URL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	// A host that is not loopback: the proxy of the environment would be used for it.
	e, err := NewExchanges("http://exchange.invalid", "http://exchange.invalid", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = e.Quote(context.Background(), rate.Coinbase, rate.ETHUSD)
	if proxied {
		t.Fatal("the request went through the proxy of the environment")
	}
}

func TestNewExchangesValidatesURLs(t *testing.T) {
	for _, bad := range []string{"", "ftp://x", "http://", "::"} {
		if _, err := NewExchanges(bad, DefaultOKXURL, time.Second); err == nil {
			t.Errorf("Coinbase URL %q accepted", bad)
		}
		if _, err := NewExchanges(DefaultCoinbaseURL, bad, time.Second); err == nil {
			t.Errorf("OKX URL %q accepted", bad)
		}
	}
	if _, err := NewExchanges(DefaultCoinbaseURL, DefaultOKXURL, time.Second); err != nil {
		t.Fatal(err)
	}
}
