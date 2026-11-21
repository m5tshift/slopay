package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

// Public APIs of the exchanges (SRS 4.1.3). No keys are used.
const (
	// DefaultCoinbaseURL serves GET /products/{pair}/ticker.
	DefaultCoinbaseURL = "https://api.exchange.coinbase.com"
	// DefaultOKXURL serves GET /api/v5/market/ticker?instId={pair}.
	DefaultOKXURL = "https://www.okx.com"
)

// Exchanges reads the public APIs of the exchanges: Coinbase quotes ETH-USD and USDT-USD, OKX quotes
// ETH-USDT. A region block (403, 451) is an error like any other: it is never bypassed, there is no proxy
// and no VPN, and the operator switches the stand to the mock mode (SRS 4.1.3, risks).
type Exchanges struct {
	coinbase string
	okx      string
	client   *http.Client
}

// NewExchanges returns a client of the exchanges at the given base URLs. timeout bounds one request.
func NewExchanges(coinbaseURL, okxURL string, timeout time.Duration) (*Exchanges, error) {
	for name, raw := range map[string]string{"Coinbase": coinbaseURL, "OKX": okxURL} {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("source: invalid %s URL %q", name, raw)
		}
	}
	// Proxy is nil on purpose: HTTP(S)_PROXY of the environment is not honoured (no proxies, SRS 4.1.3).
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	return &Exchanges{
		coinbase: strings.TrimSuffix(coinbaseURL, "/"),
		okx:      strings.TrimSuffix(okxURL, "/"),
		client:   &http.Client{Timeout: timeout, Transport: tr},
	}, nil
}

// Quote implements Fetcher. A pair the source does not quote, a status other than 200 and a body that is
// not a usable quote are errors, which the Circuit Breaker of the source counts.
func (e *Exchanges) Quote(ctx context.Context, src rate.Source, pair rate.Pair) (q rate.Quote, err error) {
	var u string
	switch {
	case src == rate.Coinbase && (pair == rate.ETHUSD || pair == rate.USDTUSD):
		u = e.coinbase + "/products/" + url.PathEscape(string(pair)) + "/ticker"
	case src == rate.OKX && pair == rate.ETHUSDT:
		u = e.okx + "/api/v5/market/ticker?instId=" + url.QueryEscape(string(pair))
	default:
		return rate.Quote{}, fmt.Errorf("%w: %s does not quote %s", ErrBadQuote, src, pair)
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "GET "+string(src)+" ticker",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("rate.source", string(src)), attribute.String("rate.pair", string(pair))))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "quote failed")
		}
		span.End()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return rate.Quote{}, fmt.Errorf("source: %s %s: %w", src, pair, err)
	}
	req.Header.Set("Accept", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := e.client.Do(req)
	if err != nil {
		return rate.Quote{}, fmt.Errorf("source: %s %s: %w", src, pair, err)
	}
	defer func() { _ = resp.Body.Close() }()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		return rate.Quote{}, fmt.Errorf("source: %s %s: status %d", src, pair, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return rate.Quote{}, fmt.Errorf("source: %s %s: %w", src, pair, err)
	}
	if src == rate.Coinbase {
		return parseCoinbase(body, pair)
	}
	return parseOKX(body, pair)
}

// parseCoinbase reads the ticker of Coinbase Exchange: {"price":"3694.25","time":"2026-11-21T10:00:00.123456Z",...}.
func parseCoinbase(body []byte, pair rate.Pair) (rate.Quote, error) {
	var b struct {
		Price string    `json:"price"`
		Time  time.Time `json:"time"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: %w", ErrBadQuote, rate.Coinbase, pair, err)
	}
	return buildQuote(rate.Coinbase, pair, b.Price, b.Time)
}

// parseOKX reads the ticker of OKX: {"code":"0","data":[{"instId":"ETH-USDT","last":"3690.1","ts":"1763719200123"}]}.
func parseOKX(body []byte, pair rate.Pair) (rate.Quote, error) {
	var b struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID string `json:"instId"`
			Last   string `json:"last"`
			TS     string `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: %w", ErrBadQuote, rate.OKX, pair, err)
	}
	if b.Code != "0" {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: code %q", ErrBadQuote, rate.OKX, pair, b.Code)
	}
	if len(b.Data) != 1 || b.Data[0].InstID != string(pair) {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: unexpected data", ErrBadQuote, rate.OKX, pair)
	}
	ms, err := strconv.ParseInt(b.Data[0].TS, 10, 64)
	if err != nil || ms <= 0 {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: bad ts %q", ErrBadQuote, rate.OKX, pair, b.Data[0].TS)
	}
	return buildQuote(rate.OKX, pair, b.Data[0].Last, time.UnixMilli(ms).UTC())
}

func buildQuote(src rate.Source, pair rate.Pair, price string, at time.Time) (rate.Quote, error) {
	if at.IsZero() {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: no time", ErrBadQuote, src, pair)
	}
	p, err := rate.ParsePrice(price)
	if err != nil {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: %w", ErrBadQuote, src, pair, err)
	}
	return rate.Quote{Source: src, Pair: pair, Price: p, QuotedAt: at}, nil
}
