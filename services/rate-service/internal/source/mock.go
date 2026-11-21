// Package source fetches quotes from the rate sources. The mock mode reads rate-mock
// (contracts/openapi/rate-mock.yaml); the exchanges mode reads the public APIs of Coinbase and OKX.
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

// Fetcher returns the latest quote of a pair from a source.
type Fetcher interface {
	Quote(ctx context.Context, src rate.Source, pair rate.Pair) (rate.Quote, error)
}

// ErrBadQuote marks an answer of a source that is not a usable quote.
var ErrBadQuote = errors.New("source: bad quote")

// maxBodyBytes limits a quote body; a valid one is about 100 bytes.
const maxBodyBytes = 16 << 10

const tracerName = "github.com/m5tshift/weigate/services/rate-service/internal/source"

// Mock reads quotes from rate-mock: GET {base}/sources/{source}/quotes/{pair}.
type Mock struct {
	base   string
	client *http.Client
}

// NewMock returns a client of rate-mock at baseURL. timeout bounds one request.
func NewMock(baseURL string, timeout time.Duration) (*Mock, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("source: invalid rate-mock URL %q", baseURL)
	}
	return &Mock{base: strings.TrimSuffix(baseURL, "/"), client: &http.Client{Timeout: timeout}}, nil
}

type quoteBody struct {
	Source   string    `json:"source"`
	Pair     string    `json:"pair"`
	Price    string    `json:"price"`
	QuotedAt time.Time `json:"quotedAt"`
}

// Quote implements Fetcher. Any answer other than a valid 200 quote of the requested source and pair is
// an error, which the Circuit Breaker of the source counts.
func (m *Mock) Quote(ctx context.Context, src rate.Source, pair rate.Pair) (q rate.Quote, err error) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "GET /sources/{source}/quotes/{pair}",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("rate.source", string(src)), attribute.String("rate.pair", string(pair))))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "quote failed")
		}
		span.End()
	}()

	u := m.base + "/sources/" + url.PathEscape(string(src)) + "/quotes/" + url.PathEscape(string(pair))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return rate.Quote{}, fmt.Errorf("source: %s %s: %w", src, pair, err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := m.client.Do(req)
	if err != nil {
		return rate.Quote{}, fmt.Errorf("source: %s %s: %w", src, pair, err)
	}
	defer func() { _ = resp.Body.Close() }()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		return rate.Quote{}, fmt.Errorf("source: %s %s: status %d", src, pair, resp.StatusCode)
	}
	var body quoteBody
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&body); err != nil {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: %w", ErrBadQuote, src, pair, err)
	}
	return parse(body, src, pair)
}

func parse(b quoteBody, src rate.Source, pair rate.Pair) (rate.Quote, error) {
	if rate.Source(b.Source) != src || rate.Pair(b.Pair) != pair {
		return rate.Quote{}, fmt.Errorf("%w: asked %s %s, got %s %s", ErrBadQuote, src, pair, b.Source, b.Pair)
	}
	if b.QuotedAt.IsZero() {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: no quotedAt", ErrBadQuote, src, pair)
	}
	p, err := rate.ParsePrice(b.Price)
	if err != nil {
		return rate.Quote{}, fmt.Errorf("%w: %s %s: %w", ErrBadQuote, src, pair, err)
	}
	return rate.Quote{Source: src, Pair: pair, Price: p, QuotedAt: b.QuotedAt}, nil
}
