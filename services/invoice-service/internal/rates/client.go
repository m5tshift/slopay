// Package rates is the client of rate-service GET /rates/{pair} (contracts/openapi/rate-service.yaml). It
// is one of the two synchronous internal calls of SRS 4.1.2. Resilience is the mesh's (ADR 0003: Istio
// outlier detection, timeout 500 ms, 1 retry); the client only bounds a call with its own timeout.
package rates

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
)

// maxBody bounds the answer read from rate-service.
const maxBody = 64 << 10

// Client calls rate-service.
type Client struct {
	base *url.URL
	http *http.Client
}

var _ invoice.RateSource = (*Client)(nil)

// New returns a client of the rate-service at baseURL. timeout bounds one call.
func New(baseURL string, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("rates: invalid rate-service URL %q", baseURL)
	}
	return &Client{base: u, http: &http.Client{
		Timeout:   timeout,
		Transport: tracing{next: http.DefaultTransport},
	}}, nil
}

// rateBody is Rate of the contract.
type rateBody struct {
	Pair       string    `json:"pair"`
	Price      string    `json:"price"`
	QuotedAt   time.Time `json:"quotedAt"`
	AgeSeconds int       `json:"ageSeconds"`
}

// errorBody is Error of the contract.
type errorBody struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// Rate implements invoice.RateSource. Every failure is an *invoice.RateError: 503 keeps the reason of
// rate-service (fault_injected becomes unavailable) and its Retry-After; a transport error, a timeout or any
// other answer is unavailable.
func (c *Client) Rate(ctx context.Context, pair string) (invoice.Quote, error) {
	u := c.base.JoinPath("rates", pair)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return invoice.Quote{}, unavailable(err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return invoice.Quote{}, unavailable(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return invoice.Quote{}, unavailable(err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		var r rateBody
		if err := json.Unmarshal(body, &r); err != nil {
			return invoice.Quote{}, unavailable(fmt.Errorf("decode rate: %w", err))
		}
		if r.Price == "" || r.QuotedAt.IsZero() || r.AgeSeconds < 0 {
			return invoice.Quote{}, unavailable(errors.New("incomplete rate"))
		}
		return invoice.Quote{Price: r.Price, QuotedAt: r.QuotedAt, Age: time.Duration(r.AgeSeconds) * time.Second}, nil
	case http.StatusServiceUnavailable:
		var e errorBody
		_ = json.Unmarshal(body, &e)
		reason := invoice.ReasonUnavailable
		if e.Code == "rate_unavailable" && (e.Reason == invoice.ReasonStale || e.Reason == invoice.ReasonDiverged) {
			reason = e.Reason
		}
		return invoice.Quote{}, &invoice.RateError{
			Reason: reason, RetryAfter: retryAfter(resp.Header.Get("Retry-After")),
			Err: fmt.Errorf("rate-service answered 503 %s", e.Code),
		}
	default:
		return invoice.Quote{}, unavailable(fmt.Errorf("rate-service answered %d", resp.StatusCode))
	}
}

func unavailable(err error) error {
	return &invoice.RateError{Reason: invoice.ReasonUnavailable, Err: err}
}

// retryAfter parses Retry-After in seconds; anything else is zero (the configured default).
func retryAfter(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 1 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// tracing starts a client span for a call and passes its context in the W3C headers (SRS 4.5.1).
type tracing struct{ next http.RoundTripper }

func (t tracing) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, span := otel.Tracer("github.com/m5tshift/weigate/services/invoice-service/internal/rates").Start(
		req.Context(), "GET /rates/{pair}", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("http.request.method", req.Method), attribute.String("server.address", req.URL.Host)))
	defer span.End()
	req = req.Clone(ctx)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 500 {
		span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
	}
	return resp, nil
}
