package rates

import (
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
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

// contractRouter loads contracts/openapi/rate-service.yaml: the requests of the client and the answers the
// tests feed it are checked against the contract of rate-service.
func contractRouter(t *testing.T) routers.Router {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../../contracts/openapi/rate-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	r, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type answer struct {
	status int
	header map[string]string
	body   string
}

// server answers every request with a, after checking the request and the answer against the contract.
func server(t *testing.T, a answer) *httptest.Server {
	t.Helper()
	router := contractRouter(t)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, params, err := router.FindRoute(r)
		if err != nil {
			t.Errorf("request not in the contract: %s %s: %v", r.Method, r.URL, err)
			http.Error(w, "", http.StatusNotFound)
			return
		}
		in := &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route}
		if err := openapi3filter.ValidateRequest(r.Context(), in); err != nil {
			t.Errorf("request does not match the contract: %v", err)
		}
		if route.Operation.OperationID != "getRate" {
			t.Errorf("operation %s", route.Operation.OperationID)
		}
		h := http.Header{"Content-Type": {"application/json"}}
		for k, v := range a.header {
			h.Set(k, v)
		}
		if err := openapi3filter.ValidateResponse(r.Context(), &openapi3filter.ResponseValidationInput{
			RequestValidationInput: in, Status: a.status, Header: h, Body: io.NopCloser(strings.NewReader(a.body)),
		}); err != nil {
			t.Errorf("fixture answer does not match the contract: %v", err)
		}
		for k, v := range h {
			w.Header()[k] = v
		}
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	}))
}

func TestRateOK(t *testing.T) {
	srv := server(t, answer{status: 200, body: `{"pair":"ETH-USD","price":"3694.25","quotedAt":"2026-11-28T12:00:00Z","ageSeconds":3,"sources":["coinbase","okx"]}`})
	defer srv.Close()
	c, err := New(srv.URL+"/", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	q, err := c.Rate(context.Background(), "ETH-USD")
	if err != nil {
		t.Fatal(err)
	}
	if q.Price != "3694.25" || !q.QuotedAt.Equal(time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)) || q.Age != 3*time.Second {
		t.Errorf("quote %+v", q)
	}
}

func TestRateErrors(t *testing.T) {
	cases := map[string]struct {
		a      answer
		reason string
		retry  time.Duration
	}{
		"stale": {answer{503, map[string]string{"Retry-After": "7"}, `{"code":"rate_unavailable","message":"Exchange rate is unavailable","reason":"stale"}`},
			invoice.ReasonStale, 7 * time.Second},
		"diverged": {answer{503, map[string]string{"Retry-After": "5"}, `{"code":"rate_unavailable","message":"Exchange rate is unavailable","reason":"diverged"}`},
			invoice.ReasonDiverged, 5 * time.Second},
		"no quote": {answer{503, map[string]string{"Retry-After": "2"}, `{"code":"rate_unavailable","message":"Exchange rate is unavailable","reason":"unavailable"}`},
			invoice.ReasonUnavailable, 2 * time.Second},
		"fault injected": {answer{503, map[string]string{"Retry-After": "1"}, `{"code":"fault_injected","message":"Fault injected"}`},
			invoice.ReasonUnavailable, time.Second},
		"unknown pair": {answer{404, nil, `{"code":"unknown_pair","message":"Pair is not supported"}`}, invoice.ReasonUnavailable, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := server(t, tc.a)
			defer srv.Close()
			c, err := New(srv.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Rate(context.Background(), "ETH-USD")
			var re *invoice.RateError
			if !errors.As(err, &re) || re.Reason != tc.reason || re.RetryAfter != tc.retry {
				t.Errorf("%v (%+v), want %s retry %s", err, re, tc.reason, tc.retry)
			}
		})
	}
}

// raw answers without the contract check: broken answers that rate-service must never send.
func raw(status int, header, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if header != "" {
			k, v, _ := strings.Cut(header, ": ")
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestRateBrokenAnswers(t *testing.T) {
	cases := map[string]*httptest.Server{
		"not json":         raw(200, "", `nope`),
		"no price":         raw(200, "", `{"pair":"ETH-USD","quotedAt":"2026-11-28T12:00:00Z","ageSeconds":3}`),
		"negative age":     raw(200, "", `{"pair":"ETH-USD","price":"1","quotedAt":"2026-11-28T12:00:00Z","ageSeconds":-1}`),
		"500":              raw(500, "", ``),
		"503 bad header":   raw(503, "Retry-After: soon", `{}`),
		"503 unknown code": raw(503, "", `{"code":"rate_unavailable","reason":"weird"}`),
	}
	for name, srv := range cases {
		t.Run(name, func(t *testing.T) {
			defer srv.Close()
			c, err := New(srv.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Rate(context.Background(), "ETH-USD")
			var re *invoice.RateError
			if !errors.As(err, &re) || re.Reason != invoice.ReasonUnavailable || re.RetryAfter != 0 {
				t.Errorf("%v, want unavailable without Retry-After", err)
			}
		})
	}
}

func TestRateTransport(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	c, err := New(slow.URL, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var re *invoice.RateError
	if _, err := c.Rate(context.Background(), "ETH-USD"); !errors.As(err, &re) || re.Reason != invoice.ReasonUnavailable {
		t.Errorf("timeout: %v", err)
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	c, err = New(closed.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Rate(context.Background(), "ETH-USD"); !errors.As(err, &re) || re.Reason != invoice.ReasonUnavailable {
		t.Errorf("connection refused: %v", err)
	}
	if _, err := c.Rate(context.Background(), "bad pair\x7f"); !errors.As(err, &re) {
		t.Errorf("bad request: %v", err)
	}
}

func TestTraceContextPropagated(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c, err := New(srv.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prev)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	_, _ = c.Rate(trace.ContextWithRemoteSpanContext(context.Background(), sc), "ETH-USD")
	if got.Get("Accept") != "application/json" || !strings.Contains(got.Get("Traceparent"), sc.TraceID().String()) {
		t.Errorf("headers %v", got)
	}
}

func TestNewInvalidURL(t *testing.T) {
	for _, u := range []string{"", "rate-service", "ftp://rate-service", "http://", ":%"} {
		if _, err := New(u, time.Second); err == nil {
			t.Errorf("%q accepted", u)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	for in, want := range map[string]time.Duration{"5": 5 * time.Second, " 2 ": 2 * time.Second, "0": 0, "-1": 0, "": 0, "x": 0} {
		if got := retryAfter(in); got != want {
			t.Errorf("%q: %s, want %s", in, got, want)
		}
	}
}
