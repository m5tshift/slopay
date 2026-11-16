package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func newTel(t *testing.T) *Telemetry {
	t.Helper()
	tel, err := New(context.Background(), Config{ServiceName: "svc", TraceSampleRatio: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	return tel
}

func metricsBody(t *testing.T, tel *Telemetry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	tel.MetricsHandler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

func TestMiddlewareRED(t *testing.T) {
	tel := newTel(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /invoices/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("POST /invoices", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	mux.HandleFunc("GET /nowrite", func(http.ResponseWriter, *http.Request) {})
	h := tel.Middleware(mux)

	for _, c := range []struct {
		method, path string
		code         int
	}{
		{"GET", "/invoices/1", 200},
		{"GET", "/invoices/2", 200},
		{"POST", "/invoices", 500},
		{"GET", "/nowrite", 200},
		{"GET", "/unknown/123", 404},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), c.method, c.path, nil))
		if rec.Code != c.code {
			t.Fatalf("%s %s = %d, want %d", c.method, c.path, rec.Code, c.code)
		}
	}

	body := metricsBody(t, tel)
	for _, want := range []string{
		`http_server_requests_total{http_request_method="GET",http_response_status_code="200",http_route="GET /invoices/{id}"`,
		`http_server_request_errors_total{http_request_method="POST",http_response_status_code="500",http_route="POST /invoices"`,
		`http_server_request_duration_seconds_count{http_request_method="GET",http_response_status_code="200",http_route="GET /invoices/{id}"`,
		`http_route="unmatched"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics lack %s\n%s", want, body)
		}
	}
	if strings.Contains(body, "/invoices/1") {
		t.Error("raw path leaked into labels")
	}
	if strings.Contains(body, `http_server_request_errors_total{http_request_method="GET"`) {
		t.Error("non-5xx counted as error")
	}
}

func TestMiddlewareTrace(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tel := newTel(t)
	tel.Tracer.RegisterSpanProcessor(sr)
	var gotTrace string
	log := NewLogger(io.Discard, slog.LevelInfo)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /x", func(w http.ResponseWriter, r *http.Request) {
		log.InfoContext(r.Context(), "in handler")
		gotTrace = traceIDOf(r.Context())
		w.WriteHeader(http.StatusBadGateway)
	})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01")
	tel.Middleware(mux).ServeHTTP(httptest.NewRecorder(), req)

	if gotTrace != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("incoming trace not continued: %q", gotTrace)
	}
	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "GET /x" || spans[0].Status().Code != 1 {
		t.Fatalf("spans: %+v", spans)
	}
}

func traceIDOf(ctx context.Context) string {
	return trace.SpanContextFromContext(ctx).TraceID().String()
}

func TestHealth(t *testing.T) {
	var logBuf strings.Builder
	h := NewHealth(slog.New(slog.NewTextHandler(&logBuf, nil)), 50*time.Millisecond)
	tel := newTel(t)
	mux := http.NewServeMux()
	Register(mux, h, tel)

	get := func(path string) (int, string) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		return rec.Code, rec.Body.String()
	}

	if c, b := get("/health/live"); c != 200 || !strings.Contains(b, "ok") {
		t.Errorf("live: %d %s", c, b)
	}
	if c, b := get("/health/ready"); c != 200 || !strings.Contains(b, `"status":"ok"`) {
		t.Errorf("ready without checks: %d %s", c, b)
	}

	h.AddReadiness("db", func(context.Context) error { return nil })
	h.AddReadiness("kafka", func(context.Context) error { return errors.New("broker down password=pw") })
	h.AddReadiness("slow", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	c, b := get("/health/ready")
	if c != 503 || !strings.Contains(b, `"db":"ok"`) || !strings.Contains(b, `"kafka":"fail"`) || !strings.Contains(b, `"slow":"fail"`) {
		t.Errorf("ready with failures: %d %s", c, b)
	}
	if strings.Contains(b, "broker down") {
		t.Error("error details exposed in response")
	}
	if !strings.Contains(logBuf.String(), "kafka") {
		t.Errorf("failure not logged: %s", logBuf.String())
	}

	if c, b := get("/metrics"); c != 200 || !strings.Contains(b, "target_info") {
		t.Errorf("metrics: %d", c)
	}
}

func TestNewHealthDefaults(t *testing.T) {
	h := NewHealth(nil, 0)
	if h.timeout != 2*time.Second || h.log == nil {
		t.Errorf("defaults: %+v", h)
	}
}
