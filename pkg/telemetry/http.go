package telemetry

import (
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const instrumentation = "github.com/m5tshift/slopay/pkg/telemetry"

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Middleware records RED metrics per route and starts a server span, continuing an incoming
// W3C trace context. The route is the mux pattern (bounded cardinality); requests that match
// no pattern are reported as "unmatched". Wrap the mux: Middleware(mux).
func (t *Telemetry) Middleware(next http.Handler) http.Handler {
	meter := t.Meter.Meter(instrumentation)
	requests, _ := meter.Int64Counter("http.server.requests", metric.WithDescription("HTTP requests"))
	errs, _ := meter.Int64Counter("http.server.request.errors", metric.WithDescription("HTTP requests with status >= 500"))
	duration, _ := meter.Float64Histogram("http.server.request.duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
		metric.WithDescription("HTTP request duration"))
	tracer := t.Tracer.Tracer(instrumentation)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		sw := &statusWriter{ResponseWriter: w}
		req := r.WithContext(ctx)
		next.ServeHTTP(sw, req)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}

		route := req.Pattern
		if route == "" {
			route = "unmatched"
		}
		attrs := metric.WithAttributes(
			attribute.String("http.route", route),
			attribute.String("http.request.method", r.Method),
			attribute.String("http.response.status_code", strconv.Itoa(sw.status)),
		)
		requests.Add(ctx, 1, attrs)
		duration.Record(ctx, time.Since(start).Seconds(), attrs)
		span.SetName(route)
		span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", sw.status))
		if sw.status >= 500 {
			errs.Add(ctx, 1, attrs)
			span.SetStatus(codes.Error, http.StatusText(sw.status))
		}
	})
}
