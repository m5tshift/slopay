package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Config is the telemetry configuration; all values come from the environment (see ConfigFromEnv).
type Config struct {
	ServiceName    string
	ServiceVersion string
	// OTLPEndpoint is the OTel Collector OTLP/HTTP address, host:port or a URL.
	// Empty disables OTLP export; /metrics keeps working.
	OTLPEndpoint string
	// Insecure uses plain HTTP to the collector when OTLPEndpoint has no scheme.
	Insecure bool
	// TraceSampleRatio in [0,1]; 1 samples everything (SRS 6.2 lets operators lower it).
	TraceSampleRatio float64
}

// ConfigFromEnv reads OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT,
// OTEL_EXPORTER_OTLP_INSECURE (true/false) and OTEL_TRACES_SAMPLER_ARG.
func ConfigFromEnv(defaultService string) Config {
	c := Config{ServiceName: defaultService, TraceSampleRatio: 1, Insecure: os.Getenv("OTEL_EXPORTER_OTLP_INSECURE") == "true"}
	if v := os.Getenv("OTEL_SERVICE_NAME"); v != "" {
		c.ServiceName = v
	}
	c.OTLPEndpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err == nil && f >= 0 && f <= 1 {
			c.TraceSampleRatio = f
		}
	}
	return c
}

// Telemetry owns the OTel providers and the Prometheus registry served on /metrics.
type Telemetry struct {
	Tracer   *sdktrace.TracerProvider
	Meter    *sdkmetric.MeterProvider
	Registry *prometheus.Registry
	shutdown []func(context.Context) error
}

// New builds the tracer and meter providers, sets the global providers and W3C propagator.
func New(ctx context.Context, cfg Config) (*Telemetry, error) {
	if cfg.ServiceName == "" {
		return nil, errors.New("telemetry: service name is required")
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName), semconv.ServiceVersion(cfg.ServiceVersion)))
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	reg := prometheus.NewRegistry()
	promReader, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
	}
	meterOpts := []sdkmetric.Option{sdkmetric.WithResource(res), sdkmetric.WithReader(promReader)}
	traceOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio))),
	}

	if cfg.OTLPEndpoint != "" {
		te, err := otlptracehttp.New(ctx, traceExporterOpts(cfg)...)
		if err != nil {
			return nil, fmt.Errorf("telemetry: trace exporter: %w", err)
		}
		me, err := otlpmetrichttp.New(ctx, metricExporterOpts(cfg)...)
		if err != nil {
			return nil, fmt.Errorf("telemetry: metric exporter: %w", err)
		}
		traceOpts = append(traceOpts, sdktrace.WithBatcher(te))
		meterOpts = append(meterOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me)))
	}

	t := &Telemetry{Registry: reg}
	t.Tracer = sdktrace.NewTracerProvider(traceOpts...)
	t.Meter = sdkmetric.NewMeterProvider(meterOpts...)
	t.shutdown = []func(context.Context) error{t.Tracer.Shutdown, t.Meter.Shutdown}

	otel.SetTracerProvider(t.Tracer)
	otel.SetMeterProvider(t.Meter)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return t, nil
}

func traceExporterOpts(cfg Config) []otlptracehttp.Option {
	host, insecure := splitEndpoint(cfg)
	o := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
	if insecure {
		o = append(o, otlptracehttp.WithInsecure())
	}
	return o
}

func metricExporterOpts(cfg Config) []otlpmetrichttp.Option {
	host, insecure := splitEndpoint(cfg)
	o := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(host)}
	if insecure {
		o = append(o, otlpmetrichttp.WithInsecure())
	}
	return o
}

// splitEndpoint accepts host:port or http(s)://host:port and returns host:port and whether TLS is off.
func splitEndpoint(cfg Config) (string, bool) {
	switch {
	case strings.HasPrefix(cfg.OTLPEndpoint, "http://"):
		return strings.TrimSuffix(strings.TrimPrefix(cfg.OTLPEndpoint, "http://"), "/"), true
	case strings.HasPrefix(cfg.OTLPEndpoint, "https://"):
		return strings.TrimSuffix(strings.TrimPrefix(cfg.OTLPEndpoint, "https://"), "/"), false
	}
	return cfg.OTLPEndpoint, cfg.Insecure
}

// Shutdown flushes and stops the providers.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, f := range t.shutdown {
		errs = append(errs, f(ctx))
	}
	return errors.Join(errs...)
}

// MetricsHandler serves the Prometheus exposition of all OTel metrics.
func (t *Telemetry) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{})
}
