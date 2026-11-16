package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewRequiresServiceName(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	c := ConfigFromEnv("svc")
	if c.ServiceName != "svc" || c.OTLPEndpoint != "" || c.Insecure || c.TraceSampleRatio != 1 {
		t.Errorf("defaults: %+v", c)
	}
	t.Setenv("OTEL_SERVICE_NAME", "other")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	c = ConfigFromEnv("svc")
	if c.ServiceName != "other" || c.OTLPEndpoint != "collector:4318" || !c.Insecure || c.TraceSampleRatio != 0.25 {
		t.Errorf("env: %+v", c)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "7")
	if ConfigFromEnv("svc").TraceSampleRatio != 1 {
		t.Error("out-of-range ratio must be ignored")
	}
}

// collector is a fake OTLP/HTTP endpoint recording request paths.
func collector(t *testing.T) (*httptest.Server, func() map[string]bool) {
	t.Helper()
	var mu sync.Mutex
	paths := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.URL.Path] = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]bool{}
		for k, v := range paths {
			out[k] = v
		}
		return out
	}
}

func TestExportsOTLP(t *testing.T) {
	for name, endpoint := range map[string]func(string) (string, bool){
		"url":      func(u string) (string, bool) { return u, false },
		"hostport": func(u string) (string, bool) { return strings.TrimPrefix(u, "http://"), true },
	} {
		t.Run(name, func(t *testing.T) {
			srv, seen := collector(t)
			ep, insecure := endpoint(srv.URL)
			tel, err := New(context.Background(), Config{ServiceName: "svc", OTLPEndpoint: ep, Insecure: insecure, TraceSampleRatio: 1})
			if err != nil {
				t.Fatal(err)
			}
			_, span := tel.Tracer.Tracer("t").Start(context.Background(), "op")
			span.End()
			c, _ := tel.Meter.Meter("t").Int64Counter("c")
			c.Add(context.Background(), 1)
			if err := tel.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && (!seen()["/v1/traces"] || !seen()["/v1/metrics"]) {
				time.Sleep(10 * time.Millisecond)
			}
			if p := seen(); !p["/v1/traces"] || !p["/v1/metrics"] {
				t.Errorf("collector paths = %v", p)
			}
		})
	}
}

func TestNoOTLPStillServesMetrics(t *testing.T) {
	tel, err := New(context.Background(), Config{ServiceName: "svc", TraceSampleRatio: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background()) //nolint:errcheck
	rec := httptest.NewRecorder()
	tel.MetricsHandler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "target_info") {
		t.Errorf("code %d body %q", rec.Code, rec.Body.String())
	}
}
