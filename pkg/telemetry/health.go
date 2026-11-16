package telemetry

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"
)

// CheckFunc reports whether a dependency is ready; nil means ready.
type CheckFunc func(ctx context.Context) error

// Health serves /health/live and /health/ready.
type Health struct {
	mu      sync.RWMutex
	checks  map[string]CheckFunc
	timeout time.Duration
	log     *slog.Logger
}

// NewHealth creates a Health with a per-check timeout (default 2s when <= 0).
func NewHealth(log *slog.Logger, timeout time.Duration) *Health {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Health{checks: map[string]CheckFunc{}, timeout: timeout, log: log}
}

// AddReadiness registers a readiness check under name.
func (h *Health) AddReadiness(name string, f CheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = f
}

// Live always answers 200: the process is running.
func (h *Health) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready runs all checks; 503 when any fails or times out. Error details go to the log only.
func (h *Health) Ready(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	names := make([]string, 0, len(h.checks))
	for n := range h.checks {
		names = append(names, n)
	}
	h.mu.RUnlock()
	sort.Strings(names)

	status, code := "ok", http.StatusOK
	results := make(map[string]string, len(names))
	for _, n := range names {
		h.mu.RLock()
		f := h.checks[n]
		h.mu.RUnlock()
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		err := f(ctx)
		cancel()
		if err != nil {
			h.log.WarnContext(r.Context(), "readiness check failed", "check", n, "error", err)
			results[n], status, code = "fail", "fail", http.StatusServiceUnavailable
			continue
		}
		results[n] = "ok"
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": results})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Register mounts /health/live, /health/ready and /metrics on mux.
func Register(mux *http.ServeMux, h *Health, t *Telemetry) {
	mux.HandleFunc("GET /health/live", h.Live)
	mux.HandleFunc("GET /health/ready", h.Ready)
	mux.Handle("GET /metrics", t.MetricsHandler())
}
