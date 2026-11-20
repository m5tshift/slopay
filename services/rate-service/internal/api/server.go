package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
)

// Error codes of the contract.
const (
	codeUnknownPair     = "unknown_pair"
	codeRateUnavailable = "rate_unavailable"
	codeFaultInjected   = "fault_injected"
	codeNotFound        = "not_found"
	codeInvalidRequest  = "invalid_request"
	codeInternal        = "internal"
)

// maxBodyBytes limits request bodies; the only body is {"enabled": true}.
const maxBodyBytes = 1 << 10

// Rates returns the snapshot a replica serves (store.Latest).
type Rates interface {
	Get(ctx context.Context, pair rate.Pair) (rate.Snapshot, bool)
}

// Options are the dependencies and settings of the HTTP API.
type Options struct {
	Rates Rates
	// MaxAge is the age after which a rate is stale (60s).
	MaxAge time.Duration
	// RetryAfter is the Retry-After of a 503, rounded up to whole seconds.
	RetryAfter time.Duration
	// FaultInjection enables /test/fault.
	FaultInjection bool
	// Now substitutes the clock in tests; defaults to time.Now.
	Now    func() time.Time
	Logger *slog.Logger
}

// Server implements StrictServerInterface.
type Server struct {
	o     Options
	log   *slog.Logger
	fault atomic.Bool
}

var _ StrictServerInterface = (*Server)(nil)

// Register mounts the API of contracts/openapi/rate-service.yaml on mux.
func Register(mux *http.ServeMux, o Options) *Server {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	srv := &Server{o: o, log: o.Logger}
	strict := NewStrictHandlerWithOptions(srv, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  srv.requestError,
		ResponseErrorHandlerFunc: srv.internalError,
	})
	HandlerWithOptions(strict, StdHTTPServerOptions{
		BaseRouter:       mux,
		Middlewares:      []MiddlewareFunc{srv.middleware},
		ErrorHandlerFunc: srv.requestError,
	})
	return srv
}

// GetRate implements StrictServerInterface. All replicas serve it (SRS 4.1.3).
func (s *Server) GetRate(ctx context.Context, req GetRateRequestObject) (GetRateResponseObject, error) {
	if s.fault.Load() {
		return s.unavailable(Error{Code: codeFaultInjected, Message: "Fault injection is on for this replica"}), nil
	}
	pair := rate.Pair(req.Pair)
	if !rate.IsServed(pair) {
		return GetRate404JSONResponse{Code: codeUnknownPair, Message: "Pair is not supported"}, nil
	}
	snap, ok := s.o.Rates.Get(ctx, pair)
	r, age, err := rate.Evaluate(snap, ok, s.o.Now(), s.o.MaxAge)
	if err != nil {
		reason := Unavailable
		switch {
		case errors.Is(err, rate.ErrStale):
			reason = Stale
		case errors.Is(err, rate.ErrDiverged):
			reason = Diverged
		}
		s.log.DebugContext(ctx, "rate is not served", "pair", pair, "reason", reason)
		return s.unavailable(Error{Code: codeRateUnavailable, Message: "Exchange rate is unavailable", Reason: &reason}), nil
	}
	sources := make([]Source, len(r.Sources))
	for i, src := range r.Sources {
		sources[i] = Source(src)
	}
	return GetRate200JSONResponse{
		Pair: req.Pair, Price: r.Price.String(), QuotedAt: r.QuotedAt.UTC(), AgeSeconds: age, Sources: sources,
	}, nil
}

func (s *Server) unavailable(e Error) GetRate503JSONResponse {
	secs := int((s.o.RetryAfter + time.Second - 1) / time.Second)
	return GetRate503JSONResponse{Body: e, Headers: GetRate503ResponseHeaders{RetryAfter: max(secs, 1)}}
}

func faultDisabled() FaultInjectionDisabledJSONResponse {
	return FaultInjectionDisabledJSONResponse{Code: codeNotFound, Message: "Fault injection is disabled"}
}

// GetFault implements StrictServerInterface. Without FAULT_INJECTION_ENABLED the middleware answers 404.
func (s *Server) GetFault(_ context.Context, _ GetFaultRequestObject) (GetFaultResponseObject, error) {
	return GetFault200JSONResponse{Enabled: s.fault.Load()}, nil
}

// SetFault implements StrictServerInterface. The flag lives in the memory of this replica (SRS 8.2а).
func (s *Server) SetFault(ctx context.Context, req SetFaultRequestObject) (SetFaultResponseObject, error) {
	if prev := s.fault.Swap(req.Body.Enabled); prev != req.Body.Enabled {
		s.log.WarnContext(ctx, "fault injection changed", "enabled", req.Body.Enabled)
	}
	return SetFault200JSONResponse{Enabled: req.Body.Enabled}, nil
}

// middleware limits bodies. /test/fault answers 404 before the body is read when fault injection is disabled;
// otherwise the body of setFault is checked: "enabled" is a required boolean, which the generated decoder
// would read as false when it is missing.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		if r.URL.Path == "/test/fault" && !s.o.FaultInjection {
			writeJSON(w, http.StatusNotFound, faultDisabled())
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == "/test/fault" {
			raw, err := io.ReadAll(r.Body)
			var body struct {
				Enabled *bool `json:"enabled"`
			}
			if err == nil {
				err = json.Unmarshal(raw, &body)
			}
			if err != nil || body.Enabled == nil {
				s.requestError(w, r, err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// requestError answers 400; the request is not echoed.
func (s *Server) requestError(w http.ResponseWriter, _ *http.Request, _ error) {
	writeJSON(w, http.StatusBadRequest, Error{Code: codeInvalidRequest, Message: "Request body is not valid"})
}

// internalError answers 500 and logs the error.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed", "error", err)
	writeJSON(w, http.StatusInternalServerError, Error{Code: codeInternal, Message: "Internal error"})
}
