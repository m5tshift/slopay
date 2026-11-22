package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/m5tshift/weigate/services/reporting-service/internal/jwks"
	"github.com/m5tshift/weigate/services/reporting-service/internal/store"
)

// Error codes of the API.
const (
	codeValidation   = "validation_failed"
	codeUnauthorized = "unauthorized"
	codeUnavailable  = "unavailable"
)

// Reports is the source of the reports (the ClickHouse views). *store.Store implements it.
type Reports interface {
	Turnover(ctx context.Context, merchant uuid.UUID, from, to time.Time) ([]store.TurnoverDay, error)
	Conversion(ctx context.Context, merchant uuid.UUID, from, to time.Time) ([]store.ConversionDay, error)
}

// Options are the dependencies of the HTTP API.
type Options struct {
	Reports Reports
	// JWTIssuer is the iss claim a token must carry; empty accepts any issuer.
	JWTIssuer string
	// Keys verifies the JWT signatures (required).
	Keys KeyResolver
	// MaxDays is the longest period of a report in days (required, > 0).
	MaxDays int
	// QueryTimeout bounds one report query (default 10 s).
	QueryTimeout time.Duration
	// RetryAfter is sent with 503 (at least one second).
	RetryAfter time.Duration
	Logger     *slog.Logger
	// Now is the clock of token expiry (default time.Now).
	Now func() time.Time
}

// Server implements StrictServerInterface.
type Server struct {
	reports    Reports
	maxDays    int
	timeout    time.Duration
	retryAfter int
	log        *slog.Logger
}

var _ StrictServerInterface = (*Server)(nil)

var bearer = "Bearer"

func unauthorized() UnauthorizedJSONResponse {
	return UnauthorizedJSONResponse{
		Body:    Error{Code: codeUnauthorized, Message: "Invalid credentials"},
		Headers: UnauthorizedResponseHeaders{WWWAuthenticate: &bearer},
	}
}

// Register mounts the API of contracts/openapi/reporting-service.yaml on mux. Every /api/ request needs a bearer
// JWT (401) before its parameters are looked at (400).
func Register(mux *http.ServeMux, o Options) error {
	if o.Keys == nil {
		return errors.New("api: Options.Keys is required: the JWT signature is verified by the service")
	}
	if o.MaxDays < 1 {
		return errors.New("api: Options.MaxDays must be positive")
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	timeout := o.QueryTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	retry := max(int(o.RetryAfter.Seconds()), 1)
	srv := &Server{reports: o.Reports, maxDays: o.MaxDays, timeout: timeout, retryAfter: retry, log: log}
	inner := http.NewServeMux()
	strict := NewStrictHandlerWithOptions(srv, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  func(w http.ResponseWriter, _ *http.Request, _ error) { writeValidation(w) },
		ResponseErrorHandlerFunc: srv.internalError,
	})
	HandlerWithOptions(strict, StdHTTPServerOptions{
		BaseRouter:       inner,
		ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, _ error) { writeValidation(w) },
	})
	g := &gate{issuer: o.JWTIssuer, keys: o.Keys, now: now, log: log}
	mux.Handle("/api/", g.middleware(inner))
	return nil
}

type gate struct {
	issuer string
	keys   KeyResolver
	now    func() time.Time
	log    *slog.Logger
}

type merchantKey struct{}

func merchantID(ctx context.Context) uuid.UUID {
	id, _ := ctx.Value(merchantKey{}).(uuid.UUID)
	return id
}

// middleware reads the merchant from the bearer JWT. Only the outcome is logged, never the token.
func (g *gate) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		var id uuid.UUID
		var err error
		if ok && strings.EqualFold(scheme, "Bearer") && strings.TrimSpace(token) != "" {
			id, err = merchantFromToken(r.Context(), strings.TrimSpace(token), g.issuer, g.now(), g.keys)
		} else {
			err = errMalformedToken
		}
		if err != nil {
			// An unreachable JWKS is the operator's problem: log it louder. The client still gets 401.
			level := slog.LevelInfo
			if errors.Is(err, jwks.ErrUnavailable) {
				level = slog.LevelWarn
			}
			g.log.Log(r.Context(), level, "jwt rejected", "reason", err.Error())
			u := unauthorized()
			w.Header().Set("WWW-Authenticate", *u.Headers.WWWAuthenticate)
			writeJSON(w, http.StatusUnauthorized, u.Body)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), merchantKey{}, id)))
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeValidation(w http.ResponseWriter) {
	writeJSON(w, http.StatusBadRequest, Error{Code: codeValidation, Message: "Request validation failed"})
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed", "error", err)
	writeJSON(w, http.StatusInternalServerError, Error{Code: "internal", Message: "Internal error"})
}

func (s *Server) unavailable(ctx context.Context, err error) UnavailableJSONResponse {
	s.log.ErrorContext(ctx, "clickhouse query failed", "error", err)
	return UnavailableJSONResponse{
		Body:    Error{Code: codeUnavailable, Message: "Reports are temporarily unavailable"},
		Headers: UnavailableResponseHeaders{RetryAfter: s.retryAfter},
	}
}

// validPeriod reports whether from..to is a period of at most maxDays days.
func (s *Server) validPeriod(from, to openapi_types.Date) bool {
	if from.After(to.Time) {
		return false
	}
	days := int(to.Sub(from.Time).Hours()/24) + 1
	return days <= s.maxDays
}

// GetTurnoverReport implements StrictServerInterface.
func (s *Server) GetTurnoverReport(ctx context.Context, req GetTurnoverReportRequestObject) (GetTurnoverReportResponseObject, error) {
	p := req.Params
	if !s.validPeriod(p.From, p.To) {
		return GetTurnoverReport400JSONResponse{ValidationErrorJSONResponse(validationError())}, nil
	}
	qctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.reports.Turnover(qctx, merchantID(ctx), p.From.Time, p.To.Time)
	if err != nil {
		return GetTurnoverReport503JSONResponse{s.unavailable(ctx, err)}, nil
	}
	items := make([]TurnoverDay, 0, len(rows))
	for _, r := range rows {
		items = append(items, TurnoverDay{Day: openapi_types.Date{Time: r.Day}, PaidInvoices: r.PaidInvoices, TurnoverUsdCents: r.TurnoverUSDCents})
	}
	return GetTurnoverReport200JSONResponse{From: p.From, To: p.To, Items: items}, nil
}

// GetConversionReport implements StrictServerInterface.
func (s *Server) GetConversionReport(ctx context.Context, req GetConversionReportRequestObject) (GetConversionReportResponseObject, error) {
	p := req.Params
	if !s.validPeriod(p.From, p.To) {
		return GetConversionReport400JSONResponse{ValidationErrorJSONResponse(validationError())}, nil
	}
	qctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.reports.Conversion(qctx, merchantID(ctx), p.From.Time, p.To.Time)
	if err != nil {
		return GetConversionReport503JSONResponse{s.unavailable(ctx, err)}, nil
	}
	items := make([]ConversionDay, 0, len(rows))
	for _, r := range rows {
		items = append(items, ConversionDay{
			Day: openapi_types.Date{Time: r.Day}, CreatedInvoices: r.CreatedInvoices, PaidInvoices: r.PaidInvoices, Conversion: r.Conversion,
		})
	}
	return GetConversionReport200JSONResponse{From: p.From, To: p.To, Items: items}, nil
}

func validationError() Error {
	return Error{Code: codeValidation, Message: "Request validation failed"}
}
