package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
	"github.com/m5tshift/weigate/services/invoice-service/internal/jwks"
)

// maxBodyBytes limits request bodies; a valid creation request is under 300 bytes.
const maxBodyBytes = 16 << 10

// Options are the dependencies of the HTTP API.
type Options struct {
	Service *invoice.Service
	// JWTIssuer is the iss claim a token must carry; empty accepts any issuer.
	JWTIssuer string
	// Keys verifies the JWT signatures (required).
	Keys KeyResolver
	// RetryAfter is sent with 503 when rate-service did not suggest a pause.
	RetryAfter time.Duration
	Logger     *slog.Logger
	// Now is the clock of token expiry (default time.Now).
	Now func() time.Time
}

type gate struct {
	router routers.Router
	doc    *openapi3.T
	issuer string
	keys   KeyResolver
	now    func() time.Time
	log    *slog.Logger
}

// Register mounts the API of contracts/openapi/invoice-service.yaml on mux. Every request is checked
// against the contract: operations with security need a bearer JWT (401), then the request is validated (400).
func Register(mux *http.ServeMux, o Options) error {
	doc, err := GetSpec()
	if err != nil {
		return fmt.Errorf("api: load spec: %w", err)
	}
	// The contract names the stand host; requests are matched by path only.
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		return fmt.Errorf("api: router: %w", err)
	}
	if o.Keys == nil {
		return errors.New("api: Options.Keys is required: the JWT signature is verified by the service")
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	retry := o.RetryAfter
	if retry < time.Second {
		retry = time.Second
	}
	g := &gate{router: router, doc: doc, issuer: o.JWTIssuer, keys: o.Keys, now: now, log: log}
	srv := &Server{svc: o.Service, retryAfter: retry, log: log}
	strict := NewStrictHandlerWithOptions(srv, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  g.requestError,
		ResponseErrorHandlerFunc: g.internalError,
	})
	HandlerWithOptions(strict, StdHTTPServerOptions{
		BaseRouter:       mux,
		Middlewares:      []MiddlewareFunc{g.middleware},
		ErrorHandlerFunc: g.paramError,
	})
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err := buf.WriteTo(w)
	return err
}

func writeUnauthorized(w http.ResponseWriter) {
	u := unauthorized()
	w.Header().Set("WWW-Authenticate", *u.Headers.WWWAuthenticate)
	_ = writeJSON(w, http.StatusUnauthorized, u.Body)
}

func writeValidation(w http.ResponseWriter, details []fieldError) {
	_ = writeJSON(w, http.StatusBadRequest, validationResponse(details))
}

// authenticate reads the merchant from the bearer JWT. Only the outcome is logged, never the token.
func (g *gate) authenticate(r *http.Request) (uuid.UUID, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return uuid.Nil, false
	}
	id, err := merchantFromToken(r.Context(), strings.TrimSpace(token), g.issuer, g.now(), g.keys)
	if err != nil {
		// An unreachable JWKS is the operator's problem, not the client's: log it louder. The client still gets
		// 401, the only auth failure of the contract.
		level := slog.LevelInfo
		if errors.Is(err, jwks.ErrUnavailable) {
			level = slog.LevelWarn
		}
		g.log.Log(r.Context(), level, "jwt rejected", "reason", err.Error())
		return uuid.Nil, false
	}
	return id, true
}

func (g *gate) needsAuth(op *openapi3.Operation) bool {
	sec := op.Security
	if sec == nil {
		sec = &g.doc.Security
	}
	return len(*sec) > 0
}

func (g *gate) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		route, params, err := g.router.FindRoute(r)
		if err != nil { // the mux matched the same contract, so this is a bug
			g.internalError(w, r, fmt.Errorf("api: route not in the contract: %w", err))
			return
		}
		ctx := r.Context()
		if g.needsAuth(route.Operation) {
			id, ok := g.authenticate(r)
			if !ok {
				writeUnauthorized(w)
				return
			}
			ctx = withMerchant(ctx, id)
			r = r.WithContext(ctx)
		}
		err = openapi3filter.ValidateRequest(ctx, &openapi3filter.RequestValidationInput{
			Request: r, PathParams: params, Route: route,
			Options: &openapi3filter.Options{
				MultiError:          true,
				SkipSettingDefaults: true,
				AuthenticationFunc:  openapi3filter.NoopAuthenticationFunc,
			},
		})
		if err != nil {
			writeValidation(w, requestErrorDetails(err))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requestErrorDetails turns validation errors into details. Values of the request are never echoed.
func requestErrorDetails(err error) []fieldError {
	const notJSON = "the request body is not valid JSON of the expected type"
	var out []fieldError
	var walk func(error)
	walk = func(err error) {
		var se *openapi3.SchemaError
		switch e := err.(type) { //nolint:errorlint // the error tree of kin-openapi is walked level by level
		case openapi3.MultiError:
			for _, inner := range e {
				walk(inner)
			}
		case *openapi3filter.RequestError:
			var multi openapi3.MultiError
			switch {
			case e.Parameter != nil:
				out = append(out, fieldError{Field: e.Parameter.Name, Message: "invalid value"})
			case errors.Is(e.Err, openapi3filter.ErrInvalidRequired):
				out = append(out, fieldError{Field: "body", Message: "the request body is required"})
			case errors.As(e.Err, &multi):
				walk(multi)
			case errors.As(e.Err, &se):
				walk(se)
			default:
				out = append(out, fieldError{Field: "body", Message: notJSON})
			}
		default:
			if !errors.As(err, &se) {
				out = append(out, fieldError{Field: "body", Message: notJSON})
				return
			}
			field := strings.Join(se.JSONPointer(), ".")
			if field == "" {
				field = "body"
			}
			out = append(out, fieldError{Field: field, Message: se.Reason})
		}
	}
	walk(err)
	return out
}

// paramError handles parameters the generated wrapper could not parse; it runs before the gate middleware.
// A malformed invoice id identifies no invoice, so the answer is 404, after authentication on the merchant
// API. A malformed query parameter of the list is 400.
func (g *gate) paramError(w http.ResponseWriter, r *http.Request, err error) {
	route, _, ferr := g.router.FindRoute(r)
	if ferr == nil && g.needsAuth(route.Operation) {
		if _, ok := g.authenticate(r); !ok {
			writeUnauthorized(w)
			return
		}
	}
	if r.PathValue("id") == "" {
		writeValidation(w, []fieldError{{Field: paramName(err), Message: "invalid value"}})
		return
	}
	_ = writeJSON(w, http.StatusNotFound, notFound())
}

func paramName(err error) string {
	var (
		format   *InvalidParamFormatError
		header   *RequiredHeaderError
		required *RequiredParamError
	)
	switch {
	case errors.As(err, &format):
		return format.ParamName
	case errors.As(err, &header):
		return header.ParamName
	case errors.As(err, &required):
		return required.ParamName
	default:
		return "query"
	}
}

// requestError handles bodies the strict handler could not decode; the gate has validated them already.
func (g *gate) requestError(w http.ResponseWriter, _ *http.Request, _ error) {
	writeValidation(w, []fieldError{{Field: "body", Message: "the request body is not valid JSON of the expected type"}})
}

// internalError answers 500 and logs the error. Errors of the service carry no secrets.
func (g *gate) internalError(w http.ResponseWriter, r *http.Request, err error) {
	g.log.ErrorContext(r.Context(), "request failed", "error", err)
	_ = writeJSON(w, http.StatusInternalServerError, Error{Code: codeInternal, Message: "Internal error"})
}

type merchantKey struct{}

func withMerchant(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, merchantKey{}, id)
}

// merchantID is the merchant of the JWT; the gate middleware guarantees it for protected operations.
func merchantID(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(merchantKey{}).(uuid.UUID)
	return id, ok
}
