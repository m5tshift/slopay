package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/m5tshift/slopay/services/merchant-service/internal/jwt"
	"github.com/m5tshift/slopay/services/merchant-service/internal/merchant"
)

// maxBodyBytes limits request bodies; the largest valid body (webhook settings) is about 2 KiB.
const maxBodyBytes = 64 << 10

// Options are the dependencies of the HTTP API.
type Options struct {
	Service    *merchant.Service
	Issuer     *jwt.Issuer
	Signer     jwt.Signer
	JWKSMaxAge time.Duration
	Logger     *slog.Logger
}

var defineFormats sync.Once

type gate struct {
	router routers.Router
	doc    *openapi3.T
	issuer *jwt.Issuer
	log    *slog.Logger
}

// Register mounts the API of contracts/openapi/merchant-service.yaml on mux. Every request is checked
// against the contract: operations with security need a valid JWT (401), then the request is validated (400).
func Register(mux *http.ServeMux, o Options) error {
	defineFormats.Do(func() {
		// The generated types check e-mail when decoding; the validator checks it first to name the field.
		openapi3.DefineStringFormatValidator("email", openapi3.NewRegexpFormatValidator(openapi3.FormatOfStringForEmail))
	})
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
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	g := &gate{router: router, doc: doc, issuer: o.Issuer, log: log}
	srv := &Server{svc: o.Service, issuer: o.Issuer, signer: o.Signer, jwksMaxAge: o.JWKSMaxAge}
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

// authenticate checks the bearer JWT. Only the outcome is logged, never the token.
func (g *gate) authenticate(r *http.Request) (uuid.UUID, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return uuid.Nil, false
	}
	claims, err := g.issuer.Verify(r.Context(), strings.TrimSpace(token))
	if err != nil {
		g.log.InfoContext(r.Context(), "jwt rejected", "reason", errorReason(err))
		return uuid.Nil, false
	}
	id, err := uuid.Parse(claims.Subject)
	if err != nil {
		g.log.InfoContext(r.Context(), "jwt rejected", "reason", "subject is not a merchant id")
		return uuid.Nil, false
	}
	return id, true
}

func errorReason(err error) string {
	if errors.Is(err, jwt.ErrInvalidToken) {
		return "invalid token"
	}
	return "verification failed"
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

// requestErrorDetails turns validation errors into details. Values of the request are never echoed:
// a field could hold a private key or an API key.
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

// paramError handles path parameters the generated wrapper could not parse. Authentication still comes first;
// a malformed id identifies no resource, so the answer is 404.
func (g *gate) paramError(w http.ResponseWriter, r *http.Request, _ error) {
	if _, ok := g.authenticate(r); !ok {
		writeUnauthorized(w)
		return
	}
	_ = writeJSON(w, http.StatusNotFound, Error{Code: codeKeyNotFound, Message: "Not found"})
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
