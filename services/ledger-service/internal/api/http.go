package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/m5tshift/weigate/services/ledger-service/internal/ledger"
)

// Options are the dependencies of the HTTP API.
type Options struct {
	Service *ledger.Service
	Logger  *slog.Logger
}

// Error codes of the API.
const (
	codeValidation = "validation_failed"
	codeNotFound   = "not_found"
	codeInternal   = "internal"
)

type gate struct {
	router routers.Router
	log    *slog.Logger
}

// Register mounts the statement API of contracts/openapi/ledger-service.yaml on mux. The API is internal:
// callers are authenticated by mesh mTLS, so there is no application-level authentication. Every request is
// validated against the contract (400).
func Register(mux *http.ServeMux, o Options) error {
	doc, err := GetSpec()
	if err != nil {
		return fmt.Errorf("api: load spec: %w", err)
	}
	// The contract names the in-cluster host; requests are matched by path only.
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		return fmt.Errorf("api: router: %w", err)
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	g := &gate{router: router, log: log}
	strict := NewStrictHandlerWithOptions(&Server{svc: o.Service}, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  g.requestError,
		ResponseErrorHandlerFunc: g.internalError,
	})
	HandlerWithOptions(strict, StdHTTPServerOptions{
		BaseRouter:       mux,
		Middlewares:      []MiddlewareFunc{g.middleware},
		ErrorHandlerFunc: g.requestError,
	})
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = buf.WriteTo(w)
}

func validationError(fields []string) Error {
	msg := "Request validation failed"
	if len(fields) > 0 {
		msg += ": " + strings.Join(fields, ", ")
	}
	return Error{Code: codeValidation, Message: msg}
}

func (g *gate) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, params, err := g.router.FindRoute(r)
		if err != nil { // the mux matched the same contract, so this is a bug
			g.internalError(w, r, fmt.Errorf("api: route not in the contract: %w", err))
			return
		}
		err = openapi3filter.ValidateRequest(r.Context(), &openapi3filter.RequestValidationInput{
			Request: r, PathParams: params, Route: route,
			Options: &openapi3filter.Options{MultiError: true, SkipSettingDefaults: true},
		})
		if err != nil {
			writeJSON(w, http.StatusBadRequest, validationError(invalidParameters(err)))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// invalidParameters returns the names of the invalid parameters, sorted and without duplicates.
func invalidParameters(err error) []string {
	seen := map[string]bool{}
	var walk func(error)
	walk = func(err error) {
		switch e := err.(type) { //nolint:errorlint // the error tree of kin-openapi is walked level by level
		case openapi3.MultiError:
			for _, inner := range e {
				walk(inner)
			}
		case *openapi3filter.RequestError:
			if e.Parameter != nil {
				seen[e.Parameter.Name] = true
			}
		}
	}
	walk(err)
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// requestError handles parameters the generated wrapper could not parse. The wrapper parses parameters
// before the gate runs, so a missing or malformed parameter is reported here.
func (g *gate) requestError(w http.ResponseWriter, _ *http.Request, err error) {
	var fields []string
	if name := parameterName(err); name != "" {
		fields = []string{name}
	}
	writeJSON(w, http.StatusBadRequest, validationError(fields))
}

// parameterName returns the parameter of an error of the generated wrapper, or "".
func parameterName(err error) string {
	switch e := err.(type) { //nolint:errorlint // the wrapper passes its own error types unwrapped
	case *InvalidParamFormatError:
		return e.ParamName
	case *RequiredParamError:
		return e.ParamName
	case *UnmarshalingParamError:
		return e.ParamName
	case *TooManyValuesForParamError:
		return e.ParamName
	case *RequiredHeaderError:
		return e.ParamName
	case *UnescapedCookieParamError:
		return e.ParamName
	}
	return ""
}

// internalError answers 500 and logs the error.
func (g *gate) internalError(w http.ResponseWriter, r *http.Request, err error) {
	g.log.ErrorContext(r.Context(), "request failed", "error", err)
	writeJSON(w, http.StatusInternalServerError, Error{Code: codeInternal, Message: "Internal error"})
}
