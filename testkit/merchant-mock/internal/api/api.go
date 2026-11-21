// Package api is the HTTP layer of merchant-mock: the webhook endpoint and the commands of
// contracts/openapi/merchant-mock.yaml.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/m5tshift/slopay/testkit/merchant-mock/internal/receiver"
)

const (
	// maxWebhookBytes limits a webhook body; an invoice event is a few kilobytes.
	maxWebhookBytes = 1 << 20
	maxBodyBytes    = 1 << 10
	defaultLimit    = 100
	maxLimit        = 10000
)

type server struct {
	r   *receiver.Receiver
	log *slog.Logger
}

// Register mounts the API of contracts/openapi/merchant-mock.yaml on mux.
func Register(mux *http.ServeMux, r *receiver.Receiver, log *slog.Logger) {
	s := &server{r: r, log: log}
	mux.HandleFunc("POST /webhook/{endpoint}", s.webhook)
	mux.HandleFunc("GET /control/endpoints/{endpoint}", s.endpoint)
	mux.HandleFunc("PUT /control/endpoints/{endpoint}/secret", s.setSecret)
	mux.HandleFunc("PUT /control/endpoints/{endpoint}/fault", s.setFault)
	mux.HandleFunc("GET /control/endpoints/{endpoint}/journal", s.journal)
	mux.HandleFunc("POST /control/reset", s.reset)
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (s *server) write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Warn("write response", "error", err)
	}
}

func (s *server) notFound(w http.ResponseWriter) {
	s.write(w, http.StatusNotFound, apiError{"not_found", "Unknown endpoint"})
}

func (s *server) invalid(w http.ResponseWriter, msg string) {
	s.write(w, http.StatusBadRequest, apiError{"invalid_request", msg})
}

// fail answers a command error.
func (s *server) fail(w http.ResponseWriter, err error, msg string) {
	switch {
	case errors.Is(err, receiver.ErrUnknownEndpoint):
		s.notFound(w)
	default:
		s.invalid(w, msg)
	}
}

func (s *server) webhook(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("endpoint")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		s.write(w, http.StatusBadRequest, apiError{string(receiver.Malformed), "Body cannot be read"})
		return
	}
	res, err := s.r.Handle(r.Context(), name, receiver.Headers{
		Timestamp:     r.Header.Get("X-SLOPay-Timestamp"),
		Signature:     r.Header.Get("X-SLOPay-Signature"),
		SecretVersion: r.Header.Get("X-SLOPay-Secret-Version"),
	}, body)
	switch {
	case errors.Is(err, receiver.ErrUnknownEndpoint):
		s.notFound(w)
		return
	case err != nil:
		// The sender gave up during an injected delay.
		return
	}
	s.log.Info("webhook", "endpoint", name, "outcome", res.Outcome, "status", res.Status)
	if res.Status == http.StatusOK {
		s.write(w, res.Status, map[string]string{"outcome": string(res.Outcome)})
		return
	}
	s.write(w, res.Status, apiError{string(res.Outcome), res.Message})
}

func (s *server) endpoint(w http.ResponseWriter, r *http.Request) {
	v, err := s.r.Endpoint(r.PathValue("endpoint"))
	if err != nil {
		s.notFound(w)
		return
	}
	s.write(w, http.StatusOK, v)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	return dec.Decode(v) == nil
}

func (s *server) setSecret(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Version int    `json:"version"`
		Secret  string `json:"secret"`
	}
	if !decode(w, r, &body) {
		s.invalid(w, "Body must be {\"version\": <integer>, \"secret\": <string>}")
		return
	}
	name := r.PathValue("endpoint")
	v, err := s.r.SetSecret(name, body.Version, body.Secret)
	if err != nil {
		s.fail(w, err, "version must be a positive integer not lower than the current one, secret must not be empty, endpoint name must match [a-z0-9][a-z0-9-]{0,62}")
		return
	}
	// The secret itself is never logged.
	s.log.Info("secret set", "endpoint", name, "version", body.Version)
	s.write(w, http.StatusOK, v)
}

func (s *server) setFault(w http.ResponseWriter, r *http.Request) {
	var body receiver.Fault
	if !decode(w, r, &body) {
		s.invalid(w, "Body must be {\"status\": <500-599>, \"delayMs\": <integer>}")
		return
	}
	name := r.PathValue("endpoint")
	v, err := s.r.SetFault(name, body)
	if err != nil {
		s.fail(w, err, "status must be 5xx, delayMs from 0 to 60000, endpoint name must match [a-z0-9][a-z0-9-]{0,62}")
		return
	}
	s.log.Info("fault set", "endpoint", name, "status", body.Status, "delayMs", body.DelayMs)
	s.write(w, http.StatusOK, v)
}

func (s *server) journal(w http.ResponseWriter, r *http.Request) {
	limit := defaultLimit
	if q := r.URL.Query().Get("limit"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n < 1 || n > maxLimit {
			s.invalid(w, "limit must be an integer from 1 to 10000")
			return
		}
		limit = n
	}
	entries, err := s.r.Journal(r.PathValue("endpoint"), limit)
	if err != nil {
		s.fail(w, err, "Invalid endpoint name")
		return
	}
	s.write(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *server) reset(w http.ResponseWriter, _ *http.Request) {
	s.r.Reset()
	s.log.Info("all endpoints forgotten")
	w.WriteHeader(http.StatusNoContent)
}
