// Package api is the HTTP layer of rate-mock: quotes of the two sources and the commands of
// contracts/openapi/rate-mock.yaml.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/m5tshift/slopay/testkit/rate-mock/internal/market"
)

// maxBodyBytes limits command bodies; the largest one is a few bytes.
const maxBodyBytes = 1 << 10

type server struct {
	m   *market.Market
	log *slog.Logger
}

// Register mounts the API of contracts/openapi/rate-mock.yaml on mux.
func Register(mux *http.ServeMux, m *market.Market, log *slog.Logger) {
	s := &server{m: m, log: log}
	mux.HandleFunc("GET /sources/{source}/quotes/{pair}", s.quote)
	mux.HandleFunc("GET /control", s.state)
	mux.HandleFunc("POST /control/sources/{source}/disable", s.setEnabled(false))
	mux.HandleFunc("POST /control/sources/{source}/enable", s.setEnabled(true))
	mux.HandleFunc("PUT /control/divergence", s.divergence)
	mux.HandleFunc("POST /control/reset", s.reset)
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type quoteBody struct {
	Source   string `json:"source"`
	Pair     string `json:"pair"`
	Price    string `json:"price"`
	QuotedAt string `json:"quotedAt"`
}

type sourceState struct {
	Enabled bool `json:"enabled"`
}

type stateBody struct {
	Sources       map[string]sourceState `json:"sources"`
	DivergenceBps int                    `json:"divergenceBps"`
}

func (s *server) write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Warn("write response", "error", err)
	}
}

func (s *server) quote(w http.ResponseWriter, r *http.Request) {
	q, err := s.m.Quote(market.Source(r.PathValue("source")), market.Pair(r.PathValue("pair")))
	switch {
	case errors.Is(err, market.ErrUnknownSource), errors.Is(err, market.ErrUnknownPair):
		s.write(w, http.StatusNotFound, apiError{"not_found", "Source does not quote the pair"})
	case errors.Is(err, market.ErrDisabled):
		s.write(w, http.StatusServiceUnavailable, apiError{"source_disabled", "Source is disabled"})
	case err != nil:
		s.write(w, http.StatusInternalServerError, apiError{"internal", "Internal error"})
	default:
		s.write(w, http.StatusOK, quoteBody{
			Source: string(q.Source), Pair: string(q.Pair), Price: market.FormatPrice(q.Price),
			QuotedAt: q.QuotedAt.UTC().Format(time.RFC3339),
		})
	}
}

func (s *server) writeState(w http.ResponseWriter) {
	st := s.m.State()
	s.write(w, http.StatusOK, stateBody{
		Sources: map[string]sourceState{
			string(market.Coinbase): {st.Enabled[market.Coinbase]},
			string(market.OKX):      {st.Enabled[market.OKX]},
		},
		DivergenceBps: st.DivergenceBps,
	})
}

func (s *server) state(w http.ResponseWriter, _ *http.Request) { s.writeState(w) }

func (s *server) setEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		src := market.Source(r.PathValue("source"))
		if err := s.m.SetEnabled(src, enabled); err != nil {
			s.write(w, http.StatusNotFound, apiError{"not_found", "Unknown source"})
			return
		}
		s.log.Info("source switched", "source", src, "enabled", enabled)
		s.writeState(w)
	}
}

func (s *server) divergence(w http.ResponseWriter, r *http.Request) {
	var body struct {
		BPS *int `json:"bps"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil || body.BPS == nil {
		s.write(w, http.StatusBadRequest, apiError{"invalid_request", "Body must be {\"bps\": <integer>}"})
		return
	}
	if err := s.m.SetDivergence(*body.BPS); err != nil {
		s.write(w, http.StatusBadRequest, apiError{"invalid_request", "bps must be between -5000 and 5000"})
		return
	}
	s.log.Info("divergence set", "bps", *body.BPS)
	s.writeState(w)
}

func (s *server) reset(w http.ResponseWriter, _ *http.Request) {
	s.m.Reset()
	s.log.Info("normal mode restored")
	s.writeState(w)
}
