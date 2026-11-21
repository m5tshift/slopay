// Package api is the HTTP layer of payer-sim, contracts/openapi/payer-sim.yaml.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/testkit/payer-sim/internal/payer"
)

const maxBodyBytes = 1 << 10

// Payer is what the API needs from the payer.
type Payer interface {
	Pay(ctx context.Context, invoiceID string) (payer.Payment, error)
	Payments() []payer.Payment
	Replay(ctx context.Context, invoiceID string) (payer.ReplayResult, error)
}

type server struct {
	p   Payer
	log *slog.Logger
}

// Register mounts the API of contracts/openapi/payer-sim.yaml on mux.
func Register(mux *http.ServeMux, p Payer, log *slog.Logger) {
	s := &server{p: p, log: log}
	mux.HandleFunc("POST /payments", s.create)
	mux.HandleFunc("GET /payments", s.list)
	mux.HandleFunc("POST /control/replay", s.replay)
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type paymentBody struct {
	InvoiceID string `json:"invoiceId"`
	Outcome   string `json:"outcome"`
	TxHash    string `json:"txHash,omitempty"`
	Amount    string `json:"amount,omitempty"`
}

type idBody struct {
	InvoiceID string `json:"invoiceId"`
}

func (s *server) write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Warn("write response", "error", err)
	}
}

func toBody(p payer.Payment) paymentBody {
	b := paymentBody{InvoiceID: p.InvoiceID, Outcome: string(p.Outcome)}
	if p.Amount != nil {
		b.TxHash, b.Amount = p.TxHash.Hex(), p.Amount.String()
	}
	return b
}

// readID decodes {"invoiceId"}; an empty body is allowed when optional.
func (s *server) readID(w http.ResponseWriter, r *http.Request, optional bool) (string, bool) {
	var b idBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	err := dec.Decode(&b)
	if err != nil && (!optional || !errors.Is(err, io.EOF)) {
		s.write(w, http.StatusBadRequest, apiError{"invalid_request", "Body must be JSON with invoiceId"})
		return "", false
	}
	if b.InvoiceID == "" && optional {
		return "", true
	}
	if _, err := uuid.Parse(b.InvoiceID); err != nil {
		s.write(w, http.StatusBadRequest, apiError{"invalid_request", "invoiceId must be a UUID"})
		return "", false
	}
	return b.InvoiceID, true
}

func (s *server) create(w http.ResponseWriter, r *http.Request) {
	id, ok := s.readID(w, r, false)
	if !ok {
		return
	}
	pay, err := s.p.Pay(r.Context(), id)
	if err != nil {
		s.log.Warn("pay invoice", "invoice_id", id, "error", err)
		s.write(w, http.StatusBadGateway, apiError{"upstream_failed", errMessage(err)})
		return
	}
	s.write(w, http.StatusOK, toBody(pay))
}

func errMessage(err error) string {
	if errors.Is(err, payer.ErrNotETH) {
		return "Invoice is not an ETH invoice"
	}
	return "Paying the invoice failed"
}

func (s *server) list(w http.ResponseWriter, _ *http.Request) {
	out := []paymentBody{}
	for _, p := range s.p.Payments() {
		out = append(out, toBody(p))
	}
	s.write(w, http.StatusOK, out)
}

func (s *server) replay(w http.ResponseWriter, r *http.Request) {
	id, ok := s.readID(w, r, true)
	if !ok {
		return
	}
	res, err := s.p.Replay(r.Context(), id)
	if err != nil {
		s.log.Warn("replay", "error", err)
		s.write(w, http.StatusBadGateway, apiError{"upstream_failed", "Send transaction failed"})
		return
	}
	s.write(w, http.StatusOK, map[string]int{"sent": res.Sent, "known": res.Known})
}
