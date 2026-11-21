// Package invoice reads the payment data of an invoice from invoice-service (GET /pay/{id}).
package invoice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
)

// Payment is what the payer needs from PaymentPage of contracts/openapi/invoice-service.yaml.
type Payment struct {
	Address string
	Amount  *big.Int
	Asset   string
}

// Client calls invoice-service.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for the service at base (no trailing slash).
func NewClient(base string, h *http.Client) *Client {
	return &Client{base: base, http: h}
}

type page struct {
	Address string `json:"address"`
	Amount  string `json:"amount"`
	Asset   string `json:"asset"`
}

// Get returns the payment data of the invoice.
func (c *Client) Get(ctx context.Context, id string) (Payment, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/pay/"+url.PathEscape(id), nil)
	if err != nil {
		return Payment{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Payment{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Payment{}, fmt.Errorf("invoice-service answered %d", resp.StatusCode)
	}
	var p page
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&p); err != nil {
		return Payment{}, fmt.Errorf("decode payment page: %w", err)
	}
	amount, ok := new(big.Int).SetString(p.Amount, 10)
	if !ok || amount.Sign() < 0 {
		return Payment{}, fmt.Errorf("invalid amount %q", p.Amount)
	}
	return Payment{Address: p.Address, Amount: amount, Asset: p.Asset}, nil
}
