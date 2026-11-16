// Package client is a minimal HTTP client for the merchant-service API
// (contracts/openapi/merchant-service.yaml). It handles 429 by waiting
// Retry-After and never puts keys, tokens or response bodies into errors.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// APIError is a non-2xx answer in the contract's Error shape.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("merchant-service: HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// Merchant is the Merchant schema.
type Merchant struct {
	MerchantID           string  `json:"merchantId"`
	Name                 string  `json:"name"`
	Email                string  `json:"email"`
	Xpub                 *string `json:"xpub"`
	WebhookURL           *string `json:"webhookUrl"`
	ToleranceBps         int     `json:"toleranceBps"`
	WebhookSecretVersion int     `json:"webhookSecretVersion"`
}

// Registered is the RegisterMerchantResponse schema.
type Registered struct {
	MerchantID string `json:"merchantId"`
	APIKey     struct {
		Key string `json:"key"`
	} `json:"apiKey"`
}

// Client talks to one merchant-service base URL.
type Client struct {
	base string
	http *http.Client
	// Sleep waits d or until ctx is done; replaced in tests.
	Sleep func(ctx context.Context, d time.Duration) error
	// MaxAttempts bounds retries on 429 per request.
	MaxAttempts int
}

// New returns a client; hc may be nil.
func New(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{base: strings.TrimRight(baseURL, "/"), http: hc, Sleep: sleep, MaxAttempts: 20}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Register creates a merchant and its first API key (the key is shown once).
func (c *Client) Register(ctx context.Context, name, email string) (Registered, error) {
	var out Registered
	err := c.do(ctx, http.MethodPost, "/api/v1/merchants", "",
		map[string]string{"name": name, "email": email}, &out)
	return out, err
}

// Token exchanges an API key for a JWT.
func (c *Client) Token(ctx context.Context, apiKey string) (string, error) {
	var out struct {
		AccessToken string `json:"accessToken"`
	}
	err := c.do(ctx, http.MethodPost, "/api/v1/auth/token", "", map[string]string{"apiKey": apiKey}, &out)
	return out.AccessToken, err
}

// Me returns the settings of the authenticated merchant.
func (c *Client) Me(ctx context.Context, jwt string) (Merchant, error) {
	var out Merchant
	err := c.do(ctx, http.MethodGet, "/api/v1/merchants/me", jwt, nil, &out)
	return out, err
}

// SetXpub uploads the xpub and returns the first derived address.
func (c *Client) SetXpub(ctx context.Context, jwt, xpub string) (string, error) {
	var out struct {
		FirstAddress string `json:"firstAddress"`
	}
	err := c.do(ctx, http.MethodPut, "/api/v1/merchants/me/xpub", jwt, map[string]string{"xpub": xpub}, &out)
	return out.FirstAddress, err
}

// SetWebhook sets the webhook URL and the tolerance.
func (c *Client) SetWebhook(ctx context.Context, jwt, url string, toleranceBps int) error {
	return c.do(ctx, http.MethodPut, "/api/v1/merchants/me/webhook", jwt,
		map[string]any{"webhookUrl": url, "toleranceBps": toleranceBps}, nil)
}

func (c *Client) do(ctx context.Context, method, path, jwt string, in, out any) error {
	var payload []byte
	if in != nil {
		var err error
		if payload, err = json.Marshal(in); err != nil {
			return err
		}
	}
	for attempt := 1; ; attempt++ {
		wait, err := c.once(ctx, method, path, jwt, payload, out)
		if wait == 0 {
			return err
		}
		if attempt >= c.MaxAttempts {
			return fmt.Errorf("%s %s: still rate limited after %d attempts: %w", method, path, attempt, err)
		}
		if serr := c.Sleep(ctx, wait); serr != nil {
			return serr
		}
	}
}

// once makes one request; a non-zero wait means "429, retry after wait".
func (c *Client) once(ctx context.Context, method, path, jwt string, payload []byte, out any) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// The URL has no secrets; the request body is not part of the error.
		return 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode/100 == 2 {
		if out == nil || len(body) == 0 {
			return 0, nil
		}
		if err := json.Unmarshal(body, out); err != nil {
			return 0, fmt.Errorf("%s %s: decode response: %w", method, path, err)
		}
		return 0, nil
	}
	ae := &APIError{Status: resp.StatusCode}
	var e struct{ Code, Message string }
	if json.Unmarshal(body, &e) == nil {
		ae.Code, ae.Message = e.Code, e.Message
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		secs, perr := strconv.Atoi(resp.Header.Get("Retry-After"))
		if perr != nil || secs < 1 {
			secs = 1
		}
		return time.Duration(secs) * time.Second, ae
	}
	return 0, ae
}

// IsStatus reports whether err is an APIError with the given status.
func IsStatus(err error, status int) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == status
}
