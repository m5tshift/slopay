// Package openbao is a minimal client of OpenBao: the KV v2 engine for webhook signing secrets and the
// Transit engine for the JWT signing key (SRS 4.1.10, INF-2.4). webhook-service reads the secrets by the
// version carried in merchant.updated.
package openbao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SecretsPrefix is the path prefix inside the KV mount: <mount>/data/webhook/<merchantId>.
const SecretsPrefix = "webhook"

// ErrUnavailable is returned when OpenBao cannot take the secret (sealed, unreachable, token rejected).
// It never contains the secret.
var ErrUnavailable = errors.New("openbao: unavailable")

// Config is the connection to OpenBao.
type Config struct {
	// Addr is the base URL, e.g. https://openbao.openbao.svc:8200.
	Addr string
	// TokenFile is the mounted file with the token; it is read on every request, so a token renewed
	// by External Secrets is picked up without restart (SRS INF-2.4).
	TokenFile string
	// Mount is the KV v2 mount, default "secret".
	Mount string
	// HTTP is the client; default has a 5 s timeout.
	HTTP *http.Client
}

// Client writes webhook secrets to OpenBao KV v2.
type Client struct {
	cfg  Config
	base *url.URL
}

// New validates cfg and returns a Client.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.Addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("openbao: address must be an http(s) URL")
	}
	if cfg.TokenFile == "" {
		return nil, errors.New("openbao: token file is required")
	}
	if cfg.Mount == "" {
		cfg.Mount = "secret"
	}
	cfg.Mount = strings.Trim(cfg.Mount, "/")
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{cfg: cfg, base: u}, nil
}

// Path is the KV v2 API path of the webhook secret of a merchant. Every write creates a new KV version.
func Path(mount string, merchantID uuid.UUID) string {
	return fmt.Sprintf("/v1/%s/data/%s/%s", strings.Trim(mount, "/"), SecretsPrefix, merchantID)
}

// PutWebhookSecret writes the secret to <mount>/data/webhook/<merchantId> and returns the KV version it got.
// That version is the secret version published in merchant.updated; older versions stay readable for the
// 24 h the merchant accepts both.
func (c *Client) PutWebhookSecret(ctx context.Context, merchantID uuid.UUID, secret string) (int, error) {
	body, err := json.Marshal(map[string]any{"data": map[string]any{"secret": secret}})
	if err != nil {
		return 0, err
	}
	out, err := c.do(ctx, http.MethodPost, Path(c.cfg.Mount, merchantID), body)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Data struct {
			Version int `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil || resp.Data.Version < 1 {
		return 0, fmt.Errorf("%w: no version in response", ErrUnavailable)
	}
	return resp.Data.Version, nil
}

// do sends one request with the current token and returns the body of a 2xx answer. Errors are
// ErrUnavailable and never contain the request body.
func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	tok, err := os.ReadFile(c.cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("%w: read token file", ErrUnavailable)
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", strings.TrimSpace(string(tok)))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		// The transport error may quote the URL, not the body; keep only the fact.
		return nil, fmt.Errorf("%w: request failed", ErrUnavailable)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}
	return out, nil
}
