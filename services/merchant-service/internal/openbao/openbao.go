// Package openbao is a minimal client of the OpenBao KV v2 engine: it writes webhook signing secrets
// (SRS 4.1.10, INF-2.4). webhook-service reads them by the version carried in merchant.updated.
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

// SecretsPrefix is the path prefix inside the KV mount: <mount>/data/webhook-secrets/<merchantId>/<version>.
const SecretsPrefix = "webhook-secrets"

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

// Path is the KV v2 API path of one version of a merchant secret.
func Path(mount string, merchantID uuid.UUID, version int) string {
	return fmt.Sprintf("/v1/%s/data/%s/%s/%d", strings.Trim(mount, "/"), SecretsPrefix, merchantID, version)
}

// PutWebhookSecret stores the secret of the given version. Each version has its own path, so the previous
// version stays readable for the 24 h the merchant accepts both. Writing the same version again overwrites it
// (a version whose event was never published).
func (c *Client) PutWebhookSecret(ctx context.Context, merchantID uuid.UUID, version int, secret string) error {
	tok, err := os.ReadFile(c.cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("%w: read token file", ErrUnavailable)
	}
	body, err := json.Marshal(map[string]any{"data": map[string]any{"secret": secret, "version": version}})
	if err != nil {
		return err
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + Path(c.cfg.Mount, merchantID, version)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Vault-Token", strings.TrimSpace(string(tok)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		// The transport error may quote the URL, not the secret; keep only the fact.
		return fmt.Errorf("%w: request failed", ErrUnavailable)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}
	return nil
}
