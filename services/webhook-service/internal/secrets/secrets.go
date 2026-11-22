// Package secrets reads webhook signing secrets from OpenBao KV v2 by version (SRS 4.1.10). merchant-service writes
// the secret to <mount>/data/webhook/<merchantId>; every write is a new KV version, and that version is the
// webhookSecretVersion of merchant.updated. webhook-service only reads (policy webhook-service).
package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/m5tshift/weigate/pkg/storage"
)

var (
	// ErrUnavailable is returned when OpenBao cannot give the secret (sealed, unreachable, token rejected).
	ErrUnavailable = errors.New("secrets: openbao unavailable")
	// ErrNotFound is returned when the merchant has no secret of the version.
	ErrNotFound = errors.New("secrets: secret version not found")
	// errDenied is a rejected token: the token file is read again and the request repeated.
	errDenied = errors.New("secrets: token rejected")
)

// Config is the connection to OpenBao.
type Config struct {
	// Addr is the base URL, e.g. http://openbao.secrets.svc:8200.
	Addr string
	// TokenFile is the file with the token; it is read on every request, so a renewed token is picked up
	// without restart (SRS INF-2.4).
	TokenFile string
	// Mount is the KV v2 mount, default "secret".
	Mount string
	// HTTP is the client; default has a 5 s timeout.
	HTTP *http.Client
	// Backoff bounds the repeats of a rejected token; the zero value is 2 min in total.
	Backoff storage.Backoff
}

// Client reads secrets. A KV version never changes, so a secret read once is kept in memory.
type Client struct {
	cfg  Config
	base *url.URL

	mu    sync.Mutex
	cache map[string][]byte
}

// New validates cfg and returns a Client.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.Addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("secrets: address must be an http(s) URL")
	}
	if cfg.TokenFile == "" {
		return nil, errors.New("secrets: token file is required")
	}
	if cfg.Mount == "" {
		cfg.Mount = "secret"
	}
	cfg.Mount = strings.Trim(cfg.Mount, "/")
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{cfg: cfg, base: u, cache: map[string][]byte{}}, nil
}

// Path is the KV v2 API path of the webhook secret of a merchant.
func Path(mount, merchantID string) string {
	return fmt.Sprintf("/v1/%s/data/webhook/%s", strings.Trim(mount, "/"), url.PathEscape(merchantID))
}

// Secret returns the secret bytes of the version as merchant-service issued them. A rejected token is retried with
// backoff; errors never contain the secret or the token.
func (c *Client) Secret(ctx context.Context, merchantID string, version int) ([]byte, error) {
	key := merchantID + ":" + strconv.Itoa(version)
	c.mu.Lock()
	s, ok := c.cache[key]
	c.mu.Unlock()
	if ok {
		return s, nil
	}
	err := storage.Retry(ctx, c.cfg.Backoff, func(err error) bool { return errors.Is(err, errDenied) }, nil,
		func(ctx context.Context) error {
			var err error
			s, err = c.read(ctx, merchantID, version)
			return err
		})
	if errors.Is(err, errDenied) {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, errDenied)
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.cache[key] = s
	c.mu.Unlock()
	return s, nil
}

func (c *Client) read(ctx context.Context, merchantID string, version int) ([]byte, error) {
	tok, err := os.ReadFile(c.cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("%w: read token file", ErrUnavailable)
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + Path(c.cfg.Mount, merchantID)
	u.RawQuery = url.Values{"version": {strconv.Itoa(version)}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build request", ErrUnavailable)
	}
	req.Header.Set("X-Vault-Token", strings.TrimSpace(string(tok)))
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: request failed", ErrUnavailable)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode == http.StatusForbidden:
		return nil, errDenied
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotFound
	case resp.StatusCode/100 != 2:
		return nil, fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}
	var out struct {
		Data struct {
			Data struct {
				Secret string `json:"secret"`
			} `json:"data"`
			Metadata struct {
				Version int `json:"version"`
			} `json:"metadata"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("%w: invalid response", ErrUnavailable)
	}
	// A deleted or destroyed version answers 200 without data.
	if out.Data.Data.Secret == "" || out.Data.Metadata.Version != version {
		return nil, ErrNotFound
	}
	return []byte(out.Data.Data.Secret), nil
}
