// Package setup registers the stand merchants and configures them (SRS 8.1):
// xpub from the test mnemonic, webhook URL of merchant-mock, tolerance.
package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/m5tshift/slopay/pkg/hdwallet"
	"github.com/m5tshift/slopay/testkit/stand-setup/internal/client"
)

// API is what the setup needs from merchant-service.
type API interface {
	Register(ctx context.Context, name, email string) (client.Registered, error)
	Token(ctx context.Context, apiKey string) (string, error)
	Me(ctx context.Context, jwt string) (client.Merchant, error)
	SetXpub(ctx context.Context, jwt, xpub string) (string, error)
	SetWebhook(ctx context.Context, jwt, url string, toleranceBps int) error
}

// Xpubs gives the account-level xpub for the account index.
type Xpubs interface {
	AccountXpub(account uint32) (string, error)
}

// Entry is one record of the keys file. It holds a secret: the file lives
// outside git and is written with mode 0600.
type Entry struct {
	Name       string `json:"name"`
	MerchantID string `json:"merchantId"`
	APIKey     string `json:"apiKey"`
}

// KeysFile is the JSON document with the issued API keys.
type KeysFile struct {
	Merchants []Entry `json:"merchants"`
}

// Config is the run configuration.
type Config struct {
	Count        int // regular merchants (SRS 8.1: 60)
	Extra        int // additional merchants for 8.3
	NamePrefix   string
	EmailDomain  string
	WebhookURL   string
	ToleranceBps int
	KeysPath     string
}

// Names returns merchant names in order: regular ones, then the extra ones.
func (c Config) Names() []string {
	var out []string
	for i := 1; i <= c.Count; i++ {
		out = append(out, fmt.Sprintf("%s-%03d", c.NamePrefix, i))
	}
	for i := 1; i <= c.Extra; i++ {
		out = append(out, fmt.Sprintf("%s-extra-%02d", c.NamePrefix, i))
	}
	return out
}

// Validate checks the configuration.
func (c Config) Validate() error {
	switch {
	case c.Count < 0 || c.Extra < 0 || c.Count+c.Extra == 0:
		return errors.New("setup: nothing to register")
	case c.NamePrefix == "" || c.EmailDomain == "":
		return errors.New("setup: name prefix and email domain are required")
	case c.WebhookURL == "":
		return errors.New("setup: webhook URL is required")
	case c.ToleranceBps < 0 || c.ToleranceBps > 500:
		return errors.New("setup: tolerance must be 0..500 bps")
	case c.KeysPath == "":
		return errors.New("setup: keys file path is required")
	}
	return nil
}

// Result counts what a run did.
type Result struct{ Registered, Reused, Configured, Unchanged int }

// Run registers the missing merchants and brings every one to the wanted
// settings. It is idempotent: merchants found in the keys file are not
// registered again and settings that already match are not written.
func Run(ctx context.Context, api API, wallet Xpubs, cfg Config, log *slog.Logger) (Result, error) {
	var res Result
	if err := cfg.Validate(); err != nil {
		return res, err
	}
	keys, err := LoadKeys(cfg.KeysPath)
	if err != nil {
		return res, err
	}
	byName := map[string]*Entry{}
	for i := range keys.Merchants {
		byName[keys.Merchants[i].Name] = &keys.Merchants[i]
	}
	for account, name := range cfg.Names() {
		e, ok := byName[name]
		if ok {
			res.Reused++
		} else {
			reg, err := api.Register(ctx, name, fmt.Sprintf("%s@%s", name, cfg.EmailDomain))
			if err != nil {
				return res, fmt.Errorf("register %s: %w", name, err)
			}
			keys.Merchants = append(keys.Merchants, Entry{Name: name, MerchantID: reg.MerchantID, APIKey: reg.APIKey.Key})
			// Save at once: the key is shown only once.
			if err := SaveKeys(cfg.KeysPath, keys); err != nil {
				return res, fmt.Errorf("save keys after registering %s: %w", name, err)
			}
			e = &keys.Merchants[len(keys.Merchants)-1]
			byName[name] = e
			res.Registered++
		}
		changed, err := configure(ctx, api, wallet, cfg, e, uint32(account)) //nolint:gosec // account is a small loop index
		if err != nil {
			return res, fmt.Errorf("configure %s: %w", name, err)
		}
		if changed {
			res.Configured++
		} else {
			res.Unchanged++
		}
		log.Info("merchant ready", "name", name, "merchantId", e.MerchantID, "changed", changed)
	}
	return res, nil
}

func configure(ctx context.Context, api API, w Xpubs, cfg Config, e *Entry, account uint32) (bool, error) {
	xpub, err := w.AccountXpub(account)
	if err != nil {
		return false, err
	}
	if err := hdwallet.Validate(xpub); err != nil {
		return false, fmt.Errorf("derived xpub is invalid: %w", err)
	}
	jwt, err := api.Token(ctx, e.APIKey)
	if err != nil {
		return false, fmt.Errorf("token: %w", err)
	}
	me, err := api.Me(ctx, jwt)
	if err != nil {
		return false, fmt.Errorf("read settings: %w", err)
	}
	changed := false
	if me.Xpub == nil || *me.Xpub != xpub {
		first, err := api.SetXpub(ctx, jwt, xpub)
		if err != nil {
			return false, fmt.Errorf("set xpub: %w", err)
		}
		want, err := hdwallet.Derive(xpub, 0)
		if err != nil {
			return false, err
		}
		if first != want {
			return false, fmt.Errorf("first address %s differs from the expected %s", first, want)
		}
		changed = true
	}
	if me.WebhookURL == nil || *me.WebhookURL != cfg.WebhookURL || me.ToleranceBps != cfg.ToleranceBps {
		if err := api.SetWebhook(ctx, jwt, cfg.WebhookURL, cfg.ToleranceBps); err != nil {
			return false, fmt.Errorf("set webhook: %w", err)
		}
		changed = true
	}
	return changed, nil
}

// LoadKeys reads the keys file; a missing file is an empty one.
func LoadKeys(path string) (*KeysFile, error) {
	b, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied argument
	if errors.Is(err, os.ErrNotExist) {
		return &KeysFile{}, nil
	}
	if err != nil {
		return nil, err
	}
	var k KeysFile
	if err := json.Unmarshal(b, &k); err != nil {
		return nil, fmt.Errorf("keys file %s is not valid JSON", path)
	}
	return &k, nil
}

// SaveKeys writes the keys file atomically with mode 0600.
func SaveKeys(path string, k *KeysFile) error {
	b, err := json.MarshalIndent(k, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".keys-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// InsideGitRepo reports whether path lies in a git work tree: secrets must
// stay out of git.
func InsideGitRepo(path string) (bool, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	for dir := filepath.Dir(abs); ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return true, nil
		}
		if dir == filepath.Dir(dir) {
			return false, nil
		}
	}
}
