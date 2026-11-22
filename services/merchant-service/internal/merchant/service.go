package merchant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/hdwallet"
)

// ValidationError is a rejected input field. Message never contains the value.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Message }

func invalid(field, msg string) error { return &ValidationError{Field: field, Message: msg} }

// Limits of the contract (contracts/openapi/merchant-service.yaml).
const (
	maxNameLen  = 200
	maxEmailLen = 254
	maxURLLen   = 2048
)

// Config holds the numbers of SRS 4.1.4 and 4.1.10.
type Config struct {
	KeyRotationGrace    time.Duration
	MaxActiveKeys       int
	DefaultToleranceBps int
	MaxToleranceBps     int
}

// Service implements the use cases UC-06 and UC-07.
type Service struct {
	store   Store
	secrets SecretStore
	cfg     Config
	log     *slog.Logger
	now     func() time.Time
}

// NewService returns a Service.
func NewService(store Store, cfg Config, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, cfg: cfg, log: log, now: now}
}

// now is UTC with the microsecond precision of PostgreSQL, so that returned and stored times are equal.
func now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// IssuedKey is a new API key; Key is shown once and never stored.
type IssuedKey struct {
	APIKey
	Key string
}

func (s *Service) newKey(merchantID uuid.UUID) (IssuedKey, NewKey) {
	key, hash := GenerateKey()
	k := APIKey{ID: uuid.New(), MerchantID: merchantID, CreatedAt: s.now().UTC()}
	return IssuedKey{APIKey: k, Key: key}, NewKey{APIKey: k, Hash: hash}
}

// Register creates a merchant and its first API key (UC-06). The merchant is not published yet:
// merchant.updated needs an xpub.
func (s *Service) Register(ctx context.Context, name, email string) (Merchant, IssuedKey, error) {
	if err := validateName(name); err != nil {
		return Merchant{}, IssuedKey{}, err
	}
	if err := validateEmail(email); err != nil {
		return Merchant{}, IssuedKey{}, err
	}
	m := Merchant{
		ID: uuid.New(), Name: name, Email: email,
		ToleranceBps: s.cfg.DefaultToleranceBps, WebhookSecretVersion: 1, Version: 1, CreatedAt: s.now().UTC(),
	}
	if s.secrets != nil {
		// The first secret version exists in the store from the start, so that every later version is the
		// store's own version number. The merchant sees a secret only after the first rotation.
		secret, err := GenerateSecret()
		if err != nil {
			return Merchant{}, IssuedKey{}, err
		}
		if m.WebhookSecretVersion, err = s.secrets.PutWebhookSecret(ctx, m.ID, secret); err != nil {
			return Merchant{}, IssuedKey{}, err
		}
	}
	issued, stored := s.newKey(m.ID)
	if err := s.store.Create(ctx, m, stored); err != nil {
		return Merchant{}, IssuedKey{}, err
	}
	s.log.InfoContext(ctx, "merchant registered", "merchant_id", m.ID.String(), "key_id", issued.ID.String())
	return m, issued, nil
}

func validateName(name string) error {
	n := utf8.RuneCountInString(name)
	switch {
	case !utf8.ValidString(name) || strings.TrimSpace(name) == "":
		return invalid("name", "must not be empty")
	case n > maxNameLen:
		return invalid("name", fmt.Sprintf("must be at most %d characters", maxNameLen))
	}
	return nil
}

func validateEmail(email string) error {
	if len(email) > maxEmailLen {
		return invalid("email", fmt.Sprintf("must be at most %d characters", maxEmailLen))
	}
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email || addr.Name != "" {
		return invalid("email", "must be an email address")
	}
	return nil
}

// Authenticate returns the merchant of an active API key (UC-07). Any failure is ErrBadAPIKey.
func (s *Service) Authenticate(ctx context.Context, apiKey string) (uuid.UUID, error) {
	if !wellFormedKey(apiKey) {
		return uuid.Nil, ErrBadAPIKey
	}
	id, err := s.store.KeyOwner(ctx, HashKey(apiKey), s.now())
	if errors.Is(err, ErrNotFound) {
		return uuid.Nil, ErrBadAPIKey
	}
	return id, err
}

// Get returns the merchant.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Merchant, error) {
	return s.store.Get(ctx, id)
}

// SetXpub validates and stores the xpub and returns the first derived address (index 0 of the external
// chain). A private key (xprv, tprv) is rejected; the value is never logged or put into errors.
func (s *Service) SetXpub(ctx context.Context, id uuid.UUID, xpub string) (Merchant, string, error) {
	key, err := hdwallet.Parse(xpub)
	switch {
	case errors.Is(err, hdwallet.ErrPrivateKey):
		return Merchant{}, "", invalid("xpub", "a private key is not accepted, upload the extended public key")
	case err != nil:
		return Merchant{}, "", invalid("xpub", "must be a valid BIP-32 extended public key")
	}
	first, err := key.DeriveAddress(0)
	if err != nil {
		return Merchant{}, "", invalid("xpub", "no address can be derived at index 0")
	}
	canonical := key.String()
	m, err := s.store.Update(ctx, id, func(m *Merchant) error {
		m.Xpub = &canonical
		return nil
	})
	if err != nil {
		return Merchant{}, "", err
	}
	s.log.InfoContext(ctx, "xpub set", "merchant_id", id.String(), "first_address", first)
	return m, first, nil
}

// SetWebhook stores the webhook URL and the tolerance; a nil tolerance means the default (SRS 4.1.4).
func (s *Service) SetWebhook(ctx context.Context, id uuid.UUID, webhookURL string, toleranceBps *int) (Merchant, error) {
	if err := validateWebhookURL(webhookURL); err != nil {
		return Merchant{}, err
	}
	tol := s.cfg.DefaultToleranceBps
	if toleranceBps != nil {
		tol = *toleranceBps
	}
	if tol < 0 || tol > s.cfg.MaxToleranceBps {
		return Merchant{}, invalid("toleranceBps", fmt.Sprintf("must be between 0 and %d", s.cfg.MaxToleranceBps))
	}
	m, err := s.store.Update(ctx, id, func(m *Merchant) error {
		m.WebhookURL = &webhookURL
		m.ToleranceBps = tol
		return nil
	})
	if err != nil {
		return Merchant{}, err
	}
	s.log.InfoContext(ctx, "webhook settings set", "merchant_id", id.String(), "tolerance_bps", tol)
	return m, nil
}

// validateWebhookURL accepts an absolute https URL with a host and without whitespace (SRS 4.1.10: the
// webhook URL is set only here).
func validateWebhookURL(raw string) error {
	if len(raw) > maxURLLen {
		return invalid("webhookUrl", fmt.Sprintf("must be at most %d characters", maxURLLen))
	}
	if strings.IndexFunc(raw, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return invalid("webhookUrl", "must not contain whitespace")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.Opaque != "" {
		return invalid("webhookUrl", "must be an absolute https URL")
	}
	return nil
}

// ListKeys returns the active and expiring keys.
func (s *Service) ListKeys(ctx context.Context, id uuid.UUID) ([]APIKey, error) {
	return s.store.ActiveKeys(ctx, id, s.now())
}

// IssueKey issues a new key for a planned rotation (SRS 4.1.10).
func (s *Service) IssueKey(ctx context.Context, id uuid.UUID) (IssuedKey, error) {
	issued, stored := s.newKey(id)
	now := s.now()
	err := s.store.ChangeKeys(ctx, id, now, func(active []APIKey) (KeyChanges, error) {
		return PlanIssue(active, stored, now.UTC(), s.cfg.KeyRotationGrace, s.cfg.MaxActiveKeys)
	})
	if err != nil {
		return IssuedKey{}, err
	}
	s.log.InfoContext(ctx, "api key issued", "merchant_id", id.String(), "key_id", issued.ID.String())
	return issued, nil
}

// RevokeKey revokes a key at once (emergency revoke). Issued JWTs are not revoked and expire on their own.
func (s *Service) RevokeKey(ctx context.Context, id, keyID uuid.UUID) error {
	err := s.store.ChangeKeys(ctx, id, s.now(), func(active []APIKey) (KeyChanges, error) {
		return PlanRevoke(active, keyID)
	})
	if err != nil {
		return err
	}
	s.log.InfoContext(ctx, "api key revoked", "merchant_id", id.String(), "key_id", keyID.String())
	return nil
}
