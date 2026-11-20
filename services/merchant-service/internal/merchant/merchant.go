// Package merchant is the domain of merchant-service (SRS 4.1.3, 4.1.10, UC-06, UC-07): registration,
// API keys stored as hashes, xpub and webhook settings, and the merchant.updated event of the outbox.
package merchant

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Merchant is the merchant state.
type Merchant struct {
	ID    uuid.UUID
	Name  string
	Email string
	// Xpub is nil until uploaded.
	Xpub *string
	// WebhookURL is nil until set.
	WebhookURL           *string
	ToleranceBps         int
	WebhookSecretVersion int
	// Version is 1 on registration and grows with every change of the published state.
	Version   int64
	CreatedAt time.Time
}

// PublishedEqual reports whether the state carried by merchant.updated is the same.
func PublishedEqual(a, b Merchant) bool {
	return eqPtr(a.Xpub, b.Xpub) && eqPtr(a.WebhookURL, b.WebhookURL) &&
		a.ToleranceBps == b.ToleranceBps && a.WebhookSecretVersion == b.WebhookSecretVersion
}

func eqPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// APIKey is a stored API key; the key itself is never stored, only its hash.
type APIKey struct {
	ID         uuid.UUID
	MerchantID uuid.UUID
	CreatedAt  time.Time
	// ExpiresAt is set on the previous key when a new one is issued (planned rotation).
	ExpiresAt *time.Time
}

// Key statuses of the contract.
const (
	StatusActive   = "active"
	StatusExpiring = "expiring"
)

// Status is active, or expiring for a key that is deactivated at ExpiresAt.
func (k APIKey) Status() string {
	if k.ExpiresAt != nil {
		return StatusExpiring
	}
	return StatusActive
}

// NewKey is a key to insert together with its hash.
type NewKey struct {
	APIKey
	Hash []byte
}

// KeyChanges are the changes to the API keys of one merchant, applied in one transaction.
type KeyChanges struct {
	Insert *NewKey
	// SetExpiry sets (non-nil) or clears (nil) the expiry of keys.
	SetExpiry map[uuid.UUID]*time.Time
	Revoke    []uuid.UUID
}

// Errors of the store and the service.
var (
	ErrNotFound    = errors.New("merchant: not found")
	ErrKeyNotFound = errors.New("merchant: api key not found")
	ErrTooManyKeys = errors.New("merchant: too many active api keys")
	ErrLastKey     = errors.New("merchant: the last active api key cannot be revoked")
	ErrBadAPIKey   = errors.New("merchant: invalid api key")
)

// Store keeps merchants and API keys. Implementations must lock the merchant for Update and ChangeKeys,
// so that changes of one merchant are serialised and its outbox events keep their order.
type Store interface {
	// Create stores a new merchant with its first API key.
	Create(ctx context.Context, m Merchant, key NewKey) error
	// Get returns the merchant or ErrNotFound.
	Get(ctx context.Context, id uuid.UUID) (Merchant, error)
	// Update locks the merchant and applies fn. When the published state changed, it increments Version,
	// saves the merchant and, if the merchant has an xpub, adds merchant.updated to the outbox, all in one
	// transaction. It returns the resulting state.
	Update(ctx context.Context, id uuid.UUID, fn func(m *Merchant) error) (Merchant, error)
	// KeyOwner returns the merchant of the key with the hash if the key is active at now, else ErrNotFound.
	KeyOwner(ctx context.Context, hash []byte, now time.Time) (uuid.UUID, error)
	// ActiveKeys returns the keys of the merchant that are active at now, oldest first.
	ActiveKeys(ctx context.Context, merchantID uuid.UUID, now time.Time) ([]APIKey, error)
	// ChangeKeys locks the merchant, passes its active keys (oldest first) to fn and applies the changes
	// fn returns. ErrNotFound when there is no such merchant.
	ChangeKeys(ctx context.Context, merchantID uuid.UUID, now time.Time, fn func(active []APIKey) (KeyChanges, error)) error
}
