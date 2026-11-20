// Package store is the PostgreSQL implementation of merchant.Store. Changes that are published go to the
// outbox table in the same transaction (SRS 4.1.5).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/outbox"
	"github.com/m5tshift/slopay/services/merchant-service/internal/merchant"
)

// Postgres is a merchant.Store on a pgx pool.
type Postgres struct {
	pool  *pgxpool.Pool
	codec *kafka.Codec
	now   func() time.Time
}

var _ merchant.Store = (*Postgres)(nil)

// New returns the store. The codec validates events before they are written to the outbox.
func New(pool *pgxpool.Pool, codec *kafka.Codec) *Postgres {
	return &Postgres{pool: pool, codec: codec, now: time.Now}
}

const merchantColumns = `id, name, email, xpub, webhook_url, tolerance_bps, webhook_secret_version, version, created_at`

func scanMerchant(row pgx.Row) (merchant.Merchant, error) {
	var m merchant.Merchant
	err := row.Scan(&m.ID, &m.Name, &m.Email, &m.Xpub, &m.WebhookURL, &m.ToleranceBps,
		&m.WebhookSecretVersion, &m.Version, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return merchant.Merchant{}, merchant.ErrNotFound
	}
	if err != nil {
		return merchant.Merchant{}, fmt.Errorf("store: read merchant: %w", err)
	}
	m.CreatedAt = m.CreatedAt.UTC()
	return m, nil
}

// Create implements merchant.Store.
func (s *Postgres) Create(ctx context.Context, m merchant.Merchant, key merchant.NewKey) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO merchants (`+merchantColumns+`, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
			m.ID, m.Name, m.Email, m.Xpub, m.WebhookURL, m.ToleranceBps, m.WebhookSecretVersion, m.Version, m.CreatedAt); err != nil {
			return fmt.Errorf("store: insert merchant: %w", err)
		}
		return insertKey(ctx, tx, key)
	})
}

func insertKey(ctx context.Context, tx pgx.Tx, key merchant.NewKey) error {
	if _, err := tx.Exec(ctx, `INSERT INTO api_keys (id, merchant_id, key_hash, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5)`, key.ID, key.MerchantID, key.Hash, key.CreatedAt, key.ExpiresAt); err != nil {
		return fmt.Errorf("store: insert api key: %w", err)
	}
	return nil
}

// Get implements merchant.Store.
func (s *Postgres) Get(ctx context.Context, id uuid.UUID) (merchant.Merchant, error) {
	return scanMerchant(s.pool.QueryRow(ctx, `SELECT `+merchantColumns+` FROM merchants WHERE id = $1`, id))
}

// Update implements merchant.Store.
func (s *Postgres) Update(ctx context.Context, id uuid.UUID, fn func(m *merchant.Merchant) error) (merchant.Merchant, error) {
	var result merchant.Merchant
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		before, err := scanMerchant(tx.QueryRow(ctx, `SELECT `+merchantColumns+` FROM merchants WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		after := before
		if err := fn(&after); err != nil {
			return err
		}
		if merchant.PublishedEqual(before, after) {
			result = before
			return nil
		}
		after.Version++
		if _, err := tx.Exec(ctx, `UPDATE merchants SET xpub = $2, webhook_url = $3, tolerance_bps = $4,
webhook_secret_version = $5, version = $6, updated_at = $7 WHERE id = $1`,
			id, after.Xpub, after.WebhookURL, after.ToleranceBps, after.WebhookSecretVersion, after.Version, s.now().UTC()); err != nil {
			return fmt.Errorf("store: update merchant: %w", err)
		}
		if merchant.Publishable(after) {
			ev, err := merchant.UpdatedEvent(after)
			if err != nil {
				return err
			}
			if err := outbox.Add(ctx, tx, s.codec, merchant.Topic, ev); err != nil {
				return err
			}
		}
		result = after
		return nil
	})
	if err != nil {
		return merchant.Merchant{}, err
	}
	return result, nil
}

const activeKeyCond = `revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2)`

// KeyOwner implements merchant.Store.
func (s *Postgres) KeyOwner(ctx context.Context, hash []byte, now time.Time) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT merchant_id FROM api_keys WHERE key_hash = $1 AND `+activeKeyCond, hash, now).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, merchant.ErrNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("store: find api key: %w", err)
	}
	return id, nil
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func activeKeys(ctx context.Context, q querier, merchantID uuid.UUID, now time.Time) ([]merchant.APIKey, error) {
	rows, err := q.Query(ctx, `SELECT id, merchant_id, created_at, expires_at FROM api_keys
WHERE merchant_id = $1 AND `+activeKeyCond+` ORDER BY created_at, id`, merchantID, now)
	if err != nil {
		return nil, fmt.Errorf("store: list api keys: %w", err)
	}
	keys, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (merchant.APIKey, error) {
		var k merchant.APIKey
		err := r.Scan(&k.ID, &k.MerchantID, &k.CreatedAt, &k.ExpiresAt)
		k.CreatedAt = k.CreatedAt.UTC()
		if k.ExpiresAt != nil {
			t := k.ExpiresAt.UTC()
			k.ExpiresAt = &t
		}
		return k, err
	})
	if err != nil {
		return nil, fmt.Errorf("store: list api keys: %w", err)
	}
	return keys, nil
}

// ActiveKeys implements merchant.Store.
func (s *Postgres) ActiveKeys(ctx context.Context, merchantID uuid.UUID, now time.Time) ([]merchant.APIKey, error) {
	return activeKeys(ctx, s.pool, merchantID, now)
}

// ChangeKeys implements merchant.Store.
func (s *Postgres) ChangeKeys(ctx context.Context, merchantID uuid.UUID, now time.Time,
	fn func(active []merchant.APIKey) (merchant.KeyChanges, error),
) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var locked uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM merchants WHERE id = $1 FOR UPDATE`, merchantID).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return merchant.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("store: lock merchant: %w", err)
		}
		active, err := activeKeys(ctx, tx, merchantID, now)
		if err != nil {
			return err
		}
		ch, err := fn(active)
		if err != nil {
			return err
		}
		for id, exp := range ch.SetExpiry {
			if _, err := tx.Exec(ctx, `UPDATE api_keys SET expires_at = $3 WHERE id = $1 AND merchant_id = $2`,
				id, merchantID, exp); err != nil {
				return fmt.Errorf("store: update api key: %w", err)
			}
		}
		for _, id := range ch.Revoke {
			if _, err := tx.Exec(ctx, `UPDATE api_keys SET revoked_at = $3 WHERE id = $1 AND merchant_id = $2`,
				id, merchantID, now); err != nil {
				return fmt.Errorf("store: revoke api key: %w", err)
			}
		}
		if ch.Insert != nil {
			return insertKey(ctx, tx, *ch.Insert)
		}
		return nil
	})
}
