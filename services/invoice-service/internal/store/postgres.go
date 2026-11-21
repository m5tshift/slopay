// Package store is the PostgreSQL implementation of invoice.Store. A new invoice and its invoice.created
// go to the invoices and outbox tables in one transaction (SRS 4.1.5).
package store

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/outbox"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
)

// Postgres is an invoice.Store on a pgx pool.
type Postgres struct {
	pool  *pgxpool.Pool
	codec *kafka.Codec
	now   func() time.Time
}

var _ invoice.Store = (*Postgres)(nil)

// New returns the store. The codec validates events before they are written to the outbox.
func New(pool *pgxpool.Pool, codec *kafka.Codec) *Postgres {
	return &Postgres{pool: pool, codec: codec, now: time.Now}
}

const invoiceColumns = `id, merchant_id, order_id, amount_usd_cents, asset_kind, COALESCE(asset_contract, ''),
asset_decimals, rate::text, rate_quoted_at, amount::text, address, derivation_index, status, sequence, confirmations,
received_amount::text, overpaid, created_at, expires_at`

func scanInvoice(row pgx.Row, extra ...any) (invoice.Invoice, error) {
	var (
		inv              invoice.Invoice
		amount, received string
		index            int64
		status           string
	)
	dest := []any{&inv.ID, &inv.MerchantID, &inv.OrderID, &inv.AmountUSDCents, &inv.Asset.Kind, &inv.Asset.Contract,
		&inv.Asset.Decimals, &inv.Rate, &inv.RateQuotedAt, &amount, &inv.Address, &index, &status, &inv.Sequence,
		&inv.Confirmations, &received, &inv.Overpaid, &inv.CreatedAt, &inv.ExpiresAt}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return invoice.Invoice{}, invoice.ErrNotFound
		}
		return invoice.Invoice{}, fmt.Errorf("store: read invoice: %w", err)
	}
	var ok bool
	if inv.Amount, ok = new(big.Int).SetString(amount, 10); !ok {
		return invoice.Invoice{}, fmt.Errorf("store: invoice %s: invalid amount %q", inv.ID, amount)
	}
	if inv.Received, ok = new(big.Int).SetString(received, 10); !ok {
		return invoice.Invoice{}, fmt.Errorf("store: invoice %s: invalid received amount %q", inv.ID, received)
	}
	inv.DerivationIndex = uint32(index) //nolint:gosec // CHECK constraint: 0 <= derivation_index < 2^31.
	inv.Status = invoice.Status(status)
	inv.RateQuotedAt, inv.CreatedAt, inv.ExpiresAt = inv.RateQuotedAt.UTC(), inv.CreatedAt.UTC(), inv.ExpiresAt.UTC()
	return inv, nil
}

// FindByKey implements invoice.Store.
func (s *Postgres) FindByKey(ctx context.Context, merchantID uuid.UUID, key string) (invoice.Invoice, []byte, error) {
	return findByKey(ctx, s.pool, merchantID, key)
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func findByKey(ctx context.Context, q querier, merchantID uuid.UUID, key string) (invoice.Invoice, []byte, error) {
	var hash []byte
	inv, err := scanInvoice(q.QueryRow(ctx, `SELECT `+invoiceColumns+`, request_hash FROM invoices
WHERE merchant_id = $1 AND idempotency_key = $2`, merchantID, key), &hash)
	return inv, hash, err
}

// Merchant implements invoice.Store.
func (s *Postgres) Merchant(ctx context.Context, id uuid.UUID) (invoice.Merchant, error) {
	m := invoice.Merchant{ID: id}
	err := s.pool.QueryRow(ctx, `SELECT version, xpub, tolerance_bps FROM merchants WHERE id = $1`, id).
		Scan(&m.Version, &m.Xpub, &m.ToleranceBps)
	if errors.Is(err, pgx.ErrNoRows) {
		return invoice.Merchant{}, invoice.ErrNoXpub
	}
	if err != nil {
		return invoice.Merchant{}, fmt.Errorf("store: read merchant: %w", err)
	}
	return m, nil
}

// Create implements invoice.Store. The merchant row lock serialises the creations of a merchant: the key is
// checked again under it and the derivation counter is taken and incremented under it. The unique indexes
// (merchant_id, idempotency_key) and (merchant_id, derivation_index) are the last guard.
func (s *Postgres) Create(ctx context.Context, d invoice.Draft, derive invoice.DeriveFunc) (invoice.Invoice, []byte, bool, error) {
	var (
		result  invoice.Invoice
		hash    []byte
		created bool
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var (
			xpub string
			next int64
		)
		err := tx.QueryRow(ctx, `SELECT xpub, next_derivation_index FROM merchants WHERE id = $1 FOR UPDATE`,
			d.MerchantID).Scan(&xpub, &next)
		if errors.Is(err, pgx.ErrNoRows) {
			return invoice.ErrNoXpub
		}
		if err != nil {
			return fmt.Errorf("store: lock merchant: %w", err)
		}
		existing, h, err := findByKey(ctx, tx, d.MerchantID, d.IdempotencyKey)
		if err == nil {
			result, hash = existing, h
			return nil
		}
		if !errors.Is(err, invoice.ErrNotFound) {
			return err
		}
		addr, index, newNext, err := invoice.NextAddress(xpub, uint64(next), derive) //nolint:gosec // CHECK: 0 <= next <= 2^31.
		if err != nil {
			return err
		}
		inv := d.Invoice
		inv.Address, inv.DerivationIndex = addr, index
		if _, err := tx.Exec(ctx, `UPDATE merchants SET next_derivation_index = $2 WHERE id = $1`,
			d.MerchantID, int64(newNext)); err != nil { //nolint:gosec // newNext <= 2^31.
			return fmt.Errorf("store: advance derivation index: %w", err)
		}
		var contract *string
		if inv.Asset.Contract != "" {
			contract = &inv.Asset.Contract
		}
		if _, err := tx.Exec(ctx, `INSERT INTO invoices (id, merchant_id, idempotency_key, request_hash, order_id,
amount_usd_cents, asset_kind, asset_contract, asset_decimals, rate, rate_quoted_at, amount, address, derivation_index,
status, sequence, confirmations, received_amount, overpaid, created_at, expires_at, deadline, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::numeric, $11, $12::numeric, $13, $14, $15, $16, $17, $18::numeric, $19,
$20, $21, $21, $20)`,
			inv.ID, inv.MerchantID, d.IdempotencyKey, d.RequestHash, inv.OrderID, inv.AmountUSDCents, inv.Asset.Kind,
			contract, inv.Asset.Decimals, inv.Rate, inv.RateQuotedAt, inv.Amount.String(), inv.Address,
			int64(inv.DerivationIndex), string(inv.Status), inv.Sequence, inv.Confirmations, inv.Received.String(),
			inv.Overpaid, inv.CreatedAt, inv.ExpiresAt); err != nil {
			return fmt.Errorf("store: insert invoice: %w", err)
		}
		ev, err := invoice.CreatedEvent(inv)
		if err != nil {
			return err
		}
		if err := outbox.Add(ctx, tx, s.codec, invoice.TopicInvoices, ev); err != nil {
			return err
		}
		result, hash, created = inv, d.RequestHash, true
		return nil
	})
	if err != nil {
		return invoice.Invoice{}, nil, false, err
	}
	return result, hash, created, nil
}

// Get implements invoice.Store.
func (s *Postgres) Get(ctx context.Context, merchantID, id uuid.UUID) (invoice.Invoice, error) {
	return scanInvoice(s.pool.QueryRow(ctx, `SELECT `+invoiceColumns+` FROM invoices WHERE id = $1 AND merchant_id = $2`,
		id, merchantID))
}

// GetPublic implements invoice.Store.
func (s *Postgres) GetPublic(ctx context.Context, id uuid.UUID) (invoice.Invoice, error) {
	return scanInvoice(s.pool.QueryRow(ctx, `SELECT `+invoiceColumns+` FROM invoices WHERE id = $1`, id))
}

// List implements invoice.Store with keyset pagination on (created_at, id).
func (s *Postgres) List(ctx context.Context, merchantID uuid.UUID, f invoice.Filter) ([]invoice.Invoice, error) {
	args := []any{merchantID}
	where := []string{"merchant_id = $1"}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if len(f.Statuses) > 0 {
		st := make([]string, len(f.Statuses))
		for i, v := range f.Statuses {
			st[i] = string(v)
		}
		where = append(where, "status = ANY("+arg(st)+")")
	}
	if f.OrderID != "" {
		where = append(where, "order_id = "+arg(f.OrderID))
	}
	if !f.CreatedFrom.IsZero() {
		where = append(where, "created_at >= "+arg(f.CreatedFrom))
	}
	if !f.CreatedTo.IsZero() {
		where = append(where, "created_at < "+arg(f.CreatedTo))
	}
	if f.After != nil {
		where = append(where, "(created_at, id) < ("+arg(f.After.CreatedAt)+", "+arg(f.After.ID)+")")
	}
	sql := `SELECT ` + invoiceColumns + ` FROM invoices WHERE ` + strings.Join(where, " AND ") +
		` ORDER BY created_at DESC, id DESC LIMIT ` + arg(f.Limit+1)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list invoices: %w", err)
	}
	defer rows.Close()
	var out []invoice.Invoice
	for rows.Next() {
		inv, err := scanInvoice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inv)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list invoices: %w", err)
	}
	return out, nil
}

// UpsertMerchant implements invoice.Store. The derivation counter keeps its value on update.
func (s *Postgres) UpsertMerchant(ctx context.Context, m invoice.Merchant) (bool, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO merchants (id, version, xpub, tolerance_bps, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, xpub = EXCLUDED.xpub,
    tolerance_bps = EXCLUDED.tolerance_bps, updated_at = EXCLUDED.updated_at
WHERE merchants.version < EXCLUDED.version`, m.ID, m.Version, m.Xpub, m.ToleranceBps, s.now().UTC())
	if err != nil {
		return false, fmt.Errorf("store: upsert merchant: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
