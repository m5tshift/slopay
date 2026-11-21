// Package store is the PostgreSQL implementation of ledger.Store. Postings, their lines and their events in
// the outbox are written in one transaction (SRS 4.1.5); the schema rejects unbalanced postings and any
// change of a stored one.
package store

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/outbox"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledger"
)

// Postgres is a ledger.Store on a pgx pool.
type Postgres struct {
	pool  *pgxpool.Pool
	codec *kafka.Codec
}

var _ ledger.Store = (*Postgres)(nil)

// New returns the store. The codec validates events before they are written to the outbox.
func New(pool *pgxpool.Pool, codec *kafka.Codec) *Postgres {
	return &Postgres{pool: pool, codec: codec}
}

const insertPosting = `INSERT INTO postings (id, merchant_id, invoice_id, address, block_number, block_hash, tx_hash,
log_index, asset_kind, asset_contract, asset_decimals, amount, fee_bps, fee, posted_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::numeric, $13, $14::numeric, $15)
ON CONFLICT (id) DO NOTHING`

// Record implements ledger.Store.
func (s *Postgres) Record(ctx context.Context, merchant uuid.UUID, postings []ledger.Posting) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		n = 0
		if _, err := tx.Exec(ctx, `INSERT INTO merchants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, merchant); err != nil {
			return fmt.Errorf("store: insert merchant: %w", err)
		}
		if len(postings) == 0 {
			return nil
		}
		// Serialises the outbox events of the merchant, the Kafka key of ledger.events.
		if _, err := tx.Exec(ctx, `SELECT 1 FROM merchants WHERE id = $1 FOR UPDATE`, merchant); err != nil {
			return fmt.Errorf("store: lock merchant: %w", err)
		}
		for _, p := range postings {
			fresh, err := s.insert(ctx, tx, p)
			if err != nil {
				return err
			}
			if fresh {
				n++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// insert writes the posting, its lines and its events; a stored posting ID is skipped (false).
func (s *Postgres) insert(ctx context.Context, tx pgx.Tx, p ledger.Posting) (bool, error) {
	t := p.Transfer
	var logIndex *int64
	if t.LogIndex != nil {
		v := int64(*t.LogIndex) //nolint:gosec // log indexes are small
		logIndex = &v
	}
	tag, err := tx.Exec(ctx, insertPosting, p.ID, p.MerchantID, p.InvoiceID, p.Address, int64(t.BlockNumber), //nolint:gosec // block numbers fit int64
		t.BlockHash, t.TxHash, logIndex, t.Asset.Kind, t.Asset.Contract, t.Asset.Decimals, t.Amount.String(),
		p.FeeBps, p.Fee.String(), p.PostedAt)
	if err != nil {
		return false, fmt.Errorf("store: insert posting %s: %w", p.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	for i, e := range p.Entries {
		if _, err := tx.Exec(ctx, `INSERT INTO entries (posting_id, line, account, side, amount)
VALUES ($1, $2, $3, $4, $5::numeric)`, p.ID, i+1, e.Account, string(e.Side), e.Amount.String()); err != nil {
			return false, fmt.Errorf("store: insert entry of %s: %w", p.ID, err)
		}
	}
	events, err := p.Events()
	if err != nil {
		return false, err
	}
	for _, ev := range events {
		if err := outbox.Add(ctx, tx, s.codec, ledger.TopicLedger, ev); err != nil {
			return false, err
		}
	}
	return true, nil
}

// MerchantExists implements ledger.Store.
func (s *Postgres) MerchantExists(ctx context.Context, merchant uuid.UUID) (bool, error) {
	var ok bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM merchants WHERE id = $1)`, merchant).Scan(&ok); err != nil {
		return false, fmt.Errorf("store: merchant: %w", err)
	}
	return ok, nil
}

const totalsColumns = `asset_kind, asset_contract, asset_decimals, sum(amount)::text, sum(fee)::text, count(*)`

func scanTotals(row pgx.Row) (ledger.Totals, error) {
	var t ledger.Totals
	var received, fee string
	if err := row.Scan(&t.Asset.Kind, &t.Asset.Contract, &t.Asset.Decimals, &received, &fee, &t.Postings); err != nil {
		return t, err
	}
	var err error
	if t.Received, err = parseAmount(received); err != nil {
		return t, err
	}
	t.Fee, err = parseAmount(fee)
	return t, err
}

func parseAmount(s string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("store: amount %q is not an integer", s)
	}
	return v, nil
}

// MerchantTotals implements ledger.Store.
func (s *Postgres) MerchantTotals(ctx context.Context, merchant uuid.UUID, from, to time.Time) ([]ledger.Totals, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+totalsColumns+` FROM postings
WHERE merchant_id = $1 AND posted_at >= $2 AND posted_at < $3
GROUP BY asset_kind, asset_contract, asset_decimals`, merchant, from, to)
	if err != nil {
		return nil, fmt.Errorf("store: merchant totals: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (ledger.Totals, error) { return scanTotals(r) })
	if err != nil {
		return nil, fmt.Errorf("store: merchant totals: %w", err)
	}
	ledger.SortTotals(out)
	return out, nil
}

// AddressTotals implements ledger.Store.
func (s *Postgres) AddressTotals(ctx context.Context, addresses []string) (map[string][]ledger.Totals, error) {
	rows, err := s.pool.Query(ctx, `SELECT address, `+totalsColumns+` FROM postings
WHERE address = ANY($1) GROUP BY address, asset_kind, asset_contract, asset_decimals`, addresses)
	if err != nil {
		return nil, fmt.Errorf("store: address totals: %w", err)
	}
	defer rows.Close()
	out := map[string][]ledger.Totals{}
	for rows.Next() {
		var addr string
		var t ledger.Totals
		var received, fee string
		if err := rows.Scan(&addr, &t.Asset.Kind, &t.Asset.Contract, &t.Asset.Decimals, &received, &fee, &t.Postings); err != nil {
			return nil, fmt.Errorf("store: address totals: %w", err)
		}
		if t.Received, err = parseAmount(received); err != nil {
			return nil, err
		}
		if t.Fee, err = parseAmount(fee); err != nil {
			return nil, err
		}
		out[addr] = append(out[addr], t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: address totals: %w", err)
	}
	for _, ts := range out {
		ledger.SortTotals(ts)
	}
	return out, nil
}

const postingColumns = `id, merchant_id, invoice_id, address, block_number, block_hash, tx_hash, log_index,
asset_kind, asset_contract, asset_decimals, amount::text, fee_bps, fee::text, posted_at`

func scanPosting(row pgx.CollectableRow) (ledger.Posting, error) {
	var p ledger.Posting
	var blockNumber int64
	var logIndex *int64
	var amount, fee string
	err := row.Scan(&p.ID, &p.MerchantID, &p.InvoiceID, &p.Address, &blockNumber, &p.Transfer.BlockHash,
		&p.Transfer.TxHash, &logIndex, &p.Transfer.Asset.Kind, &p.Transfer.Asset.Contract, &p.Transfer.Asset.Decimals,
		&amount, &p.FeeBps, &fee, &p.PostedAt)
	if err != nil {
		return p, err
	}
	p.Transfer.BlockNumber = uint64(blockNumber) //nolint:gosec // the column is checked to be non-negative
	if logIndex != nil {
		v := uint(*logIndex) //nolint:gosec // the column is checked to be non-negative
		p.Transfer.LogIndex = &v
	}
	if p.Transfer.Amount, err = parseAmount(amount); err != nil {
		return p, err
	}
	if p.Fee, err = parseAmount(fee); err != nil {
		return p, err
	}
	p.PostedAt = p.PostedAt.UTC()
	return p, nil
}

// cursorArgs returns the keyset bound; a nil cursor starts from the first posting.
func cursorArgs(after *ledger.Cursor) (*time.Time, string) {
	if after == nil {
		return nil, ""
	}
	return &after.PostedAt, after.ID
}

// MerchantPostings implements ledger.Store.
func (s *Postgres) MerchantPostings(ctx context.Context, merchant uuid.UUID, from, to time.Time, after *ledger.Cursor, limit int) ([]ledger.Posting, error) {
	at, id := cursorArgs(after)
	rows, err := s.pool.Query(ctx, `SELECT `+postingColumns+` FROM postings
WHERE merchant_id = $1 AND posted_at >= $2 AND posted_at < $3
  AND ($4::timestamptz IS NULL OR (posted_at, id) > ($4::timestamptz, $5::text COLLATE "C"))
ORDER BY posted_at, id LIMIT $6`, merchant, from, to, at, id, limit)
	if err != nil {
		return nil, fmt.Errorf("store: merchant postings: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanPosting)
	if err != nil {
		return nil, fmt.Errorf("store: merchant postings: %w", err)
	}
	return out, nil
}

// AddressPostings implements ledger.Store.
func (s *Postgres) AddressPostings(ctx context.Context, address string, after *ledger.Cursor, limit int) ([]ledger.Posting, error) {
	at, id := cursorArgs(after)
	rows, err := s.pool.Query(ctx, `SELECT `+postingColumns+` FROM postings
WHERE address = $1 AND ($2::timestamptz IS NULL OR (posted_at, id) > ($2::timestamptz, $3::text COLLATE "C"))
ORDER BY posted_at, id LIMIT $4`, address, at, id, limit)
	if err != nil {
		return nil, fmt.Errorf("store: address postings: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanPosting)
	if err != nil {
		return nil, fmt.Errorf("store: address postings: %w", err)
	}
	return out, nil
}
