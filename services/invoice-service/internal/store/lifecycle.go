package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/outbox"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
	"github.com/m5tshift/weigate/services/invoice-service/internal/lifecycle"
)

// Lifecycle is the PostgreSQL lifecycle.Store. The chain_state row of the chain is the chain lock: the events
// of chain.blocks take it FOR UPDATE, transfers FOR SHARE. An invoice is evaluated under its row lock, and its
// events go to the outbox in the same transaction (SRS 4.1.5).
type Lifecycle struct {
	pool  *pgxpool.Pool
	codec *kafka.Codec
	now   func() time.Time
}

var _ lifecycle.Store = (*Lifecycle)(nil)

// NewLifecycle returns the store. The codec validates events before they are written to the outbox.
func NewLifecycle(pool *pgxpool.Pool, codec *kafka.Codec) *Lifecycle {
	return &Lifecycle{pool: pool, codec: codec, now: time.Now}
}

// Chain implements lifecycle.Store.
func (s *Lifecycle) Chain(ctx context.Context, chainID int64, exclusive bool, fn func(lifecycle.Tx) error) error {
	lock := "FOR SHARE"
	if exclusive {
		lock = "FOR UPDATE"
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		t := &pgTx{tx: tx, chainID: chainID, codec: s.codec, now: s.now}
		found, err := t.lockState(ctx, lock)
		if err != nil {
			return err
		}
		if !found {
			if _, err := tx.Exec(ctx, `INSERT INTO chain_state (chain_id, updated_at) VALUES ($1, $2)
ON CONFLICT (chain_id) DO NOTHING`, chainID, s.now().UTC()); err != nil {
				return fmt.Errorf("store: create chain state: %w", err)
			}
			if found, err = t.lockState(ctx, lock); err != nil {
				return err
			} else if !found {
				return errors.New("store: chain state not found")
			}
		}
		return fn(t)
	})
}

type pgTx struct {
	tx      pgx.Tx
	chainID int64
	codec   *kafka.Codec
	now     func() time.Time
	state   lifecycle.ChainState
}

func (t *pgTx) lockState(ctx context.Context, lock string) (bool, error) {
	var (
		head      int64
		chainTime *time.Time
	)
	st := &t.state
	err := t.tx.QueryRow(ctx, `SELECT known, head_number, head_hash, chain_time, blocks_offset FROM chain_state
WHERE chain_id = $1 `+lock, t.chainID).Scan(&st.Known, &head, &st.HeadHash, &chainTime, &st.Offset)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: lock chain state: %w", err)
	}
	st.Head = uint64(head) //nolint:gosec // CHECK: head_number >= 0.
	st.Time = timeOf(chainTime)
	return true, nil
}

func (t *pgTx) State(context.Context) (lifecycle.ChainState, error) { return t.state, nil }

func (t *pgTx) SetState(ctx context.Context, st lifecycle.ChainState) error {
	head, err := toInt64(st.Head)
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, `UPDATE chain_state SET known = $2, head_number = $3, head_hash = $4, chain_time = $5,
blocks_offset = $6, updated_at = $7 WHERE chain_id = $1`, t.chainID, st.Known, head, st.HeadHash, nullTime(st.Time),
		st.Offset, t.now().UTC()); err != nil {
		return fmt.Errorf("store: update chain state: %w", err)
	}
	t.state = st
	return nil
}

func (t *pgTx) Block(ctx context.Context, number uint64) (lifecycle.Block, bool, error) {
	n, err := toInt64(number)
	if err != nil {
		return lifecycle.Block{}, false, err
	}
	b := lifecycle.Block{Number: number}
	err = t.tx.QueryRow(ctx, `SELECT hash, parent_hash, block_time FROM chain_blocks WHERE chain_id = $1 AND number = $2`,
		t.chainID, n).Scan(&b.Hash, &b.ParentHash, &b.Time)
	if errors.Is(err, pgx.ErrNoRows) {
		return lifecycle.Block{}, false, nil
	}
	if err != nil {
		return lifecycle.Block{}, false, fmt.Errorf("store: read block: %w", err)
	}
	b.Time = b.Time.UTC()
	return b, true, nil
}

func (t *pgTx) PutBlock(ctx context.Context, b lifecycle.Block) error {
	n, err := toInt64(b.Number)
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, `INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, block_time)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (chain_id, number) DO UPDATE SET hash = EXCLUDED.hash, parent_hash = EXCLUDED.parent_hash,
    block_time = EXCLUDED.block_time`, t.chainID, n, b.Hash, b.ParentHash, b.Time); err != nil {
		return fmt.Errorf("store: put block: %w", err)
	}
	return nil
}

func (t *pgTx) DropBlocks(ctx context.Context, from uint64) error {
	n, err := toInt64(from)
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, `DELETE FROM chain_blocks WHERE chain_id = $1 AND number >= $2`, t.chainID, n); err != nil {
		return fmt.Errorf("store: drop blocks: %w", err)
	}
	return nil
}

func (t *pgTx) PruneBlocks(ctx context.Context, below uint64) error {
	n, err := toInt64(below)
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, `DELETE FROM chain_blocks WHERE chain_id = $1 AND number < $2`, t.chainID, n); err != nil {
		return fmt.Errorf("store: prune blocks: %w", err)
	}
	return nil
}

// InvoiceByAddress locks the invoice row FOR UPDATE. The lock must come before AddTransfer: the foreign key
// check of the transfer insert takes FOR KEY SHARE on the invoice, and two transfers to one invoice that
// both hold it would each wait for the other to release it when Load asks for FOR UPDATE (SQLSTATE 40P01).
// Taking the exclusive lock first serialises the transfers of one invoice and keeps one lock order.
func (t *pgTx) InvoiceByAddress(ctx context.Context, address string) (uuid.UUID, invoice.Asset, bool, error) {
	var (
		id    uuid.UUID
		asset invoice.Asset
	)
	err := t.tx.QueryRow(ctx, `SELECT id, asset_kind, COALESCE(asset_contract, ''), asset_decimals FROM invoices
WHERE address = $1 ORDER BY created_at LIMIT 1 FOR UPDATE`, address).Scan(&id, &asset.Kind, &asset.Contract, &asset.Decimals)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, invoice.Asset{}, false, nil
	}
	if err != nil {
		return uuid.Nil, invoice.Asset{}, false, fmt.Errorf("store: find invoice by address: %w", err)
	}
	return id, asset, true, nil
}

func (t *pgTx) AddTransfer(ctx context.Context, id uuid.UUID, tr lifecycle.Transfer) (bool, error) {
	n, err := toInt64(tr.BlockNumber)
	if err != nil {
		return false, err
	}
	var contract *string
	if tr.Asset.Contract != "" {
		contract = &tr.Asset.Contract
	}
	tag, err := t.tx.Exec(ctx, `INSERT INTO transfers (block_hash, tx_hash, log_index, invoice_id, block_number,
block_time, amount, asset_kind, asset_contract, asset_decimals, canonical, role, grace_until, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::numeric, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (block_hash, tx_hash, log_index) DO NOTHING`, tr.BlockHash, tr.TxHash, logIndex(tr.LogIndex), id, n,
		tr.BlockTime, tr.Amount.String(), tr.Asset.Kind, contract, tr.Asset.Decimals, tr.Canonical, string(tr.Role),
		nullTime(tr.GraceUntil), t.now().UTC())
	if err != nil {
		return false, fmt.Errorf("store: insert transfer: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t *pgTx) Canonize(ctx context.Context, b lifecycle.Block) ([]uuid.UUID, error) {
	n, err := toInt64(b.Number)
	if err != nil {
		return nil, err
	}
	return t.ids(ctx, "canonize transfers", `UPDATE transfers SET canonical = (block_hash = $2)
WHERE block_number = $1 AND canonical <> (block_hash = $2) RETURNING invoice_id`, n, b.Hash)
}

func (t *pgTx) Orphan(ctx context.Context, from uint64) ([]uuid.UUID, error) {
	n, err := toInt64(from)
	if err != nil {
		return nil, err
	}
	return t.ids(ctx, "orphan transfers", `UPDATE transfers SET canonical = false WHERE block_number >= $1 AND canonical
RETURNING invoice_id`, n)
}

func (t *pgTx) Due(ctx context.Context, now time.Time) ([]uuid.UUID, error) {
	return t.ids(ctx, "find due invoices", `SELECT id FROM invoices WHERE watch
UNION SELECT id FROM invoices WHERE deadline < $1`, now)
}

// ids runs a query returning invoice ids and returns them distinct, in id order.
func (t *pgTx) ids(ctx context.Context, what, sql string, args ...any) ([]uuid.UUID, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: %s: %w", what, err)
	}
	seen := map[uuid.UUID]bool{}
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: %s: %w", what, err)
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: %s: %w", what, err)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out, nil
}

func (t *pgTx) Load(ctx context.Context, id uuid.UUID) (lifecycle.Record, []lifecycle.Transfer, error) {
	var (
		rec              lifecycle.Record
		paidAt, deadline *time.Time
		err              error
	)
	rec.Invoice, err = scanInvoice(t.tx.QueryRow(ctx, `SELECT `+invoiceColumns+`,
(SELECT tolerance_bps FROM merchants WHERE merchants.id = invoices.merchant_id), paid_at, watch, deadline
FROM invoices WHERE id = $1 FOR UPDATE`, id), &rec.ToleranceBps, &paidAt, &rec.Watch, &deadline)
	if err != nil {
		return lifecycle.Record{}, nil, err
	}
	rec.PaidAt, rec.Deadline = timeOf(paidAt), timeOf(deadline)
	rows, err := t.tx.Query(ctx, `SELECT block_number, block_hash, block_time, tx_hash, log_index, amount::text,
asset_kind, COALESCE(asset_contract, ''), asset_decimals, canonical, role, grace_until
FROM transfers WHERE invoice_id = $1`, id)
	if err != nil {
		return lifecycle.Record{}, nil, fmt.Errorf("store: read transfers: %w", err)
	}
	defer rows.Close()
	var ts []lifecycle.Transfer
	for rows.Next() {
		var (
			tr         lifecycle.Transfer
			number, li int64
			amount     string
			role       string
			graceUntil *time.Time
		)
		if err := rows.Scan(&number, &tr.BlockHash, &tr.BlockTime, &tr.TxHash, &li, &amount, &tr.Asset.Kind,
			&tr.Asset.Contract, &tr.Asset.Decimals, &tr.Canonical, &role, &graceUntil); err != nil {
			return lifecycle.Record{}, nil, fmt.Errorf("store: read transfer: %w", err)
		}
		var ok bool
		if tr.Amount, ok = new(big.Int).SetString(amount, 10); !ok {
			return lifecycle.Record{}, nil, fmt.Errorf("store: transfer %s: invalid amount %q", tr.TxHash, amount)
		}
		tr.BlockNumber = uint64(number) //nolint:gosec // CHECK: block_number >= 0.
		tr.BlockTime = tr.BlockTime.UTC()
		if li >= 0 {
			tr.LogIndex = &li
		}
		tr.Role, tr.GraceUntil = lifecycle.Role(role), timeOf(graceUntil)
		ts = append(ts, tr)
	}
	if err := rows.Err(); err != nil {
		return lifecycle.Record{}, nil, fmt.Errorf("store: read transfers: %w", err)
	}
	return rec, ts, nil
}

func (t *pgTx) Save(ctx context.Context, rec lifecycle.Record, changed []lifecycle.Transfer, events []kafka.Event) error {
	if _, err := t.tx.Exec(ctx, `UPDATE invoices SET status = $2, sequence = $3, confirmations = $4,
received_amount = $5::numeric, overpaid = $6, paid_at = $7, watch = $8, deadline = $9, updated_at = $10 WHERE id = $1`,
		rec.ID, string(rec.Status), rec.Sequence, rec.Confirmations, rec.Received.String(), rec.Overpaid,
		nullTime(rec.PaidAt), rec.Watch, nullTime(rec.Deadline), t.now().UTC()); err != nil {
		return fmt.Errorf("store: update invoice: %w", err)
	}
	for _, tr := range changed {
		if _, err := t.tx.Exec(ctx, `UPDATE transfers SET role = $4, grace_until = $5
WHERE block_hash = $1 AND tx_hash = $2 AND log_index = $3`, tr.BlockHash, tr.TxHash, logIndex(tr.LogIndex),
			string(tr.Role), nullTime(tr.GraceUntil)); err != nil {
			return fmt.Errorf("store: update transfer: %w", err)
		}
	}
	for _, ev := range events {
		if err := outbox.Add(ctx, t.tx, t.codec, invoice.TopicInvoices, ev); err != nil {
			return err
		}
	}
	return nil
}

func logIndex(li *int64) int64 {
	if li == nil {
		return -1
	}
	return *li
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func timeOf(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}

func toInt64(n uint64) (int64, error) {
	if n > math.MaxInt64 {
		return 0, fmt.Errorf("store: block number %d out of range", n)
	}
	return int64(n), nil
}
