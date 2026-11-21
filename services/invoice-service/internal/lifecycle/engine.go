package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

const meterName = "github.com/m5tshift/slopay/services/invoice-service/internal/lifecycle"

// Block is a canonical block of chain.new_head.
type Block struct {
	Number     uint64
	Hash       string
	ParentHash string
	Time       time.Time
}

// ChainState is the canonical chain as invoice-service knows it.
type ChainState struct {
	// Known: a block was processed; Head, HeadHash and Time are set.
	Known    bool
	Head     uint64
	HeadHash string
	// Time is the chain time: the largest processed block timestamp.
	Time time.Time
	// Offset is the offset of the last handled chain.blocks record, -1 before the first.
	Offset int64
}

// Store keeps the chain, the transfers and the invoice states.
type Store interface {
	// Chain runs fn in one transaction holding the lock of the chain: exclusive for the events of chain.blocks
	// (they are serialised), shared for transfers (they run in parallel, never during a block).
	Chain(ctx context.Context, chainID int64, exclusive bool, fn func(Tx) error) error
}

// Tx is the store inside the chain lock.
type Tx interface {
	State(ctx context.Context) (ChainState, error)
	SetState(ctx context.Context, st ChainState) error
	// Block returns the canonical block with the number.
	Block(ctx context.Context, number uint64) (Block, bool, error)
	// PutBlock stores a canonical block, replacing one with the same number.
	PutBlock(ctx context.Context, b Block) error
	// DropBlocks deletes the canonical blocks from the number on.
	DropBlocks(ctx context.Context, from uint64) error
	// PruneBlocks deletes the blocks below the number.
	PruneBlocks(ctx context.Context, below uint64) error
	// InvoiceByAddress returns the id and the asset of the invoice with the payment address and locks the
	// invoice until the end of the transaction, before any of its transfers is written.
	InvoiceByAddress(ctx context.Context, address string) (uuid.UUID, invoice.Asset, bool, error)
	// AddTransfer stores a transfer of the invoice; false if its key is stored already.
	AddTransfer(ctx context.Context, id uuid.UUID, t Transfer) (bool, error)
	// Canonize marks the transfers of the block number canonical when their hash is b.Hash and returns
	// their invoices.
	Canonize(ctx context.Context, b Block) ([]uuid.UUID, error)
	// Orphan marks the transfers from the block number on not canonical and returns the invoices of those
	// that were canonical.
	Orphan(ctx context.Context, from uint64) ([]uuid.UUID, error)
	// Due returns the invoices to evaluate on every block (Watch) and those whose Deadline is before now.
	Due(ctx context.Context, now time.Time) ([]uuid.UUID, error)
	// Load locks the invoice and returns it with its transfers.
	Load(ctx context.Context, id uuid.UUID) (Record, []Transfer, error)
	// Save writes the invoice, the changed transfers and the events (to the outbox).
	Save(ctx context.Context, rec Record, changed []Transfer, events []kafka.Event) error
}

// NewHead is the data of chain.new_head.
type NewHead struct {
	ChainID    int64  `json:"chainId"`
	Number     uint64 `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  int64  `json:"timestamp"`
}

// Reorg is the data of chain.reorg.
type Reorg struct {
	ChainID   int64  `json:"chainId"`
	FromBlock uint64 `json:"fromBlock"`
}

// TransferEvent is the data of chain.transfer.
type TransferEvent struct {
	ChainID        int64         `json:"chainId"`
	BlockNumber    uint64        `json:"blockNumber"`
	BlockHash      string        `json:"blockHash"`
	BlockTimestamp int64         `json:"blockTimestamp"`
	TxHash         string        `json:"txHash"`
	LogIndex       *int64        `json:"logIndex"`
	From           string        `json:"from"`
	To             string        `json:"to"`
	Amount         string        `json:"amount"`
	Asset          invoice.Asset `json:"asset"`
}

// EngineConfig configures an Engine.
type EngineConfig struct {
	Config
	// ChainID is the chain of the stand (31337 on Anvil); events of other chains are skipped.
	ChainID int64
	// KeepBlocks is how many recent canonical blocks are kept (more than the deepest reorg).
	KeepBlocks uint64
	// Now is the processing clock for event times (default time.Now). Deadlines never use it.
	Now func() time.Time
	// Meter receives the metrics (default: the global OTel meter).
	Meter metric.Meter
}

// Engine applies chain.blocks and chain.transfers to the invoices.
type Engine struct {
	cfg   EngineConfig
	store Store
	cache invoice.StatusCache
	log   *slog.Logger

	transitions metric.Int64Counter
	transfers   metric.Int64Counter
}

// NewEngine returns the engine. cache may be nil.
func NewEngine(store Store, cache invoice.StatusCache, cfg EngineConfig, log *slog.Logger) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.ChainID <= 0 || cfg.KeepBlocks <= uint64(cfg.Confirmations) { //nolint:gosec // Validate: Confirmations >= 2.
		return nil, errors.New("lifecycle: ChainID must be positive and KeepBlocks greater than Confirmations")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cache == nil {
		cache = invoice.NopCache{}
	}
	if log == nil {
		log = slog.Default()
	}
	m := cfg.Meter
	if m == nil {
		m = otel.Meter(meterName)
	}
	e := &Engine{cfg: cfg, store: store, cache: cache, log: log}
	var err error
	if e.transitions, err = m.Int64Counter("slopay_invoice_transitions",
		metric.WithDescription("invoice.* events of the lifecycle, by type")); err != nil {
		return nil, err
	}
	if e.transfers, err = m.Int64Counter("slopay_invoice_chain_transfers",
		metric.WithDescription("chain.transfer events, by result: stored, duplicate or ignored")); err != nil {
		return nil, err
	}
	return e, nil
}

// changed is an invoice whose state changed in a transaction.
type changed struct {
	rec   Record
	types []string
}

// NewHead applies a chain.new_head read at offset of chain.blocks. A record at or below the stored offset was
// applied already and is skipped, so redelivery is harmless. The block becomes the head; its transfers become
// canonical; the invoices with them, the watched ones and those past their deadline are evaluated.
func (e *Engine) NewHead(ctx context.Context, offset int64, h NewHead) error {
	if h.ChainID != e.cfg.ChainID {
		return e.skipBlockEvent(ctx, offset, "chain.new_head of another chain", "chain_id", h.ChainID)
	}
	b := Block{Number: h.Number, Hash: strings.ToLower(h.Hash), ParentHash: strings.ToLower(h.ParentHash),
		Time: time.Unix(h.Timestamp, 0).UTC()}
	return e.run(ctx, true, func(tx Tx, out *[]changed) error {
		st, err := tx.State(ctx)
		if err != nil {
			return err
		}
		if offset <= st.Offset {
			return nil
		}
		st.Offset = offset
		ids := map[uuid.UUID]bool{}
		var from uint64
		if st.Known {
			switch {
			case b.Number <= st.Head:
				cur, ok, err := tx.Block(ctx, b.Number)
				if err != nil {
					return err
				}
				if ok && cur.Hash == b.Hash {
					return tx.SetState(ctx, st)
				}
				// A replaced block without chain.reorg: handled as a reorg from its number.
				e.log.WarnContext(ctx, "chain.new_head replaces a canonical block, handled as a reorg",
					"number", b.Number, "hash", b.Hash)
				if err := e.orphan(ctx, tx, &st, b.Number, ids); err != nil {
					return err
				}
				from = b.Number
			case b.Number == st.Head+1 && b.ParentHash != st.HeadHash:
				e.log.WarnContext(ctx, "chain.new_head does not extend the head", "number", b.Number,
					"parent_hash", b.ParentHash, "head_hash", st.HeadHash)
			case b.Number > st.Head+1:
				e.log.WarnContext(ctx, "chain.new_head after a gap", "number", b.Number, "head", st.Head)
			}
		}
		if err := tx.PutBlock(ctx, b); err != nil {
			return err
		}
		st.Known, st.Head, st.HeadHash = true, b.Number, b.Hash
		if b.Time.After(st.Time) {
			st.Time = b.Time
		}
		if b.Number >= e.cfg.KeepBlocks {
			if err := tx.PruneBlocks(ctx, b.Number-e.cfg.KeepBlocks+1); err != nil {
				return err
			}
		}
		canon, err := tx.Canonize(ctx, b)
		if err != nil {
			return err
		}
		due, err := tx.Due(ctx, st.Time)
		if err != nil {
			return err
		}
		for _, id := range append(canon, due...) {
			ids[id] = true
		}
		if err := tx.SetState(ctx, st); err != nil {
			return err
		}
		return e.evaluate(ctx, tx, ids, Chain{Head: st.Head, Time: st.Time, FromBlock: from}, out)
	})
}

// Reorg applies a chain.reorg read at offset of chain.blocks: the blocks from fromBlock on leave the canonical
// chain, their transfers are not canonical any more and the invoices that counted them are recalculated.
func (e *Engine) Reorg(ctx context.Context, offset int64, r Reorg) error {
	if r.ChainID != e.cfg.ChainID {
		return e.skipBlockEvent(ctx, offset, "chain.reorg of another chain", "chain_id", r.ChainID)
	}
	return e.run(ctx, true, func(tx Tx, out *[]changed) error {
		st, err := tx.State(ctx)
		if err != nil {
			return err
		}
		if offset <= st.Offset {
			return nil
		}
		st.Offset = offset
		ids := map[uuid.UUID]bool{}
		if st.Known && r.FromBlock <= st.Head {
			if err := e.orphan(ctx, tx, &st, r.FromBlock, ids); err != nil {
				return err
			}
			// The watched invoices lose confirmations; their stored count stays exact.
			due, err := tx.Due(ctx, st.Time)
			if err != nil {
				return err
			}
			for _, id := range due {
				ids[id] = true
			}
		}
		if err := tx.SetState(ctx, st); err != nil {
			return err
		}
		return e.evaluate(ctx, tx, ids, Chain{Head: st.Head, Time: st.Time, FromBlock: r.FromBlock}, out)
	})
}

// orphan drops the canonical blocks from the number on; the head becomes the block before it.
func (e *Engine) orphan(ctx context.Context, tx Tx, st *ChainState, from uint64, ids map[uuid.UUID]bool) error {
	affected, err := tx.Orphan(ctx, from)
	if err != nil {
		return err
	}
	for _, id := range affected {
		ids[id] = true
	}
	if err := tx.DropBlocks(ctx, from); err != nil {
		return err
	}
	if from == 0 {
		st.Known, st.Head, st.HeadHash = false, 0, ""
		return nil
	}
	st.Head, st.HeadHash = from-1, ""
	if prev, ok, err := tx.Block(ctx, from-1); err != nil {
		return err
	} else if ok {
		st.HeadHash = prev.Hash
	}
	return nil
}

func (e *Engine) skipBlockEvent(ctx context.Context, offset int64, msg string, args ...any) error {
	e.log.WarnContext(ctx, msg, args...)
	return e.run(ctx, true, func(tx Tx, _ *[]changed) error {
		st, err := tx.State(ctx)
		if err != nil {
			return err
		}
		if offset > st.Offset {
			st.Offset = offset
			return tx.SetState(ctx, st)
		}
		return nil
	})
}

// Transfer applies a chain.transfer. A transfer to an unknown address, of another chain or of another asset
// than the invoice is skipped. A stored transfer waits until its block is canonical; one whose block is
// canonical already is evaluated at once. A repeated transfer (same blockHash, txHash, logIndex) is skipped.
func (e *Engine) Transfer(ctx context.Context, te TransferEvent) error {
	result := "ignored"
	defer func() { e.transfers.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result))) }()
	if te.ChainID != e.cfg.ChainID {
		e.log.WarnContext(ctx, "chain.transfer of another chain skipped", "chain_id", te.ChainID)
		return nil
	}
	t, err := transferOf(te)
	if err != nil {
		return err
	}
	return e.run(ctx, false, func(tx Tx, out *[]changed) error {
		id, asset, ok, err := tx.InvoiceByAddress(ctx, strings.ToLower(te.To))
		if err != nil || !ok {
			return err
		}
		if !sameAsset(asset, t.Asset) {
			e.log.InfoContext(ctx, "chain.transfer of another asset skipped", "invoice_id", id, "tx_hash", t.TxHash)
			return nil
		}
		st, err := tx.State(ctx)
		if err != nil {
			return err
		}
		if st.Known && t.BlockNumber <= st.Head {
			b, ok, err := tx.Block(ctx, t.BlockNumber)
			if err != nil {
				return err
			}
			t.Canonical = ok && b.Hash == t.BlockHash
		}
		added, err := tx.AddTransfer(ctx, id, t)
		if err != nil {
			return err
		}
		result = "duplicate"
		if !added {
			return nil
		}
		result = "stored"
		if !t.Canonical {
			return nil
		}
		return e.evaluate(ctx, tx, map[uuid.UUID]bool{id: true}, Chain{Head: st.Head, Time: st.Time}, out)
	})
}

func transferOf(te TransferEvent) (Transfer, error) {
	t := Transfer{BlockNumber: te.BlockNumber, BlockHash: strings.ToLower(te.BlockHash),
		BlockTime: time.Unix(te.BlockTimestamp, 0).UTC(), TxHash: strings.ToLower(te.TxHash), LogIndex: te.LogIndex,
		Asset: te.Asset}
	t.Asset.Contract = strings.ToLower(t.Asset.Contract)
	amount, ok := parseAmount(te.Amount)
	if !ok {
		return Transfer{}, fmt.Errorf("lifecycle: chain.transfer %s: invalid amount %q", te.TxHash, te.Amount)
	}
	t.Amount = amount
	return t, nil
}

func parseAmount(s string) (*big.Int, bool) {
	if s == "" || (len(s) > 1 && s[0] == '0') || len(s) > 78 {
		return nil, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return nil, false
		}
	}
	return new(big.Int).SetString(s, 10)
}

func sameAsset(a, b invoice.Asset) bool {
	return a.Kind == b.Kind && strings.EqualFold(a.Contract, b.Contract)
}

// evaluate runs Evaluate for the invoices in id order (a stable lock order) and saves what changed.
func (e *Engine) evaluate(ctx context.Context, tx Tx, ids map[uuid.UUID]bool, chain Chain, out *[]changed) error {
	sorted := make([]uuid.UUID, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i][:], sorted[j][:]) < 0 })
	now := e.cfg.Now()
	for _, id := range sorted {
		rec, ts, err := tx.Load(ctx, id)
		if err != nil {
			return err
		}
		res := Evaluate(e.cfg.Config, rec, ts, chain)
		if len(res.Transitions) == 0 && len(res.Changed) == 0 && sameSchedule(rec, res.Record) {
			continue
		}
		events := make([]kafka.Event, len(res.Transitions))
		types := make([]string, len(res.Transitions))
		for i, tr := range res.Transitions {
			if events[i], err = NewEvent(res.Record.Invoice, tr, now); err != nil {
				return err
			}
			types[i] = tr.Type
		}
		changedTs := make([]Transfer, len(res.Changed))
		for i, k := range res.Changed {
			changedTs[i] = res.Transfers[k]
		}
		if err := tx.Save(ctx, res.Record, changedTs, events); err != nil {
			return err
		}
		if len(types) > 0 {
			*out = append(*out, changed{rec: res.Record, types: types})
		}
	}
	return nil
}

func sameSchedule(a, b Record) bool {
	return a.Watch == b.Watch && a.Deadline.Equal(b.Deadline) && a.Confirmations == b.Confirmations
}

// run executes fn in a chain transaction; after the commit the status cache gets the new payment pages.
func (e *Engine) run(ctx context.Context, exclusive bool, fn func(Tx, *[]changed) error) error {
	var out []changed
	err := e.store.Chain(ctx, e.cfg.ChainID, exclusive, func(tx Tx) error {
		out = out[:0]
		return fn(tx, &out)
	})
	if err != nil {
		return err
	}
	for _, c := range out {
		for _, typ := range c.types {
			e.transitions.Add(ctx, 1, metric.WithAttributes(attribute.String("type", typ)))
		}
		e.log.InfoContext(ctx, "invoice status changed", "invoice_id", c.rec.ID, "status", c.rec.Status,
			"sequence", c.rec.Sequence, "events", c.types)
		e.cache.Set(ctx, c.rec.Page())
	}
	return nil
}
