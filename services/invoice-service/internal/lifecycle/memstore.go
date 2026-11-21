package lifecycle

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
)

// MemStore is an in-memory Store for tests with the rules of the PostgreSQL store: a mutex is the chain lock
// and a failed transaction is rolled back. With a codec, events are validated against their schemas.
type MemStore struct {
	mu    sync.Mutex
	codec *kafka.Codec
	d     memData
	// Fail, when set, is called on every Save; an error aborts the transaction (failure injection).
	Fail func() error
}

type memData struct {
	state     ChainState
	blocks    map[uint64]Block
	invoices  map[uuid.UUID]Record
	addresses map[string]uuid.UUID
	transfers map[uuid.UUID][]Transfer
	keys      map[string]bool
	events    []kafka.Event
}

func (d memData) clone() memData {
	c := d
	c.blocks = maps.Clone(d.blocks)
	c.invoices = maps.Clone(d.invoices)
	c.addresses = maps.Clone(d.addresses)
	c.keys = maps.Clone(d.keys)
	c.transfers = make(map[uuid.UUID][]Transfer, len(d.transfers))
	for k, v := range d.transfers {
		c.transfers[k] = append([]Transfer(nil), v...)
	}
	c.events = append([]kafka.Event(nil), d.events...)
	return c
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty store; codec may be nil.
func NewMemStore(codec *kafka.Codec) *MemStore {
	return &MemStore{codec: codec, d: memData{state: ChainState{Offset: -1}, blocks: map[uint64]Block{},
		invoices: map[uuid.UUID]Record{}, addresses: map[string]uuid.UUID{}, transfers: map[uuid.UUID][]Transfer{},
		keys: map[string]bool{}}}
}

// AddInvoice stores a new invoice (status NEW, sequence 1) with the tolerance of its merchant.
func (s *MemStore) AddInvoice(rec Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.Deadline = rec.ExpiresAt
	s.d.invoices[rec.ID] = rec
	s.d.addresses[rec.Address] = rec.ID
}

// Record returns the invoice and its transfers.
func (s *MemStore) Record(id uuid.UUID) (Record, []Transfer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.d.invoices[id], append([]Transfer(nil), s.d.transfers[id]...)
}

// Events returns the events written to the outbox so far.
func (s *MemStore) Events() []kafka.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]kafka.Event(nil), s.d.events...)
}

// State returns the chain state.
func (s *MemStore) State() ChainState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.d.state
}

// Chain implements Store.
func (s *MemStore) Chain(ctx context.Context, _ int64, _ bool, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	saved := s.d.clone()
	if err := fn(memTx{s}); err != nil {
		s.d = saved
		return err
	}
	return ctx.Err()
}

type memTx struct{ s *MemStore }

func (t memTx) State(context.Context) (ChainState, error) { return t.s.d.state, nil }

func (t memTx) SetState(_ context.Context, st ChainState) error {
	t.s.d.state = st
	return nil
}

func (t memTx) Block(_ context.Context, n uint64) (Block, bool, error) {
	b, ok := t.s.d.blocks[n]
	return b, ok, nil
}

func (t memTx) PutBlock(_ context.Context, b Block) error {
	t.s.d.blocks[b.Number] = b
	return nil
}

func (t memTx) DropBlocks(_ context.Context, from uint64) error {
	for n := range t.s.d.blocks {
		if n >= from {
			delete(t.s.d.blocks, n)
		}
	}
	return nil
}

func (t memTx) PruneBlocks(_ context.Context, below uint64) error {
	for n := range t.s.d.blocks {
		if n < below {
			delete(t.s.d.blocks, n)
		}
	}
	return nil
}

func (t memTx) InvoiceByAddress(_ context.Context, addr string) (uuid.UUID, invoice.Asset, bool, error) {
	id, ok := t.s.d.addresses[addr]
	if !ok {
		return uuid.Nil, invoice.Asset{}, false, nil
	}
	return id, t.s.d.invoices[id].Asset, true, nil
}

func (t memTx) AddTransfer(_ context.Context, id uuid.UUID, tr Transfer) (bool, error) {
	if t.s.d.keys[tr.Key()] {
		return false, nil
	}
	t.s.d.keys[tr.Key()] = true
	t.s.d.transfers[id] = append(t.s.d.transfers[id], tr)
	return true, nil
}

// mark sets the canonical flag of the transfers selected by match and returns the invoices of those changed
// from was to !was.
func (t memTx) mark(match func(Transfer) bool, to func(Transfer) bool) []uuid.UUID {
	var ids []uuid.UUID
	for id, ts := range t.s.d.transfers {
		hit := false
		for i := range ts {
			if match(ts[i]) && ts[i].Canonical != to(ts[i]) {
				ts[i].Canonical = to(ts[i])
				hit = true
			}
		}
		if hit {
			ids = append(ids, id)
		}
	}
	return ids
}

func (t memTx) Canonize(_ context.Context, b Block) ([]uuid.UUID, error) {
	return t.mark(func(tr Transfer) bool { return tr.BlockNumber == b.Number },
		func(tr Transfer) bool { return tr.BlockHash == b.Hash }), nil
}

func (t memTx) Orphan(_ context.Context, from uint64) ([]uuid.UUID, error) {
	return t.mark(func(tr Transfer) bool { return tr.BlockNumber >= from },
		func(Transfer) bool { return false }), nil
}

func (t memTx) Due(_ context.Context, now time.Time) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	for id, r := range t.s.d.invoices {
		if r.Watch || (!r.Deadline.IsZero() && r.Deadline.Before(now)) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (t memTx) Load(_ context.Context, id uuid.UUID) (Record, []Transfer, error) {
	r, ok := t.s.d.invoices[id]
	if !ok {
		return Record{}, nil, fmt.Errorf("lifecycle: invoice %s not found", id)
	}
	return r, append([]Transfer(nil), t.s.d.transfers[id]...), nil
}

func (t memTx) Save(_ context.Context, rec Record, changed []Transfer, events []kafka.Event) error {
	if t.s.Fail != nil {
		if err := t.s.Fail(); err != nil {
			return err
		}
	}
	for _, ev := range events {
		if t.s.codec != nil {
			if _, err := t.s.codec.Encode(ev); err != nil {
				return err
			}
		}
	}
	t.s.d.invoices[rec.ID] = rec
	ts := t.s.d.transfers[rec.ID]
	for _, c := range changed {
		for i := range ts {
			if ts[i].Key() == c.Key() {
				ts[i].Role, ts[i].GraceUntil = c.Role, c.GraceUntil
			}
		}
	}
	t.s.d.events = append(t.s.d.events, events...)
	return nil
}
