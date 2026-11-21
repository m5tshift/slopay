package invoice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/hdwallet"
	"github.com/m5tshift/weigate/pkg/kafka"
)

// MemStore is an in-memory Store for tests: the same rules as store.Postgres with a mutex instead of the
// merchant row lock. Events go to Events instead of the outbox.
type MemStore struct {
	mu        sync.Mutex
	merchants map[uuid.UUID]*memMerchant
	invoices  map[uuid.UUID]memInvoice
	keys      map[string]uuid.UUID
	events    []kafka.Event
}

type memMerchant struct {
	Merchant
	next uint64
}

type memInvoice struct {
	Invoice
	hash []byte
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{merchants: map[uuid.UUID]*memMerchant{}, invoices: map[uuid.UUID]memInvoice{}, keys: map[string]uuid.UUID{}}
}

func keyOf(merchantID uuid.UUID, key string) string { return merchantID.String() + "/" + key }

// FindByKey implements Store.
func (s *MemStore) FindByKey(_ context.Context, merchantID uuid.UUID, key string) (Invoice, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findLocked(merchantID, key)
}

func (s *MemStore) findLocked(merchantID uuid.UUID, key string) (Invoice, []byte, error) {
	id, ok := s.keys[keyOf(merchantID, key)]
	if !ok {
		return Invoice{}, nil, ErrNotFound
	}
	inv := s.invoices[id]
	return inv.Invoice, bytes.Clone(inv.hash), nil
}

// Merchant implements Store.
func (s *MemStore) Merchant(_ context.Context, id uuid.UUID) (Merchant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.merchants[id]
	if !ok {
		return Merchant{}, ErrNoXpub
	}
	return m.Merchant, nil
}

// Create implements Store.
func (s *MemStore) Create(_ context.Context, d Draft, derive DeriveFunc) (Invoice, []byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.merchants[d.MerchantID]
	if !ok {
		return Invoice{}, nil, false, ErrNoXpub
	}
	if inv, h, err := s.findLocked(d.MerchantID, d.IdempotencyKey); err == nil {
		return inv, h, false, nil
	}
	addr, index, next, err := NextAddress(m.Xpub, m.next, derive)
	if err != nil {
		return Invoice{}, nil, false, err
	}
	inv := d.Invoice
	inv.Address, inv.DerivationIndex = addr, index
	ev, err := CreatedEvent(inv)
	if err != nil {
		return Invoice{}, nil, false, err
	}
	m.next = next
	s.invoices[inv.ID] = memInvoice{Invoice: inv, hash: bytes.Clone(d.RequestHash)}
	s.keys[keyOf(inv.MerchantID, d.IdempotencyKey)] = inv.ID
	s.events = append(s.events, ev)
	return inv, bytes.Clone(d.RequestHash), true, nil
}

// MaxDerivationIndex is the last non-hardened index (2^31 - 1).
const MaxDerivationIndex = math.MaxInt32

// NextAddress derives the address with the first usable index from next and returns it, the index and the
// next counter value. Indexes whose child key is invalid (hdwallet.ErrInvalidChildKey, probability about
// 2^-127) are skipped, so the counter may grow by more than one.
func NextAddress(xpub string, next uint64, derive DeriveFunc) (addr string, index uint32, newNext uint64, err error) {
	for ; next <= MaxDerivationIndex; next++ {
		addr, err = derive(xpub, uint32(next))
		if errors.Is(err, hdwallet.ErrInvalidChildKey) {
			continue
		}
		if err != nil {
			return "", 0, 0, fmt.Errorf("invoice: derive address %d: %w", next, err)
		}
		return addr, uint32(next), next + 1, nil
	}
	return "", 0, 0, ErrIndexExhausted
}

// Get implements Store.
func (s *MemStore) Get(_ context.Context, merchantID, id uuid.UUID) (Invoice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv, ok := s.invoices[id]
	if !ok || inv.MerchantID != merchantID {
		return Invoice{}, ErrNotFound
	}
	return inv.Invoice, nil
}

// GetPublic implements Store.
func (s *MemStore) GetPublic(_ context.Context, id uuid.UUID) (Invoice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv, ok := s.invoices[id]
	if !ok {
		return Invoice{}, ErrNotFound
	}
	return inv.Invoice, nil
}

// List implements Store.
func (s *MemStore) List(_ context.Context, merchantID uuid.UUID, f Filter) ([]Invoice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Invoice
	for _, inv := range s.invoices {
		if inv.MerchantID == merchantID && f.Matches(inv.Invoice) {
			out = append(out, inv.Invoice)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return compareUUID(out[i].ID, out[j].ID) > 0
	})
	if len(out) > f.Limit+1 {
		out = out[:f.Limit+1]
	}
	return out, nil
}

// UpsertMerchant implements Store.
func (s *MemStore) UpsertMerchant(_ context.Context, m Merchant) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.merchants[m.ID]
	if !ok {
		s.merchants[m.ID] = &memMerchant{Merchant: m}
		return true, nil
	}
	if m.Version <= cur.Version {
		return false, nil
	}
	cur.Merchant = m
	return true, nil
}

// SetStatus changes the status of an invoice, for tests of the status cache.
func (s *MemStore) SetStatus(id uuid.UUID, st Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv := s.invoices[id]
	inv.Status = st
	s.invoices[id] = inv
}

// Events returns the invoice.created events stored so far.
func (s *MemStore) Events() []kafka.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]kafka.Event(nil), s.events...)
}
