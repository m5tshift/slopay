package ledger

import (
	"context"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// MemStore is an in-memory Store for tests. Events are validated by the codec like outbox.Add does and kept
// in Outbox in the order they were added.
type MemStore struct {
	codec *kafka.Codec

	mu        sync.Mutex
	merchants map[uuid.UUID]bool
	postings  map[string]Posting
	// Outbox holds the published events (see Events).
	Outbox []kafka.Event
	// Fail, when set, is returned by Record without changing anything. Set it before concurrent use.
	Fail error
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty store.
func NewMemStore(codec *kafka.Codec) *MemStore {
	return &MemStore{codec: codec, merchants: map[uuid.UUID]bool{}, postings: map[string]Posting{}}
}

// Events returns a copy of the outbox.
func (m *MemStore) Events() []kafka.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]kafka.Event(nil), m.Outbox...)
}

// Postings returns the stored postings in page order.
func (m *MemStore) Postings() []Posting {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sorted(func(Posting) bool { return true })
}

// Record implements Store.
func (m *MemStore) Record(_ context.Context, merchant uuid.UUID, postings []Posting) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail != nil {
		return 0, m.Fail
	}
	var events []kafka.Event
	var fresh []Posting
	for _, p := range postings {
		if _, ok := m.postings[p.ID]; ok {
			continue
		}
		evs, err := p.Events()
		if err != nil {
			return 0, err
		}
		for _, ev := range evs {
			if _, err := m.codec.Encode(ev); err != nil {
				return 0, err
			}
		}
		events = append(events, evs...)
		fresh = append(fresh, p)
	}
	m.merchants[merchant] = true
	for _, p := range fresh {
		m.postings[p.ID] = p
	}
	m.Outbox = append(m.Outbox, events...)
	return len(fresh), nil
}

// MerchantExists implements Store.
func (m *MemStore) MerchantExists(_ context.Context, merchant uuid.UUID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.merchants[merchant], nil
}

func inPeriod(p Posting, from, to time.Time) bool {
	return !p.PostedAt.Before(from) && p.PostedAt.Before(to)
}

// MerchantTotals implements Store.
func (m *MemStore) MerchantTotals(_ context.Context, merchant uuid.UUID, from, to time.Time) ([]Totals, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sum(m.sorted(func(p Posting) bool { return p.MerchantID == merchant && inPeriod(p, from, to) })), nil
}

// MerchantPostings implements Store.
func (m *MemStore) MerchantPostings(_ context.Context, merchant uuid.UUID, from, to time.Time, after *Cursor, limit int) ([]Posting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.sorted(func(p Posting) bool {
		return p.MerchantID == merchant && inPeriod(p, from, to) && (after == nil || !after.Less(p))
	})
	return items[:min(limit, len(items))], nil
}

// AddressTotals implements Store.
func (m *MemStore) AddressTotals(_ context.Context, addresses []string) (map[string][]Totals, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string][]Totals{}
	for _, a := range addresses {
		if ts := sum(m.sorted(func(p Posting) bool { return p.Address == a })); len(ts) > 0 {
			out[a] = ts
		}
	}
	return out, nil
}

// AddressPostings implements Store.
func (m *MemStore) AddressPostings(_ context.Context, address string, after *Cursor, limit int) ([]Posting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.sorted(func(p Posting) bool { return p.Address == address && (after == nil || !after.Less(p)) })
	return items[:min(limit, len(items))], nil
}

func (m *MemStore) sorted(keep func(Posting) bool) []Posting {
	var out []Posting
	for _, p := range m.postings {
		if keep(p) {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].PostedAt.Equal(out[j].PostedAt) {
			return out[i].PostedAt.Before(out[j].PostedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// sum totals postings per asset, ordered by SortTotals.
func sum(postings []Posting) []Totals {
	byAsset := map[string]*Totals{}
	for _, p := range postings {
		t := byAsset[p.Transfer.Asset.Key()]
		if t == nil {
			t = &Totals{Asset: p.Transfer.Asset, Received: new(big.Int), Fee: new(big.Int)}
			byAsset[p.Transfer.Asset.Key()] = t
		}
		t.Received.Add(t.Received, p.Transfer.Amount)
		t.Fee.Add(t.Fee, p.Fee)
		t.Postings++
	}
	out := make([]Totals, 0, len(byAsset))
	for _, t := range byAsset {
		out = append(out, *t)
	}
	SortTotals(out)
	return out
}
