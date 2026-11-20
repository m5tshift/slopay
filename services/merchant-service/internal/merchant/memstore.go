package merchant

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
)

// MemStore is an in-memory Store for tests. It follows the contract of the PostgreSQL store, and keeps
// the events the outbox would receive in Events. With a codec the events are validated like outbox.Add does.
type MemStore struct {
	mu        sync.Mutex
	codec     *kafka.Codec
	merchants map[uuid.UUID]Merchant
	keys      map[uuid.UUID]*memKey
	Events    []kafka.Event
	// Err, when set, is returned by every method.
	Err error
}

type memKey struct {
	NewKey
	revoked bool
}

// NewMemStore returns an empty MemStore; codec may be nil.
func NewMemStore(codec *kafka.Codec) *MemStore {
	return &MemStore{codec: codec, merchants: map[uuid.UUID]Merchant{}, keys: map[uuid.UUID]*memKey{}}
}

// Create implements Store.
func (s *MemStore) Create(_ context.Context, m Merchant, key NewKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	s.merchants[m.ID] = m
	s.keys[key.ID] = &memKey{NewKey: key}
	return nil
}

// Get implements Store.
func (s *MemStore) Get(_ context.Context, id uuid.UUID) (Merchant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return Merchant{}, s.Err
	}
	m, ok := s.merchants[id]
	if !ok {
		return Merchant{}, ErrNotFound
	}
	return m, nil
}

// Update implements Store.
func (s *MemStore) Update(_ context.Context, id uuid.UUID, fn func(m *Merchant) error) (Merchant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return Merchant{}, s.Err
	}
	before, ok := s.merchants[id]
	if !ok {
		return Merchant{}, ErrNotFound
	}
	after := before
	if err := fn(&after); err != nil {
		return Merchant{}, err
	}
	if PublishedEqual(before, after) {
		return before, nil
	}
	after.Version++
	if Publishable(after) {
		ev, err := UpdatedEvent(after)
		if err != nil {
			return Merchant{}, err
		}
		if s.codec != nil {
			if _, err := s.codec.Encode(ev); err != nil {
				return Merchant{}, err
			}
		}
		s.Events = append(s.Events, ev)
	}
	s.merchants[id] = after
	return after, nil
}

func (k *memKey) activeAt(now time.Time) bool {
	return !k.revoked && (k.ExpiresAt == nil || now.Before(*k.ExpiresAt))
}

// KeyOwner implements Store.
func (s *MemStore) KeyOwner(_ context.Context, hash []byte, now time.Time) (uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return uuid.Nil, s.Err
	}
	for _, k := range s.keys {
		if bytes.Equal(k.Hash, hash) && k.activeAt(now) {
			return k.MerchantID, nil
		}
	}
	return uuid.Nil, ErrNotFound
}

func (s *MemStore) active(merchantID uuid.UUID, now time.Time) []APIKey {
	var out []APIKey
	for _, k := range s.keys {
		if k.MerchantID == merchantID && k.activeAt(now) {
			out = append(out, k.APIKey)
		}
	}
	slices.SortFunc(out, func(a, b APIKey) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out
}

// ActiveKeys implements Store.
func (s *MemStore) ActiveKeys(_ context.Context, merchantID uuid.UUID, now time.Time) ([]APIKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	return s.active(merchantID, now), nil
}

// ChangeKeys implements Store.
func (s *MemStore) ChangeKeys(_ context.Context, merchantID uuid.UUID, now time.Time, fn func([]APIKey) (KeyChanges, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	if _, ok := s.merchants[merchantID]; !ok {
		return ErrNotFound
	}
	ch, err := fn(s.active(merchantID, now))
	if err != nil {
		return err
	}
	for id, exp := range ch.SetExpiry {
		s.keys[id].ExpiresAt = exp
	}
	for _, id := range ch.Revoke {
		s.keys[id].revoked = true
	}
	if ch.Insert != nil {
		s.keys[ch.Insert.ID] = &memKey{NewKey: *ch.Insert}
	}
	return nil
}

// StoredHashes returns the stored hashes of all keys, for tests that check no key is kept in clear.
func (s *MemStore) StoredHashes() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out [][]byte
	for _, k := range s.keys {
		out = append(out, k.Hash)
	}
	return out
}
