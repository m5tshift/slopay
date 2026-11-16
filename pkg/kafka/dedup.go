package kafka

import (
	"context"
	"sync"
)

// DedupStore records which events were already handled. The service picks the implementation
// (PostgreSQL in the same transaction, Valkey, ...); the store must be shared by all replicas.
type DedupStore interface {
	// Claim marks the event ID as taken and reports whether the caller is the first to take it.
	// It is atomic: of several concurrent calls with one ID exactly one gets true.
	Claim(ctx context.Context, id string) (bool, error)
	// Release undoes Claim after a failed handling so that the redelivery is processed.
	Release(ctx context.Context, id string) error
}

// Deduper skips events whose ID was already handled (SRS 4.1.5).
type Deduper struct {
	Store DedupStore
}

// Wrap returns a handler that calls next once per event.ID. If next fails, the claim is released
// and the error is returned, so the redelivery is handled again.
func (d Deduper) Wrap(next Handler) Handler {
	return func(ctx context.Context, ev Event) error {
		first, err := d.Store.Claim(ctx, ev.ID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
		if err := next(ctx, ev); err != nil {
			// Release uses a context that outlives cancellation of the failed handling.
			if rerr := d.Store.Release(context.WithoutCancel(ctx), ev.ID); rerr != nil {
				return &releaseError{handler: err, release: rerr}
			}
			return err
		}
		return nil
	}
}

type releaseError struct{ handler, release error }

func (e *releaseError) Error() string {
	return e.handler.Error() + "; releasing dedup claim: " + e.release.Error()
}

func (e *releaseError) Unwrap() []error { return []error{e.handler, e.release} }

// MemoryStore is an in-process DedupStore for tests and single-replica tools.
type MemoryStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// Claim implements DedupStore.
func (m *MemoryStore) Claim(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seen[id]; ok {
		return false, nil
	}
	if m.seen == nil {
		m.seen = map[string]struct{}{}
	}
	m.seen[id] = struct{}{}
	return true, nil
}

// Release implements DedupStore.
func (m *MemoryStore) Release(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.seen, id)
	return nil
}
