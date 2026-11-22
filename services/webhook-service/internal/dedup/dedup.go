// Package dedup is the best-effort deduplication of webhooks by event.id in Valkey (SRS 4.1.7): SET NX with a 24 h
// TTL. Without Valkey, or when a call fails, the event is delivered without deduplication: the merchant deduplicates
// by event.id anyway.
package dedup

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// Store is a kafka.DedupStore that attaches its backend later and never fails.
type Store struct {
	timeout time.Duration
	log     *slog.Logger

	mu      sync.RWMutex
	backend kafka.DedupStore
}

var _ kafka.DedupStore = (*Store)(nil)

// New returns a store without a backend; timeout bounds one backend call.
func New(timeout time.Duration, log *slog.Logger) *Store {
	if log == nil {
		log = slog.Default()
	}
	return &Store{timeout: timeout, log: log}
}

// Attach sets the backend, e.g. storage.ValkeyDedup.
func (s *Store) Attach(b kafka.DedupStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backend = b
}

// Connect calls open until it succeeds or ctx is done, then attaches the backend.
func (s *Store) Connect(ctx context.Context, open func(context.Context) (kafka.DedupStore, error), retry time.Duration) {
	for {
		b, err := open(ctx)
		if err == nil {
			s.Attach(b)
			s.log.InfoContext(ctx, "valkey connected, webhooks are deduplicated")
			return
		}
		s.log.WarnContext(ctx, "valkey unavailable, delivering without deduplication", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

func (s *Store) get() kafka.DedupStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.backend
}

// Claim implements kafka.DedupStore; without a working backend every event is new.
func (s *Store) Claim(ctx context.Context, id string) (bool, error) {
	b := s.get()
	if b == nil {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	first, err := b.Claim(ctx, id)
	if err != nil {
		s.log.WarnContext(ctx, "dedup claim failed, delivering without deduplication", "event_id", id, "error", err)
		return true, nil
	}
	return first, nil
}

// Wrap returns a handler that delivers an event once per event.id. A failure of next is repeated with backoff
// (initial, doubling up to max) without claiming again, so a repeat is never taken for a duplicate; only when ctx
// ends is the claim released for the redelivery after a restart.
func (s *Store) Wrap(next kafka.Handler, initial, maxDelay time.Duration) kafka.Handler {
	return func(ctx context.Context, ev kafka.Event) error {
		first, err := s.Claim(ctx, ev.ID)
		if err != nil || !first {
			return err
		}
		for delay := initial; ; delay = min(delay*2, maxDelay) {
			err := next(ctx, ev)
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				_ = s.Release(context.WithoutCancel(ctx), ev.ID)
				return err
			}
			s.log.ErrorContext(ctx, "handling failed, repeating", "event_id", ev.ID, "error", err)
			select {
			case <-ctx.Done():
				_ = s.Release(context.WithoutCancel(ctx), ev.ID)
				return errors.Join(err, ctx.Err())
			case <-time.After(delay):
			}
		}
	}
}

// Release implements kafka.DedupStore; a failure only leaves the key until its TTL.
func (s *Store) Release(ctx context.Context, id string) error {
	b := s.get()
	if b == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := b.Release(ctx, id); err != nil {
		s.log.WarnContext(ctx, "dedup release failed", "event_id", id, "error", err)
	}
	return nil
}
