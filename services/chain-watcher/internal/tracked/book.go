// Package tracked answers which addresses chain-watcher tracks (SRS 4.1.3, 4.1.7). The projection in MongoDB is
// the source of truth; the searched addresses are cached in a Valkey SET and checked with SISMEMBER. Without
// Valkey, or after its data was lost, the addresses are checked in MongoDB until the cache is loaded again.
package tracked

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/m5tshift/slopay/services/chain-watcher/internal/projection"
)

// batch bounds one query or command over many addresses.
const batch = 1000

// Store is the projection in MongoDB (projection.Store).
type Store interface {
	Lookup(ctx context.Context, addrs []string) (map[string]bool, error)
	Pending(ctx context.Context, now time.Time, limit int) ([]projection.Pending, error)
	MarkRescanned(ctx context.Context, addrs []string) error
	Expired(ctx context.Context, now time.Time, limit int) ([]string, error)
	Delete(ctx context.Context, addrs []string, now time.Time) error
	Searched(ctx context.Context, batch int, fn func([]string) error) error
}

// Cache is the set in Valkey (Valkey).
type Cache interface {
	Members(ctx context.Context, token string, addrs []string) (map[string]bool, error)
	Load(ctx context.Context, token string, fill func(add func([]string) error) error) error
	Add(ctx context.Context, addrs []string) error
	Remove(ctx context.Context, addrs []string) error
}

// Book is the watcher.Addresses of the leader. The cache is used only after Book has loaded it: the token of the
// load is kept in memory and in the set, so a restarted service or Valkey always loads it again.
type Book struct {
	store  Store
	cache  Cache
	log    *slog.Logger
	retry  time.Duration
	now    func() time.Time
	token  func() string
	source metric.Int64Counter

	mu       sync.Mutex
	loaded   string // token of the loaded cache, "" while MongoDB is used
	nextLoad time.Time
	warned   bool
}

// NewBook returns a Book. retry is the pause between attempts to load the cache; meter defaults to the global
// OTel provider.
func NewBook(store Store, cache Cache, retry time.Duration, meter metric.Meter, log *slog.Logger) (*Book, error) {
	if log == nil {
		log = slog.Default()
	}
	if meter == nil {
		meter = otel.Meter("github.com/m5tshift/slopay/services/chain-watcher/internal/tracked")
	}
	source, err := meter.Int64Counter("slopay_chain_address_lookups",
		metric.WithDescription("Address checks of transaction recipients, by source: cache or mongodb."))
	if err != nil {
		return nil, err
	}
	return &Book{store: store, cache: cache, log: log, retry: retry, now: time.Now, token: uuid.NewString, source: source}, nil
}

// Tracked returns which of addrs are tracked and searched.
func (b *Book) Tracked(ctx context.Context, addrs []string) (map[string]bool, error) {
	if len(addrs) == 0 {
		return map[string]bool{}, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loaded == "" {
		b.load(ctx)
	}
	if b.loaded != "" {
		m, err := b.cache.Members(ctx, b.loaded, addrs)
		if err == nil {
			b.source.Add(ctx, int64(len(addrs)), metric.WithAttributes(attribute.String("source", "cache")))
			return m, nil
		}
		b.drop(ctx, err)
	}
	b.source.Add(ctx, int64(len(addrs)), metric.WithAttributes(attribute.String("source", "mongodb")))
	return b.store.Lookup(ctx, addrs)
}

// Pending returns the addresses whose earlier blocks have not been searched, with a window open at chain time now.
func (b *Book) Pending(ctx context.Context, now time.Time) ([]projection.Pending, error) {
	return b.store.Pending(ctx, now, batch)
}

// Rescanned records that the earlier blocks of addrs have been searched: from now on they are checked in new
// blocks.
func (b *Book) Rescanned(ctx context.Context, addrs []string) error {
	if err := b.store.MarkRescanned(ctx, addrs); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loaded != "" {
		if err := b.cache.Add(ctx, addrs); err != nil {
			b.drop(ctx, err)
		}
	}
	return nil
}

// Expire stops tracking the addresses whose window ended before chain time now. The address is deleted from
// MongoDB first: a cache left with it is loaded again from MongoDB after a restart.
func (b *Book) Expire(ctx context.Context, now time.Time) error {
	for {
		addrs, err := b.store.Expired(ctx, now, batch)
		if err != nil || len(addrs) == 0 {
			return err
		}
		if err := b.store.Delete(ctx, addrs, now); err != nil {
			return err
		}
		// An address whose window was extended meanwhile is not deleted and stays in the cache.
		kept, err := b.store.Lookup(ctx, addrs)
		if err != nil {
			return err
		}
		gone := make([]string, 0, len(addrs))
		for _, a := range addrs {
			if !kept[a] {
				gone = append(gone, a)
			}
		}
		b.mu.Lock()
		if b.loaded != "" {
			if err := b.cache.Remove(ctx, gone); err != nil {
				b.drop(ctx, err)
			}
		}
		b.mu.Unlock()
		b.log.InfoContext(ctx, "tracking window ended", "addresses", len(gone))
		if len(addrs) < batch {
			return nil
		}
	}
}

// load fills the cache from MongoDB, at most once per retry.
func (b *Book) load(ctx context.Context) {
	now := b.now()
	if now.Before(b.nextLoad) {
		return
	}
	b.nextLoad = now.Add(b.retry)
	token := b.token()
	err := b.cache.Load(ctx, token, func(add func([]string) error) error { return b.store.Searched(ctx, batch, add) })
	if err != nil {
		if !b.warned {
			b.log.WarnContext(ctx, "address cache not loaded, checking addresses in mongodb", "error", err)
			b.warned = true
		}
		return
	}
	b.loaded, b.warned = token, false
	b.log.InfoContext(ctx, "address cache loaded")
}

// drop stops using the cache after a failed call; the next check loads it again.
func (b *Book) drop(ctx context.Context, err error) {
	b.log.WarnContext(ctx, "address cache failed, checking addresses in mongodb", "error", err)
	b.loaded, b.nextLoad, b.warned = "", time.Time{}, true
}
