// Package store keeps the latest snapshot of every pair in Valkey (SRS 4.1.7: HASH "last rate") and picks
// the snapshot a replica serves: the newer of Valkey and the in-memory cache filled from rate.latest. Valkey is
// a cache without persistence; when it is unavailable a replica serves from memory (SRS 4.1.7).
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
)

// KeyPrefix is the prefix of the HASH of a pair: rate:latest:ETH-USD.
const KeyPrefix = "rate:latest:"

// Fields of the HASH.
const (
	fieldStatus    = "status"    // ok or diverged
	fieldUpdatedAt = "updatedAt" // RFC 3339 with nanoseconds, the time of the snapshot
	fieldPrice     = "price"     // decimal string
	fieldQuotedAt  = "quotedAt"  // RFC 3339 with nanoseconds
	fieldSources   = "sources"   // comma-separated

	statusOK       = "ok"
	statusDiverged = "diverged"
)

// ErrNotConnected is returned until the first connection to Valkey succeeds.
var ErrNotConnected = errors.New("store: valkey is not connected")

// Valkey reads and writes snapshots. The client is attached when the background connection succeeds, so
// the service starts and serves from memory while Valkey is down. Safe for concurrent use.
type Valkey struct {
	timeout time.Duration

	mu     sync.RWMutex
	client valkey.Client
}

// New returns a store without a client. timeout bounds one Valkey call.
func New(timeout time.Duration) *Valkey { return &Valkey{timeout: timeout} }

// Attach sets the client.
func (v *Valkey) Attach(c valkey.Client) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.client = c
}

func (v *Valkey) get() valkey.Client {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.client
}

// Client returns the attached client, nil until the connection succeeds.
func (v *Valkey) Client() valkey.Client { return v.get() }

// Connect calls open until it succeeds or ctx is done and attaches the client. open retries authentication
// errors itself (storage.OpenValkey); other errors are retried here every retry.
func (v *Valkey) Connect(ctx context.Context, open func(context.Context) (valkey.Client, error), retry time.Duration, log *slog.Logger) {
	for {
		c, err := open(ctx)
		if err == nil {
			v.Attach(c)
			log.InfoContext(ctx, "valkey connected")
			return
		}
		log.WarnContext(ctx, "valkey unavailable, serving from memory", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

// Close closes the client if it is attached.
func (v *Valkey) Close() {
	if c := v.get(); c != nil {
		c.Close()
	}
}

// Put writes the snapshot of its pair.
func (v *Valkey) Put(ctx context.Context, pair rate.Pair, s rate.Snapshot) error {
	c := v.get()
	if c == nil {
		return ErrNotConnected
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	fv := c.B().Hset().Key(KeyPrefix + string(pair)).FieldValue()
	for _, f := range encode(s) {
		fv = fv.FieldValue(f[0], f[1])
	}
	if err := c.Do(ctx, fv.Build()).Error(); err != nil {
		return fmt.Errorf("store: put %s: %w", pair, err)
	}
	return nil
}

// Get reads the snapshot of a pair; ok is false when Valkey has none.
func (v *Valkey) Get(ctx context.Context, pair rate.Pair) (rate.Snapshot, bool, error) {
	c := v.get()
	if c == nil {
		return rate.Snapshot{}, false, ErrNotConnected
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	m, err := c.Do(ctx, c.B().Hgetall().Key(KeyPrefix+string(pair)).Build()).AsStrMap()
	if err != nil {
		return rate.Snapshot{}, false, fmt.Errorf("store: get %s: %w", pair, err)
	}
	return decode(pair, m)
}

// encode turns a snapshot into HASH fields. A diverged snapshot keeps the last reliable rate if it has one:
// its age stays observable, and it is never served while the mark is set.
func encode(s rate.Snapshot) [][2]string {
	status := statusOK
	if s.Diverged {
		status = statusDiverged
	}
	out := [][2]string{{fieldStatus, status}, {fieldUpdatedAt, s.UpdatedAt.UTC().Format(time.RFC3339Nano)}}
	if !s.Rate.Price.IsZero() {
		srcs := make([]string, len(s.Rate.Sources))
		for i, src := range s.Rate.Sources {
			srcs[i] = string(src)
		}
		out = append(out,
			[2]string{fieldPrice, s.Rate.Price.String()},
			[2]string{fieldQuotedAt, s.Rate.QuotedAt.UTC().Format(time.RFC3339Nano)},
			[2]string{fieldSources, strings.Join(srcs, ",")})
	}
	return out
}

// decode reads HASH fields; an empty map is no snapshot.
func decode(pair rate.Pair, m map[string]string) (rate.Snapshot, bool, error) {
	if len(m) == 0 {
		return rate.Snapshot{}, false, nil
	}
	var s rate.Snapshot
	switch m[fieldStatus] {
	case statusOK:
	case statusDiverged:
		s.Diverged = true
	default:
		return rate.Snapshot{}, false, fmt.Errorf("store: %s: unknown status %q", pair, m[fieldStatus])
	}
	var err error
	if s.UpdatedAt, err = time.Parse(time.RFC3339Nano, m[fieldUpdatedAt]); err != nil {
		return rate.Snapshot{}, false, fmt.Errorf("store: %s: updatedAt: %w", pair, err)
	}
	if m[fieldPrice] == "" {
		if !s.Diverged {
			return rate.Snapshot{}, false, fmt.Errorf("store: %s: no price", pair)
		}
		return s, true, nil
	}
	r := rate.Rate{Pair: pair}
	if r.Price, err = rate.ParsePrice(m[fieldPrice]); err != nil {
		return rate.Snapshot{}, false, fmt.Errorf("store: %s: %w", pair, err)
	}
	if r.QuotedAt, err = time.Parse(time.RFC3339Nano, m[fieldQuotedAt]); err != nil {
		return rate.Snapshot{}, false, fmt.Errorf("store: %s: quotedAt: %w", pair, err)
	}
	for src := range strings.SplitSeq(m[fieldSources], ",") {
		if src != string(rate.Coinbase) && src != string(rate.OKX) {
			return rate.Snapshot{}, false, fmt.Errorf("store: %s: unknown source %q", pair, src)
		}
		r.Sources = append(r.Sources, rate.Source(src))
	}
	s.Rate = r
	return s, true, nil
}

// Getter reads a snapshot from Valkey.
type Getter interface {
	Get(ctx context.Context, pair rate.Pair) (rate.Snapshot, bool, error)
}

// Latest picks the snapshot a replica serves.
type Latest struct {
	store Getter
	cache *rate.Cache
	log   *slog.Logger
}

// NewLatest combines Valkey and the in-memory cache.
func NewLatest(store Getter, cache *rate.Cache, log *slog.Logger) *Latest {
	return &Latest{store: store, cache: cache, log: log}
}

// Get returns the newer of the Valkey and in-memory snapshots. A Valkey error is not an error of the request:
// the replica serves from memory (SRS 4.1.7).
func (l *Latest) Get(ctx context.Context, pair rate.Pair) (rate.Snapshot, bool) {
	vs, vok, err := l.store.Get(ctx, pair)
	if err != nil && !errors.Is(err, ErrNotConnected) {
		l.log.DebugContext(ctx, "valkey read failed, serving from memory", "pair", pair, "error", err)
	}
	ms, mok := l.cache.Get(pair)
	return rate.Newer(vs, vok && err == nil, ms, mok)
}
