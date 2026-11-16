package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/m5tshift/slopay/pkg/kafka"
)

const meterName = "github.com/m5tshift/slopay/pkg/outbox"

// Publisher publishes one event and returns after the brokers acknowledged it. *kafka.Producer implements it.
type Publisher interface {
	Publish(ctx context.Context, topic string, ev kafka.Event) error
}

// Config configures a Relay. Zero values take the defaults.
type Config struct {
	Pool      *pgxpool.Pool
	Publisher Publisher
	// Interval is the pause between polls when the outbox is empty (default 500ms).
	Interval time.Duration
	// MaxBackoff caps the pause after failed cycles, which grows from Interval (default 30s).
	MaxBackoff time.Duration
	// BatchSize is the number of rows taken per cycle (default 100).
	BatchSize int
	// Retention is how long published rows are kept before cleanup (default 24h).
	Retention time.Duration
	Logger    *slog.Logger
	// MeterProvider receives the metrics (default: the global provider).
	MeterProvider metric.MeterProvider
}

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = 500 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.Retention <= 0 {
		c.Retention = 24 * time.Hour
	}
	if c.MeterProvider == nil {
		c.MeterProvider = otel.GetMeterProvider()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Relay moves events from the outbox table to Kafka.
type Relay struct {
	cfg   Config
	store store

	pending   atomic.Int64 // rows waiting, as of the last stats refresh
	oldestNs  atomic.Int64 // age of the oldest waiting row
	published metric.Int64Counter
	failures  metric.Int64Counter
}

// NewRelay builds a relay and registers its metrics:
//
//	outbox.pending             rows waiting for publication
//	outbox.oldest_age_seconds  age of the oldest waiting row (the publication lag)
//	outbox.published           events published
//	outbox.publish.errors      failed publications
func NewRelay(cfg Config) (*Relay, error) {
	if cfg.Pool == nil || cfg.Publisher == nil {
		return nil, errors.New("outbox: pool and publisher are required")
	}
	cfg.applyDefaults()
	return newRelay(cfg, pgStore{cfg.Pool})
}

func newRelay(cfg Config, st store) (*Relay, error) {
	r := &Relay{cfg: cfg, store: st}
	m := cfg.MeterProvider.Meter(meterName)
	var err error
	if r.published, err = m.Int64Counter("outbox.published", metric.WithDescription("Events published to Kafka")); err != nil {
		return nil, err
	}
	if r.failures, err = m.Int64Counter("outbox.publish.errors", metric.WithDescription("Failed outbox cycles")); err != nil {
		return nil, err
	}
	if _, err = m.Int64ObservableGauge("outbox.pending",
		metric.WithDescription("Events waiting for publication"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.pending.Load())
			return nil
		})); err != nil {
		return nil, err
	}
	if _, err = m.Float64ObservableGauge("outbox.oldest_age_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest event waiting for publication"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(time.Duration(r.oldestNs.Load()).Seconds())
			return nil
		})); err != nil {
		return nil, err
	}
	return r, nil
}

// Flush publishes one batch and returns the number of events published. It returns 0 without error when
// another replica holds the relay lock or the outbox is empty. After a failure the events published
// before it stay published and the failed one is retried on the next call.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	n := 0
	_, err := r.store.withBatch(ctx, r.cfg.BatchSize, func(rows []row) ([]int64, error) {
		done := make([]int64, 0, len(rows))
		for _, rw := range rows {
			if err := r.cfg.Publisher.Publish(rw.traceContext(ctx), rw.topic, rw.event); err != nil {
				r.failures.Add(ctx, 1)
				return done, fmt.Errorf("outbox: publish row %d (%s): %w", rw.id, rw.event.ID, err)
			}
			done = append(done, rw.id)
			n++
			r.published.Add(ctx, 1)
		}
		return done, nil
	})
	return n, err
}

// Run relays until ctx is cancelled and returns nil. Errors are logged and retried with growing pauses.
func (r *Relay) Run(ctx context.Context) error {
	wait := r.cfg.Interval
	lastPurge := time.Time{}
	for ctx.Err() == nil {
		n, err := r.Flush(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			r.cfg.Logger.ErrorContext(ctx, "outbox: relay cycle failed", "error", err, "retry_in", wait)
			r.refresh(ctx)
			sleep(ctx, wait)
			wait = min(wait*2, r.cfg.MaxBackoff)
			continue
		case err != nil:
			return nil
		}
		wait = r.cfg.Interval
		r.refresh(ctx)
		if time.Since(lastPurge) >= time.Minute {
			lastPurge = time.Now()
			if _, err := r.store.purge(ctx, r.cfg.Retention); err != nil && ctx.Err() == nil {
				r.cfg.Logger.WarnContext(ctx, "outbox: purge failed", "error", err)
			}
		}
		if n < r.cfg.BatchSize { // a full batch means more is waiting
			sleep(ctx, r.cfg.Interval)
		}
	}
	return nil
}

// refresh reads the lag from the database into the gauges.
func (r *Relay) refresh(ctx context.Context) {
	pending, oldest, err := r.store.stats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.cfg.Logger.WarnContext(ctx, "outbox: lag stats failed", "error", err)
		}
		return
	}
	r.pending.Store(pending)
	r.oldestNs.Store(int64(oldest))
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
