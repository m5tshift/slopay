// Package breaker implements a Circuit Breaker (SRS 4.1.9, NFR-09) with closed, open and half-open states.
// Thresholds come from configuration; the current state is exported as an OpenTelemetry gauge
// labelled with the dependency name (dashboard SRS 4.5.3, item 4).
package breaker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// State is the breaker state. Its numeric value is what the state gauge reports.
type State int

const (
	Closed   State = 0
	HalfOpen State = 1
	Open     State = 2
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half-open"
	case Open:
		return "open"
	}
	return "unknown"
}

// ErrOpen is returned by Do and Allow while the breaker rejects calls.
var ErrOpen = errors.New("breaker: circuit open")

// Config holds the breaker thresholds.
type Config struct {
	// Dependency names the protected dependency; it is the "dependency" label of the state metric.
	Dependency string
	// FailureThreshold is the number of consecutive failures that opens the breaker.
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before letting probes through.
	OpenTimeout time.Duration
	// HalfOpenMax is the number of concurrent probe calls allowed in half-open state;
	// it is also the number of consecutive probe successes that closes the breaker.
	HalfOpenMax int
	// Now substitutes the clock in tests; defaults to time.Now.
	Now func() time.Time
	// Meter registers the state gauge; defaults to the global OTel meter.
	Meter metric.Meter
	// IsFailure classifies an error returned by the protected call; defaults to err != nil.
	IsFailure func(error) bool
}

// ConfigFromEnv reads <prefix>_FAILURE_THRESHOLD, <prefix>_OPEN_TIMEOUT (Go duration) and
// <prefix>_HALF_OPEN_MAX on top of the given defaults for dependency.
func ConfigFromEnv(prefix, dependency string, def Config) (Config, error) {
	c := def
	c.Dependency = dependency
	if v := os.Getenv(prefix + "_FAILURE_THRESHOLD"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("breaker: %s_FAILURE_THRESHOLD: %w", prefix, err)
		}
		c.FailureThreshold = n
	}
	if v := os.Getenv(prefix + "_OPEN_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("breaker: %s_OPEN_TIMEOUT: %w", prefix, err)
		}
		c.OpenTimeout = d
	}
	if v := os.Getenv(prefix + "_HALF_OPEN_MAX"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("breaker: %s_HALF_OPEN_MAX: %w", prefix, err)
		}
		c.HalfOpenMax = n
	}
	return c, nil
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg Config

	mu        sync.Mutex
	state     State
	failures  int // consecutive failures while closed
	openedAt  time.Time
	inFlight  int // probes running in half-open
	successes int // consecutive probe successes in half-open
}

// New validates cfg and registers the state gauge.
func New(cfg Config) (*Breaker, error) {
	switch {
	case cfg.Dependency == "":
		return nil, errors.New("breaker: dependency name is required")
	case cfg.FailureThreshold < 1:
		return nil, errors.New("breaker: failure threshold must be >= 1")
	case cfg.OpenTimeout <= 0:
		return nil, errors.New("breaker: open timeout must be > 0")
	case cfg.HalfOpenMax < 1:
		return nil, errors.New("breaker: half-open max must be >= 1")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(err error) bool { return err != nil }
	}
	m := cfg.Meter
	if m == nil {
		m = otel.Meter("github.com/m5tshift/slopay/pkg/breaker")
	}
	b := &Breaker{cfg: cfg}
	_, err := m.Int64ObservableGauge("slopay_circuit_breaker_state",
		metric.WithDescription("Circuit Breaker state: 0 closed, 1 half-open, 2 open"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(b.State()), metric.WithAttributes(attribute.String("dependency", cfg.Dependency)))
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("breaker: gauge: %w", err)
	}
	return b, nil
}

// State returns the current state, moving open to half-open when the open timeout has elapsed.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	return b.state
}

// refresh must be called with mu held.
func (b *Breaker) refresh() {
	if b.state == Open && b.cfg.Now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
		b.state = HalfOpen
		b.inFlight, b.successes = 0, 0
	}
}

// Do runs fn if the breaker admits the call and records the outcome.
func (b *Breaker) Do(fn func() error) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	err = fn()
	done(!b.cfg.IsFailure(err))
	return err
}

// Allow admits a call or returns ErrOpen. The returned callback must be called exactly once with
// the outcome of the call.
func (b *Breaker) Allow() (done func(success bool), err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	switch b.state {
	case Open:
		return nil, ErrOpen
	case HalfOpen:
		if b.inFlight >= b.cfg.HalfOpenMax {
			return nil, ErrOpen
		}
		b.inFlight++
		gen := b.openedAt
		return func(ok bool) { b.record(ok, true, gen) }, nil
	}
	return func(ok bool) { b.record(ok, false, time.Time{}) }, nil
}

func (b *Breaker) record(ok, probe bool, gen time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		// The result belongs to a probe from an earlier half-open period: ignore it.
		if b.state != HalfOpen || !b.openedAt.Equal(gen) {
			return
		}
		b.inFlight--
		if !ok {
			b.trip()
			return
		}
		b.successes++
		if b.successes >= b.cfg.HalfOpenMax {
			b.state = Closed
			b.failures = 0
		}
		return
	}
	// A call admitted while closed that finishes after the state changed does not affect it.
	if b.state != Closed {
		return
	}
	if ok {
		b.failures = 0
		return
	}
	b.failures++
	if b.failures >= b.cfg.FailureThreshold {
		b.trip()
	}
}

func (b *Breaker) trip() {
	b.state = Open
	b.openedAt = b.cfg.Now()
	b.failures, b.inFlight, b.successes = 0, 0, 0
}
