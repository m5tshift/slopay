package storage

import (
	"context"
	"errors"
	"time"
)

// Backoff is an exponential retry policy. The zero value means: 500 ms initial delay, 15 s
// maximum delay, 2 min in total (SRS INF-2.4).
type Backoff struct {
	Initial time.Duration
	Max     time.Duration
	// Total is the time after the first attempt when retrying stops.
	Total time.Duration

	// sleep and now are replaced in tests.
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time
}

func (b Backoff) withDefaults() Backoff {
	if b.Initial <= 0 {
		b.Initial = 500 * time.Millisecond
	}
	if b.Max <= 0 {
		b.Max = 15 * time.Second
	}
	if b.Total <= 0 {
		b.Total = 2 * time.Minute
	}
	if b.sleep == nil {
		b.sleep = sleepCtx
	}
	if b.now == nil {
		b.now = time.Now
	}
	return b
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Retry calls op until it succeeds, fails with an error for which retryable is false, the
// backoff budget is spent or ctx ends. onRetry (may be nil) is called before each wait.
func Retry(ctx context.Context, b Backoff, retryable func(error) bool, onRetry func(attempt int, err error, wait time.Duration), op func(ctx context.Context) error) error {
	b = b.withDefaults()
	start := b.now()
	delay := b.Initial
	for attempt := 1; ; attempt++ {
		err := op(ctx)
		if err == nil || !retryable(err) {
			return err
		}
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		if b.now().Add(delay).Sub(start) > b.Total {
			return err
		}
		if onRetry != nil {
			onRetry(attempt, err, delay)
		}
		if serr := b.sleep(ctx, delay); serr != nil {
			return errors.Join(err, serr)
		}
		delay = min(delay*2, b.Max)
	}
}
