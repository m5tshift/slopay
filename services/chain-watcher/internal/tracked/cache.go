package tracked

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ErrNoClient is returned while Valkey has not been connected.
var ErrNoClient = errors.New("tracked: valkey is not connected")

// ErrStale is returned by Members when the set does not hold the token of the last load: Valkey restarted, lost
// the data or was loaded by another instance.
var ErrStale = errors.New("tracked: valkey set is stale")

// tokenPrefix marks the member that confirms a complete load; addresses start with 0x and never match it.
const tokenPrefix = "token:"

// Valkey is the cache of the searched tracked addresses: a SET under chain-watcher:<chainId>:tracked (SRS 4.1.7).
// Besides the addresses the set holds the token of the load that filled it, so the loss of the data is noticed.
// The client is attached when the background connection succeeds. Safe for concurrent use.
type Valkey struct {
	key     string
	timeout time.Duration
	log     *slog.Logger

	mu     sync.RWMutex
	client valkey.Client
}

// NewValkey returns a cache without a client. timeout bounds one Valkey call.
func NewValkey(chainID int64, timeout time.Duration, log *slog.Logger) *Valkey {
	if log == nil {
		log = slog.Default()
	}
	return &Valkey{key: Key(chainID), timeout: timeout, log: log}
}

// Key returns the key of the set of a chain.
func Key(chainID int64) string { return "chain-watcher:" + strconv.FormatInt(chainID, 10) + ":tracked" }

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

// Connect calls open until it succeeds or ctx is done and attaches the client. open retries authentication
// errors itself (storage.OpenValkey); other errors are retried here every retry.
func (v *Valkey) Connect(ctx context.Context, open func(context.Context) (valkey.Client, error), retry time.Duration) {
	for {
		c, err := open(ctx)
		if err == nil {
			v.Attach(c)
			v.log.InfoContext(ctx, "valkey connected")
			return
		}
		v.log.WarnContext(ctx, "valkey unavailable, checking addresses in mongodb", "error", err)
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

func (v *Valkey) conn(ctx context.Context) (valkey.Client, context.Context, context.CancelFunc, error) {
	c := v.get()
	if c == nil {
		return nil, ctx, func() {}, ErrNoClient
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	return c, ctx, cancel, nil
}

// Members checks addrs with SISMEMBER. It returns ErrStale when the set does not hold token.
func (v *Valkey) Members(ctx context.Context, token string, addrs []string) (map[string]bool, error) {
	c, ctx, cancel, err := v.conn(ctx)
	defer cancel()
	if err != nil {
		return nil, err
	}
	cmds := make(valkey.Commands, 0, len(addrs)+1)
	cmds = append(cmds, c.B().Sismember().Key(v.key).Member(tokenPrefix+token).Build())
	for _, a := range addrs {
		cmds = append(cmds, c.B().Sismember().Key(v.key).Member(a).Build())
	}
	res := c.DoMulti(ctx, cmds...)
	if n, err := res[0].AsInt64(); err != nil {
		return nil, fmt.Errorf("tracked: sismember: %w", err)
	} else if n == 0 {
		return nil, ErrStale
	}
	out := make(map[string]bool, len(addrs))
	for i, a := range addrs {
		n, err := res[i+1].AsInt64()
		if err != nil {
			return nil, fmt.Errorf("tracked: sismember: %w", err)
		}
		if n == 1 {
			out[a] = true
		}
	}
	return out, nil
}

// Load replaces the set: it deletes the key, adds every batch fill passes to add, and adds the token last, so
// an interrupted load leaves the set stale.
func (v *Valkey) Load(ctx context.Context, token string, fill func(add func([]string) error) error) error {
	if v.get() == nil {
		return ErrNoClient
	}
	if err := v.do(ctx, func(c valkey.Client) valkey.Completed { return c.B().Del().Key(v.key).Build() }); err != nil {
		return fmt.Errorf("tracked: load: %w", err)
	}
	if err := fill(func(addrs []string) error { return v.Add(ctx, addrs) }); err != nil {
		return fmt.Errorf("tracked: load: %w", err)
	}
	if err := v.Add(ctx, []string{tokenPrefix + token}); err != nil {
		return fmt.Errorf("tracked: load: %w", err)
	}
	return nil
}

// Add adds addrs with SADD.
func (v *Valkey) Add(ctx context.Context, addrs []string) error {
	if len(addrs) == 0 {
		return nil
	}
	return v.do(ctx, func(c valkey.Client) valkey.Completed { return c.B().Sadd().Key(v.key).Member(addrs...).Build() })
}

// Remove removes addrs with SREM.
func (v *Valkey) Remove(ctx context.Context, addrs []string) error {
	if len(addrs) == 0 {
		return nil
	}
	return v.do(ctx, func(c valkey.Client) valkey.Completed { return c.B().Srem().Key(v.key).Member(addrs...).Build() })
}

func (v *Valkey) do(ctx context.Context, cmd func(valkey.Client) valkey.Completed) error {
	c, ctx, cancel, err := v.conn(ctx)
	defer cancel()
	if err != nil {
		return err
	}
	return c.Do(ctx, cmd(c)).Error()
}
