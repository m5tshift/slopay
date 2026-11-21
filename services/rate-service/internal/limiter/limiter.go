// Package limiter is the outgoing rate limit of the requests to the exchanges (SRS 4.1.7, 4.1.8): a token
// bucket per source in Valkey. When Valkey is unavailable the leader counts the limit in the memory of the
// process (SRS 4.1.7), so a Valkey loss never lifts the limit.
package limiter

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

// KeyPrefix is the prefix of the HASH of a bucket: ratelimit:out:coinbase.
const KeyPrefix = "ratelimit:out:"

// Config is the limit of one source: Capacity requests per Window. The bucket holds Capacity tokens and
// gets one back every Window/Capacity.
type Config struct {
	Capacity int
	Window   time.Duration
}

// Validate checks that the limit is usable.
func (c Config) Validate() error {
	if c.Capacity <= 0 || c.Window <= 0 || c.Window/time.Duration(c.Capacity) < time.Millisecond {
		return fmt.Errorf("limiter: %d requests per %s is not a usable limit", c.Capacity, c.Window)
	}
	return nil
}

func (c Config) refill() time.Duration { return c.Window / time.Duration(c.Capacity) }

// script is one atomic step of the bucket on the clock of Valkey: refill by the elapsed time, take a token.
// KEYS[1] bucket; ARGV[1] capacity, ARGV[2] refill period in ms. Returns 1 when a token was taken, else 0.
const script = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local cap = tonumber(ARGV[1])
local per = tonumber(ARGV[2])
local h = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(h[1])
local ts = tonumber(h[2])
if tokens == nil or ts == nil then
  tokens = cap
  ts = now
else
  local add = math.floor((now - ts) / per)
  if add > 0 then
    tokens = math.min(cap, tokens + add)
    ts = ts + add * per
  end
  if tokens >= cap then ts = now end
end
local ok = 0
if tokens >= 1 then
  tokens = tokens - 1
  ok = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
redis.call('PEXPIRE', KEYS[1], cap * per * 2)
return ok
`

// Limiter is a set of token buckets by key. Safe for concurrent use.
type Limiter struct {
	limits  map[string]Config
	client  func() valkey.Client
	timeout time.Duration
	now     func() time.Time
	log     *slog.Logger
	lua     *valkey.Lua

	mu  sync.Mutex
	mem map[string]*bucket
}

// Option customises a Limiter.
type Option func(*Limiter)

// WithClock substitutes the clock of the in-memory buckets (tests).
func WithClock(now func() time.Time) Option { return func(l *Limiter) { l.now = now } }

// WithLogger sets the logger; the default is slog.Default().
func WithLogger(log *slog.Logger) Option { return func(l *Limiter) { l.log = log } }

// New returns a limiter. client returns the Valkey client or nil while it is not connected; timeout
// bounds one Valkey call.
func New(limits map[string]Config, client func() valkey.Client, timeout time.Duration, opts ...Option) (*Limiter, error) {
	if len(limits) == 0 {
		return nil, errors.New("limiter: no limits")
	}
	for k, c := range limits {
		if err := c.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
	}
	l := &Limiter{
		limits: limits, client: client, timeout: timeout, now: time.Now, log: slog.Default(),
		lua: valkey.NewLuaScript(script), mem: map[string]*bucket{},
	}
	for _, o := range opts {
		o(l)
	}
	return l, nil
}

// Allow takes a token of the bucket key and reports whether the request may go out. A key without a
// limit is refused.
func (l *Limiter) Allow(ctx context.Context, key string) bool {
	cfg, ok := l.limits[key]
	if !ok {
		return false
	}
	if l.client != nil {
		if c := l.client(); c != nil {
			allowed, err := l.allowValkey(ctx, c, key, cfg)
			if err == nil {
				return allowed
			}
			l.log.WarnContext(ctx, "valkey rate limit failed, counting in memory", "source", key, "error", err)
		}
	}
	return l.allowMemory(key, cfg)
}

func (l *Limiter) allowValkey(ctx context.Context, c valkey.Client, key string, cfg Config) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	n, err := l.lua.Exec(ctx, c, []string{KeyPrefix + key},
		[]string{strconv.Itoa(cfg.Capacity), strconv.FormatInt(cfg.refill().Milliseconds(), 10)}).AsInt64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (l *Limiter) allowMemory(key string, cfg Config) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.mem[key]
	if !ok {
		b = &bucket{}
		l.mem[key] = b
	}
	return b.take(l.now(), cfg)
}

// bucket is the in-memory twin of the Valkey script.
type bucket struct {
	init   bool
	tokens int
	ts     time.Time
}

func (b *bucket) take(now time.Time, cfg Config) bool {
	per := cfg.refill()
	if !b.init {
		b.init, b.tokens, b.ts = true, cfg.Capacity, now
	} else if add := int(now.Sub(b.ts) / per); add > 0 {
		b.tokens = min(cfg.Capacity, b.tokens+add)
		b.ts = b.ts.Add(time.Duration(add) * per)
	}
	if b.tokens >= cfg.Capacity {
		b.ts = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
