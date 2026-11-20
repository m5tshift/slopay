// Package cache is the status cache of the payment page in Valkey (SRS 4.1.7): a STRING with the JSON of
// the page under invoice:status:<id>, TTL 30 s. It is a cache only: PostgreSQL is the source of truth, every
// Valkey error is a miss, and the service starts and works without Valkey.
package cache

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

// KeyPrefix is the prefix of the status key: invoice:status:<id>.
const KeyPrefix = "invoice:status:"

// Valkey is an invoice.StatusCache. The client is attached when the background connection succeeds.
// Safe for concurrent use.
type Valkey struct {
	ttl     time.Duration
	timeout time.Duration
	log     *slog.Logger

	mu     sync.RWMutex
	client valkey.Client
}

var _ invoice.StatusCache = (*Valkey)(nil)

// New returns a cache without a client. ttl is the lifetime of an entry, timeout bounds one Valkey call.
func New(ttl, timeout time.Duration, log *slog.Logger) *Valkey {
	return &Valkey{ttl: ttl, timeout: timeout, log: log}
}

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
		v.log.WarnContext(ctx, "valkey unavailable, reading statuses from postgresql", "error", err)
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

// Key returns the key of an invoice.
func Key(id uuid.UUID) string { return KeyPrefix + id.String() }

// Get implements invoice.StatusCache.
func (v *Valkey) Get(ctx context.Context, id uuid.UUID) (invoice.Page, bool) {
	c := v.get()
	if c == nil {
		return invoice.Page{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	b, err := c.Do(ctx, c.B().Get().Key(Key(id)).Build()).AsBytes()
	if err != nil {
		if !valkey.IsValkeyNil(err) {
			v.log.WarnContext(ctx, "status cache read failed", "invoice_id", id, "error", err)
		}
		return invoice.Page{}, false
	}
	var p invoice.Page
	if err := json.Unmarshal(b, &p); err != nil || p.InvoiceID != id {
		v.log.WarnContext(ctx, "status cache entry is invalid", "invoice_id", id)
		return invoice.Page{}, false
	}
	return p, true
}

// Set implements invoice.StatusCache.
func (v *Valkey) Set(ctx context.Context, p invoice.Page) {
	c := v.get()
	if c == nil {
		return
	}
	b, err := json.Marshal(p)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	if err := c.Do(ctx, c.B().Set().Key(Key(p.InvoiceID)).Value(string(b)).Px(v.ttl).Build()).Error(); err != nil {
		v.log.WarnContext(ctx, "status cache write failed", "invoice_id", p.InvoiceID, "error", err)
	}
}
