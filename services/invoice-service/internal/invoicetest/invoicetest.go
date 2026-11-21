// Package invoicetest holds test doubles of invoice-service. There is no private key material here: Xpub
// is a public test vector.
package invoicetest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

// Xpub is the master public key of BIP-32 test vector 1 (also the example of the merchant-service contract).
const Xpub = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8"

// Rates is an invoice.RateSource with a settable answer.
type Rates struct {
	mu    sync.Mutex
	quote invoice.Quote
	err   error
	calls atomic.Int64
	pair  string
}

// NewRates returns a source answering price quoted at quotedAt with age 0.
func NewRates(price string, quotedAt time.Time) *Rates {
	return &Rates{quote: invoice.Quote{Price: price, QuotedAt: quotedAt}}
}

// Set changes the answer.
func (r *Rates) Set(q invoice.Quote, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.quote, r.err = q, err
}

// Calls is the number of Rate calls.
func (r *Rates) Calls() int64 { return r.calls.Load() }

// Pair is the pair of the last Rate call.
func (r *Rates) Pair() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pair
}

// Rate implements invoice.RateSource.
func (r *Rates) Rate(_ context.Context, pair string) (invoice.Quote, error) {
	r.calls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pair = pair
	return r.quote, r.err
}

// Cache is an in-memory invoice.StatusCache.
type Cache struct {
	mu    sync.Mutex
	pages map[uuid.UUID]invoice.Page
	sets  int
}

// NewCache returns an empty cache.
func NewCache() *Cache { return &Cache{pages: map[uuid.UUID]invoice.Page{}} }

// Get implements invoice.StatusCache.
func (c *Cache) Get(_ context.Context, id uuid.UUID) (invoice.Page, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pages[id]
	return p, ok
}

// Set implements invoice.StatusCache.
func (c *Cache) Set(_ context.Context, p invoice.Page) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages[p.InvoiceID] = p
	c.sets++
}

// Sets is the number of Set calls.
func (c *Cache) Sets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sets
}

// Flush empties the cache, like FLUSHALL.
func (c *Cache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages = map[uuid.UUID]invoice.Page{}
}
