package ledger

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/money"
)

// Store keeps merchants, postings and their lines. Implementations: store.Postgres and MemStore.
type Store interface {
	// Record remembers the merchant and stores the postings whose ID is not stored yet, each with its lines
	// and its events in the outbox, in one transaction. It returns the number of new postings.
	Record(ctx context.Context, merchant uuid.UUID, postings []Posting) (int, error)
	// MerchantExists reports whether any invoice event of the merchant was seen.
	MerchantExists(ctx context.Context, merchant uuid.UUID) (bool, error)
	// MerchantTotals sums the postings of the merchant with PostedAt in [from, to) per asset.
	MerchantTotals(ctx context.Context, merchant uuid.UUID, from, to time.Time) ([]Totals, error)
	// MerchantPostings returns up to limit postings of the merchant with PostedAt in [from, to) after the
	// cursor, in page order.
	MerchantPostings(ctx context.Context, merchant uuid.UUID, from, to time.Time, after *Cursor, limit int) ([]Posting, error)
	// AddressTotals sums the postings of each address per asset; an address without postings is absent.
	AddressTotals(ctx context.Context, addresses []string) (map[string][]Totals, error)
	// AddressPostings returns up to limit postings of the address after the cursor, in page order.
	AddressPostings(ctx context.Context, address string, after *Cursor, limit int) ([]Posting, error)
}

// Totals are the sums of the postings of one asset.
type Totals struct {
	Asset    Asset
	Received *big.Int
	Fee      *big.Int
	Postings int64
}

// SortTotals orders totals by asset kind and contract, the order of the statements.
func SortTotals(ts []Totals) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].Asset.Key() < ts[j].Asset.Key() })
}

// Cursor is the position after the last posting of a page. Pages are ordered by PostedAt, ties by ID
// compared byte by byte.
type Cursor struct {
	PostedAt time.Time `json:"t"`
	ID       string    `json:"i"`
}

// Less reports whether p comes before the cursor position or at it, so it is not on the next page.
func (c Cursor) Less(p Posting) bool {
	if !p.PostedAt.Equal(c.PostedAt) {
		return p.PostedAt.Before(c.PostedAt)
	}
	return p.ID <= c.ID
}

// EncodeCursor returns the opaque nextCursor.
func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor of EncodeCursor; any other string is a ValidationError of the field cursor.
func DecodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, errInvalidCursor
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil || c.PostedAt.IsZero() || c.ID == "" {
		return Cursor{}, errInvalidCursor
	}
	return c, nil
}

// ValidationError is a request the statement API answers with 400.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Message }

var errInvalidCursor = &ValidationError{Field: "cursor", Message: "invalid cursor"}

// ErrNotFound: the merchant has no invoice events.
var ErrNotFound = errors.New("ledger: not found")

// Config of the service.
type Config struct {
	// FeeBps is the platform fee in basis points (SRS 4.1.3: 50 = 0.5 %).
	FeeBps int
	// DefaultLimit and MaxLimit bound a statement page (contract: 200 and 1000).
	DefaultLimit, MaxLimit int
}

// Service posts receipts and builds statements.
type Service struct {
	store Store
	cfg   Config
	now   func() time.Time
	log   *slog.Logger
}

// NewService returns the service; the fee rate must be 0..10000 basis points.
func NewService(store Store, cfg Config, log *slog.Logger) (*Service, error) {
	if cfg.FeeBps < 0 || cfg.FeeBps > money.BpsDenominator {
		return nil, fmt.Errorf("ledger: fee %d bps out of 0..%d", cfg.FeeBps, money.BpsDenominator)
	}
	if cfg.DefaultLimit <= 0 {
		cfg.DefaultLimit = 200
	}
	if cfg.MaxLimit < cfg.DefaultLimit {
		cfg.MaxLimit = max(1000, cfg.DefaultLimit)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, cfg: cfg, now: time.Now, log: log}, nil
}

// Apply handles one invoice event: it remembers the merchant and posts every transfer of PAID, UNDERPAID,
// LATE_PAID and invoice.late_transfer that is not posted yet. A repeated event changes nothing.
func (s *Service) Apply(ctx context.Context, ev kafka.Event) error {
	b, err := s.Prepare(ev)
	if err != nil {
		return err
	}
	return s.Record(ctx, b)
}

// Batch is what one invoice event changes in the ledger.
type Batch struct {
	EventType, EventID string
	MerchantID         uuid.UUID
	Postings           []Posting
}

// Prepare reads the invoice event and builds its postings with the configured fee at the current time.
// An error means the event can never be posted (ErrNotInvoiceEvent, ErrInvalidReceipt, malformed data).
func (s *Service) Prepare(ev kafka.Event) (Batch, error) {
	inv, err := FromEvent(ev)
	if err != nil {
		return Batch{}, err
	}
	at := s.now()
	b := Batch{EventType: ev.Type, EventID: ev.ID, MerchantID: inv.MerchantID, Postings: make([]Posting, 0, len(inv.Receipts))}
	seen := map[string]bool{}
	for _, r := range inv.Receipts {
		p, err := NewPosting(r, s.cfg.FeeBps, at)
		if err != nil {
			return Batch{}, fmt.Errorf("ledger: %s %s: %w", ev.Type, ev.ID, err)
		}
		if seen[p.ID] {
			continue
		}
		seen[p.ID] = true
		b.Postings = append(b.Postings, p)
	}
	return b, nil
}

// Record stores the batch; postings stored before are skipped by their ID.
func (s *Service) Record(ctx context.Context, b Batch) error {
	n, err := s.store.Record(ctx, b.MerchantID, b.Postings)
	if err != nil {
		return err
	}
	if len(b.Postings) > 0 {
		s.log.InfoContext(ctx, "receipts posted", "event_type", b.EventType, "event_id", b.EventID,
			"merchant_id", b.MerchantID, "new", n, "repeated", len(b.Postings)-n)
	}
	return nil
}

// Page is a page of postings with the cursor of the next one.
type Page struct {
	Items []Posting
	// Next is empty on the last page.
	Next string
}

// MerchantStatement is the statement of a merchant for [From, To).
type MerchantStatement struct {
	MerchantID uuid.UUID
	From, To   time.Time
	Totals     []Totals
	Page
}

func (s *Service) pageParams(limit *int, cursor *string) (int, *Cursor, error) {
	n := s.cfg.DefaultLimit
	if limit != nil {
		if *limit < 1 || *limit > s.cfg.MaxLimit {
			return 0, nil, &ValidationError{Field: "limit", Message: fmt.Sprintf("must be 1..%d", s.cfg.MaxLimit)}
		}
		n = *limit
	}
	if cursor == nil {
		return n, nil, nil
	}
	c, err := DecodeCursor(*cursor)
	if err != nil {
		return 0, nil, err
	}
	return n, &c, nil
}

func page(items []Posting, limit int) Page {
	if len(items) <= limit {
		return Page{Items: items}
	}
	items = items[:limit]
	last := items[len(items)-1]
	return Page{Items: items, Next: EncodeCursor(Cursor{PostedAt: last.PostedAt, ID: last.ID})}
}

// MerchantStatement returns the postings of the merchant in [from, to) and the totals of the whole period.
func (s *Service) MerchantStatement(ctx context.Context, merchant uuid.UUID, from, to time.Time, limit *int, cursor *string) (MerchantStatement, error) {
	if !to.After(from) {
		return MerchantStatement{}, &ValidationError{Field: "to", Message: "must be later than from"}
	}
	n, after, err := s.pageParams(limit, cursor)
	if err != nil {
		return MerchantStatement{}, err
	}
	ok, err := s.store.MerchantExists(ctx, merchant)
	if err != nil {
		return MerchantStatement{}, err
	}
	if !ok {
		return MerchantStatement{}, ErrNotFound
	}
	totals, err := s.store.MerchantTotals(ctx, merchant, from, to)
	if err != nil {
		return MerchantStatement{}, err
	}
	items, err := s.store.MerchantPostings(ctx, merchant, from, to, after, n+1)
	if err != nil {
		return MerchantStatement{}, err
	}
	return MerchantStatement{MerchantID: merchant, From: from, To: to, Totals: totals, Page: page(items, n)}, nil
}

// AddressTotals is the statement of one address.
type AddressTotals struct {
	Address string
	Totals  []Totals
}

var addressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// maxAddresses is the limit of the contract on addresses per request.
const maxAddresses = 100

// AddressesStatement returns the totals of each address over its whole life, in the request order.
func (s *Service) AddressesStatement(ctx context.Context, addresses []string) ([]AddressTotals, error) {
	if len(addresses) == 0 || len(addresses) > maxAddresses {
		return nil, &ValidationError{Field: "address", Message: fmt.Sprintf("1..%d addresses are required", maxAddresses)}
	}
	seen := map[string]bool{}
	for _, a := range addresses {
		if !addressPattern.MatchString(a) || seen[a] {
			return nil, &ValidationError{Field: "address", Message: "addresses must be unique lower-case hex"}
		}
		seen[a] = true
	}
	byAddress, err := s.store.AddressTotals(ctx, addresses)
	if err != nil {
		return nil, err
	}
	out := make([]AddressTotals, 0, len(addresses))
	for _, a := range addresses {
		ts := byAddress[a]
		if ts == nil {
			ts = []Totals{}
		}
		out = append(out, AddressTotals{Address: a, Totals: ts})
	}
	return out, nil
}

// AddressPostings returns a page of the postings of the address.
func (s *Service) AddressPostings(ctx context.Context, address string, limit *int, cursor *string) (Page, error) {
	if !addressPattern.MatchString(address) {
		return Page{}, &ValidationError{Field: "address", Message: "must be lower-case hex"}
	}
	n, after, err := s.pageParams(limit, cursor)
	if err != nil {
		return Page{}, err
	}
	items, err := s.store.AddressPostings(ctx, address, after, n+1)
	if err != nil {
		return Page{}, err
	}
	return page(items, n), nil
}
