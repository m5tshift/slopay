package recon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"

	"github.com/m5tshift/slopay/pkg/breaker"
)

const instrumentation = "github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"

// Run results: the result attribute of slopay_reconciliation_runs.
const (
	ResultOK        = "ok"
	ResultMismatch  = "mismatch"
	ResultPostponed = "postponed"
	ResultFailed    = "failed"
)

// LedgerAddressBatch is the most addresses of one statement request (contracts/openapi/ledger-service.yaml).
const LedgerAddressBatch = 100

// BalanceQuery is a balance to read: native ETH (eth_getBalance) or a token (balanceOf).
type BalanceQuery struct {
	Address string
	Asset   Asset
}

// Node reads the chain.
type Node interface {
	ChainID(ctx context.Context) (uint64, error)
	HeadNumber(ctx context.Context) (uint64, error)
	BlockTime(ctx context.Context, n uint64) (time.Time, error)
	// Balances reads the balances at block in one JSON-RPC batch, in the order of qs.
	Balances(ctx context.Context, block uint64, qs []BalanceQuery) ([]*big.Int, error)
}

// LedgerTotals are the postings of an address in one asset.
type LedgerTotals struct {
	Received *big.Int
	Postings int
}

// Posting is a receipt posting of an address.
type Posting struct {
	Asset       Asset
	BlockNumber uint64
	Amount      *big.Int
}

// Ledger is the statement API of ledger-service.
type Ledger interface {
	// AddressTotals returns the totals by address and asset key for at most LedgerAddressBatch addresses.
	AddressTotals(ctx context.Context, addresses []string) (map[string]map[string]LedgerTotals, error)
	// AddressPostings returns every posting of the address.
	AddressPostings(ctx context.Context, address string) ([]Posting, error)
}

// Projection is the projection of invoice.events.
type Projection interface {
	// Invoices returns the invoices created at or after since.
	Invoices(ctx context.Context, since time.Time) ([]Invoice, error)
}

// Row is a line of a report: a discrepancy or an address with transfers after its tracking window.
type Row struct {
	Kind          string
	Address       string
	MerchantID    string
	InvoiceIDs    []string
	Asset         Asset
	ChainBalance  *big.Int
	InvoicesTotal *big.Int
	LedgerTotal   *big.Int
	// Excess is the balance above the accounted transfers, for KindAfterWindow.
	Excess     *big.Int
	TrackUntil time.Time
}

// Mismatch reports whether the row is a discrepancy.
func (r Row) Mismatch() bool { return r.Kind == KindChainVsInvoices || r.Kind == KindInvoicesVsLedger }

// Report is the result of a run, stored in MongoDB.
type Report struct {
	RunID            string
	ChainID          uint64
	BlockNumber      uint64
	BlockTime        time.Time
	StartedAt        time.Time
	FinishedAt       time.Time
	AddressesChecked int
	Mismatches       int
	AfterWindow      int
	Rows             []Row
	Published        bool
}

// Result is OK or MISMATCH, as in reconciliation.completed.
func (r Report) Result() string {
	if r.Mismatches > 0 {
		return "MISMATCH"
	}
	return "OK"
}

// Reports stores the reports.
type Reports interface {
	Save(ctx context.Context, r Report) error
	MarkPublished(ctx context.Context, runID string) error
	// Unpublished returns the stored reports whose events have not been published, oldest first.
	Unpublished(ctx context.Context) ([]Report, error)
	// Latest returns the latest report or nil.
	Latest(ctx context.Context) (*Report, error)
}

// Publisher publishes the events of a report: one reconciliation.mismatch per discrepancy, then
// reconciliation.completed. Event IDs are derived from the run, so publishing again is idempotent.
type Publisher interface {
	Publish(ctx context.Context, r Report) error
}

// Config configures a Reconciler.
type Config struct {
	Node       Node
	Breaker    *breaker.Breaker
	Ledger     Ledger
	Projection Projection
	Reports    Reports
	Publisher  Publisher

	ChainID uint64
	// Confirmations is N: balances are read at head - N (UC-08).
	Confirmations uint64
	// Lookback is how far back the invoice addresses are checked (UC-08: 7 days).
	Lookback time.Duration
	// TrackingWindow is the address tracking window (SRS 4.1.4: 24 h).
	TrackingWindow time.Duration
	// SettleDelay is waited after the block is fixed, so that the invoice and ledger events of its transfers
	// reach the projection and the ledger.
	SettleDelay time.Duration
	// BatchSize is the most balances in one JSON-RPC batch.
	BatchSize int
	// Interval is the period of the runs (UC-08: 10 min); RunTimeout bounds one run.
	Interval   time.Duration
	RunTimeout time.Duration

	Logger *slog.Logger
	Meter  metric.Meter
	Now    func() time.Time
	// Sleep waits d or until ctx is done; tests replace it.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Reconciler runs the reconciliation.
type Reconciler struct {
	cfg Config
	log *slog.Logger

	leading atomic.Bool
	mu      sync.Mutex
	last    *Report

	runs     metric.Int64Counter
	duration metric.Float64Histogram
	requests metric.Int64Counter
}

// ErrLedger marks a run postponed because ledger-service did not answer (SRS 4.1.9).
var ErrLedger = errors.New("ledger-service unavailable")

// New validates cfg and registers the metrics.
func New(cfg Config) (*Reconciler, error) {
	switch {
	case cfg.Node == nil || cfg.Breaker == nil || cfg.Ledger == nil || cfg.Projection == nil || cfg.Reports == nil || cfg.Publisher == nil:
		return nil, errors.New("recon: node, breaker, ledger, projection, reports and publisher are required")
	case cfg.ChainID == 0:
		return nil, errors.New("recon: chain id is required")
	case cfg.Lookback <= 0 || cfg.TrackingWindow <= 0 || cfg.Interval <= 0 || cfg.RunTimeout <= 0 || cfg.SettleDelay < 0:
		return nil, errors.New("recon: lookback, tracking window, interval and run timeout must be positive")
	case cfg.BatchSize < 1:
		return nil, errors.New("recon: batch size must be positive")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleep
	}
	r := &Reconciler{cfg: cfg, log: cfg.Logger}
	if err := r.registerMetrics(cfg.Meter); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reconciler) registerMetrics(m metric.Meter) error {
	if m == nil {
		m = otel.Meter(instrumentation)
	}
	var err error
	if r.runs, err = m.Int64Counter("slopay_reconciliation_runs",
		metric.WithDescription("Reconciliation runs by result: ok, mismatch, postponed (ledger-service unavailable), failed")); err != nil {
		return err
	}
	if r.duration, err = m.Float64Histogram("slopay_reconciliation_run_duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 20, 30, 60, 120, 300),
		metric.WithDescription("Duration of a reconciliation run, including the settle delay")); err != nil {
		return err
	}
	if r.requests, err = m.Int64Counter("slopay_reconciliation_node_requests",
		metric.WithDescription("Node calls by method and result: ok, error, rejected (breaker open)")); err != nil {
		return err
	}
	// The gauges describe the latest completed run and are reported only by the leader, so that a replica that
	// lost the lease does not keep an old value (alert ReconciliationMismatch on slopay_reconciliation_mismatches > 0).
	gauge := func(name, desc string, v func(*Report) int64) error {
		_, err := m.Int64ObservableGauge(name, metric.WithDescription(desc),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				if last := r.Last(); last != nil && r.leading.Load() {
					o.Observe(v(last))
				}
				return nil
			}))
		return err
	}
	if err = gauge("slopay_reconciliation_mismatches", "Discrepancies found by the latest run (ReconciliationMismatch)",
		func(rp *Report) int64 { return int64(rp.Mismatches) }); err != nil {
		return err
	}
	if err = gauge("slopay_reconciliation_after_window", "Addresses with transfers after the tracking window in the latest run",
		func(rp *Report) int64 { return int64(rp.AfterWindow) }); err != nil {
		return err
	}
	if err = gauge("slopay_reconciliation_addresses_checked", "Addresses checked by the latest run",
		func(rp *Report) int64 { return int64(rp.AddressesChecked) }); err != nil {
		return err
	}
	if err = gauge("slopay_reconciliation_block", "Block the latest run read the balances at",
		func(rp *Report) int64 { return int64(min(rp.BlockNumber, 1<<62)) }); err != nil { //nolint:gosec // clamped.
		return err
	}
	return gauge("slopay_reconciliation_last_run_timestamp", "Unix time the latest completed run finished",
		func(rp *Report) int64 { return rp.FinishedAt.Unix() })
}

// Last returns the latest completed report known to this replica.
func (r *Reconciler) Last() *Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

func (r *Reconciler) setLast(rp *Report) {
	r.mu.Lock()
	r.last = rp
	r.mu.Unlock()
}

// Schedule runs the reconciliation every Interval, at multiples of Interval of the wall clock, until ctx is done.
// It is the work of the leader replica (SRS 4.1.3): leading starts with the latest stored report, so the gauges
// keep their value across a change of leader.
func (r *Reconciler) Schedule(ctx context.Context) {
	r.leading.Store(true)
	defer r.leading.Store(false)
	if last, err := r.cfg.Reports.Latest(ctx); err != nil {
		r.log.WarnContext(ctx, "latest report not loaded", "error", err)
	} else if last != nil && r.Last() == nil {
		r.setLast(last)
	}
	for {
		next := NextRun(r.cfg.Now(), r.cfg.Interval)
		if err := r.cfg.Sleep(ctx, next.Sub(r.cfg.Now())); err != nil {
			return
		}
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RunTimeout)
		_, _, _ = r.RunOnce(rctx)
		cancel()
		if ctx.Err() != nil {
			return
		}
	}
}

// NextRun returns the first multiple of interval after now.
func NextRun(now time.Time, interval time.Duration) time.Time {
	return now.Truncate(interval).Add(interval)
}

// RunOnce performs one run and returns its result and report. A run whose ledger statement failed is postponed
// to the next one: nothing is stored or published (SRS 4.1.9).
func (r *Reconciler) RunOnce(ctx context.Context) (string, *Report, error) {
	started := r.cfg.Now()
	ctx, span := otel.Tracer(instrumentation).Start(ctx, "reconciliation run")
	defer span.End()
	rp, err := r.run(ctx, started)
	result := ResultOK
	switch {
	case errors.Is(err, ErrLedger):
		result = ResultPostponed
		r.log.WarnContext(ctx, "reconciliation postponed to the next run", "error", err)
	case err != nil:
		result = ResultFailed
		r.log.ErrorContext(ctx, "reconciliation failed", "error", err)
	case rp.Mismatches > 0:
		result = ResultMismatch
		r.log.WarnContext(ctx, "reconciliation found discrepancies", "alert", "ReconciliationMismatch",
			"run_id", rp.RunID, "block", rp.BlockNumber, "mismatches", rp.Mismatches, "addresses", rp.AddressesChecked)
	default:
		r.log.InfoContext(ctx, "reconciliation completed", "run_id", rp.RunID, "block", rp.BlockNumber,
			"addresses", rp.AddressesChecked, "after_window", rp.AfterWindow)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, result)
	}
	span.SetAttributes(attribute.String("reconciliation.result", result))
	attrs := metric.WithAttributes(attribute.String("result", result))
	r.runs.Add(ctx, 1, attrs)
	r.duration.Record(ctx, r.cfg.Now().Sub(started).Seconds(), attrs)
	return result, rp, err
}

func (r *Reconciler) run(ctx context.Context, started time.Time) (*Report, error) {
	// Reports stored by an earlier run whose events were not published are published first.
	if err := r.flush(ctx); err != nil {
		return nil, err
	}
	var id uint64
	if err := r.node(ctx, "eth_chainId", func() (err error) { id, err = r.cfg.Node.ChainID(ctx); return }); err != nil {
		return nil, err
	}
	if id != r.cfg.ChainID {
		return nil, fmt.Errorf("node chain %d, expected %d", id, r.cfg.ChainID)
	}
	var head uint64
	if err := r.node(ctx, "eth_blockNumber", func() (err error) { head, err = r.cfg.Node.HeadNumber(ctx); return }); err != nil {
		return nil, err
	}
	if head < r.cfg.Confirmations {
		return nil, fmt.Errorf("head %d is below %d confirmations", head, r.cfg.Confirmations)
	}
	block := head - r.cfg.Confirmations
	var blockTime time.Time
	if err := r.node(ctx, "eth_getBlockByNumber", func() (err error) { blockTime, err = r.cfg.Node.BlockTime(ctx, block); return }); err != nil {
		return nil, err
	}
	if err := r.cfg.Sleep(ctx, r.cfg.SettleDelay); err != nil {
		return nil, err
	}

	invoices, err := r.cfg.Projection.Invoices(ctx, blockTime.Add(-r.cfg.Lookback))
	if err != nil {
		return nil, err
	}
	accounts := Accounts(invoices, block, r.cfg.TrackingWindow)
	balances, err := r.balances(ctx, block, accounts)
	if err != nil {
		return nil, err
	}
	ledger, err := r.ledgerTotals(ctx, block, accounts)
	if err != nil {
		return nil, err
	}

	rp := &Report{RunID: uuid.NewString(), ChainID: r.cfg.ChainID, BlockNumber: block, BlockTime: blockTime,
		StartedAt: started, AddressesChecked: len(accounts)}
	for i, a := range accounts {
		f := Evaluate(balances[i], a.InvoicesTotal, ledger[i], blockTime.After(a.TrackUntil))
		row := Row{Address: a.Address, MerchantID: a.MerchantID, InvoiceIDs: a.InvoiceIDs, Asset: a.Asset,
			ChainBalance: balances[i], InvoicesTotal: a.InvoicesTotal, LedgerTotal: ledger[i], TrackUntil: a.TrackUntil}
		if f.AfterWindow != nil {
			aw := row
			aw.Kind, aw.Excess = KindAfterWindow, f.AfterWindow
			rp.Rows = append(rp.Rows, aw)
			rp.AfterWindow++
		}
		if f.Mismatch() {
			row.Kind = f.Kind
			rp.Rows = append(rp.Rows, row)
			rp.Mismatches++
		}
	}
	rp.FinishedAt = r.cfg.Now()
	if err := r.cfg.Reports.Save(ctx, *rp); err != nil {
		return nil, err
	}
	r.setLast(rp)
	if err := r.publish(ctx, *rp); err != nil {
		return nil, err
	}
	return rp, nil
}

func (r *Reconciler) flush(ctx context.Context) error {
	pending, err := r.cfg.Reports.Unpublished(ctx)
	if err != nil {
		return err
	}
	for _, rp := range pending {
		if err := r.publish(ctx, rp); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) publish(ctx context.Context, rp Report) error {
	if err := r.cfg.Publisher.Publish(ctx, rp); err != nil {
		return fmt.Errorf("publish run %s: %w", rp.RunID, err)
	}
	return r.cfg.Reports.MarkPublished(ctx, rp.RunID)
}

// node calls the node through the breaker and counts the call.
func (r *Reconciler) node(ctx context.Context, method string, fn func() error) error {
	err := r.cfg.Breaker.Do(fn)
	result := "ok"
	switch {
	case errors.Is(err, breaker.ErrOpen):
		result = "rejected"
	case err != nil:
		result = "error"
	}
	r.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("method", method), attribute.String("result", result)))
	if err != nil {
		return fmt.Errorf("node %s: %w", method, err)
	}
	return nil
}

// balances reads the balance of every account at block, BatchSize per JSON-RPC batch.
func (r *Reconciler) balances(ctx context.Context, block uint64, accounts []*Account) ([]*big.Int, error) {
	out := make([]*big.Int, 0, len(accounts))
	for from := 0; from < len(accounts); from += r.cfg.BatchSize {
		batch := accounts[from:min(from+r.cfg.BatchSize, len(accounts))]
		qs := make([]BalanceQuery, len(batch))
		for i, a := range batch {
			qs[i] = BalanceQuery{Address: a.Address, Asset: a.Asset}
		}
		var got []*big.Int
		if err := r.node(ctx, "balances", func() (err error) { got, err = r.cfg.Node.Balances(ctx, block, qs); return }); err != nil {
			return nil, err
		}
		if len(got) != len(qs) {
			return nil, fmt.Errorf("node returned %d balances for %d addresses", len(got), len(qs))
		}
		out = append(out, got...)
	}
	return out, nil
}

// ledgerTotals returns the ledger postings of every account in blocks up to block. The statement totals cover the
// whole life of an address; when they equal the accounted transfers in count and sum, they are taken as they are.
// Otherwise the postings of the address are read and those of later blocks (accounted after the checked block)
// are left out. Any failure of ledger-service postpones the run.
func (r *Reconciler) ledgerTotals(ctx context.Context, block uint64, accounts []*Account) ([]*big.Int, error) {
	var addrs []string
	seen := map[string]bool{}
	for _, a := range accounts {
		if !seen[a.Address] {
			seen[a.Address] = true
			addrs = append(addrs, a.Address)
		}
	}
	totals := map[string]map[string]LedgerTotals{}
	for from := 0; from < len(addrs); from += LedgerAddressBatch {
		got, err := r.cfg.Ledger.AddressTotals(ctx, addrs[from:min(from+LedgerAddressBatch, len(addrs))])
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLedger, err)
		}
		for addr, byAsset := range got {
			totals[addr] = byAsset
		}
	}
	postings := map[string][]Posting{}
	out := make([]*big.Int, len(accounts))
	for i, a := range accounts {
		t, ok := totals[a.Address][a.Asset.Key()]
		if !ok {
			t = LedgerTotals{Received: new(big.Int)}
		}
		if t.Postings == a.InvoicesCount && t.Received.Cmp(a.InvoicesTotal) == 0 {
			out[i] = new(big.Int).Set(t.Received)
			continue
		}
		ps, ok := postings[a.Address]
		if !ok {
			var err error
			if ps, err = r.cfg.Ledger.AddressPostings(ctx, a.Address); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrLedger, err)
			}
			postings[a.Address] = ps
		}
		sum := new(big.Int)
		for _, p := range ps {
			if p.Asset.Key() == a.Asset.Key() && p.BlockNumber <= block {
				sum.Add(sum, p.Amount)
			}
		}
		out[i] = sum
	}
	return out, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
