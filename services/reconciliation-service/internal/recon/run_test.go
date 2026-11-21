package recon

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/slopay/pkg/breaker"
)

const chainID = 31337

type fakeNode struct {
	mu       sync.Mutex
	chainID  uint64
	head     uint64
	time     time.Time
	balances map[string]*big.Int // by AccountKey
	err      error
	batches  [][]BalanceQuery
	atBlock  uint64
}

func (n *fakeNode) ChainID(context.Context) (uint64, error) { return n.chainID, n.err }
func (n *fakeNode) HeadNumber(context.Context) (uint64, error) {
	return n.head, n.err
}
func (n *fakeNode) BlockTime(context.Context, uint64) (time.Time, error) { return n.time, n.err }
func (n *fakeNode) Balances(_ context.Context, block uint64, qs []BalanceQuery) ([]*big.Int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return nil, n.err
	}
	n.batches = append(n.batches, qs)
	n.atBlock = block
	out := make([]*big.Int, len(qs))
	for i, q := range qs {
		out[i] = new(big.Int)
		if b, ok := n.balances[AccountKey(q.Address, q.Asset)]; ok {
			out[i].Set(b)
		}
	}
	return out, nil
}

type fakeLedger struct {
	postings     map[string][]Posting // by address; totals are derived from them
	totalsErr    error
	postingsErr  error
	totalsCalls  int
	postingCalls []string
}

func (l *fakeLedger) AddressTotals(_ context.Context, addrs []string) (map[string]map[string]LedgerTotals, error) {
	l.totalsCalls++
	if l.totalsErr != nil {
		return nil, l.totalsErr
	}
	if len(addrs) > LedgerAddressBatch {
		return nil, fmt.Errorf("%d addresses", len(addrs))
	}
	out := map[string]map[string]LedgerTotals{}
	for _, a := range addrs {
		byAsset := map[string]LedgerTotals{}
		for _, p := range l.postings[a] {
			t, ok := byAsset[p.Asset.Key()]
			if !ok {
				t.Received = new(big.Int)
			}
			t.Received.Add(t.Received, p.Amount)
			t.Postings++
			byAsset[p.Asset.Key()] = t
		}
		out[a] = byAsset
	}
	return out, nil
}

func (l *fakeLedger) AddressPostings(_ context.Context, a string) ([]Posting, error) {
	l.postingCalls = append(l.postingCalls, a)
	if l.postingsErr != nil {
		return nil, l.postingsErr
	}
	return l.postings[a], nil
}

type fakeProjection struct {
	invoices []Invoice
	since    time.Time
	err      error
}

func (p *fakeProjection) Invoices(_ context.Context, since time.Time) ([]Invoice, error) {
	p.since = since
	return p.invoices, p.err
}

type memReports struct {
	mu      sync.Mutex
	reports []Report
	saveErr error
}

func (m *memReports) Save(_ context.Context, r Report) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.reports = append(m.reports, r)
	return nil
}

func (m *memReports) MarkPublished(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.reports {
		if m.reports[i].RunID == id {
			m.reports[i].Published = true
		}
	}
	return nil
}

func (m *memReports) Unpublished(context.Context) ([]Report, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Report
	for _, r := range m.reports {
		if !r.Published {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *memReports) Latest(context.Context) (*Report, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.reports) == 0 {
		return nil, nil
	}
	r := m.reports[len(m.reports)-1]
	return &r, nil
}

type fakePublisher struct {
	published []string
	err       error
}

func (p *fakePublisher) Publish(_ context.Context, r Report) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, r.RunID)
	return nil
}

const (
	addrA = "0x00000000000000000000000000000000000000aa"
	addrB = "0x00000000000000000000000000000000000000bb"
	addrC = "0x00000000000000000000000000000000000000cc"
	merch = "7c0b6a51-5f0e-4f57-9d35-3f7f2d2b1a10"
)

// blockTime is the time of the checked block, head 100 - 7 confirmations = 93.
var blockTime = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

type env struct {
	node    *fakeNode
	ledger  *fakeLedger
	proj    *fakeProjection
	reports *memReports
	pub     *fakePublisher
	reader  *sdkmetric.ManualReader
	r       *Reconciler
	slept   []time.Duration
}

func newEnv(t *testing.T, mutate ...func(*Config)) *env {
	t.Helper()
	e := &env{
		node:    &fakeNode{chainID: chainID, head: 100, time: blockTime, balances: map[string]*big.Int{}},
		ledger:  &fakeLedger{postings: map[string][]Posting{}},
		proj:    &fakeProjection{},
		reports: &memReports{},
		pub:     &fakePublisher{},
		reader:  sdkmetric.NewManualReader(),
	}
	br, err := breaker.New(breaker.Config{Dependency: "node", FailureThreshold: 2, OpenTimeout: time.Hour, HalfOpenMax: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := blockTime.Add(time.Minute)
	cfg := Config{
		Node: e.node, Breaker: br, Ledger: e.ledger, Projection: e.proj, Reports: e.reports, Publisher: e.pub,
		ChainID: chainID, Confirmations: 7, Lookback: 7 * 24 * time.Hour, TrackingWindow: 24 * time.Hour,
		SettleDelay: 15 * time.Second, BatchSize: 100, Interval: 10 * time.Minute, RunTimeout: time.Minute,
		Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(e.reader)).Meter("test"),
		Now:   func() time.Time { return now },
		Sleep: func(_ context.Context, d time.Duration) error { e.slept = append(e.slept, d); return nil },
	}
	for _, m := range mutate {
		m(&cfg)
	}
	if e.r, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	return e
}

// paid adds an invoice with one accounted transfer that is on the chain and in the ledger.
func (e *env) paid(id, addr string, created time.Time, key string, block uint64, amount int64) {
	e.proj.invoices = append(e.proj.invoices, Invoice{InvoiceID: id, MerchantID: merch, Address: addr, Asset: eth,
		CreatedAt: created, ExpiresAt: created.Add(time.Hour), PaidAt: created.Add(10 * time.Minute),
		Transfers: []Transfer{tr(key, block, amount)}})
	k := AccountKey(addr, eth)
	if e.node.balances[k] == nil {
		e.node.balances[k] = new(big.Int)
	}
	e.node.balances[k].Add(e.node.balances[k], big.NewInt(amount))
	e.ledger.postings[addr] = append(e.ledger.postings[addr], Posting{Asset: eth, BlockNumber: block, Amount: big.NewInt(amount)})
}

func (e *env) metric(t *testing.T, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				return d.DataPoints
			case metricdata.Gauge[int64]:
				return d.DataPoints
			}
		}
	}
	return nil
}

func (e *env) runs(t *testing.T, result string) int64 {
	t.Helper()
	for _, dp := range e.metric(t, "slopay_reconciliation_runs") {
		if v, _ := dp.Attributes.Value(attribute.Key("result")); v.AsString() == result {
			return dp.Value
		}
	}
	return 0
}

func TestRunOK(t *testing.T) {
	e := newEnv(t)
	e.paid("i1", addrA, blockTime.Add(-time.Hour), "k1", 90, 1000)
	e.paid("i2", addrB, blockTime.Add(-2*time.Hour), "k2", 80, 500)
	result, rp, err := e.r.RunOnce(context.Background())
	if err != nil || result != ResultOK {
		t.Fatalf("result %s, err %v", result, err)
	}
	if rp.BlockNumber != 93 || rp.AddressesChecked != 2 || rp.Mismatches != 0 || len(rp.Rows) != 0 || rp.Result() != "OK" {
		t.Errorf("report %+v", rp)
	}
	if e.node.atBlock != 93 || len(e.node.batches) != 1 || len(e.node.batches[0]) != 2 {
		t.Errorf("balances at %d in %d batches", e.node.atBlock, len(e.node.batches))
	}
	if !e.proj.since.Equal(blockTime.Add(-7 * 24 * time.Hour)) {
		t.Errorf("lookback since %v", e.proj.since)
	}
	if len(e.slept) != 1 || e.slept[0] != 15*time.Second {
		t.Errorf("settle delay %v", e.slept)
	}
	// Totals equal the invoices: the postings are not read.
	if e.ledger.totalsCalls != 1 || len(e.ledger.postingCalls) != 0 {
		t.Errorf("ledger calls: totals %d, postings %v", e.ledger.totalsCalls, e.ledger.postingCalls)
	}
	if len(e.reports.reports) != 1 || !e.reports.reports[0].Published || len(e.pub.published) != 1 {
		t.Errorf("report stored %d, published %v", len(e.reports.reports), e.pub.published)
	}
	if e.runs(t, ResultOK) != 1 {
		t.Error("runs{ok} not counted")
	}
}

func TestRunDetectsDiscrepancies(t *testing.T) {
	e := newEnv(t)
	// A: the ledger lost the posting of a second transfer.
	e.paid("i1", addrA, blockTime.Add(-time.Hour), "k1", 90, 1000)
	e.paid("i2", addrA, blockTime.Add(-time.Hour), "k2", 91, 300)
	e.ledger.postings[addrA] = e.ledger.postings[addrA][:1]
	// B: an unaccounted transfer within the tracking window.
	e.paid("i3", addrB, blockTime.Add(-2*time.Hour), "k3", 80, 500)
	e.node.balances[AccountKey(addrB, eth)].Add(e.node.balances[AccountKey(addrB, eth)], big.NewInt(70))
	// C: a transfer after the tracking window ended (invoice three days old).
	e.paid("i4", addrC, blockTime.Add(-72*time.Hour), "k4", 10, 200)
	e.node.balances[AccountKey(addrC, eth)].Add(e.node.balances[AccountKey(addrC, eth)], big.NewInt(25))

	result, rp, err := e.r.RunOnce(context.Background())
	if err != nil || result != ResultMismatch {
		t.Fatalf("result %s, err %v", result, err)
	}
	if rp.Mismatches != 2 || rp.AfterWindow != 1 || rp.AddressesChecked != 3 || len(rp.Rows) != 3 || rp.Result() != "MISMATCH" {
		t.Fatalf("report %+v", rp)
	}
	byAddr := map[string]Row{}
	for _, row := range rp.Rows {
		byAddr[row.Address] = row
	}
	if a := byAddr[addrA]; a.Kind != KindInvoicesVsLedger || a.ChainBalance.Int64() != 1300 || a.InvoicesTotal.Int64() != 1300 ||
		a.LedgerTotal.Int64() != 1000 || len(a.InvoiceIDs) != 2 || a.MerchantID != merch {
		t.Errorf("A: %+v", a)
	}
	if b := byAddr[addrB]; b.Kind != KindChainVsInvoices || b.ChainBalance.Int64() != 570 || b.InvoicesTotal.Int64() != 500 {
		t.Errorf("B: %+v", b)
	}
	if c := byAddr[addrC]; c.Kind != KindAfterWindow || c.Excess.Int64() != 25 || c.Mismatch() {
		t.Errorf("C: %+v", c)
	}
	// The totals of A differ: its postings were read.
	if len(e.ledger.postingCalls) != 1 || e.ledger.postingCalls[0] != addrA {
		t.Errorf("postings read for %v", e.ledger.postingCalls)
	}
	if e.runs(t, ResultMismatch) != 1 {
		t.Error("runs{mismatch} not counted")
	}
	// The gauges are reported by the leader only.
	if dps := e.metric(t, "slopay_reconciliation_mismatches"); len(dps) != 0 {
		t.Errorf("gauge outside leadership: %v", dps)
	}
	e.r.leading.Store(true)
	if dps := e.metric(t, "slopay_reconciliation_mismatches"); len(dps) != 1 || dps[0].Value != 2 {
		t.Errorf("mismatches gauge %v", dps)
	}
	if dps := e.metric(t, "slopay_reconciliation_after_window"); len(dps) != 1 || dps[0].Value != 1 {
		t.Errorf("after window gauge %v", dps)
	}
}

func TestLedgerPostingsAfterBlockLeftOut(t *testing.T) {
	e := newEnv(t)
	e.paid("i1", addrA, blockTime.Add(-time.Hour), "k1", 90, 1000)
	// A transfer accounted and posted in block 95, above the checked block 93: the statement totals include it,
	// the invoices and the balance at 93 do not.
	e.proj.invoices[0].Transfers = append(e.proj.invoices[0].Transfers, tr("k2", 95, 400))
	e.ledger.postings[addrA] = append(e.ledger.postings[addrA], Posting{Asset: eth, BlockNumber: 95, Amount: big.NewInt(400)})
	result, rp, err := e.r.RunOnce(context.Background())
	if err != nil || result != ResultOK || rp.Mismatches != 0 {
		t.Fatalf("result %s, err %v, report %+v", result, err, rp)
	}
	if len(e.ledger.postingCalls) != 1 {
		t.Errorf("postings read %v", e.ledger.postingCalls)
	}
}

func TestLedgerUnavailablePostpones(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*env)
	}{
		{"statement", func(e *env) { e.ledger.totalsErr = errors.New("connection refused") }},
		{"postings", func(e *env) {
			e.ledger.postings[addrA] = nil // the totals differ, so the postings are read
			e.ledger.postingsErr = errors.New("timeout")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.paid("i1", addrA, blockTime.Add(-time.Hour), "k1", 90, 1000)
			tc.set(e)
			result, rp, err := e.r.RunOnce(context.Background())
			if result != ResultPostponed || !errors.Is(err, ErrLedger) || rp != nil {
				t.Fatalf("result %s, err %v", result, err)
			}
			if len(e.reports.reports) != 0 || len(e.pub.published) != 0 {
				t.Error("a postponed run stores or publishes nothing")
			}
			if e.runs(t, ResultPostponed) != 1 {
				t.Error("runs{postponed} not counted")
			}
		})
	}
}

func TestNodeFailures(t *testing.T) {
	e := newEnv(t)
	e.paid("i1", addrA, blockTime.Add(-time.Hour), "k1", 90, 1000)
	e.node.err = errors.New("connection refused")
	for range 2 {
		if result, _, err := e.r.RunOnce(context.Background()); result != ResultFailed || err == nil {
			t.Fatalf("result %s, err %v", result, err)
		}
	}
	// The breaker opened after two failures: the next call is rejected without reaching the node.
	_, _, err := e.r.RunOnce(context.Background())
	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("err %v, want breaker open", err)
	}
	rejected := false
	for _, dp := range e.metric(t, "slopay_reconciliation_node_requests") {
		if v, _ := dp.Attributes.Value("result"); v.AsString() == "rejected" {
			rejected = true
		}
	}
	if !rejected || e.runs(t, ResultFailed) != 3 || len(e.reports.reports) != 0 {
		t.Errorf("rejected counted %v, failed runs %d", rejected, e.runs(t, ResultFailed))
	}

	other := newEnv(t)
	other.node.chainID = 1
	if result, _, err := other.r.RunOnce(context.Background()); result != ResultFailed || err == nil {
		t.Fatalf("other chain: result %s, err %v", result, err)
	}
	low := newEnv(t)
	low.node.head = 3
	if result, _, _ := low.r.RunOnce(context.Background()); result != ResultFailed {
		t.Fatalf("head below confirmations: %s", result)
	}
}

func TestUnpublishedReportRepublished(t *testing.T) {
	e := newEnv(t)
	e.paid("i1", addrA, blockTime.Add(-time.Hour), "k1", 90, 1000)
	e.pub.err = errors.New("kafka down")
	result, _, err := e.r.RunOnce(context.Background())
	if result != ResultFailed || err == nil || len(e.reports.reports) != 1 || e.reports.reports[0].Published {
		t.Fatalf("result %s, err %v, reports %+v", result, err, e.reports.reports)
	}
	first := e.reports.reports[0].RunID
	e.pub.err = nil
	if result, _, err := e.r.RunOnce(context.Background()); result != ResultOK || err != nil {
		t.Fatalf("result %s, err %v", result, err)
	}
	if len(e.pub.published) != 2 || e.pub.published[0] != first || !e.reports.reports[0].Published {
		t.Errorf("published %v", e.pub.published)
	}
}

func TestBatches(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.BatchSize = 2 })
	for i := range 150 {
		e.paid(fmt.Sprintf("i%d", i), fmt.Sprintf("0x%040x", i+1), blockTime.Add(-time.Hour), fmt.Sprintf("k%d", i), 50, 10)
	}
	result, rp, err := e.r.RunOnce(context.Background())
	if err != nil || result != ResultOK || rp.AddressesChecked != 150 {
		t.Fatalf("result %s, err %v", result, err)
	}
	if len(e.node.batches) != 75 || e.ledger.totalsCalls != 2 {
		t.Errorf("node batches %d, statement calls %d", len(e.node.batches), e.ledger.totalsCalls)
	}
}

func TestScheduleLeads(t *testing.T) {
	e := newEnv(t)
	prev := Report{RunID: "prev", Mismatches: 4, FinishedAt: blockTime, Published: true}
	e.reports.reports = []Report{prev}
	ctx, cancel := context.WithCancel(context.Background())
	runs := 0
	e.r.cfg.Sleep = func(ctx context.Context, d time.Duration) error {
		if d == e.r.cfg.SettleDelay {
			return nil
		}
		if d != 9*time.Minute { // now is 12:01, the next run is at 12:10
			t.Errorf("wait %v", d)
		}
		if !e.r.leading.Load() {
			t.Error("not leading while scheduled")
		}
		if runs == 0 {
			if dps := e.metric(t, "slopay_reconciliation_mismatches"); len(dps) != 1 || dps[0].Value != 4 {
				t.Errorf("gauge from the stored report: %v", dps)
			}
		}
		if runs == 2 {
			cancel()
			return ctx.Err()
		}
		runs++
		return nil
	}
	e.r.Schedule(ctx)
	if runs != 2 || len(e.reports.reports) != 3 || e.r.leading.Load() {
		t.Fatalf("runs %d, reports %d", runs, len(e.reports.reports))
	}
	if dps := e.metric(t, "slopay_reconciliation_mismatches"); len(dps) != 0 {
		t.Errorf("gauge after leadership: %v", dps)
	}
}

func TestNewValidates(t *testing.T) {
	for _, m := range []func(*Config){
		func(c *Config) { c.Node = nil },
		func(c *Config) { c.ChainID = 0 },
		func(c *Config) { c.Interval = 0 },
		func(c *Config) { c.BatchSize = 0 },
		func(c *Config) { c.SettleDelay = -time.Second },
	} {
		base := newEnv(t).r.cfg
		m(&base)
		base.Meter = nil
		if _, err := New(base); err == nil {
			t.Errorf("config %+v: want error", base)
		}
	}
}
