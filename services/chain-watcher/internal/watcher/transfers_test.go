package watcher

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
)

const (
	payee = "0x00000000000000000000000000000000000000aa"
	other = "0x00000000000000000000000000000000000000bb"
)

// at returns the time of block n of the fake node.
func at(n uint64) time.Time { return time.Unix(1_700_000_000+12*chain.Int64(n), 0) }

// order returns the events as kind:number, transfers with their transaction hash.
func order(evs []event) []string {
	var out []string
	for _, e := range evs {
		s := e.kind + ":" + strconv.FormatUint(e.number, 10)
		if e.kind == "transfer" {
			s += ":" + e.hash
		}
		out = append(out, s)
	}
	return out
}

func (e *env) histogram(t *testing.T, name, mode string) (metricdata.HistogramDataPoint[float64], bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != name {
				continue
			}
			for _, dp := range h.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key("mode")); ok && v.AsString() == mode {
					return dp, true
				}
			}
		}
	}
	return metricdata.HistogramDataPoint[float64]{}, false
}

func TestLiveTransfersBeforeNewHead(t *testing.T) {
	e := newEnv(t, 3, func(c *Config) { c.StartBlock = ptr(0) })
	e.addrs.track(payee)
	e.addrs.track(other)
	var st chain.State
	e.sync(t, &st)
	e.node.extend(2, "a")
	tx1 := e.node.pay(3, payee)
	e.node.pay(3, "0x00000000000000000000000000000000000000cc") // not tracked
	tx2 := e.node.pay(3, other)
	tx3 := e.node.pay(3, payee)
	tx4 := e.node.pay(4, other)
	e.clk.advance(at(4).Sub(e.clk.now()) + 2*time.Second)
	e.sync(t, &st)

	want := []string{"head:0", "head:1", "head:2", "transfer:3:" + tx1, "transfer:3:" + tx2, "transfer:3:" + tx3,
		"head:3", "transfer:4:" + tx4, "head:4"}
	if got := order(e.pub.all()); !slices.Equal(got, want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}
	// Each recipient is checked once per block.
	if q := e.addrs.queries[3]; len(q) != 3 {
		t.Errorf("recipients of block 3: %v", q)
	}
	m := e.metrics(t)
	if v, _ := value(m, "weigate_chain_transfers_published", "mode=live", "chain_id=31337"); v != 4 {
		t.Errorf("published %d", v)
	}
	dp, ok := e.histogram(t, "weigate_chain_transfer_delay", "live")
	// Block 4 is 2 s old, block 3 is 12 s older.
	if lo, _ := dp.Min.Value(); !ok || dp.Count != 4 || lo != 2 || dp.Sum != 2+3*14 {
		t.Errorf("delay %+v", dp)
	}
}

func TestFailedTransferSkipped(t *testing.T) {
	e := newEnv(t, 2, func(c *Config) { c.StartBlock = ptr(0) })
	e.addrs.track(payee)
	e.node.failed[e.node.pay(1, payee)] = true
	var st chain.State
	e.sync(t, &st)
	if got := order(e.pub.all()); !slices.Equal(got, []string{"head:0", "head:1"}) {
		t.Fatalf("events %v", got)
	}
}

func TestReceiptOfAnotherBlockRetried(t *testing.T) {
	e := newEnv(t, 2, func(c *Config) { c.StartBlock = ptr(0) })
	e.addrs.track(payee)
	tx := e.node.pay(1, payee)
	e.node.receipts[tx] = chain.Receipt{BlockHash: "b-1", Success: true}
	var st chain.State
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, errReplaced) {
		t.Fatalf("err = %v", err)
	}
	if tip, _ := st.Tip(); tip.Number != 0 || len(e.pub.all()) != 1 {
		t.Fatalf("cursor %d, events %v", tip.Number, e.pub.all())
	}
	delete(e.node.receipts, tx)
	e.node.lost[tx] = true
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, errReplaced) {
		t.Fatalf("no receipt: err = %v", err)
	}
}

func TestAddressErrorsStopTheStep(t *testing.T) {
	e := newEnv(t, 2, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	boom := errors.New("mongodb down")
	e.addrs.err = boom
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, boom) {
		t.Fatalf("pending without cursor: err = %v", err)
	}
	e.addrs.err = nil
	e.sync(t, &st)
	e.node.extend(1, "a")
	e.addrs.err = boom
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, boom) {
		t.Fatalf("expire: err = %v", err)
	}
	e.w.expired = st.Window[len(st.Window)-1].Number + 1 // expiry done, the pending query fails
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, boom) {
		t.Fatalf("pending: err = %v", err)
	}
	if got := heads(e.pub.all()); !slices.Equal(got, []uint64{0, 1}) {
		t.Fatalf("published %v", got)
	}
	// Tracked fails after the maintenance.
	e.addrs.err = nil
	failing := &failTracked{Addresses: e.addrs, err: boom}
	e.w.cfg.Addresses = failing
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, boom) {
		t.Fatalf("tracked: err = %v", err)
	}
	if got := heads(e.pub.all()); !slices.Equal(got, []uint64{0, 1}) {
		t.Fatalf("published %v", got)
	}
}

type failTracked struct {
	Addresses
	err error
}

func (f *failTracked) Tracked(context.Context, []string) (map[string]bool, error) { return nil, f.err }

func TestExpireOncePerTip(t *testing.T) {
	e := newEnv(t, 3, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	for range 3 {
		if _, err := e.w.Step(context.Background(), &st); err != nil {
			t.Fatal(err)
		}
	}
	// Steps at blocks 0, 1 and 2; nothing before the first block.
	want := []time.Time{at(0), at(1), at(2)}
	if !slices.EqualFunc(e.addrs.expired, want, time.Time.Equal) {
		t.Fatalf("expired at %v", e.addrs.expired)
	}
}

func TestPendingWithoutCursorIsSearched(t *testing.T) {
	e := newEnv(t, 5, nil)
	e.addrs.add(payee, e.clk.now())
	var st chain.State
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if len(e.addrs.rescanned) != 1 || !e.addrs.tracked[payee] {
		t.Fatalf("rescanned %v", e.addrs.rescanned)
	}
}

// A transfer in a block processed before its address was tracked is published before the next chain.new_head.
func TestRescanBeforeNextNewHead(t *testing.T) {
	e := newEnv(t, 8, func(c *Config) { c.StartBlock = ptr(0) })
	early := e.node.pay(2, payee) // before the invoice
	tx5 := e.node.pay(5, payee)
	tx6 := e.node.pay(6, payee)
	e.node.pay(6, other) // not tracked
	var st chain.State
	e.sync(t, &st)
	blocks, _ := e.node.calls()

	// The invoice was created at block 6 time; the skew of 30 s reaches back to block 4.
	e.addrs.add(payee, at(6))
	e.clk.advance(at(7).Sub(e.clk.now()) + 5*time.Second)
	if progressed, err := e.w.Step(context.Background(), &st); progressed || err != nil {
		t.Fatalf("step = %v, %v", progressed, err)
	}
	want := []string{"transfer:5:" + tx5, "transfer:6:" + tx6}
	if got := order(e.pub.all()[8:]); !slices.Equal(got, want) {
		t.Fatalf("events after sync %v", got)
	}
	if b, _ := e.node.calls(); b != blocks {
		t.Errorf("processed blocks read again from the node: %d calls", b-blocks)
	}
	if !e.addrs.tracked[payee] || len(e.addrs.pending) != 0 {
		t.Fatalf("not marked: %+v", e.addrs)
	}

	e.node.extend(1, "a")
	tx8 := e.node.pay(8, payee)
	e.sync(t, &st)
	want = append(want, "transfer:8:"+tx8, "head:8")
	if got := order(e.pub.all()[8:]); !slices.Equal(got, want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}
	for _, ev := range e.pub.all() {
		if ev.hash == early {
			t.Fatal("transfer before the invoice published")
		}
	}
	m := e.metrics(t)
	if v, _ := value(m, "weigate_chain_transfers_published", "mode=rescan"); v != 2 {
		t.Errorf("rescan transfers %d", v)
	}
	if v, _ := value(m, "weigate_chain_rescan_blocks"); v != 4 {
		t.Errorf("rescan blocks %d", v) // 4..7
	}
	if dp, ok := e.histogram(t, "weigate_chain_transfer_delay", "rescan"); !ok || dp.Count != 2 {
		t.Errorf("rescan delay %+v", dp)
	}
}

// The rescan of a new address runs before the next block is read, so with a new block waiting its transfers still
// come before that block's chain.new_head.
func TestRescanBeforeWaitingBlock(t *testing.T) {
	e := newEnv(t, 4, func(c *Config) { c.StartBlock = ptr(0) })
	tx := e.node.pay(3, payee)
	var st chain.State
	e.sync(t, &st)
	e.node.extend(1, "a")
	e.addrs.add(payee, at(3))
	if progressed, err := e.w.Step(context.Background(), &st); !progressed || err != nil {
		t.Fatalf("step = %v, %v", progressed, err)
	}
	if got := order(e.pub.all()[4:]); !slices.Equal(got, []string{"transfer:3:" + tx, "head:4"}) {
		t.Fatalf("events %v", got)
	}
}

func TestRescanBelowWindow(t *testing.T) {
	for name, tc := range map[string]struct {
		lookback time.Duration
		found    bool
	}{
		"within lookback": {26 * time.Hour, true},
		"beyond lookback": {10 * 12 * time.Second, false},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 30, func(c *Config) { c.StartBlock = ptr(0); c.WindowSize = 8; c.RescanLookback = tc.lookback })
			tx := e.node.pay(3, payee)
			var st chain.State
			e.sync(t, &st)
			_, headers := e.node.calls()
			e.addrs.add(payee, at(3).Add(10*time.Second))
			if _, err := e.w.Step(context.Background(), &st); err != nil {
				t.Fatal(err)
			}
			found := slices.ContainsFunc(e.pub.all(), func(ev event) bool { return ev.hash == tx })
			if found != tc.found {
				t.Fatalf("found = %v, events %v", found, order(e.pub.all()[30:]))
			}
			if _, h := e.node.calls(); h-headers > 6 {
				t.Errorf("binary search read %d headers", h-headers)
			}
			if !e.addrs.tracked[payee] {
				t.Fatal("not marked")
			}
		})
	}
}

func TestRescanAfterTipIsMarked(t *testing.T) {
	e := newEnv(t, 3, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.addrs.add(payee, at(10))
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if !e.addrs.tracked[payee] || len(e.pub.all()) != 3 {
		t.Fatalf("tracked %v, events %v", e.addrs.tracked, e.pub.all())
	}
}

// A processed block replaced on the node postpones the search until the reorg is handled; then the new branch
// is searched.
func TestRescanPostponedByReorg(t *testing.T) {
	e := newEnv(t, 10, func(c *Config) { c.StartBlock = ptr(0) })
	e.node.pay(8, payee)
	var st chain.State
	e.sync(t, &st)
	e.node.fork(8, 3, "b") // the transfer moves to block 9 of the new branch
	moved := e.node.pay(9, payee)
	e.addrs.add(payee, at(7))

	// The search is postponed and the same step finds the reorg.
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if got := order(e.pub.all()[10:]); len(e.addrs.rescanned) != 0 || !slices.Equal(got, []string{"reorg:8"}) {
		t.Fatalf("searched the replaced block: %v %v", e.addrs.rescanned, got)
	}
	e.sync(t, &st)
	want := []string{"reorg:8", "head:8", "transfer:9:" + moved, "head:9", "head:10"}
	if got := order(e.pub.all()[10:]); !slices.Equal(got, want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}
}

// A block of the window that the node answers with another hash postpones the search too.
func TestRescanPostponedByReplacedHeader(t *testing.T) {
	e := newEnv(t, 80, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.w.recent = map[uint64]chain.Block{} // as after a restart
	e.node.fork(75, 6, "b")
	e.addrs.add(payee, at(74))
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if len(e.addrs.rescanned) != 0 {
		t.Fatal("searched a replaced block")
	}
	if got := order(e.pub.all()[80:]); !slices.Equal(got, []string{"reorg:75"}) {
		t.Fatalf("events %v", got)
	}
}

func TestRescanPublishFailureKeepsPending(t *testing.T) {
	e := newEnv(t, 4, func(c *Config) { c.StartBlock = ptr(0) })
	e.node.pay(3, payee)
	var st chain.State
	e.sync(t, &st)
	e.addrs.add(payee, at(3))
	e.pub.err = errors.New("kafka down")
	if _, err := e.w.Step(context.Background(), &st); err == nil {
		t.Fatal("no error")
	}
	if len(e.addrs.rescanned) != 0 {
		t.Fatal("marked after a failed publication")
	}
	e.pub.err = nil
	e.node.failNext = 1
	if _, err := e.w.Step(context.Background(), &st); err == nil {
		t.Fatal("receipt: no error")
	}
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if len(e.addrs.rescanned) != 1 {
		t.Fatal("not marked")
	}
}

func TestRescanOldBlockErrors(t *testing.T) {
	e := newEnv(t, 30, func(c *Config) { c.StartBlock = ptr(0); c.WindowSize = 8 })
	var st chain.State
	e.sync(t, &st)
	e.addrs.add(payee, at(3))
	e.node.failNext = 1 // the binary search fails
	if _, err := e.w.Step(context.Background(), &st); err == nil {
		t.Fatal("header: no error")
	}
	calls := 0
	e.w.cfg.Node = &failAfter{Node: e.node, after: 6, calls: &calls} // the search passes, a block fails
	if _, err := e.w.Step(context.Background(), &st); err == nil {
		t.Fatal("block: no error")
	}
	e.w.cfg.Node = e.node
	e.node.notFound[7] = true // a pruned block postpones the search
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if len(e.addrs.rescanned) != 0 {
		t.Fatal("marked without the block")
	}
	delete(e.node.notFound, 7)
	if _, err := e.w.Step(context.Background(), &st); err != nil || len(e.addrs.rescanned) != 1 {
		t.Fatalf("err = %v, rescanned %v", err, e.addrs.rescanned)
	}
}
