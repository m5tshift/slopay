package watcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/projection"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/store"
)

const chainID = 31337

// fakeNode is a chain whose blocks are canonical[number]; branches are told apart by the hash tag.
type fakeNode struct {
	mu        sync.Mutex
	id        uint64
	canonical []chain.Header
	fail      error                   // every call fails with it
	failNext  int                     // the next calls fail
	notFound  map[uint64]bool         // HeaderByNumber answers not found
	override  map[uint64]chain.Header // HeaderByNumber answers this header once
	headCalls int

	txs        map[string][]chain.Tx    // transfers by block hash
	failed     map[string]bool          // reverted transactions
	receipts   map[string]chain.Receipt // receipts answered instead of the real ones
	lost       map[string]bool          // transactions without a receipt
	blockCalls int                      // BlockByNumber calls
	hdrCalls   int                      // HeaderByNumber calls
}

func mkHeader(n uint64, tag, parent string) chain.Header {
	return chain.Header{Number: n, Hash: fmt.Sprintf("%s-%d", tag, n), ParentHash: parent, Timestamp: 1_700_000_000 + 12*n}
}

func newNode(blocks uint64) *fakeNode {
	f := &fakeNode{id: chainID, notFound: map[uint64]bool{}, override: map[uint64]chain.Header{},
		txs: map[string][]chain.Tx{}, failed: map[string]bool{}, receipts: map[string]chain.Receipt{},
		lost: map[string]bool{}}
	f.extend(blocks, "a")
	return f
}

// extend mines count blocks with the tag on top of the current head.
func (f *fakeNode) extend(count uint64, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for range count {
		n := uint64(len(f.canonical))
		parent := "genesis-parent"
		if n > 0 {
			parent = f.canonical[n-1].Hash
		}
		f.canonical = append(f.canonical, mkHeader(n, tag, parent))
	}
}

// fork drops the blocks from number from and mines count blocks with the tag instead.
func (f *fakeNode) fork(from, count uint64, tag string) {
	f.mu.Lock()
	f.canonical = f.canonical[:from]
	f.mu.Unlock()
	f.extend(count, tag)
}

func (f *fakeNode) failure() error {
	if f.fail != nil {
		return f.fail
	}
	if f.failNext > 0 {
		f.failNext--
		return errors.New("node unavailable")
	}
	return nil
}

func (f *fakeNode) ChainID(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failure(); err != nil {
		return 0, err
	}
	return f.id, nil
}

func (f *fakeNode) HeadNumber(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headCalls++
	if err := f.failure(); err != nil {
		return 0, err
	}
	return uint64(len(f.canonical)) - 1, nil
}

func (f *fakeNode) HeaderByNumber(_ context.Context, n uint64) (chain.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hdrCalls++
	return f.header(n)
}

func (f *fakeNode) BlockByNumber(_ context.Context, n uint64) (chain.Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockCalls++
	h, err := f.header(n)
	if err != nil {
		return chain.Block{}, err
	}
	return chain.Block{Header: h, Txs: slices.Clone(f.txs[h.Hash])}, nil
}

func (f *fakeNode) Receipt(_ context.Context, tx string) (chain.Receipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failure(); err != nil {
		return chain.Receipt{}, err
	}
	if r, ok := f.receipts[tx]; ok {
		return r, nil
	}
	if f.lost[tx] {
		return chain.Receipt{}, chain.ErrNotFound
	}
	for _, h := range f.canonical {
		for _, t := range f.txs[h.Hash] {
			if t.Hash == tx {
				return chain.Receipt{BlockHash: h.Hash, Success: !f.failed[tx]}, nil
			}
		}
	}
	return chain.Receipt{}, chain.ErrNotFound
}

// pay adds a transfer to the canonical block n and returns its hash.
func (f *fakeNode) pay(n uint64, to string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.canonical[n].Hash
	tx := chain.Tx{Hash: fmt.Sprintf("tx-%s-%d", h, len(f.txs[h])), From: "0xpayer", To: to, Value: big.NewInt(1000)}
	f.txs[h] = append(f.txs[h], tx)
	return tx.Hash
}

func (f *fakeNode) calls() (blocks, headers int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blockCalls, f.hdrCalls
}

func (f *fakeNode) header(n uint64) (chain.Header, error) {
	if err := f.failure(); err != nil {
		return chain.Header{}, err
	}
	if h, ok := f.override[n]; ok {
		delete(f.override, n)
		return h, nil
	}
	if f.notFound[n] || n >= uint64(len(f.canonical)) {
		return chain.Header{}, chain.ErrNotFound
	}
	return f.canonical[n], nil
}

func (f *fakeNode) head() chain.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.canonical[len(f.canonical)-1]
}

type memStore struct {
	mu        sync.Mutex
	st        chain.State
	loads     int
	saves     int
	loadErr   error
	saveErr   error
	conflicts int // the next saves fail with store.ErrConflict
}

func (s *memStore) Load(_ context.Context, id uint64) (chain.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.loadErr != nil {
		return chain.State{}, s.loadErr
	}
	st := s.st
	st.ChainID = id
	st.Window = slices.Clone(st.Window)
	return st, nil
}

func (s *memStore) Save(_ context.Context, st *chain.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.conflicts > 0 {
		s.conflicts--
		return store.ErrConflict
	}
	if st.Version != s.st.Version {
		return store.ErrConflict
	}
	s.saves++
	st.Version++
	s.st = *st
	s.st.Window = slices.Clone(st.Window)
	return nil
}

func (s *memStore) cursor() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tip, ok := s.st.Tip()
	return tip.Number, ok
}

type event struct {
	kind   string // head, reorg or transfer
	number uint64 // block number or fromBlock
	hash   string // block hash, orphan hash or transaction hash
}

type recPublisher struct {
	mu     sync.Mutex
	events []event
	err    error
}

func (p *recPublisher) PublishNewHead(_ context.Context, id uint64, h chain.Header) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != chainID {
		return fmt.Errorf("chain id %d", id)
	}
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event{"head", h.Number, h.Hash})
	return nil
}

func (p *recPublisher) PublishReorg(_ context.Context, _, from uint64, orphan string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event{"reorg", from, orphan})
	return nil
}

func (p *recPublisher) PublishTransfer(_ context.Context, id uint64, t chain.Transfer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != chainID {
		return fmt.Errorf("chain id %d", id)
	}
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event{"transfer", t.Block.Number, t.Tx.Hash})
	return nil
}

func (p *recPublisher) all() []event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.events)
}

// fakeAddresses are the tracked addresses: searched ones in tracked, the others in pending.
type fakeAddresses struct {
	mu        sync.Mutex
	tracked   map[string]bool
	pending   map[string]time.Time
	expired   []time.Time // Expire calls
	queries   [][]string  // Tracked calls
	rescanned [][]string  // Rescanned calls
	err       error       // every call fails with it
}

func newAddresses() *fakeAddresses {
	return &fakeAddresses{tracked: map[string]bool{}, pending: map[string]time.Time{}}
}

func (a *fakeAddresses) Tracked(_ context.Context, addrs []string) (map[string]bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	a.queries = append(a.queries, slices.Clone(addrs))
	out := map[string]bool{}
	for _, x := range addrs {
		if a.tracked[x] {
			out[x] = true
		}
	}
	return out, nil
}

func (a *fakeAddresses) Pending(context.Context, time.Time) ([]projection.Pending, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	var out []projection.Pending
	for x, t := range a.pending {
		out = append(out, projection.Pending{Address: x, CreatedAt: t})
	}
	slices.SortFunc(out, func(a, b projection.Pending) int { return strings.Compare(a.Address, b.Address) })
	return out, nil
}

func (a *fakeAddresses) Rescanned(_ context.Context, addrs []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.rescanned = append(a.rescanned, slices.Clone(addrs))
	for _, x := range addrs {
		delete(a.pending, x)
		a.tracked[x] = true
	}
	return nil
}

func (a *fakeAddresses) Expire(_ context.Context, now time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.expired = append(a.expired, now)
	return nil
}

func (a *fakeAddresses) add(addr string, createdAt time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pending[addr] = createdAt
}

func (a *fakeAddresses) track(addr string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tracked[addr] = true
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type env struct {
	w      *Watcher
	node   *fakeNode
	store  *memStore
	pub    *recPublisher
	addrs  *fakeAddresses
	clk    *clock
	reader *sdkmetric.ManualReader
	logs   *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func newEnv(t *testing.T, blocks uint64, mod func(*Config)) *env {
	t.Helper()
	e := &env{node: newNode(blocks), store: &memStore{}, pub: &recPublisher{}, addrs: newAddresses(),
		clk: &clock{t: time.Unix(1_700_000_000, 0)}, reader: sdkmetric.NewManualReader(), logs: &syncBuffer{}}
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(e.reader)).Meter("test")
	br, err := breaker.New(breaker.Config{Dependency: "node", FailureThreshold: 3, OpenTimeout: 10 * time.Second,
		HalfOpenMax: 1, Now: e.clk.now, Meter: meter})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Node: e.node, Store: e.store, Publisher: e.pub, Addresses: e.addrs, Breaker: br, ChainID: chainID,
		WindowSize: 64, MaxReorgDepth: 6, RescanClockSkew: 30 * time.Second, RescanLookback: 26 * time.Hour,
		PollInterval: time.Second, now: e.clk.now,
		BackoffInitial: 500 * time.Millisecond, BackoffMax: 4 * time.Second,
		Logger: slog.New(slog.NewJSONHandler(e.logs, nil)), Meter: meter,
	}
	if mod != nil {
		mod(&cfg)
	}
	if e.w, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	return e
}

func ptr(n uint64) *uint64 { return &n }

// sync processes blocks until the cursor reaches the node head.
func (e *env) sync(t *testing.T, st *chain.State) {
	t.Helper()
	for range 1000 {
		progressed, err := e.w.Step(context.Background(), st)
		if err != nil {
			t.Fatal(err)
		}
		if !progressed {
			return
		}
	}
	t.Fatal("no end of the chain")
}

func (e *env) metrics(t *testing.T) map[string][]metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]metricdata.DataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				out[m.Name] = d.DataPoints
			case metricdata.Sum[int64]:
				out[m.Name] = d.DataPoints
			}
		}
	}
	return out
}

// value returns the data point of a metric whose attributes contain all kv pairs ("key=value").
func value(m map[string][]metricdata.DataPoint[int64], name string, kv ...string) (int64, bool) {
	for _, dp := range m[name] {
		match := true
		for _, p := range kv {
			k, v, _ := strings.Cut(p, "=")
			if got, ok := dp.Attributes.Value(attribute.Key(k)); !ok || got.AsString() != v {
				match = false
			}
		}
		if match {
			return dp.Value, true
		}
	}
	return 0, false
}

func heads(evs []event) []uint64 {
	var out []uint64
	for _, e := range evs {
		if e.kind == "head" {
			out = append(out, e.number)
		}
	}
	return out
}

func span(from, to uint64) []uint64 {
	var out []uint64
	for n := from; n <= to; n++ {
		out = append(out, n)
	}
	return out
}

func TestNewValidates(t *testing.T) {
	e := newEnv(t, 1, nil)
	base := e.w.cfg
	for name, mod := range map[string]func(*Config){
		"no node":          func(c *Config) { c.Node = nil },
		"no store":         func(c *Config) { c.Store = nil },
		"no publisher":     func(c *Config) { c.Publisher = nil },
		"no addresses":     func(c *Config) { c.Addresses = nil },
		"negative skew":    func(c *Config) { c.RescanClockSkew = -time.Second },
		"no lookback":      func(c *Config) { c.RescanLookback = 0 },
		"no breaker":       func(c *Config) { c.Breaker = nil },
		"no chain":         func(c *Config) { c.ChainID = 0 },
		"zero depth":       func(c *Config) { c.MaxReorgDepth = 0 },
		"window too small": func(c *Config) { c.WindowSize = 6 },
		"no poll":          func(c *Config) { c.PollInterval = 0 },
		"no backoff":       func(c *Config) { c.BackoffInitial = 0 },
		"backoff max":      func(c *Config) { c.BackoffMax = time.Millisecond },
	} {
		cfg := base
		cfg.Meter = nil
		mod(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestFreshStartAtHead(t *testing.T) {
	e := newEnv(t, 11, nil)
	var st chain.State
	e.sync(t, &st)
	if got := heads(e.pub.all()); !slices.Equal(got, []uint64{10}) {
		t.Fatalf("published %v", got)
	}
	e.node.extend(2, "a")
	e.sync(t, &st)
	if got := heads(e.pub.all()); !slices.Equal(got, []uint64{10, 11, 12}) {
		t.Fatalf("published %v", got)
	}
	if c, _ := e.store.cursor(); c != 12 {
		t.Fatalf("stored cursor %d", c)
	}
	m := e.metrics(t)
	if _, ok := value(m, "weigate_chain_cursor_block"); ok {
		t.Error("cursor reported by a replica that does not lead")
	}
	e.w.leading.Store(true)
	m = e.metrics(t)
	if v, _ := value(m, "weigate_chain_cursor_block", "chain_id=31337"); v != 12 {
		t.Errorf("cursor gauge %d", v)
	}
	if v, _ := value(m, "weigate_chain_head_block", "chain_id=31337"); v != 12 {
		t.Errorf("head gauge %d", v)
	}
	if v, _ := value(m, "weigate_chain_blocks_processed", "chain_id=31337"); v != 3 {
		t.Errorf("blocks %d", v)
	}
	if v, _ := value(m, "weigate_chain_node_requests", "method=eth_blockNumber", "result=ok"); v < 4 {
		t.Errorf("node requests %d", v)
	}
	if v, ok := value(m, "weigate_chain_reorg_too_deep"); !ok || v != 0 {
		t.Errorf("too deep %d, %v", v, ok)
	}
}

func TestFreshStartAtStartBlock(t *testing.T) {
	e := newEnv(t, 11, func(c *Config) { c.StartBlock = ptr(3) })
	var st chain.State
	e.sync(t, &st)
	if got := heads(e.pub.all()); !slices.Equal(got, span(3, 10)) {
		t.Fatalf("published %v", got)
	}
}

func TestStartBlockAboveHead(t *testing.T) {
	e := newEnv(t, 5, func(c *Config) { c.StartBlock = ptr(7) })
	var st chain.State
	e.sync(t, &st)
	if len(e.pub.all()) != 0 {
		t.Fatal("published before the start block")
	}
	e.node.extend(4, "a")
	e.sync(t, &st)
	if got := heads(e.pub.all()); !slices.Equal(got, span(7, 8)) {
		t.Fatalf("published %v", got)
	}
}

func TestWindowKeepsLastHeaders(t *testing.T) {
	e := newEnv(t, 30, func(c *Config) { c.StartBlock = ptr(0); c.WindowSize = 8 })
	var st chain.State
	e.sync(t, &st)
	if len(st.Window) != 8 || st.Window[0].Number != 22 || len(e.store.st.Window) != 8 {
		t.Fatalf("window %+v", st.Window)
	}
}

func TestHeaderNotFoundIsIdle(t *testing.T) {
	e := newEnv(t, 5, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.node.extend(1, "a")
	e.node.notFound[5] = true
	if progressed, err := e.w.Step(context.Background(), &st); progressed || err != nil {
		t.Fatalf("step = %v, %v", progressed, err)
	}
	if tip, _ := st.Tip(); tip.Number != 4 {
		t.Fatalf("cursor %d", tip.Number)
	}
}

func TestReorgWithinLimit(t *testing.T) {
	for _, depth := range []uint64{1, 2, 3, 4, 5, 6} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			e := newEnv(t, 21, func(c *Config) { c.StartBlock = ptr(0) })
			var st chain.State
			e.sync(t, &st)
			orphan := e.node.canonical[21-depth].Hash
			e.node.fork(21-depth, depth+1, "b")
			e.sync(t, &st)

			evs := e.pub.all()
			wantReorg := event{"reorg", 21 - depth, orphan}
			i := slices.IndexFunc(evs, func(ev event) bool { return ev.kind == "reorg" })
			if i != 21 || evs[i] != wantReorg {
				t.Fatalf("reorg %d %+v, want %+v after 21 heads", i, evs[i], wantReorg)
			}
			if got := heads(evs[i+1:]); !slices.Equal(got, span(21-depth, 21)) {
				t.Fatalf("new branch published %v", got)
			}
			for _, ev := range evs[i+1:] {
				if !strings.HasPrefix(ev.hash, "b-") {
					t.Fatalf("old branch after the reorg: %+v", ev)
				}
			}
			if tip, _ := st.Tip(); tip != e.node.head() {
				t.Fatalf("tip %+v, node %+v", tip, e.node.head())
			}
			e.w.leading.Store(true)
			m := e.metrics(t)
			if v, _ := value(m, "weigate_chain_reorgs", "depth="+fmt.Sprint(depth)); v != 1 {
				t.Errorf("reorgs{depth=%d} = %d", depth, v)
			}
		})
	}
}

func TestReorgTooDeep(t *testing.T) {
	e := newEnv(t, 21, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	before := e.pub.all()
	e.node.fork(14, 8, "b") // blocks 14..20 replaced: depth 7
	_, err := e.w.Step(context.Background(), &st)
	var deep *TooDeepError
	if !errors.As(err, &deep) || deep.Cursor != 20 || deep.MaxDepth != 6 {
		t.Fatalf("err = %v", err)
	}
	if len(e.pub.all()) != len(before) {
		t.Fatal("published after a too deep reorg")
	}
	if tip, _ := st.Tip(); tip.Number != 20 {
		t.Fatal("cursor moved")
	}
}

func TestReorgBelowWindow(t *testing.T) {
	e := newEnv(t, 21, func(c *Config) { c.StartBlock = ptr(18) })
	var st chain.State
	e.sync(t, &st)
	e.node.fork(17, 5, "b") // the ancestor 16 was never processed
	_, err := e.w.Step(context.Background(), &st)
	var deep *TooDeepError
	if !errors.As(err, &deep) {
		t.Fatalf("err = %v", err)
	}
}

func TestReorgOrphanNotFoundIsMismatch(t *testing.T) {
	e := newEnv(t, 11, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.node.fork(9, 3, "b")
	e.node.notFound[10] = true // a node that has not seen block 10 of the new branch yet
	e.node.override[11] = e.node.canonical[11]
	delete(e.node.notFound, 11)
	if progressed, err := e.w.Step(context.Background(), &st); !progressed || err != nil {
		t.Fatalf("step = %v, %v", progressed, err)
	}
	if tip, _ := st.Tip(); tip.Number != 8 {
		t.Fatalf("ancestor %d", tip.Number)
	}
}

func TestReorgRaceTipStillCanonical(t *testing.T) {
	e := newEnv(t, 11, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.node.extend(1, "a")
	// Block 11 is seen from a short-lived branch, but block 10 is canonical when the ancestor is searched.
	e.node.override[11] = mkHeader(11, "c", "c-10")
	if progressed, err := e.w.Step(context.Background(), &st); progressed || err != nil {
		t.Fatalf("step = %v, %v", progressed, err)
	}
	for _, ev := range e.pub.all() {
		if ev.kind == "reorg" {
			t.Fatal("reorg published for a canonical tip")
		}
	}
	e.sync(t, &st)
	if tip, _ := st.Tip(); tip.Hash != "a-11" {
		t.Fatalf("tip %+v", tip)
	}
}

func TestPublishFailureKeepsCursor(t *testing.T) {
	e := newEnv(t, 3, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.pub.err = errors.New("kafka down")
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, e.pub.err) {
		t.Fatalf("err = %v", err)
	}
	if len(st.Window) != 0 || e.store.saves != 0 {
		t.Fatal("state moved without a published event")
	}
	e.pub.err = nil
	e.sync(t, &st)
	if got := heads(e.pub.all()); !slices.Equal(got, span(0, 2)) {
		t.Fatalf("published %v", got)
	}
}

func TestSaveFailureKeepsCursor(t *testing.T) {
	e := newEnv(t, 3, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.store.saveErr = errors.New("mongo down")
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, e.store.saveErr) {
		t.Fatalf("err = %v", err)
	}
	if len(st.Window) != 0 {
		t.Fatal("cursor moved without a saved state")
	}
	e.store.saveErr = nil
	e.sync(t, &st)
	// Block 0 is published twice with the same deterministic event, which consumers deduplicate.
	if got := heads(e.pub.all()); !slices.Equal(got, []uint64{0, 0, 1, 2}) {
		t.Fatalf("published %v", got)
	}
}

func TestReorgPublishAndSaveFailures(t *testing.T) {
	e := newEnv(t, 11, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.node.fork(8, 4, "b")

	e.pub.err = errors.New("kafka down")
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, e.pub.err) {
		t.Fatalf("publish: err = %v", err)
	}
	e.pub.err = nil
	e.store.saveErr = errors.New("mongo down")
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, e.store.saveErr) {
		t.Fatalf("save: err = %v", err)
	}
	if tip, _ := st.Tip(); tip.Number != 10 {
		t.Fatal("cursor moved without a saved state")
	}
	e.store.saveErr = nil
	e.sync(t, &st)
	if tip, _ := st.Tip(); tip != e.node.head() {
		t.Fatalf("tip %+v", tip)
	}
}

func TestNodeErrors(t *testing.T) {
	e := newEnv(t, 11, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.node.failNext = 1
	if _, err := e.w.Step(context.Background(), &st); err == nil || !strings.Contains(err.Error(), "eth_blockNumber") {
		t.Fatalf("head: err = %v", err)
	}
	e.sync(t, &st)
	e.node.fork(9, 3, "b")
	e.node.mu.Lock()
	e.node.headCalls = 0
	e.node.mu.Unlock()
	// head and block 11 answer, the ancestor search fails.
	e.node.override[11] = e.node.canonical[11]
	e.node.fail = nil
	calls := 0
	failing := &failAfter{Node: e.node, after: 2, calls: &calls}
	e.w.cfg.Node = failing
	if _, err := e.w.Step(context.Background(), &st); err == nil || !strings.Contains(err.Error(), "eth_getBlockByNumber") {
		t.Fatalf("ancestor: err = %v", err)
	}
	if tip, _ := st.Tip(); tip.Number != 10 {
		t.Fatal("cursor moved")
	}
	e.w.cfg.Node = e.node
	e.sync(t, &st)
	if tip, _ := st.Tip(); tip != e.node.head() {
		t.Fatalf("tip %+v", tip)
	}
}

// failAfter fails every call after the first `after` calls.
type failAfter struct {
	Node
	after int
	calls *int
}

func (f *failAfter) step() error {
	*f.calls++
	if *f.calls > f.after {
		return errors.New("node unavailable")
	}
	return nil
}

func (f *failAfter) HeadNumber(ctx context.Context) (uint64, error) {
	if err := f.step(); err != nil {
		return 0, err
	}
	return f.Node.HeadNumber(ctx)
}

func (f *failAfter) BlockByNumber(ctx context.Context, n uint64) (chain.Block, error) {
	if err := f.step(); err != nil {
		return chain.Block{}, err
	}
	return f.Node.BlockByNumber(ctx, n)
}

func (f *failAfter) HeaderByNumber(ctx context.Context, n uint64) (chain.Header, error) {
	if err := f.step(); err != nil {
		return chain.Header{}, err
	}
	return f.Node.HeaderByNumber(ctx, n)
}

// runUntil runs the watcher with a fake sleep that advances the clock and records the pauses; stop is
// checked at every pause and cancels Run when it returns true.
func (e *env) runUntil(t *testing.T, stop func() bool) []time.Duration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu     sync.Mutex
		pauses []time.Duration
	)
	e.w.cfg.sleep = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		pauses = append(pauses, d)
		n := len(pauses)
		mu.Unlock()
		e.clk.advance(d)
		if stop() || n > 500 {
			cancel()
		}
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() { e.w.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	return pauses
}

func TestRunResumesFromStoredCursor(t *testing.T) {
	e := newEnv(t, 21, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	for range 6 { // a previous leader processed blocks 0..5
		if _, err := e.w.Step(context.Background(), &st); err != nil {
			t.Fatal(err)
		}
	}
	e.pub.events = nil
	e.runUntil(t, func() bool { c, _ := e.store.cursor(); return c == 20 })
	if got := heads(e.pub.all()); !slices.Equal(got, span(6, 20)) {
		t.Fatalf("published %v", got)
	}
	if e.store.loads != 1 {
		t.Fatalf("loads %d", e.store.loads)
	}
	if e.w.leading.Load() {
		t.Fatal("still leading after Run")
	}
	if _, ok := value(e.metrics(t), "weigate_chain_cursor_block"); ok {
		t.Fatal("gauge reported after Run")
	}
}

func TestRunBacksOffAndOpensBreaker(t *testing.T) {
	e := newEnv(t, 5, func(c *Config) { c.StartBlock = ptr(0) })
	e.node.mu.Lock()
	e.node.fail = errors.New("connection refused")
	e.node.mu.Unlock()
	pauses := e.runUntil(t, func() bool {
		e.node.mu.Lock()
		defer e.node.mu.Unlock()
		return e.clk.now().Sub(time.Unix(1_700_000_000, 0)) > 30*time.Second
	})
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	if len(pauses) < len(want) || !slices.Equal(pauses[:len(want)], want) {
		t.Fatalf("pauses %v", pauses)
	}
	m := e.metrics(t)
	if v, _ := value(m, "weigate_chain_node_requests", "method=eth_chainId", "result=rejected"); v == 0 {
		t.Error("the breaker never rejected a call")
	}
	if v, _ := value(m, "weigate_chain_node_requests", "method=eth_chainId", "result=error"); v < 3 {
		t.Errorf("errors %d", v)
	}
	if !strings.Contains(e.logs.String(), "chain reading paused") {
		t.Error("no log of the pause")
	}

	// The node is back: reading resumes from the start block without gaps and the backoff resets.
	e.node.mu.Lock()
	e.node.fail = nil
	e.node.mu.Unlock()
	pauses = e.runUntil(t, func() bool { c, _ := e.store.cursor(); return c == 4 })
	if got := heads(e.pub.all()); !slices.Equal(got, span(0, 4)) {
		t.Fatalf("published %v", got)
	}
	if last := pauses[len(pauses)-1]; last != time.Second {
		t.Fatalf("pauses after recovery %v, want the poll interval at the head", pauses)
	}
}

func TestRunChainIDMismatch(t *testing.T) {
	e := newEnv(t, 5, nil)
	e.node.id = 1
	pauses := e.runUntil(t, func() bool { return true })
	if len(pauses) != 1 || pauses[0] != 500*time.Millisecond || e.store.loads != 0 {
		t.Fatalf("pauses %v, loads %d", pauses, e.store.loads)
	}
	if !strings.Contains(e.logs.String(), "node chain id 1, expected 31337") {
		t.Fatal("mismatch not logged")
	}
}

func TestRunLoadFailureRetries(t *testing.T) {
	e := newEnv(t, 5, nil)
	e.store.loadErr = errors.New("mongo down")
	calls := 0
	e.runUntil(t, func() bool {
		calls++
		if calls == 2 {
			e.store.mu.Lock()
			e.store.loadErr = nil
			e.store.mu.Unlock()
		}
		c, ok := e.store.cursor()
		return ok && c == 4
	})
	if e.store.loads != 3 {
		t.Fatalf("loads %d", e.store.loads)
	}
}

func TestRunReloadsOnConflict(t *testing.T) {
	e := newEnv(t, 5, func(c *Config) { c.StartBlock = ptr(0) })
	e.store.conflicts = 1
	e.runUntil(t, func() bool { c, ok := e.store.cursor(); return ok && c == 4 })
	if e.store.loads != 2 {
		t.Fatalf("loads %d", e.store.loads)
	}
	if got := heads(e.pub.all()); !slices.Equal(got, []uint64{0, 0, 1, 2, 3, 4}) {
		t.Fatalf("published %v", got)
	}
}

func TestRunHaltsOnTooDeepReorg(t *testing.T) {
	e := newEnv(t, 21, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	e.node.fork(10, 12, "b")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.w.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, _ := value(e.metrics(t), "weigate_chain_reorg_too_deep", "chain_id=31337"); v == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("too deep gauge not raised")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.node.mu.Lock()
	calls := e.node.headCalls
	e.node.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	e.node.mu.Lock()
	if e.node.headCalls != calls {
		t.Error("reading continued after a too deep reorg")
	}
	e.node.mu.Unlock()
	select {
	case <-done:
		t.Fatal("Run returned before leadership ended")
	default:
	}
	cancel()
	<-done
	if !strings.Contains(e.logs.String(), "ChainReorgTooDeep") {
		t.Error("alert not logged")
	}
	if _, ok := value(e.metrics(t), "weigate_chain_reorg_too_deep"); ok {
		t.Error("gauge reported after leadership ended")
	}
	if tip, _ := e.store.cursor(); tip != 20 {
		t.Errorf("stored cursor %d", tip)
	}
}
