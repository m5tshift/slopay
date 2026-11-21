package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoicetest"
	"github.com/m5tshift/weigate/services/invoice-service/internal/projection"
)

const testChain = 31337

type harness struct {
	t      *testing.T
	e      *Engine
	store  *MemStore
	cache  *invoicetest.Cache
	offset int64
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	st, cache := NewMemStore(codec), invoicetest.NewCache()
	e, err := NewEngine(st, cache, EngineConfig{Config: testCfg, ChainID: testChain, KeepBlocks: 16,
		Now: func() time.Time { return base }}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	st.AddInvoice(newRecord(1000))
	return &harness{t: t, e: e, store: st, cache: cache}
}

func head(fork, n uint64, at time.Duration) NewHead {
	parent := blockHash(fork, n-1)
	return NewHead{ChainID: testChain, Number: n, Hash: blockHash(fork, n), ParentHash: parent,
		Timestamp: base.Add(at).Unix()}
}

func transferEv(fork, n uint64, at time.Duration, amount int64, tx int) TransferEvent {
	return TransferEvent{ChainID: testChain, BlockNumber: n, BlockHash: blockHash(fork, n),
		BlockTimestamp: base.Add(at).Unix(), TxHash: txHash(tx), From: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
		To: newRecord(0).Address, Amount: big.NewInt(amount).String(), Asset: invoice.ETH}
}

func (h *harness) head(fork, n uint64, at time.Duration) {
	h.t.Helper()
	h.offset++
	if err := h.e.NewHead(context.Background(), h.offset, head(fork, n, at)); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) reorg(from uint64) {
	h.t.Helper()
	h.offset++
	if err := h.e.Reorg(context.Background(), h.offset, Reorg{ChainID: testChain, FromBlock: from}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) transfer(te TransferEvent) {
	h.t.Helper()
	if err := h.e.Transfer(context.Background(), te); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) types() []string {
	var out []string
	for _, ev := range h.store.Events() {
		out = append(out, ev.Type)
	}
	return out
}

func (h *harness) wantTypes(want ...string) {
	h.t.Helper()
	if got := h.types(); len(got) != len(want) || (len(want) > 0 && fmtList(got) != fmtList(want)) {
		h.t.Fatalf("events %v, want %v", got, want)
	}
}

func fmtList(s []string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (h *harness) status() invoice.Status {
	rec, _ := h.store.Record(testID)
	return rec.Status
}

// A transfer published before its block waits for chain.new_head; the cache gets every status change.
func TestEngineTransferWaitsForBlock(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	h.transfer(transferEv(0, 100, time.Minute, 1000, 1))
	h.wantTypes()
	h.head(0, 100, time.Minute)
	h.wantTypes(EventDetected)
	if p, ok := h.cache.Get(context.Background(), testID); !ok || p.Status != invoice.StatusDetected {
		t.Fatalf("cache %+v %v", p, ok)
	}
	h.head(0, 101, 2*time.Minute)
	h.head(0, 102, 3*time.Minute)
	h.wantTypes(EventDetected, EventConfirming, EventPaid)
	if p, _ := h.cache.Get(context.Background(), testID); p.Status != invoice.StatusPaid || h.cache.Sets() != 3 {
		t.Fatalf("cache %+v, sets %d", p, h.cache.Sets())
	}
	for i, ev := range h.store.Events() {
		var d EventData
		if err := json.Unmarshal(ev.Data, &d); err != nil || d.Sequence != i+2 || ev.ID != EventID(testID, i+2) ||
			ev.Subject != testID.String() {
			t.Fatalf("event %d: %+v %v", i, d, err)
		}
	}
}

// Redelivered records and duplicate transfers change nothing.
func TestEngineIdempotent(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	te := transferEv(0, 100, time.Minute, 1000, 1)
	h.transfer(te)
	h.transfer(te)
	h.head(0, 100, time.Minute)
	ctx := context.Background()
	if err := h.e.NewHead(ctx, h.offset, head(0, 100, time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := h.e.Reorg(ctx, h.offset, Reorg{ChainID: testChain, FromBlock: 100}); err != nil {
		t.Fatal(err)
	}
	h.transfer(te)
	h.wantTypes(EventDetected)
	_, ts := h.store.Record(testID)
	if len(ts) != 1 || !ts[0].Canonical || ts[0].Role != RoleCounted {
		t.Fatalf("transfers %+v", ts)
	}
}

// A transfer whose block is canonical already is applied at once; one of a replaced block is not.
func TestEngineTransferAfterBlock(t *testing.T) {
	h := newHarness(t)
	h.head(0, 100, time.Minute)
	h.transfer(transferEv(1, 100, time.Minute, 1000, 1))
	h.wantTypes()
	h.transfer(transferEv(0, 100, time.Minute, 1000, 2))
	h.wantTypes(EventDetected)
	h.transfer(transferEv(0, 101, time.Minute, 1000, 3))
	_, ts := h.store.Record(testID)
	if len(ts) != 3 || ts[0].Canonical || !ts[1].Canonical || ts[2].Canonical {
		t.Fatalf("transfers %+v", ts)
	}
}

// chain.reorg orphans the transfers: reverted to NEW; the new fork includes the transaction again.
func TestEngineReorg(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	h.transfer(transferEv(0, 100, time.Minute, 1000, 1))
	h.head(0, 100, time.Minute)
	h.head(0, 101, 2*time.Minute)
	h.reorg(100)
	h.wantTypes(EventDetected, EventConfirming, EventReverted)
	if st := h.store.State(); st.Head != 99 || st.HeadHash != blockHash(0, 99) || !st.Time.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("state %+v", st)
	}
	var d EventData
	_ = json.Unmarshal(h.store.Events()[2].Data, &d)
	if d.Status != invoice.StatusNew || d.PreviousStatus != invoice.StatusConfirming || *d.FromBlock != 100 ||
		len(d.RevertedTransfers) != 1 || *d.Received != "0" {
		t.Fatalf("reverted %+v", d)
	}
	h.transfer(transferEv(1, 100, 90*time.Second, 1000, 1))
	h.head(1, 100, 90*time.Second)
	h.wantTypes(EventDetected, EventConfirming, EventReverted, EventDetected)
	// A reorg above the head and one of another chain change nothing.
	h.reorg(105)
	h.offset++
	if err := h.e.Reorg(context.Background(), h.offset, Reorg{ChainID: 1, FromBlock: 1}); err != nil {
		t.Fatal(err)
	}
	h.wantTypes(EventDetected, EventConfirming, EventReverted, EventDetected)
	if h.store.State().Offset != h.offset {
		t.Fatalf("offset %d", h.store.State().Offset)
	}
}

// A chain.new_head that replaces a canonical block is handled as a reorg from its number.
func TestEngineImplicitReorg(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	h.transfer(transferEv(0, 100, time.Minute, 1000, 1))
	h.head(0, 100, time.Minute)
	h.head(1, 100, time.Minute)
	h.wantTypes(EventDetected, EventReverted)
	if st := h.store.State(); st.Head != 100 || st.HeadHash != blockHash(1, 100) {
		t.Fatalf("state %+v", st)
	}
}

// A head that does not extend the chain or comes after a gap is accepted with a warning: chain-watcher is
// the source of the chain. A reorg from block 0 forgets the chain.
func TestEngineGapAndParentMismatch(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	h.head(1, 100, time.Minute)
	h.head(0, 105, 2*time.Minute)
	if st := h.store.State(); st.Head != 105 {
		t.Fatalf("state %+v", st)
	}
	h.reorg(0)
	if st := h.store.State(); st.Known {
		t.Fatalf("state %+v", st)
	}
	h.head(0, 10, 0)
	if st := h.store.State(); !st.Known || st.Head != 10 || !st.Time.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("chain time must not go back: %+v", st)
	}
}

// Block events of another chain only advance the offset.
func TestEngineOtherChain(t *testing.T) {
	h := newHarness(t)
	nh := head(0, 100, time.Minute)
	nh.ChainID = 1
	if err := h.e.NewHead(context.Background(), 7, nh); err != nil {
		t.Fatal(err)
	}
	if st := h.store.State(); st.Known || st.Offset != 7 {
		t.Fatalf("state %+v", st)
	}
	te := transferEv(0, 100, time.Minute, 1000, 1)
	te.ChainID = 1
	h.transfer(te)
	_, ts := h.store.Record(testID)
	if len(ts) != 0 {
		t.Fatal("transfer of another chain stored")
	}
}

// Transfers to unknown addresses and of another asset are skipped.
func TestEngineSkipsForeignTransfers(t *testing.T) {
	h := newHarness(t)
	te := transferEv(0, 100, time.Minute, 1000, 1)
	te.To = "0x2546bcd3c84621e976d8185a91a922ae77ecec30"
	h.transfer(te)
	te = transferEv(0, 100, time.Minute, 1000, 2)
	li := int64(0)
	te.LogIndex, te.Asset = &li, invoice.Asset{Kind: invoice.KindERC20, Contract: "0x5fbdb2315678afecb367f032d93f642f64180aa3", Decimals: 6}
	h.transfer(te)
	_, ts := h.store.Record(testID)
	if len(ts) != 0 {
		t.Fatalf("transfers %+v", ts)
	}
	te = transferEv(0, 100, time.Minute, 1000, 3)
	te.Amount = "01"
	if err := h.e.Transfer(context.Background(), te); err == nil {
		t.Fatal("invalid amount accepted")
	}
}

// The expiry and the late payment are driven by blocks only.
func TestEngineExpiryAndLatePaid(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	h.head(0, 100, 15*time.Minute)
	h.wantTypes()
	h.transfer(transferEv(0, 101, 16*time.Minute, 500, 1))
	h.head(0, 101, 16*time.Minute)
	h.wantTypes(EventExpired)
	h.head(0, 102, 17*time.Minute)
	h.head(0, 103, 18*time.Minute)
	h.wantTypes(EventExpired, EventLatePaid)
	if h.status() != invoice.StatusLatePaid {
		t.Fatal(h.status())
	}
}

// A failed transaction leaves nothing behind; the retried event is applied.
func TestEngineRollback(t *testing.T) {
	h := newHarness(t)
	h.head(0, 99, 0)
	h.transfer(transferEv(0, 100, time.Minute, 1000, 1))
	boom := errors.New("boom")
	h.store.Fail = func() error { return boom }
	if err := h.e.NewHead(context.Background(), 2, head(0, 100, time.Minute)); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if st := h.store.State(); st.Head != 99 || st.Offset != 1 || h.cache.Sets() != 0 {
		t.Fatalf("state %+v", st)
	}
	h.store.Fail = nil
	h.offset = 1
	h.head(0, 100, time.Minute)
	h.wantTypes(EventDetected)
}

func TestNewEngineValidates(t *testing.T) {
	st := NewMemStore(nil)
	for _, cfg := range []EngineConfig{
		{Config: Config{Confirmations: 1, RevertWait: 1, Window: 1}, ChainID: 1, KeepBlocks: 10},
		{Config: testCfg, ChainID: 0, KeepBlocks: 10},
		{Config: testCfg, ChainID: 1, KeepBlocks: 3},
	} {
		if _, err := NewEngine(st, nil, cfg, nil); err == nil {
			t.Fatalf("%+v accepted", cfg)
		}
	}
}

// Old blocks are pruned; the window is kept.
func TestEnginePrunesBlocks(t *testing.T) {
	h := newHarness(t)
	for n := uint64(1); n <= 40; n++ {
		h.head(0, n, time.Duration(n)*time.Second)
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if len(h.store.d.blocks) != 16 {
		t.Fatalf("%d blocks kept", len(h.store.d.blocks))
	}
}

// Every event type the lifecycle publishes validates against its schema (contracts/schemas).
func TestEventsMatchContract(t *testing.T) {
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	li := int64(2)
	erc := tr(100, 0, 5, 1)
	erc.LogIndex, erc.Asset = &li, invoice.Asset{Kind: invoice.KindERC20, Contract: "0x5fbdb2315678afecb367f032d93f642f64180aa3", Decimals: 6}
	inv := newRecord(1000).Invoice
	ts := []Transfer{tr(100, 0, 1000, 1)}
	for _, c := range []Transition{
		{Type: EventDetected, From: invoice.StatusNew, To: invoice.StatusDetected, Confirmations: 1, Received: big.NewInt(0)},
		{Type: EventConfirming, From: invoice.StatusUnderpaid, To: invoice.StatusConfirming, Confirmations: 1, Received: big.NewInt(1)},
		{Type: EventPaid, To: invoice.StatusPaid, Confirmations: 7, Received: big.NewInt(1000), PaidAt: base, Overpaid: true, Transfers: ts},
		{Type: EventUnderpaid, To: invoice.StatusUnderpaid, Confirmations: 7, Received: big.NewInt(1), Transfers: ts},
		{Type: EventExpired, To: invoice.StatusExpired},
		{Type: EventLatePaid, To: invoice.StatusLatePaid, Confirmations: 7, Received: big.NewInt(1), Transfers: ts},
		{Type: EventReverted, From: invoice.StatusConfirming, To: invoice.StatusNew, Received: big.NewInt(0), FromBlock: 100, Transfers: ts},
		{Type: EventLateTransfer, From: invoice.StatusPaid, To: invoice.StatusPaid, Transfers: []Transfer{erc}},
	} {
		c.Sequence = 5
		ev, err := NewEvent(inv, c, base)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := codec.Encode(ev); err != nil {
			t.Fatalf("%s: %v", c.Type, err)
		}
	}
}

type waiter struct{ calls, fail int }

func (w *waiter) Wait(context.Context) error {
	w.calls++
	if w.calls <= w.fail {
		return errors.New("kafka unavailable")
	}
	return nil
}

func blockEvent(t *testing.T, typ string, data any) kafka.Event {
	t.Helper()
	ev, err := kafka.NewEvent("/weigate/chain-watcher", typ, "31337", 1, data)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// The handlers wait for the transfers before a block, retry failures and skip unknown types.
func TestHandlers(t *testing.T) {
	h := newHarness(t)
	w := &waiter{fail: 1}
	b := projection.Backoff{Min: time.Millisecond, Max: time.Millisecond}
	blocks, transfers := BlocksHandler(h.e, w, b, h.e.log), TransfersHandler(h.e, b, h.e.log)
	ctx := kafka.WithPosition(context.Background(), kafka.Position{Topic: TopicBlocks, Offset: 1})
	if err := blocks(context.Background(), blockEvent(t, EventNewHead, head(0, 99, 0))); err == nil {
		t.Fatal("record without a position accepted")
	}
	if err := transfers(ctx, blockEvent(t, EventTransfer, transferEv(0, 100, time.Minute, 1000, 1))); err != nil {
		t.Fatal(err)
	}
	if err := blocks(ctx, blockEvent(t, EventNewHead, head(0, 100, time.Minute))); err != nil {
		t.Fatal(err)
	}
	if w.calls != 2 {
		t.Fatalf("waited %d times", w.calls)
	}
	h.wantTypes(EventDetected)
	ctx = kafka.WithPosition(ctx, kafka.Position{Topic: TopicBlocks, Offset: 2})
	if err := blocks(ctx, blockEvent(t, EventReorg, Reorg{ChainID: testChain, FromBlock: 100})); err != nil {
		t.Fatal(err)
	}
	h.wantTypes(EventDetected, EventReverted)
	for _, hd := range []kafka.Handler{blocks, transfers} {
		if err := hd(ctx, blockEvent(t, "chain.other", map[string]int{"a": 1})); err != nil {
			t.Fatal(err)
		}
	}
	// A cancelled context stops the retries.
	h.store.Fail = func() error { return errors.New("down") }
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := transfers(cctx, blockEvent(t, EventTransfer, transferEv(0, 100, time.Minute, 1000, 9))); err == nil {
		t.Fatal("cancelled retry returned nil")
	}
}

// An invoice in USDT counts only the Transfer logs of its token (FR-10): ETH and another token are skipped,
// the token transfer pays it, and the amount is in token base units.
func TestEngineUSDTInvoice(t *testing.T) {
	h := newHarness(t)
	usdt := invoice.Asset{Kind: invoice.KindERC20, Contract: "0x5fbdb2315678afecb367f032d93f642f64180aa3", Decimals: 6}
	rec := newRecord(10_000_000)
	rec.Asset = usdt
	h.store.AddInvoice(rec)
	h.head(0, 99, 0)
	token := func(tx int, asset invoice.Asset, amount int64) TransferEvent {
		te := transferEv(0, 100, time.Minute, amount, tx)
		li := int64(tx)
		te.LogIndex, te.Asset = &li, asset
		return te
	}
	other := invoice.Asset{Kind: invoice.KindERC20, Contract: "0xe7f1725e7734ce288f8367e1bb143e90bb3f0512", Decimals: 6}
	h.transfer(transferEv(0, 100, time.Minute, 10_000_000, 1)) // ETH
	h.transfer(token(2, other, 10_000_000))                    // another token
	h.head(0, 100, time.Minute)
	h.wantTypes()
	if _, ts := h.store.Record(testID); len(ts) != 0 {
		t.Fatalf("foreign transfers stored: %+v", ts)
	}
	ok := token(3, usdt, 10_000_000)
	ok.Asset.Contract = strings.ToUpper(ok.Asset.Contract[:2]) + ok.Asset.Contract[2:] // normalised to lower case
	h.transfer(ok)
	h.head(0, 101, 2*time.Minute)
	h.wantTypes(EventDetected, EventConfirming)
	h.head(0, 106, 3*time.Minute)
	h.wantTypes(EventDetected, EventConfirming, EventPaid)
	if got, _ := h.store.Record(testID); got.Status != invoice.StatusPaid || got.Received.String() != "10000000" {
		t.Fatalf("%+v", got)
	}
}
