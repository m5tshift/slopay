package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/money"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
)

// The property test drives the Engine with what chain-watcher publishes, delivered as Kafka may deliver it,
// and compares the published invoice.* events with a reference model.
//
// chain-watcher (generated): blocks with random intervals and transfers to the invoices (new transactions and
// orphaned ones included again), reorgs up to min(6, N-1) blocks below the highest block. For every block it
// publishes the transfers, then chain.new_head.
//
// Delivery (generated): chain.blocks in order with replays of the last records (a crash before the offset
// commit); each transfer at any point before the chain.new_head of its block (the barrier); transfers of a
// block whose chain.new_head was never published (chain-watcher lost it) at any point; random duplicates at
// any later point; failed transactions that are retried.
//
// Model: an ideal observer that sees every block together with its transfers and recomputes every invoice
// from the canonical chain after every block event, by the rules of SRS 4.1.4.

type pubKind int

const (
	pubHead pubKind = iota
	pubReorg
	// pubGhost: transfers of a block whose chain.new_head was never published.
	pubGhost
)

type pubBlock struct {
	number    uint64
	hash      string
	parent    string
	ts        time.Time
	transfers []TransferEvent
}

type pub struct {
	kind  pubKind
	block *pubBlock
	from  uint64
}

type propInvoice struct {
	id      uuid.UUID
	addr    string
	amount  int64
	bps     int
	expires time.Time
	asset   invoice.Asset
}

// The assets of the property test: an invoice is paid in ETH or in the test USDT, and transfers also come in
// another token that no invoice asks for. Only a transfer of the invoice asset counts (SRS 4.1.4, FR-10).
var (
	propUSDT  = invoice.Asset{Kind: invoice.KindERC20, Contract: "0x5fbdb2315678afecb367f032d93f642f64180aa3", Decimals: 6}
	propOther = invoice.Asset{Kind: invoice.KindERC20, Contract: "0xe7f1725e7734ce288f8367e1bb143e90bb3f0512", Decimals: 18}
	propAsset = []invoice.Asset{invoice.ETH, propUSDT, propOther}
)

// obs is an observed invoice.* event, comparable between the engine and the model.
type obs struct {
	Type, Status, Previous, Received, PaidAt string
	Sequence, Confirmations                  int
	Overpaid                                 bool
	FromBlock                                uint64
	Transfers                                []string
}

func (o obs) String() string { b, _ := json.Marshal(o); return string(b) }

func TestLifecycleMatchesModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(2, 7).Draw(rt, "confirmations")
		cfg := Config{Confirmations: n, RevertWait: 30 * time.Second, Window: 150 * time.Second}
		maxDepth := uint64(min(6, n-1)) //nolint:gosec // n >= 2.

		invs := make([]propInvoice, rapid.IntRange(1, 3).Draw(rt, "invoices"))
		for i := range invs {
			invs[i] = propInvoice{
				id:      uuid.NewSHA1(uuid.Nil, []byte{byte(i)}),
				addr:    fmt.Sprintf("0x%040x", i+1),
				amount:  1000,
				bps:     rapid.IntRange(0, 500).Draw(rt, "tolerance"),
				expires: base.Add(time.Duration(rapid.IntRange(5, 90).Draw(rt, "ttl")) * time.Second),
				asset:   propAsset[rapid.IntRange(0, 1).Draw(rt, "invoice asset")],
			}
		}
		pubs := generateChain(rt, invs, maxDepth)

		codecStore := NewMemStore(nil)
		for _, iv := range invs {
			codecStore.AddInvoice(Record{Invoice: invoice.Invoice{ID: iv.id, MerchantID: merchant,
				Amount: big.NewInt(iv.amount), Address: iv.addr, Asset: iv.asset, Status: invoice.StatusNew,
				Sequence: 1, Received: new(big.Int), ExpiresAt: iv.expires}, ToleranceBps: iv.bps})
		}
		e, err := NewEngine(codecStore, nil, EngineConfig{Config: cfg, ChainID: testChain, KeepBlocks: 16,
			Now: func() time.Time { return base }}, slog.New(slog.DiscardHandler))
		if err != nil {
			rt.Fatal(err)
		}
		rng := rand.New(rand.NewPCG(rapid.Uint64().Draw(rt, "failure seed"), 1)) //nolint:gosec // Test failure injection.
		failRate := rapid.IntRange(0, 20).Draw(rt, "failure percent")
		codecStore.Fail = func() error {
			if rng.IntN(100) < failRate {
				return errors.New("injected failure")
			}
			return nil
		}
		deliver(rt, e, pubs)

		m := newModel(cfg, invs)
		for _, p := range pubs {
			switch p.kind {
			case pubHead:
				m.newHead(p.block)
			case pubReorg:
				m.reorg(p.from)
			}
		}

		got := engineEvents(rt, codecStore.Events())
		for i, iv := range invs {
			want := m.invs[i].events
			if fmt.Sprint(got[iv.id]) != fmt.Sprint(want) {
				rt.Fatalf("invoice %d events\n got: %v\nwant: %v", i, got[iv.id], want)
			}
			rec, _ := codecStore.Record(iv.id)
			mi := m.invs[i]
			if rec.Status != mi.status || rec.Sequence != mi.seq || rec.Received.Cmp(mi.received) != 0 ||
				rec.Overpaid != mi.overpaid {
				rt.Fatalf("invoice %d: %s/%d/%s, model %s/%d/%s", i, rec.Status, rec.Sequence, rec.Received,
					mi.status, mi.seq, mi.received)
			}
		}
	})
}

// generateChain simulates chain-watcher.
func generateChain(rt *rapid.T, invs []propInvoice, depth uint64) []pub {
	genesis := &pubBlock{number: 0, hash: blockHash(1<<40, 0), ts: base}
	chain := []*pubBlock{genesis}
	var pubs []pub
	var orphaned []TransferEvent
	fork, maxHead, txs := uint64(0), uint64(0), 0
	steps := rapid.IntRange(1, 45).Draw(rt, "steps")
	for s := 0; s < steps; s++ {
		headN := chain[len(chain)-1].number
		lowest := uint64(1)
		if maxHead+1 > depth+lowest {
			lowest = maxHead + 1 - depth
		}
		if headN >= lowest && rapid.IntRange(0, 7).Draw(rt, "reorg?") == 0 {
			from := rapid.Uint64Range(lowest, headN).Draw(rt, "from")
			for _, b := range chain[from:] {
				orphaned = append(orphaned, b.transfers...)
			}
			chain = chain[:from]
			fork++
			pubs = append(pubs, pub{kind: pubReorg, from: from})
			continue
		}
		parent := chain[len(chain)-1]
		b := &pubBlock{number: parent.number + 1, parent: parent.hash,
			ts: parent.ts.Add(time.Duration(rapid.IntRange(1, 12).Draw(rt, "interval")) * time.Second)}
		b.hash = blockHash(fork, b.number)
		for k := rapid.IntRange(0, 2).Draw(rt, "transfers"); k > 0; k-- {
			te := TransferEvent{ChainID: testChain, BlockNumber: b.number, BlockHash: b.hash,
				BlockTimestamp: b.ts.Unix(), From: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", Asset: invoice.ETH}
			if len(orphaned) > 0 && rapid.IntRange(0, 2).Draw(rt, "include again?") == 0 {
				o := orphaned[rapid.IntRange(0, len(orphaned)-1).Draw(rt, "orphan")]
				te.TxHash, te.To, te.Amount, te.Asset, te.LogIndex = o.TxHash, o.To, o.Amount, o.Asset, o.LogIndex
			} else {
				txs++
				te.TxHash = txHash(txs)
				to := rapid.IntRange(0, len(invs)).Draw(rt, "to")
				te.To = fmt.Sprintf("0x%040x", to+1) // to == len(invs): an unknown address
				te.Amount = fmt.Sprint(rapid.IntRange(1, 1200).Draw(rt, "amount"))
				te.Asset = propAsset[rapid.IntRange(0, len(propAsset)-1).Draw(rt, "asset")]
				if te.Asset.Kind == invoice.KindERC20 {
					li := int64(rapid.IntRange(0, 3).Draw(rt, "log index"))
					te.LogIndex = &li
				}
			}
			// A transaction is included at most once in a chain.
			if included(append(chain, b), te.TxHash) {
				continue
			}
			b.transfers = append(b.transfers, te)
		}
		if rapid.IntRange(0, 9).Draw(rt, "ghost?") == 0 {
			// chain-watcher published the transfers and lost the block before its chain.new_head.
			b.hash = blockHash(1<<41+uint64(s), b.number)
			for i := range b.transfers {
				b.transfers[i].BlockHash = b.hash
			}
			orphaned = append(orphaned, b.transfers...)
			pubs = append(pubs, pub{kind: pubGhost, block: b})
			continue
		}
		chain = append(chain, b)
		maxHead = max(maxHead, b.number)
		pubs = append(pubs, pub{kind: pubHead, block: b})
	}
	return pubs
}

func included(chain []*pubBlock, tx string) bool {
	for _, b := range chain {
		for _, te := range b.transfers {
			if te.TxHash == tx {
				return true
			}
		}
	}
	return false
}

// deliver feeds the published events to the engine as the consumers may deliver them.
func deliver(rt *rapid.T, e *Engine, pubs []pub) {
	ctx := context.Background()
	// Block events get offsets 0..len(blocks)-1; slot k delivers transfers before the block event k.
	var blocks []pub
	slots := [][]TransferEvent{nil}
	put := func(te TransferEvent, lo, hi int) {
		k := rapid.IntRange(lo, hi).Draw(rt, "slot")
		for len(slots) <= k {
			slots = append(slots, nil)
		}
		slots[k] = append(slots[k], te)
		if rapid.IntRange(0, 4).Draw(rt, "duplicate?") == 0 {
			d := rapid.IntRange(k, hi).Draw(rt, "duplicate slot")
			for len(slots) <= d {
				slots = append(slots, nil)
			}
			slots[d] = append(slots[d], te)
		}
	}
	nBlocks := 0
	for _, p := range pubs {
		if p.kind != pubGhost {
			nBlocks++
		}
	}
	for _, p := range pubs {
		switch p.kind {
		case pubHead:
			// The barrier: before the chain.new_head of the block.
			for _, te := range p.block.transfers {
				put(te, 0, len(blocks))
			}
		case pubGhost:
			for _, te := range p.block.transfers {
				put(te, 0, nBlocks)
			}
			continue
		}
		blocks = append(blocks, p)
	}
	for len(slots) <= nBlocks {
		slots = append(slots, nil)
	}
	m := len(blocks)
	retry := func(what string, fn func() error) {
		for attempt := 0; ; attempt++ {
			err := fn()
			if err == nil {
				return
			}
			if attempt > 100 {
				rt.Fatalf("%s: %v", what, err)
			}
		}
	}
	apply := func(i int) {
		p := blocks[i]
		if p.kind == pubHead {
			h := NewHead{ChainID: testChain, Number: p.block.number, Hash: p.block.hash,
				ParentHash: p.block.parent, Timestamp: p.block.ts.Unix()}
			retry("new head", func() error { return e.NewHead(ctx, int64(i), h) })
			return
		}
		retry("reorg", func() error { return e.Reorg(ctx, int64(i), Reorg{ChainID: testChain, FromBlock: p.from}) })
	}
	for s := 0; s <= m; s++ {
		order := rapid.Permutation(slots[s]).Draw(rt, "order")
		for _, te := range order {
			retry("transfer", func() error { return e.Transfer(ctx, te) })
		}
		if s == m {
			break
		}
		apply(s)
		if rapid.IntRange(0, 5).Draw(rt, "replay?") == 0 {
			for j := rapid.IntRange(max(0, s-3), s).Draw(rt, "replay from"); j <= s; j++ {
				apply(j)
			}
		}
	}
}

func engineEvents(rt *rapid.T, events []kafka.Event) map[uuid.UUID][]obs {
	out := map[uuid.UUID][]obs{}
	for _, ev := range events {
		var d EventData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			rt.Fatal(err)
		}
		o := obs{Type: ev.Type, Status: string(d.Status), Previous: string(d.PreviousStatus), Sequence: d.Sequence}
		if d.Received != nil {
			o.Received = *d.Received
		}
		if d.Confirmations != nil {
			o.Confirmations = *d.Confirmations
		}
		if d.Overpaid != nil {
			o.Overpaid = *d.Overpaid
		}
		if d.PaidAt != nil {
			o.PaidAt = d.PaidAt.UTC().Format(time.RFC3339)
		}
		if d.FromBlock != nil {
			o.FromBlock = *d.FromBlock
		}
		for _, a := range append(d.Transfers, d.RevertedTransfers...) {
			o.Transfers = append(o.Transfers, fmt.Sprintf("%d:%s:%s:%s", a.BlockNumber, a.BlockHash, a.TxHash, a.Amount))
		}
		out[d.InvoiceID] = append(out[d.InvoiceID], o)
	}
	return out
}

// The reference model.

type mTransfer struct {
	key, tx string
	number  uint64
	hash    string
	ts      time.Time
	amount  *big.Int
}

func (t mTransfer) String() string {
	return fmt.Sprintf("%d:%s:%s:%s", t.number, t.hash, t.tx, t.amount)
}

type mInvoice struct {
	propInvoice
	status   invoice.Status
	seq      int
	received *big.Int
	paidAt   time.Time
	overpaid bool
	counted  map[string]mTransfer
	late     map[string]bool
	grace    map[string]time.Time
	events   []obs
}

type model struct {
	cfg    Config
	blocks map[uint64]*pubBlock
	head   uint64
	now    time.Time
	invs   []*mInvoice
}

func newModel(cfg Config, invs []propInvoice) *model {
	m := &model{cfg: cfg, blocks: map[uint64]*pubBlock{}}
	for _, iv := range invs {
		m.invs = append(m.invs, &mInvoice{propInvoice: iv, status: invoice.StatusNew, seq: 1,
			received: new(big.Int), counted: map[string]mTransfer{}, late: map[string]bool{},
			grace: map[string]time.Time{}})
	}
	return m
}

func (m *model) newHead(b *pubBlock) {
	m.blocks[b.number] = b
	m.head = b.number
	if b.ts.After(m.now) {
		m.now = b.ts
	}
	m.step(0)
}

func (m *model) reorg(from uint64) {
	if from > m.head {
		return
	}
	for n := range m.blocks {
		if n >= from {
			delete(m.blocks, n)
		}
	}
	m.head = from - 1
	m.step(from)
}

func (m *model) step(from uint64) {
	for _, iv := range m.invs {
		m.evaluate(iv, from)
	}
}

// canonical returns the transfers to the invoice in the canonical chain, by block.
func (m *model) canonical(iv *mInvoice) []mTransfer {
	var out []mTransfer
	for n, b := range m.blocks {
		if n > m.head {
			continue
		}
		for _, te := range b.transfers {
			if te.To != iv.addr || te.Asset != iv.asset {
				continue
			}
			a, _ := new(big.Int).SetString(te.Amount, 10)
			out = append(out, mTransfer{key: te.BlockHash + ":" + te.TxHash + ":", tx: te.TxHash, number: n,
				hash: te.BlockHash, ts: time.Unix(te.BlockTimestamp, 0).UTC(), amount: a})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].number != out[j].number {
			return out[i].number < out[j].number
		}
		return out[i].key < out[j].key
	})
	return out
}

func (m *model) emit(iv *mInvoice, o obs) {
	iv.seq++
	o.Sequence = iv.seq
	iv.status = invoice.Status(o.Status)
	iv.events = append(iv.events, o)
}

func names(ts []mTransfer) []string {
	sort.Slice(ts, func(i, j int) bool {
		if ts[i].number != ts[j].number {
			return ts[i].number < ts[j].number
		}
		return ts[i].key < ts[j].key
	})
	var out []string
	for _, t := range ts {
		out = append(out, t.String())
	}
	return out
}

func (m *model) final(iv *mInvoice) bool {
	switch iv.status {
	case invoice.StatusPaid, invoice.StatusLatePaid, invoice.StatusExpired:
		return true
	}
	return iv.status == invoice.StatusUnderpaid && m.now.After(iv.expires)
}

func (m *model) evaluate(iv *mInvoice, from uint64) {
	canon := m.canonical(iv)
	inCanon := map[string]bool{}
	for _, t := range canon {
		inCanon[t.key] = true
	}
	conf := func(t mTransfer) int { return int(m.head - t.number + 1) } //nolint:gosec // Canonical: number <= head.
	n := m.cfg.Confirmations
	if !m.final(iv) {
		var reverted []mTransfer
		for k, t := range iv.counted {
			if !inCanon[k] {
				reverted = append(reverted, t)
				if m.now.After(iv.expires) {
					iv.grace[t.tx] = m.now.Add(m.cfg.RevertWait)
				}
			}
		}
		before := iv.counted
		topUp := false
		iv.counted = map[string]mTransfer{}
		received := new(big.Int)
		var last *mTransfer
		for _, t := range canon {
			g, ok := iv.grace[t.tx]
			if iv.late[t.key] || (t.ts.After(iv.expires) && (!ok || t.ts.After(g))) {
				continue
			}
			if _, ok := before[t.key]; !ok {
				topUp = true
			}
			iv.counted[t.key] = t
			received.Add(received, t.amount)
			last = &t
		}
		iv.received = received
		c := 0
		if last != nil {
			c = conf(*last)
		}
		snap := func(typ string, st invoice.Status) obs {
			return obs{Type: typ, Status: string(st), Received: received.String(), Confirmations: c}
		}
		if len(reverted) > 0 && (iv.status == invoice.StatusDetected || iv.status == invoice.StatusConfirming) {
			st := invoice.StatusConfirming
			if last == nil {
				st = invoice.StatusNew
			} else if c == 1 {
				st = invoice.StatusDetected
			}
			m.emit(iv, obs{Type: EventReverted, Status: string(st), Previous: string(iv.status),
				Received: received.String(), FromBlock: from, Transfers: names(reverted)})
		}
		if last != nil {
			if iv.status == invoice.StatusNew {
				m.emit(iv, snap(EventDetected, invoice.StatusDetected))
			}
			if iv.status == invoice.StatusUnderpaid && topUp {
				m.emit(iv, snap(EventConfirming, invoice.StatusConfirming))
			}
			if iv.status == invoice.StatusDetected && c >= 2 {
				m.emit(iv, snap(EventConfirming, invoice.StatusConfirming))
			}
			if iv.status == invoice.StatusConfirming && c >= n {
				var counted []mTransfer
				for _, t := range iv.counted {
					counted = append(counted, t)
				}
				res, _ := money.Check(big.NewInt(iv.amount), received, iv.bps)
				o := snap(EventUnderpaid, invoice.StatusUnderpaid)
				o.Transfers = names(counted)
				if res.Paid {
					o.Type, o.Status, o.Overpaid = EventPaid, string(invoice.StatusPaid), res.Overpaid
					o.PaidAt = m.now.Format(time.RFC3339)
					iv.paidAt, iv.overpaid = m.now, res.Overpaid
				}
				m.emit(iv, o)
			}
		}
		deadline := iv.expires
		for _, g := range iv.grace {
			if g.After(deadline) {
				deadline = g
			}
		}
		if iv.status == invoice.StatusNew && m.now.After(deadline) {
			m.emit(iv, obs{Type: EventExpired, Status: string(invoice.StatusExpired)})
		}
	}
	if !m.final(iv) {
		return
	}
	end := iv.expires
	if iv.paidAt.After(end) {
		end = iv.paidAt
	}
	end = end.Add(m.cfg.Window)
	var late []mTransfer
	sum, lastConf := new(big.Int), 0
	for _, t := range canon {
		if _, counted := iv.counted[t.key]; counted || iv.late[t.key] || t.ts.After(end) || conf(t) < n {
			continue
		}
		iv.late[t.key] = true
		late = append(late, t)
		sum.Add(sum, t.amount)
		lastConf = conf(t)
	}
	if len(late) == 0 {
		return
	}
	if iv.status == invoice.StatusExpired {
		iv.received = sum
		m.emit(iv, obs{Type: EventLatePaid, Status: string(invoice.StatusLatePaid), Received: sum.String(),
			Confirmations: lastConf, Transfers: names(late)})
		return
	}
	m.emit(iv, obs{Type: EventLateTransfer, Status: string(iv.status), Transfers: names(late)})
}
