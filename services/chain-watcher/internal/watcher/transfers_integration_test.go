//go:build integration

package watcher_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel/attribute"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/events"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/projection"
)

// payer is the first unlocked account of Anvil.
const payer = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"

func newAddress(t *testing.T) string {
	t.Helper()
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "0x" + hex.EncodeToString(b)
}

// sendETH transfers wei to the address and returns the transaction and its block. Anvil may answer
// eth_sendTransaction before the block is visible, so the receipt is polled: a missing one is null.
func sendETH(t *testing.T, to string, wei int64) (string, uint64) {
	t.Helper()
	var tx string
	rpcCall(t, &tx, "eth_sendTransaction", map[string]any{"from": payer, "to": to, "value": (*hexutil.Big)(big.NewInt(wei))})
	var n uint64
	eventually(t, "receipt of "+tx, func() bool {
		var r *struct {
			BlockNumber hexutil.Uint64 `json:"blockNumber"`
		}
		rpcCall(t, &r, "eth_getTransactionReceipt", tx)
		if r == nil {
			return false
		}
		n = uint64(r.BlockNumber)
		return true
	})
	return tx, n
}

// createInvoice publishes invoice.created of an ETH invoice for the address, written at the given time.
func (h *harness) createInvoice(t *testing.T, address string, at, expires time.Time) {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	ev, err := kafka.NewEvent("/slopay/invoice-service", projection.EventCreated, id, 1, map[string]any{
		"invoiceId": id, "merchantId": uuid.NewString(), "sequence": 1, "status": "NEW", "address": address,
		"asset": map[string]any{"kind": "ETH", "decimals": 18}, "amount": "1000", "orderId": "order-" + id,
		"amountUsdCents": 100, "rate": "3000", "expiresAt": expires.UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	ev.Time = at.UTC()
	val, err := codec.Encode(ev)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	rec := &kgo.Record{Topic: h.invoices, Key: []byte(id), Value: val, Timestamp: at}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// waitSearched waits until the processed blocks have been searched for the address.
func (h *harness) waitSearched(t *testing.T, address string) {
	t.Helper()
	eventually(t, "address "+address+" searched", func() bool {
		m, err := h.addresses.Lookup(context.Background(), []string{address})
		return err == nil && m[address]
	})
}

// position returns the index of the transfer of tx and of chain.new_head of block n in the events; -1 if absent.
// A transfer must be published once.
func position(t *testing.T, evs []received, tx string, n uint64) (transfer, head int) {
	t.Helper()
	transfer, head = -1, -1
	for i, e := range evs {
		switch {
		case e.typ == events.TypeTransfer && e.TxHash == tx:
			if transfer >= 0 {
				t.Fatalf("transfer %s published twice", tx)
			}
			transfer = i
		case e.typ == events.TypeNewHead && e.Number == n && head < 0:
			head = i
		}
	}
	return transfer, head
}

func (c *collector) waitTransfer(t *testing.T, tx string) received {
	t.Helper()
	var got received
	eventually(t, "chain.transfer "+tx, func() bool {
		for _, e := range c.all(t) {
			if e.typ == events.TypeTransfer && e.TxHash == tx {
				got = e
				return true
			}
		}
		return false
	})
	return got
}

// TestTransferBeforeAddressIsFound pays an address before its invoice reaches chain-watcher: the processed block
// is searched again, and the transfer is published after that block and before the next chain.new_head. A
// transfer in a new block is published before the chain.new_head of its block.
func TestTransferBeforeAddressIsFound(t *testing.T) {
	h := newHarness(t)
	h.run(t)
	mine(t, 1)
	h.events.waitHead(t, headNumber(t))

	addr := newAddress(t)
	early, n := sendETH(t, addr, 1_000_000_000_000_000)
	h.events.waitHead(t, n)
	time.Sleep(300 * time.Millisecond)
	if tr, _ := position(t, h.events.all(t), early, n); tr >= 0 {
		t.Fatal("transfer to an unknown address published")
	}

	h.createInvoice(t, addr, time.Now(), time.Now().Add(15*time.Minute))
	h.waitSearched(t, addr)
	mine(t, 1)
	h.events.waitHead(t, n+1)
	evs := h.events.all(t)
	tr, headN := position(t, evs, early, n)
	_, headNext := position(t, evs, early, n+1)
	if tr <= headN || tr >= headNext {
		t.Fatalf("transfer at %d, head %d at %d, head %d at %d", tr, n, headN, n+1, headNext)
	}
	got := evs[tr]
	if got.BlockNumber != n || got.BlockHash != blockAt(t, n).Hash || got.To != addr || got.Amount != "1000000000000000" ||
		got.key != addr || got.id != fmt.Sprintf("%s:%s:", got.BlockHash, early) {
		t.Fatalf("transfer %+v", got)
	}

	live, m := sendETH(t, addr, 7)
	h.events.waitHead(t, m)
	evs = h.events.all(t)
	if tr, head := position(t, evs, live, m); tr < 0 || tr != head-1 {
		t.Fatalf("live transfer at %d, head %d at %d", tr, m, head)
	}
	view, _ := replay(t, evs)
	matchesNode(t, view, h.start, m)
	if got := h.metric(t, "slopay_chain_transfers_published", attribute.String("mode", "rescan")); got != 1 {
		t.Errorf("rescan transfers %d", got)
	}
	if got := h.metric(t, "slopay_chain_transfers_published", attribute.String("mode", "live")); got != 1 {
		t.Errorf("live transfers %d", got)
	}
}

// TestRestartWithValkeyStopped restarts chain-watcher while Valkey is down and blocks are mined: the addresses
// are checked in MongoDB, and no transfer is missed. When Valkey is back, the cache is loaded and used again.
func TestRestartWithValkeyStopped(t *testing.T) {
	h := newHarness(t)
	h.run(t)
	known := newAddress(t)
	h.createInvoice(t, known, time.Now(), time.Now().Add(15*time.Minute))
	h.waitSearched(t, known)
	first, _ := sendETH(t, known, 1)
	h.events.waitTransfer(t, first)
	if got := h.metric(t, "slopay_chain_address_lookups", attribute.String("source", "cache")); got == 0 {
		t.Fatal("cache not used while valkey is up")
	}
	h.stop()

	ctx := context.Background()
	stopTimeout := 10 * time.Second
	if err := valkeyC.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	restarted := false
	t.Cleanup(func() {
		if !restarted {
			_ = valkeyC.Start(ctx)
			_ = refreshValkeyAddr(ctx)
		}
	})
	added := newAddress(t)
	h.createInvoice(t, added, time.Now(), time.Now().Add(15*time.Minute))
	toKnown, n1 := sendETH(t, known, 2)
	toAdded, n2 := sendETH(t, added, 3)

	h.run(t)
	h.events.waitHead(t, n2)
	h.waitSearched(t, added)
	mine(t, 1)
	h.events.waitHead(t, n2+1)
	evs := h.events.all(t)
	if tr, head := position(t, evs, toKnown, n1); tr < 0 || tr > head {
		t.Fatalf("transfer to the known address at %d, head %d at %d", tr, n1, head)
	}
	if tr, head := position(t, evs, toAdded, n2+1); tr < 0 || tr > head {
		t.Fatalf("transfer to the added address at %d, head %d at %d", tr, n2+1, head)
	}
	if got := h.metric(t, "slopay_chain_address_lookups", attribute.String("source", "mongodb")); got == 0 {
		t.Fatal("mongodb not used without valkey")
	}

	if err := valkeyC.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted = true
	if err := refreshValkeyAddr(ctx); err != nil {
		t.Fatal(err)
	}
	before := h.metric(t, "slopay_chain_address_lookups", attribute.String("source", "cache"))
	eventually(t, "the cache used again", func() bool {
		tx, _ := sendETH(t, known, 4)
		h.events.waitTransfer(t, tx)
		return h.metric(t, "slopay_chain_address_lookups", attribute.String("source", "cache")) > before
	})
	view, _ := replay(t, h.events.all(t))
	matchesNode(t, view, h.start, headNumber(t))
}

// TestProjectionRebuiltAfterLoss drops the projection: the next start reads invoice.events of the last 26 h again,
// though the consumer group has already committed them, and the address is tracked again.
func TestProjectionRebuiltAfterLoss(t *testing.T) {
	h := newHarness(t)
	old := newAddress(t)
	h.createInvoice(t, old, time.Now().Add(-27*time.Hour), time.Now().Add(time.Hour)) // outside the window
	addr := newAddress(t)
	h.createInvoice(t, addr, time.Now(), time.Now().Add(15*time.Minute))
	h.run(t)
	h.waitSearched(t, addr)
	h.stop()

	ctx := context.Background()
	db := h.mongo.Database()
	for _, c := range []string{projection.Collection, projection.StateCollection} {
		if err := db.Collection(c).Drop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	h.run(t)
	h.waitSearched(t, addr)
	if n, err := db.Collection(projection.Collection).CountDocuments(ctx, bson.D{{Key: "_id", Value: old}}); err != nil || n != 0 {
		t.Fatalf("invoice older than the window tracked: %d %v", n, err)
	}
	tx, _ := sendETH(t, addr, 5)
	h.events.waitTransfer(t, tx)
}
