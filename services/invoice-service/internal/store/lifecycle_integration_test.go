//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
	"github.com/m5tshift/slopay/services/invoice-service/internal/lifecycle"
	"github.com/m5tshift/slopay/services/invoice-service/internal/projection"
)

// lcConfig uses N = 3 to keep the scenarios short; the rules do not depend on N.
var lcConfig = lifecycle.Config{Confirmations: 3, RevertWait: 10 * time.Minute, Window: 24 * time.Hour}

func hexOf(parts ...any) string {
	sum := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return "0x" + hex.EncodeToString(sum[:])
}

func addressOf(parts ...any) string { return hexOf(parts...)[:42] }

// lcInvoice creates an invoice and gives it an address of its own: the test merchants share one xpub, so
// the derived addresses repeat between tests.
func lcInvoice(t *testing.T, h *harness, mid uuid.UUID, key string) invoice.Invoice {
	t.Helper()
	inv, _, err := h.svc.Create(ctxT(t), mid, key, req(1999, "order-"+key))
	if err != nil {
		t.Fatal(err)
	}
	inv.Address = addressOf("invoice", inv.ID)
	if _, err := pg.Pool().Exec(ctxT(t), `UPDATE invoices SET address = $2 WHERE id = $1`, inv.ID, inv.Address); err != nil {
		t.Fatal(err)
	}
	// Later tests evaluate every watched invoice and every passed deadline: leave none behind.
	t.Cleanup(func() {
		_, _ = pg.Pool().Exec(context.Background(), `UPDATE invoices SET watch = false, deadline = NULL WHERE id = $1`, inv.ID)
	})
	return inv
}

func newEngine(t *testing.T, h *harness, chainID int64) *lifecycle.Engine {
	t.Helper()
	e, err := lifecycle.NewEngine(NewLifecycle(pg.Pool(), codec), h.cache, lifecycle.EngineConfig{
		Config: lcConfig, ChainID: chainID, KeepBlocks: 16,
	}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

type outboxEvent struct {
	Type     string
	Sequence int
	Status   string
}

func outboxEvents(t *testing.T, id uuid.UUID) []outboxEvent {
	t.Helper()
	rows, err := pg.Pool().Query(ctxT(t), `SELECT event->>'type', (event->'data'->>'sequence')::int,
event->'data'->>'status' FROM outbox WHERE key = $1 ORDER BY id`, id.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxEvent
	for rows.Next() {
		var e outboxEvent
		if err := rows.Scan(&e.Type, &e.Sequence, &e.Status); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func eventTypes(evs []outboxEvent) string {
	s := make([]string, len(evs))
	for i, e := range evs {
		s[i] = fmt.Sprintf("%s#%d", e.Type, e.Sequence)
	}
	return strings.Join(s, " ")
}

func newHead(chainID int64, fork string, n uint64, at time.Time) lifecycle.NewHead {
	return lifecycle.NewHead{ChainID: chainID, Number: n, Hash: hexOf(fork, n), ParentHash: hexOf(fork, n-1),
		Timestamp: at.Unix()}
}

func transferTo(chainID int64, inv invoice.Invoice, fork string, n uint64, at time.Time, amount *big.Int, tx int) lifecycle.TransferEvent {
	return lifecycle.TransferEvent{ChainID: chainID, BlockNumber: n, BlockHash: hexOf(fork, n), BlockTimestamp: at.Unix(),
		TxHash: hexOf("tx", inv.ID, tx), From: addressOf("payer"), To: inv.Address, Amount: amount.String(),
		Asset: invoice.ETH}
}

// chainEvent is an event of chain-watcher: the subject of a transfer is the recipient, of a block the chain.
func chainEvent(t *testing.T, typ string, data any) kafka.Event {
	t.Helper()
	subject := "31337"
	if te, ok := data.(lifecycle.TransferEvent); ok {
		subject = te.To
	}
	ev, err := kafka.NewEvent("/slopay/chain-watcher", typ, subject, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func createTopicN(ctx context.Context, topic string, partitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, partitions, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return err
		}
	}
	return nil
}

// TestLifecycleOverKafka runs the consumers of main against chain.blocks and chain.transfers published here
// (chain-watcher publishes them in #23): a transfer, a reorg that removes it, its re-inclusion and N blocks.
// Redelivered events change nothing; the events go to the outbox and the status page to Valkey.
func TestLifecycleOverKafka(t *testing.T) {
	ctx := ctxT(t)
	if err := createTopicN(ctx, lifecycle.TopicBlocks, 1); err != nil {
		t.Fatal(err)
	}
	if err := createTopicN(ctx, lifecycle.TopicTransfers, 3); err != nil {
		t.Fatal(err)
	}
	h, mid := newHarness(t)
	inv := lcInvoice(t, h, mid, "kafka")
	const chainID = 31337
	e := newEngine(t, h, chainID)

	p, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	t0 := time.Now().Add(-time.Minute)
	at := func(n uint64) time.Time { return t0.Add(time.Duration(n) * 2 * time.Second) } //nolint:gosec // n <= 3.
	publish := func(topic, typ string, data any) {
		t.Helper()
		if err := p.Publish(ctx, topic, chainEvent(t, typ, data)); err != nil {
			t.Fatal(err)
		}
	}
	// Block 1 of fork a carries the payment; the reorg replaces it with block 1 of fork b carrying it again.
	publish(lifecycle.TopicTransfers, lifecycle.EventTransfer, transferTo(chainID, inv, "a", 1, at(1), inv.Amount, 1))
	publish(lifecycle.TopicBlocks, lifecycle.EventNewHead, newHead(chainID, "a", 1, at(1)))
	publish(lifecycle.TopicBlocks, lifecycle.EventNewHead, newHead(chainID, "a", 1, at(1)))
	publish(lifecycle.TopicTransfers, lifecycle.EventTransfer, transferTo(chainID, inv, "a", 1, at(1), inv.Amount, 1))
	publish(lifecycle.TopicBlocks, lifecycle.EventReorg, lifecycle.Reorg{ChainID: chainID, FromBlock: 1})
	publish(lifecycle.TopicTransfers, lifecycle.EventTransfer, transferTo(chainID, inv, "b", 1, at(1), inv.Amount, 1))
	for n := uint64(1); n <= 3; n++ {
		publish(lifecycle.TopicBlocks, lifecycle.EventNewHead, newHead(chainID, "b", n, at(n)))
	}

	group := "invoice-" + uuid.NewString()
	transfersGroup := group + "-chain-transfers"
	tc, err := kafka.NewConsumer(codec, quiet, brokers, transfersGroup, []string{lifecycle.TopicTransfers})
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()
	bc, err := kafka.NewConsumer(codec, quiet, brokers, group+"-chain-blocks", []string{lifecycle.TopicBlocks})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	barrier := lifecycle.NewBarrier(admin, lifecycle.TopicTransfers, transfersGroup, 20*time.Millisecond, quiet)
	b := projection.Backoff{Min: 50 * time.Millisecond, Max: time.Second}

	cctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := tc.Run(cctx, lifecycle.TransfersHandler(e, b, quiet)); err != nil {
			t.Error(err)
		}
	})
	wg.Go(func() {
		if err := bc.Run(cctx, lifecycle.BlocksHandler(e, barrier, b, quiet)); err != nil {
			t.Error(err)
		}
	})
	defer func() {
		cancel()
		wg.Wait()
	}()

	want := "invoice.created#1 invoice.detected#2 invoice.reverted#3 invoice.detected#4 invoice.confirming#5 invoice.paid#6"
	deadline := time.Now().Add(timeout)
	for eventTypes(outboxEvents(t, inv.ID)) != want {
		if time.Now().After(deadline) {
			t.Fatalf("events %q, want %q", eventTypes(outboxEvents(t, inv.ID)), want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	got, err := h.store.Get(ctx, mid, inv.ID)
	if err != nil || got.Status != invoice.StatusPaid || got.Sequence != 6 || got.Confirmations != 3 ||
		got.Received.Cmp(inv.Amount) != 0 || got.Overpaid {
		t.Errorf("invoice %+v %v", got, err)
	}
	page, ok := h.cache.Get(ctx, inv.ID)
	if !ok || page.Status != invoice.StatusPaid {
		t.Errorf("cached page %+v %v", page, ok)
	}
	var known bool
	var head, offset int64
	if err := pg.Pool().QueryRow(ctx, `SELECT known, head_number, blocks_offset FROM chain_state WHERE chain_id = $1`,
		chainID).Scan(&known, &head, &offset); err != nil || !known || head != 3 || offset != 5 {
		t.Errorf("chain state %v %d %d %v", known, head, offset, err)
	}
	var stored int
	if err := pg.Pool().QueryRow(ctx, `SELECT count(*) FROM transfers WHERE invoice_id = $1`, inv.ID).Scan(&stored); err != nil || stored != 2 {
		t.Errorf("%d transfers stored, %v", stored, err)
	}
}

// TestLifecycleStore runs the engine on PostgreSQL directly: concurrent transfers to one invoice under the
// shared chain lock, a redelivered block, a reorg, and the NUMERIC(78,0) round trip of large amounts.
func TestLifecycleStore(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	inv := lcInvoice(t, h, mid, "store")
	const chainID = 31338
	e := newEngine(t, h, chainID)
	t0 := time.Now().Add(-time.Minute)
	const base = 1000
	if err := e.NewHead(ctx, 0, newHead(chainID, "s", base, t0)); err != nil {
		t.Fatal(err)
	}
	// Two transfers of a canonical block at once: both are counted, the invoice is evaluated under its row lock.
	half := new(big.Int).Rsh(inv.Amount, 1)
	rest := new(big.Int).Sub(inv.Amount, half)
	var wg sync.WaitGroup
	for i, amount := range []*big.Int{half, rest} {
		wg.Go(func() {
			if err := e.Transfer(ctx, transferTo(chainID, inv, "s", base, t0, amount, i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	got, err := h.store.Get(ctx, mid, inv.ID)
	if err != nil || got.Status != invoice.StatusDetected || got.Received.Cmp(inv.Amount) != 0 || got.Sequence != 2 {
		t.Fatalf("after transfers %+v %v", got, err)
	}
	// A redelivered block (same offset) is skipped; the next one counts a confirmation.
	if err := e.NewHead(ctx, 0, newHead(chainID, "s", base, t0)); err != nil {
		t.Fatal(err)
	}
	if err := e.NewHead(ctx, 1, newHead(chainID, "s", base+1, t0.Add(2*time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := e.NewHead(ctx, 1, newHead(chainID, "x", base+1, t0.Add(4*time.Second))); err != nil {
		t.Fatal(err)
	}
	if evs := eventTypes(outboxEvents(t, inv.ID)); evs != "invoice.created#1 invoice.detected#2 invoice.confirming#3" {
		t.Fatalf("events %q", evs)
	}
	// A reorg below the transfers reverts the invoice to NEW.
	if err := e.Reorg(ctx, 2, lifecycle.Reorg{ChainID: chainID, FromBlock: base}); err != nil {
		t.Fatal(err)
	}
	got, err = h.store.Get(ctx, mid, inv.ID)
	if err != nil || got.Status != invoice.StatusNew || got.Received.Sign() != 0 || got.Sequence != 4 {
		t.Fatalf("after reorg %+v %v", got, err)
	}
	var canonical int
	if err := pg.Pool().QueryRow(ctx, `SELECT count(*) FROM transfers WHERE invoice_id = $1 AND canonical`, inv.ID).
		Scan(&canonical); err != nil || canonical != 0 {
		t.Errorf("%d canonical transfers after the reorg, %v", canonical, err)
	}
	// The same block again, now through the replay of offset 3: the transfers count again.
	if err := e.NewHead(ctx, 3, newHead(chainID, "s", base, t0)); err != nil {
		t.Fatal(err)
	}
	got, err = h.store.Get(ctx, mid, inv.ID)
	if err != nil || got.Status != invoice.StatusDetected || got.Received.Cmp(inv.Amount) != 0 || got.Sequence != 5 {
		t.Fatalf("after re-inclusion %+v %v", got, err)
	}
	// The status page in Valkey follows every transition.
	if page, ok := h.cache.Get(ctx, inv.ID); !ok || page.Status != invoice.StatusDetected {
		t.Errorf("cached page %+v %v", page, ok)
	}

	// A transfer of 2^255 (above int64 and float64 precision) is stored and read back exactly.
	big2 := lcInvoice(t, h, mid, "big")
	huge := new(big.Int).Lsh(big.NewInt(1), 255)
	if err := e.Transfer(ctx, transferTo(chainID, big2, "s", base, t0, huge, 1)); err != nil {
		t.Fatal(err)
	}
	if err := e.NewHead(ctx, 4, newHead(chainID, "s", base+1, t0.Add(2*time.Second))); err != nil {
		t.Fatal(err)
	}
	got, err = h.store.Get(ctx, mid, big2.ID)
	if err != nil || got.Received.Cmp(huge) != 0 || got.Status != invoice.StatusConfirming {
		t.Fatalf("large transfer %+v %v", got, err)
	}
}
