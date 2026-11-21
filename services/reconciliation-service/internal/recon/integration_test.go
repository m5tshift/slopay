//go:build integration

package recon_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/events"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/invoicetest"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/ledger"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/node"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/projection"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/recon"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/report"
)

const (
	anvilImage    = "ghcr.io/foundry-rs/foundry:v1.10.0"
	anvilChainID  = 31337
	confirmations = 7
	waitFor       = 60 * time.Second
	// payer is the first unlocked account of Anvil.
	payer = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
)

var (
	brokers  []string
	mongoURI string
	anvilURL string
)

func TestMain(m *testing.M) {
	code := 1
	defer func() { os.Exit(code) }()
	ctx := context.Background()
	var ctrs []testcontainers.Container
	defer func() {
		for _, c := range ctrs {
			_ = testcontainers.TerminateContainer(c)
		}
	}()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	ctrs = append(ctrs, kc)
	if err == nil {
		brokers, err = kc.Brokers(ctx)
	}
	var mc *mongodb.MongoDBContainer
	if err == nil {
		mc, err = mongodb.Run(ctx, "mongo:7", mongodb.WithUsername("app"), mongodb.WithPassword("pw"))
		ctrs = append(ctrs, mc)
	}
	if err == nil {
		mongoURI, err = mc.PortEndpoint(ctx, "27017/tcp", "mongodb")
	}
	var anvil testcontainers.Container
	if err == nil {
		anvil, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        anvilImage,
				Entrypoint:   []string{"/bin/sh", "-c"},
				Cmd:          []string{"exec anvil --host 0.0.0.0"},
				ExposedPorts: []string{"8545/tcp"},
				WaitingFor:   wait.ForLog("Listening on").WithStartupTimeout(waitFor),
			},
			Started: true,
		})
		ctrs = append(ctrs, anvil)
	}
	if err == nil {
		anvilURL, err = anvil.PortEndpoint(ctx, "8545/tcp", "http")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "containers:", err)
		return
	}
	code = m.Run()
}

func rpcCall(t *testing.T, result any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := rpc.DialContext(ctx, anvilURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CallContext(ctx, result, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func newAddress(t *testing.T) string {
	t.Helper()
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "0x" + hex.EncodeToString(b)
}

// sendETH transfers wei to the address and returns the transfer as an invoice event accounts it.
func sendETH(t *testing.T, to string, wei int64) invoicetest.Transfer {
	t.Helper()
	var tx string
	rpcCall(t, &tx, "eth_sendTransaction", map[string]any{"from": payer, "to": to, "value": (*hexutil.Big)(big.NewInt(wei))})
	var out invoicetest.Transfer
	eventually(t, "receipt of "+tx, func() bool {
		var r *struct {
			BlockNumber hexutil.Uint64 `json:"blockNumber"`
			BlockHash   string         `json:"blockHash"`
		}
		rpcCall(t, &r, "eth_getTransactionReceipt", tx)
		if r == nil {
			return false
		}
		out = invoicetest.Transfer{BlockNumber: uint64(r.BlockNumber), BlockHash: strings.ToLower(r.BlockHash),
			TxHash: strings.ToLower(tx), Amount: big.NewInt(wei).String()}
		return true
	})
	return out
}

func createTopic(t *testing.T, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 1, 1
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			t.Fatalf("create topic %s: %v", tr.Topic, err)
		}
	}
}

// fakeLedger serves the address statement API of ledger-service from the postings it holds.
type fakeLedger struct {
	mu       sync.Mutex
	postings map[string][]invoicetest.Transfer // by address
	merchant string
}

func (l *fakeLedger) posting(addr string, t invoicetest.Transfer) map[string]any {
	return map[string]any{
		"postingId": "receipt:" + t.BlockHash + ":" + t.TxHash + ":", "merchantId": l.merchant,
		"invoiceId": uuid.NewString(), "address": addr,
		"transfer": map[string]any{"blockNumber": t.BlockNumber, "blockHash": t.BlockHash, "txHash": t.TxHash,
			"logIndex": nil, "amount": t.Amount, "asset": invoicetest.ETH},
		"debitAccount": "address:" + addr, "creditAccount": "merchant:" + l.merchant + ":received",
		"fee": "0", "postedAt": time.Now().UTC().Format(time.RFC3339),
	}
}

func (l *fakeLedger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/internal/addresses/statement" {
		items := []map[string]any{}
		for _, a := range r.URL.Query()["address"] {
			totals := []map[string]any{}
			if ps := l.postings[a]; len(ps) > 0 {
				sum := new(big.Int)
				for _, p := range ps {
					n, _ := new(big.Int).SetString(p.Amount, 10)
					sum.Add(sum, n)
				}
				totals = append(totals, map[string]any{"asset": invoicetest.ETH, "received": sum.String(), "fee": "0", "postings": len(ps)})
			}
			items = append(items, map[string]any{"address": a, "totals": totals})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
		return
	}
	addr := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/internal/addresses/"), "/postings")
	items := []map[string]any{}
	for _, p := range l.postings[addr] {
		items = append(items, l.posting(addr, p))
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"address": addr, "items": items, "nextCursor": nil})
}

type collector struct {
	mu     sync.Mutex
	events []kafka.Event
}

func collect(t *testing.T, ctx context.Context, codec *kafka.Codec) *collector {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(events.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	go func() {
		defer cl.Close()
		for ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			fs.EachRecord(func(r *kgo.Record) {
				ev, err := codec.Decode(r.Value)
				if err != nil {
					t.Errorf("reconciliation.events record does not match the contract: %v", err)
					return
				}
				if string(r.Key) != ev.Subject {
					t.Errorf("key %s, subject %s", r.Key, ev.Subject)
				}
				c.mu.Lock()
				c.events = append(c.events, ev)
				c.mu.Unlock()
			})
		}
	}()
	return c
}

func (c *collector) of(runID string) []kafka.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []kafka.Event
	for _, ev := range c.events {
		if ev.Subject == runID {
			out = append(out, ev)
		}
	}
	return out
}

func gauge(t *testing.T, reader *sdkmetric.ManualReader, name string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok && m.Name == name && len(g.DataPoints) == 1 {
				return g.DataPoints[0].Value, true
			}
		}
	}
	return 0, false
}

// TestReconciliation runs the service parts against Anvil, Kafka, MongoDB and a ledger-service stub: a posting
// deleted from the ledger, an unaccounted transfer and a transfer after the tracking window are reported.
func TestReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	createTopic(t, projection.TopicInvoices)
	createTopic(t, events.Topic)
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	pw := t.TempDir() + "/password"
	if err := os.WriteFile(pw, []byte("pw"), 0o600); err != nil {
		t.Fatal(err)
	}
	mongo, err := storage.OpenMongo(ctx, storage.MongoConfig{URI: mongoURI, Username: "app", PasswordFile: storage.SecretFile(pw),
		Database: "reconciliation", AuthSource: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mongo.Close(context.Background()) }()

	merchant := uuid.NewString()
	addrA, addrB, addrC, addrD := newAddress(t), newAddress(t), newAddress(t), newAddress(t)
	// A: paid by two transfers; the ledger lost the posting of the second one.
	a1, a2 := sendETH(t, addrA, 1e18), sendETH(t, addrA, 5e17)
	// B: paid, everything agrees.
	b1 := sendETH(t, addrB, 2e18)
	// C: paid, then a transfer within the tracking window that no invoice accounted.
	c1 := sendETH(t, addrC, 3e17)
	sendETH(t, addrC, 1e17)
	// D: an invoice three days old, whose window has ended, and a transfer after it.
	d1 := sendETH(t, addrD, 4e17)
	sendETH(t, addrD, 25e15)
	rpcCall(t, nil, "anvil_mine", hexutil.Uint64(confirmations+1))

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-72 * time.Hour)
	reverted := invoicetest.Transfer{BlockNumber: 5, BlockHash: "0x" + strings.Repeat("ee", 32),
		TxHash: "0x" + strings.Repeat("ff", 32), Amount: "1"}
	ids := map[string]string{addrA: uuid.NewString(), addrB: uuid.NewString(), addrC: uuid.NewString(), addrD: uuid.NewString()}
	invoiceEvents := []kafka.Event{
		invoicetest.Created(ids[addrA], merchant, addrA, "1500000000000000000", now.Add(-time.Hour), now.Add(time.Hour)),
		invoicetest.Paid(ids[addrA], merchant, addrA, "1500000000000000000", 4, now, a1, a2),
		invoicetest.Created(ids[addrB], merchant, addrB, "2000000000000000000", now.Add(-time.Hour), now.Add(time.Hour)),
		invoicetest.Paid(ids[addrB], merchant, addrB, "2000000000000000000", 4, now, b1),
		invoicetest.Created(ids[addrC], merchant, addrC, "300000000000000000", now.Add(-time.Hour), now.Add(time.Hour)),
		invoicetest.Paid(ids[addrC], merchant, addrC, "300000000000000000", 4, now, c1),
		// A transfer accounted and then reverted by a reorg is removed from the projection.
		invoicetest.LateTransfer(ids[addrB], merchant, addrB, "2000000000000000000", 5, now, reverted),
		invoicetest.Reverted(ids[addrB], merchant, addrB, "2000000000000000000", 6, now, reverted),
		invoicetest.Created(ids[addrD], merchant, addrD, "400000000000000000", old, old.Add(time.Hour)),
		invoicetest.Paid(ids[addrD], merchant, addrD, "400000000000000000", 4, old.Add(10*time.Minute), d1),
	}
	for _, ev := range invoiceEvents {
		if err := producer.Publish(ctx, projection.TopicInvoices, ev); err != nil {
			t.Fatal(err)
		}
	}

	// The projection, as every replica keeps it.
	invoices := projection.New(mongo, 192*time.Hour)
	if err := invoices.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	consumer, err := kafka.NewConsumer(codec, nil, brokers, "reconciliation-service", []string{projection.TopicInvoices},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	go func() { _ = consumer.Run(ctx, projection.Handler(invoices, projection.DefaultBackoff, nil)) }()
	// The events share one partition, so the invoice of D is projected last.
	eventually(t, "projection of the invoices", func() bool {
		got, err := invoices.Invoices(ctx, now.Add(-7*24*time.Hour))
		if err != nil || len(got) != 4 {
			return false
		}
		for _, inv := range got {
			if len(inv.Transfers) == 0 {
				return false
			}
		}
		return true
	})

	stub := &fakeLedger{merchant: merchant, postings: map[string][]invoicetest.Transfer{
		addrA: {a1}, addrB: {b1}, addrC: {c1}, addrD: {d1},
	}}
	ledgerSrv := httptest.NewServer(stub)
	defer ledgerSrv.Close()
	ledgerClient, err := ledger.New(ledgerSrv.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client, err := node.Dial(ctx, anvilURL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	br, err := breaker.New(breaker.Config{Dependency: "node", FailureThreshold: 5, OpenTimeout: 15 * time.Second, HalfOpenMax: 1})
	if err != nil {
		t.Fatal(err)
	}
	reports := report.New(mongo, 720*time.Hour)
	if err := reports.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	rec, err := recon.New(recon.Config{
		Node: client, Breaker: br, Ledger: ledgerClient, Projection: invoices, Reports: reports,
		Publisher: events.NewPublisher(producer), ChainID: anvilChainID, Confirmations: confirmations,
		Lookback: 7 * 24 * time.Hour, TrackingWindow: 24 * time.Hour, BatchSize: 2,
		Interval: time.Second, RunTimeout: 30 * time.Second,
		Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, ctx, codec)

	// The leader runs on schedule; the alert gauge is reported while it leads.
	sctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { rec.Schedule(sctx); close(done) }()
	var last *recon.Report
	eventually(t, "a published report and weigate_reconciliation_mismatches = 2", func() bool {
		v, ok := gauge(t, reader, "weigate_reconciliation_mismatches")
		r, err := reports.Latest(ctx)
		if err != nil || r == nil || !r.Published || !ok || v != 2 {
			return false
		}
		last = r
		return true
	})
	stop()
	<-done
	if _, ok := gauge(t, reader, "weigate_reconciliation_mismatches"); ok {
		t.Error("the gauge is reported after the leadership ended")
	}
	if last.Mismatches != 2 || last.AfterWindow != 1 || last.AddressesChecked != 4 || last.ChainID != anvilChainID {
		t.Fatalf("report %+v", last)
	}
	rows := map[string]recon.Row{}
	for _, r := range last.Rows {
		rows[r.Address] = r
	}
	if r := rows[addrA]; r.Kind != recon.KindInvoicesVsLedger || r.ChainBalance.String() != "1500000000000000000" ||
		r.InvoicesTotal.String() != "1500000000000000000" || r.LedgerTotal.String() != "1000000000000000000" {
		t.Errorf("deleted posting: %+v", r)
	}
	if _, ok := rows[addrB]; ok {
		t.Errorf("B agrees, got %+v", rows[addrB])
	}
	if r := rows[addrC]; r.Kind != recon.KindChainVsInvoices || r.ChainBalance.String() != "400000000000000000" {
		t.Errorf("unaccounted transfer: %+v", r)
	}
	if r := rows[addrD]; r.Kind != recon.KindAfterWindow || r.Excess.String() != "25000000000000000" {
		t.Errorf("after window: %+v", r)
	}

	eventually(t, "events of run "+last.RunID, func() bool { return len(got.of(last.RunID)) == 3 })
	evs := got.of(last.RunID)
	ids2 := map[string]bool{}
	for _, ev := range evs {
		ids2[ev.ID] = true
	}
	for _, want := range []string{last.RunID + ":" + addrA + ":ETH", last.RunID + ":" + addrC + ":ETH", last.RunID + ":completed"} {
		if !ids2[want] {
			t.Errorf("event %s not published; got %v", want, ids2)
		}
	}
	if evs[2].Type != events.TypeCompleted {
		t.Errorf("completed is not the last event of the run: %s", evs[2].Type)
	}

	// ledger-service unavailable: the run is postponed and stores nothing.
	ledgerSrv.Close()
	result, rp, err := rec.RunOnce(ctx)
	if result != recon.ResultPostponed || !errors.Is(err, recon.ErrLedger) || rp != nil {
		t.Fatalf("ledger down: result %s, err %v", result, err)
	}
	before, _ := reports.Latest(ctx)
	if _, _, err := rec.RunOnce(ctx); !errors.Is(err, recon.ErrLedger) {
		t.Fatalf("ledger down again: %v", err)
	}
	if after, _ := reports.Latest(ctx); after.RunID != before.RunID {
		t.Errorf("a postponed run stored report %s", after.RunID)
	}
}
