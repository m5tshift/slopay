//go:build integration

package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/storage"
)

var (
	brokers []string
	pg      *storage.Postgres
	codec   *kafka.Codec
)

const timeout = 90 * time.Second

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(kc) }()
	if brokers, err = kc.Brokers(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "brokers:", err)
		return 1
	}
	pc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("pw"),
		postgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pc) }()
	host, err := pc.Host(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		return 1
	}
	port, err := pc.MappedPort(ctx, "5432/tcp")
	if err != nil {
		fmt.Fprintln(os.Stderr, "port:", err)
		return 1
	}
	dir, err := os.MkdirTemp("", "outbox")
	if err != nil {
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	pwFile := filepath.Join(dir, "password")
	if err := os.WriteFile(pwFile, []byte("pw"), 0o600); err != nil {
		return 1
	}
	pg, err = storage.OpenPostgres(ctx, storage.PostgresConfig{
		URL:          fmt.Sprintf("postgres://app@%s:%s/app?sslmode=disable", host, port.Port()),
		PasswordFile: storage.SecretFile(pwFile),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open postgres:", err)
		return 1
	}
	defer pg.Close()
	if _, err := pg.Migrate(ctx, Migrations()); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	if codec, err = kafka.NewCodec(); err != nil {
		fmt.Fprintln(os.Stderr, "codec:", err)
		return 1
	}
	return m.Run()
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// setup truncates the outbox, creates a one-partition topic and returns its name.
func setup(t *testing.T) string {
	t.Helper()
	ctx := ctxT(t)
	if _, err := pg.Pool().Exec(ctx, `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	topic := "invoice.events." + uuid.NewString()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 1, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	return topic
}

func newProducer(t *testing.T) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func newRelayT(t *testing.T, pub Publisher, mut func(*Config)) *Relay {
	t.Helper()
	cfg := Config{Pool: pg.Pool(), Publisher: pub, Interval: 20 * time.Millisecond, BatchSize: 7}
	if mut != nil {
		mut(&cfg)
	}
	r, err := NewRelay(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// invoiceID returns a valid invoice UUID for n (the schemas require UUIDs).
func invoiceID(n int) string { return fmt.Sprintf("5d7c1a52-3b1e-4f0a-9a43-2f6f0c9b8e%02x", n) }

// event builds a valid invoice.created for the invoice (the Kafka key) whose amountUsdCents carries seq.
func event(t *testing.T, invoice string, seq int) kafka.Event {
	t.Helper()
	ev, err := kafka.NewEvent("/slopay/invoice-service", "invoice.created", invoice, 1, map[string]any{
		"invoiceId":      invoice,
		"merchantId":     "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b",
		"sequence":       1, // invoice.created is always sequence 1
		"status":         "NEW",
		"address":        "0x8ba1f109551bd432803012645ac136ddd64dba72",
		"asset":          map[string]any{"kind": "ETH", "decimals": 18},
		"amount":         "10000000000000000",
		"orderId":        "order-1001",
		"amountUsdCents": seq, // carries the ordering counter of the tests
		"rate":           "2500.00",
		"expiresAt":      "2026-11-15T10:15:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// addTx stores events in one committed transaction.
func addTx(t *testing.T, topic string, evs ...kafka.Event) {
	t.Helper()
	ctx := ctxT(t)
	tx, err := pg.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		if err := Add(ctx, tx, topic, ev); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// consume reads the topic from the start until stop returns true, and returns the events in order.
func consume(t *testing.T, topic string, stop func([]kafka.Event) bool) []kafka.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := kafka.NewConsumer(codec, nil, brokers, "g-"+uuid.NewString(), []string{topic})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var mu sync.Mutex
	var got []kafka.Event
	err = c.Run(ctx, func(_ context.Context, ev kafka.Event) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, ev)
		if stop(got) {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !stop(got) {
		t.Fatalf("timed out, got %d events", len(got))
	}
	return got
}

func TestRollbackDoesNotPublish(t *testing.T) {
	topic := setup(t)
	ctx := ctxT(t)
	ghost := event(t, invoiceID(90), 1)
	tx, err := pg.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Add(ctx, tx, topic, ghost); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	real := event(t, invoiceID(91), 1)
	addTx(t, topic, real)

	n, err := newRelayT(t, newProducer(t), nil).Flush(ctx)
	if err != nil || n != 1 {
		t.Fatalf("flush n=%d err=%v", n, err)
	}
	got := consume(t, topic, func(e []kafka.Event) bool { return len(e) >= 1 })
	// The ghost was added first: had it survived, it would be the first record and flush would count 2.
	if len(got) != 1 || got[0].ID != real.ID {
		t.Fatalf("only the committed event must be published: %v", got)
	}
}

type failingPublisher struct {
	inner Publisher
	left  int
}

func (f *failingPublisher) Publish(ctx context.Context, topic string, ev kafka.Event) error {
	if f.left <= 0 {
		return errors.New("simulated crash")
	}
	f.left--
	return f.inner.Publish(ctx, topic, ev)
}

func TestRestartLosesNothingAndKeepsOrder(t *testing.T) {
	topic := setup(t)
	ctx := ctxT(t)
	prod := newProducer(t)
	var want []string
	for i := 1; i <= 10; i++ {
		ev := event(t, invoiceID(1), i)
		want = append(want, ev.ID)
		addTx(t, topic, ev)
	}

	// First relay dies after three events.
	r1 := newRelayT(t, &failingPublisher{inner: prod, left: 3}, func(c *Config) { c.BatchSize = 10 })
	if n, err := r1.Flush(ctx); err == nil || n != 3 {
		t.Fatalf("first relay: n=%d err=%v", n, err)
	}
	pending, _, err := pgStore{pg.Pool()}.stats(ctx)
	if err != nil || pending != 7 {
		t.Fatalf("pending=%d err=%v", pending, err)
	}

	// A new relay (restart) publishes the rest.
	r2 := newRelayT(t, prod, func(c *Config) { c.BatchSize = 4 })
	rctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r2.Run(rctx) }()
	got := consume(t, topic, func(e []kafka.Event) bool { return len(e) >= 10 })
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i, ev := range got[:10] {
		if ev.ID != want[i] {
			t.Fatalf("position %d: got %s, want %s", i, ev.ID, want[i])
		}
	}
	if pending, _, _ = (pgStore{pg.Pool()}).stats(ctx); pending != 0 {
		t.Fatalf("pending after drain = %d", pending)
	}
}

// TestTwoRelaysKeepOrderPerKey runs two relays at once on a stream of several keys: whichever relay
// works, the events of one key reach Kafka in the order of their sequence.
func TestTwoRelaysKeepOrderPerKey(t *testing.T) {
	topic := setup(t)
	ctx := ctxT(t)
	prod := newProducer(t)
	const keys, perKey = 5, 20
	total := keys * perKey
	for s := 1; s <= perKey; s++ {
		for k := 0; k < keys; k++ {
			addTx(t, topic, event(t, invoiceID(k), s))
		}
	}
	rctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 2 {
		r := newRelayT(t, prod, func(c *Config) { c.BatchSize = 13 })
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Run(rctx) }()
	}
	got := consume(t, topic, func(e []kafka.Event) bool { return len(distinct(e)) >= total })
	stop()
	wg.Wait()

	next := map[string]int{}
	seen := map[string]bool{}
	for _, ev := range got {
		if seen[ev.ID] { // at-least-once: a repeat is allowed
			continue
		}
		seen[ev.ID] = true
		next[ev.Subject]++
		if seq := sequenceOf(t, ev); seq != next[ev.Subject] {
			t.Fatalf("key %s: sequence %d arrived as number %d", ev.Subject, seq, next[ev.Subject])
		}
	}
	if len(seen) != total {
		t.Fatalf("%d distinct events, want %d", len(seen), total)
	}
}

func distinct(evs []kafka.Event) map[string]bool {
	m := map[string]bool{}
	for _, e := range evs {
		m[e.ID] = true
	}
	return m
}

func sequenceOf(t *testing.T, ev kafka.Event) int {
	t.Helper()
	var d struct {
		Sequence int `json:"amountUsdCents"`
	}
	if err := jsonUnmarshal(ev.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d.Sequence
}

func TestStatsAndPurge(t *testing.T) {
	topic := setup(t)
	ctx := ctxT(t)
	st := pgStore{pg.Pool()}
	if p, o, err := st.stats(ctx); err != nil || p != 0 || o != 0 {
		t.Fatalf("empty: %d %v %v", p, o, err)
	}
	addTx(t, topic, event(t, invoiceID(1), 1), event(t, invoiceID(1), 2))
	time.Sleep(50 * time.Millisecond)
	p, o, err := st.stats(ctx)
	if err != nil || p != 2 || o < 50*time.Millisecond {
		t.Fatalf("pending=%d oldest=%v err=%v", p, o, err)
	}
	if n, err := newRelayT(t, newProducer(t), nil).Flush(ctx); err != nil || n != 2 {
		t.Fatalf("flush %d %v", n, err)
	}
	if p, _, _ = st.stats(ctx); p != 0 {
		t.Fatalf("pending after flush = %d", p)
	}
	if n, err := st.purge(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("fresh rows must stay: %d %v", n, err)
	}
	time.Sleep(20 * time.Millisecond)
	if n, err := st.purge(ctx, 10*time.Millisecond); err != nil || n != 2 {
		t.Fatalf("purge: %d %v", n, err)
	}
}

// TestRelayLockExcludesSecondRelay: while one relay holds the lock inside a batch, another gets nothing.
func TestRelayLockExcludesSecondRelay(t *testing.T) {
	topic := setup(t)
	ctx := ctxT(t)
	addTx(t, topic, event(t, invoiceID(1), 1))
	st := pgStore{pg.Pool()}
	in, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = st.withBatch(ctx, 10, func([]row) ([]int64, error) {
			close(in)
			<-release
			return nil, nil
		})
	}()
	<-in
	locked, err := st.withBatch(ctx, 10, func([]row) ([]int64, error) {
		t.Error("second relay must not process")
		return nil, nil
	})
	close(release)
	<-done
	if err != nil || locked {
		t.Fatalf("locked=%v err=%v", locked, err)
	}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
