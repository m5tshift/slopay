//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/outbox"
	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/services/invoice-service/internal/cache"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoicetest"
	"github.com/m5tshift/slopay/services/invoice-service/internal/projection"
	"github.com/m5tshift/slopay/services/invoice-service/migrations"
)

var (
	brokers []string
	pg      *storage.Postgres
	vk      *storage.Valkey
	codec   *kafka.Codec
)

const timeout = 90 * time.Second

var quiet = slog.New(slog.NewJSONHandler(io.Discard, nil))

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
	vc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8.0",
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          []string{"valkey-server", "--requirepass", "vpw"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "start valkey:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(vc) }()

	dir, err := os.MkdirTemp("", "invoice-store")
	if err != nil {
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	pwFile, vpwFile := filepath.Join(dir, "password"), filepath.Join(dir, "valkey-password")
	if os.WriteFile(pwFile, []byte("pw"), 0o600) != nil || os.WriteFile(vpwFile, []byte("vpw"), 0o600) != nil {
		return 1
	}

	host, err := pc.Host(ctx)
	if err != nil {
		return 1
	}
	port, err := pc.MappedPort(ctx, "5432/tcp")
	if err != nil {
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
	if _, err := pg.Migrate(ctx, migrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}

	addr, err := vc.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		return 1
	}
	vk, err = storage.OpenValkey(ctx, storage.ValkeyConfig{Addrs: []string{addr}, PasswordFile: storage.SecretFile(vpwFile)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open valkey:", err)
		return 1
	}
	defer vk.Close()

	if codec, err = kafka.NewCodec(); err != nil {
		return 1
	}
	for _, topic := range []string{invoice.TopicInvoices, invoice.TopicMerchants} {
		if err := createTopic(ctx, topic); err != nil {
			fmt.Fprintln(os.Stderr, "topic:", err)
			return 1
		}
	}
	return m.Run()
}

func createTopic(ctx context.Context, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 3, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil {
			return err
		}
	}
	return nil
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

type harness struct {
	svc   *invoice.Service
	store *Postgres
	cache *cache.Valkey
	rates *invoicetest.Rates
}

// newHarness wires the service to PostgreSQL and Valkey and registers a merchant with an xpub.
func newHarness(t *testing.T) (*harness, uuid.UUID) {
	t.Helper()
	st := New(pg.Pool(), codec)
	c := cache.New(30*time.Second, time.Second, quiet)
	c.Attach(vk.Client())
	rates := invoicetest.NewRates("3694.25", time.Now())
	svc, err := invoice.NewService(st, rates, c, invoice.Config{InvoiceTTL: 15 * time.Minute, RateMaxAge: time.Minute}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if ok, err := st.UpsertMerchant(ctxT(t), invoice.Merchant{ID: id, Version: 1, Xpub: invoicetest.Xpub, ToleranceBps: 100}); err != nil || !ok {
		t.Fatalf("upsert: %v %v", ok, err)
	}
	return &harness{svc: svc, store: st, cache: c, rates: rates}, id
}

func req(cents int64, order string) invoice.CreateRequest {
	return invoice.CreateRequest{AmountUSDCents: cents, OrderID: order}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	n, err := pg.Migrate(ctxT(t), migrations.FS)
	if err != nil || n != 0 {
		t.Errorf("second run applied %d, err %v", n, err)
	}
}

func outboxCount(t *testing.T, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := pg.Pool().QueryRow(ctxT(t), `SELECT count(*) FROM outbox WHERE key = $1`, id.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCreateAndRead(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	inv, created, err := h.svc.Create(ctx, mid, "k1", req(1999, "order-1"))
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	got, err := h.svc.Get(ctx, mid, inv.ID)
	if err != nil {
		t.Fatal(err)
	}
	// NUMERIC(78,0) round trip without loss; times with microseconds.
	if got.Amount.String() != "5411111863030386" || got.Received.Sign() != 0 || got.Rate != "3694.25" ||
		got.Address != inv.Address || got.DerivationIndex != 0 || !got.CreatedAt.Equal(inv.CreatedAt) ||
		!got.ExpiresAt.Equal(inv.CreatedAt.Add(15*time.Minute)) || got.Status != invoice.StatusNew || got.Sequence != 1 {
		t.Errorf("stored %+v", got)
	}
	if _, err := h.svc.Get(ctx, uuid.New(), inv.ID); !errors.Is(err, invoice.ErrNotFound) {
		t.Errorf("other merchant: %v", err)
	}
	if outboxCount(t, inv.ID) != 1 {
		t.Error("invoice.created is not in the outbox")
	}
	var amount string
	if err := pg.Pool().QueryRow(ctx, `SELECT pg_typeof(amount)::text || ':' || format_type(atttypid, atttypmod)
		FROM invoices, pg_attribute WHERE attrelid = 'invoices'::regclass AND attname = 'amount' AND id = $1`, inv.ID).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	if amount != "numeric:numeric(78,0)" {
		t.Errorf("amount column %s", amount)
	}
}

// TestIdempotencyWithoutValkey is NFR-06: the repeat is answered from PostgreSQL after Valkey lost its data.
func TestIdempotencyWithoutValkey(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	first, created, err := h.svc.Create(ctx, mid, "k1", req(500, "order-1"))
	if err != nil || !created {
		t.Fatal(err)
	}
	if _, ok := h.cache.Get(ctx, first.ID); !ok {
		t.Fatal("status is not cached")
	}
	if err := vk.Client().Do(ctx, vk.Client().B().Flushall().Build()).Error(); err != nil {
		t.Fatal(err)
	}
	h.rates.Set(invoice.Quote{}, errors.New("rate-service is down"))
	again, created, err := h.svc.Create(ctx, mid, "k1", req(500, "order-1"))
	if err != nil || created || again.ID != first.ID || again.Address != first.Address || again.Amount.Cmp(first.Amount) != 0 {
		t.Fatalf("repeat: %+v %v %v", again, created, err)
	}
	if _, _, err := h.svc.Create(ctx, mid, "k1", req(501, "order-1")); !errors.Is(err, invoice.ErrKeyReuse) {
		t.Errorf("other body: %v", err)
	}
	if outboxCount(t, first.ID) != 1 {
		t.Error("repeat wrote another event")
	}
	// The status page is read from PostgreSQL on a miss and cached again.
	page, err := h.svc.PaymentStatus(ctx, first.ID)
	if err != nil || page.InvoiceID != first.ID || page.Status != invoice.StatusNew {
		t.Fatalf("status: %+v %v", page, err)
	}
	cached, ok := h.cache.Get(ctx, first.ID)
	if !ok || cached != page {
		t.Errorf("not cached again: %+v %v", cached, ok)
	}
	ttl, err := vk.Client().Do(ctx, vk.Client().B().Pttl().Key(cache.Key(first.ID)).Build()).AsInt64()
	if err != nil || ttl <= 0 || ttl > 30_000 {
		t.Errorf("ttl %d %v", ttl, err)
	}
}

// TestConcurrentSameKey runs many creates with one key against PostgreSQL: one invoice, one event.
func TestConcurrentSameKey(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	const n = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[uuid.UUID]bool{}
		created int
	)
	for range n {
		wg.Go(func() {
			inv, c, err := h.svc.Create(ctx, mid, "same", req(700, "order-1"))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[inv.ID] = true
			if c {
				created++
			}
		})
	}
	wg.Wait()
	var rows int
	if err := pg.Pool().QueryRow(ctx, `SELECT count(*) FROM invoices WHERE merchant_id = $1`, mid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || created != 1 || rows != 1 {
		t.Errorf("ids %d, created %d, rows %d", len(ids), created, rows)
	}
}

// TestDerivationIndexes checks the per-merchant counter: distinct indexes under concurrency and the unique index.
func TestDerivationIndexes(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	const n = 12
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, _, err := h.svc.Create(ctx, mid, fmt.Sprintf("k%d", i), req(100, "o")); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	rows, err := pg.Pool().Query(ctx, `SELECT derivation_index, address FROM invoices WHERE merchant_id = $1`, mid)
	if err != nil {
		t.Fatal(err)
	}
	indexes, addrs := map[int64]bool{}, map[string]bool{}
	for rows.Next() {
		var idx int64
		var addr string
		if err := rows.Scan(&idx, &addr); err != nil {
			t.Fatal(err)
		}
		indexes[idx], addrs[addr] = true, true
	}
	if rows.Err() != nil || len(indexes) != n || len(addrs) != n {
		t.Fatalf("indexes %v, %d addresses", indexes, len(addrs))
	}
	for i := range int64(n) {
		if !indexes[i] {
			t.Errorf("index %d missing", i)
		}
	}
	var next int64
	if err := pg.Pool().QueryRow(ctx, `SELECT next_derivation_index FROM merchants WHERE id = $1`, mid).Scan(&next); err != nil || next != n {
		t.Errorf("counter %d %v", next, err)
	}
	// The unique index is the last guard against a reused index.
	_, err = pg.Pool().Exec(ctx, `INSERT INTO invoices (id, merchant_id, idempotency_key, request_hash, order_id, amount_usd_cents,
		asset_kind, asset_decimals, rate, rate_quoted_at, amount, address, derivation_index, status, sequence,
		created_at, expires_at, updated_at)
		SELECT gen_random_uuid(), merchant_id, 'dup', request_hash, order_id, amount_usd_cents, asset_kind, asset_decimals,
		rate, rate_quoted_at, amount, address, derivation_index, status, sequence, created_at, expires_at, updated_at
		FROM invoices WHERE merchant_id = $1 LIMIT 1`, mid)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "invoices_derivation_idx" {
		t.Errorf("duplicate derivation index: %v", err)
	}
}

func TestCreateWithoutXpub(t *testing.T) {
	h, _ := newHarness(t)
	if _, _, err := h.svc.Create(ctxT(t), uuid.New(), "k", req(100, "o")); !errors.Is(err, invoice.ErrNoXpub) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestListOnPostgres(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	var invs []invoice.Invoice
	for i := range 7 {
		inv, _, err := h.svc.Create(ctx, mid, fmt.Sprintf("k%d", i), req(100, fmt.Sprintf("order-%d", i%2)))
		if err != nil {
			t.Fatal(err)
		}
		invs = append(invs, inv)
	}
	if _, err := pg.Pool().Exec(ctx, `UPDATE invoices SET status = 'PAID' WHERE id = $1`, invs[2].ID); err != nil {
		t.Fatal(err)
	}
	seen := map[uuid.UUID]bool{}
	cursor := ""
	var prev *invoice.Invoice
	for {
		items, next, err := h.svc.List(ctx, mid, invoice.Filter{Limit: 3}, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for i := range items {
			if seen[items[i].ID] {
				t.Errorf("%s twice", items[i].ID)
			}
			seen[items[i].ID] = true
			if prev != nil && !(invoice.Cursor{CreatedAt: prev.CreatedAt, ID: prev.ID}).Before(items[i]) {
				t.Errorf("order broken at %s", items[i].ID)
			}
			prev = &items[i]
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 7 {
		t.Errorf("%d invoices listed", len(seen))
	}
	from := invs[0].CreatedAt
	cases := map[string]struct {
		f    invoice.Filter
		want int
	}{
		"status":   {invoice.Filter{Statuses: []invoice.Status{invoice.StatusPaid}}, 1},
		"statuses": {invoice.Filter{Statuses: []invoice.Status{invoice.StatusPaid, invoice.StatusNew}}, 7},
		"order":    {invoice.Filter{OrderID: "order-1"}, 3},
		"from":     {invoice.Filter{CreatedFrom: from}, 7},
		"to":       {invoice.Filter{CreatedTo: from}, 0},
		"combined": {invoice.Filter{OrderID: "order-0", Statuses: []invoice.Status{invoice.StatusPaid}, CreatedFrom: from}, 1},
	}
	for name, tc := range cases {
		tc.f.Limit = 100
		items, _, err := h.svc.List(ctx, mid, tc.f, "")
		if err != nil || len(items) != tc.want {
			t.Errorf("%s: %d, want %d (%v)", name, len(items), tc.want, err)
		}
	}
	if _, err := h.svc.PaymentPage(ctx, uuid.New()); !errors.Is(err, invoice.ErrNotFound) {
		t.Errorf("unknown page: %v", err)
	}
}

// TestOutboxReachesKafka checks invoice.created: the outbox row of the create is published to invoice.events.
func TestOutboxReachesKafka(t *testing.T) {
	ctx := ctxT(t)
	h, mid := newHarness(t)
	inv, _, err := h.svc.Create(ctx, mid, "k1", req(1999, "order-1"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r, err := outbox.NewRelay(outbox.Config{Pool: pg.Pool(), Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	for {
		n, err := r.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, err := kafka.NewConsumer(codec, nil, brokers, "g-"+uuid.NewString(), []string{invoice.TopicInvoices})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var got *kafka.Event
	if err := c.Run(cctx, func(_ context.Context, ev kafka.Event) error {
		if ev.Subject == inv.ID.String() {
			got = &ev
			cancel()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Type != invoice.EventCreated || got.Source != invoice.EventSource {
		t.Fatalf("event %+v", got)
	}
	var d map[string]any
	if err := json.Unmarshal(got.Data, &d); err != nil {
		t.Fatal(err)
	}
	if d["invoiceId"] != inv.ID.String() || d["merchantId"] != mid.String() || d["amount"] != "5411111863030386" ||
		d["address"] != inv.Address || d["sequence"] != float64(1) {
		t.Errorf("data %v", d)
	}
}

func merchantEvent(t *testing.T, id uuid.UUID, version int64, xpub string, tol int) kafka.Event {
	t.Helper()
	ev, err := kafka.NewEvent("/slopay/merchant-service", invoice.EventMerchantUpd, id.String(), 1, map[string]any{
		"merchantId": id, "version": version, "xpub": xpub, "webhookUrl": nil, "toleranceBps": tol, "webhookSecretVersion": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// TestMerchantProjection reads merchant.events into the projection: newer versions win, the counter stays.
func TestMerchantProjection(t *testing.T) {
	ctx := ctxT(t)
	h, _ := newHarness(t)
	id := uuid.New()
	p, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(ctx, invoice.TopicMerchants, merchantEvent(t, id, 2, invoicetest.Xpub, 150)); err != nil {
		t.Fatal(err)
	}
	c, err := projection.NewConsumer(codec, h.svc, quiet, brokers, "g-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(cctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	m := waitMerchant(t, id, func(m invoice.Merchant) bool { return m.Version == 2 })
	if m.Xpub != invoicetest.Xpub || m.ToleranceBps != 150 {
		t.Fatalf("projection %+v", m)
	}
	// The merchant can now create invoices; the counter is advanced.
	if _, _, err := h.svc.Create(ctx, id, "k", req(100, "o")); err != nil {
		t.Fatal(err)
	}
	// An older version and then a newer one: only the newer one is applied, the counter is kept.
	for _, ev := range []kafka.Event{merchantEvent(t, id, 1, invoicetest.Xpub, 10), merchantEvent(t, id, 3, invoicetest.Xpub, 200)} {
		if err := p.Publish(ctx, invoice.TopicMerchants, ev); err != nil {
			t.Fatal(err)
		}
	}
	m = waitMerchant(t, id, func(m invoice.Merchant) bool { return m.Version == 3 })
	if m.ToleranceBps != 200 {
		t.Errorf("projection %+v", m)
	}
	var next int64
	if err := pg.Pool().QueryRow(ctx, `SELECT next_derivation_index FROM merchants WHERE id = $1`, id).Scan(&next); err != nil || next != 1 {
		t.Errorf("counter %d %v", next, err)
	}
	if ok, err := h.store.UpsertMerchant(ctx, invoice.Merchant{ID: id, Version: 2, Xpub: "x", ToleranceBps: 1}); err != nil || ok {
		t.Errorf("older version applied: %v %v", ok, err)
	}
}

func waitMerchant(t *testing.T, id uuid.UUID, ok func(invoice.Merchant) bool) invoice.Merchant {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var m invoice.Merchant
		err := pg.Pool().QueryRow(ctxT(t), `SELECT id, version, xpub, tolerance_bps FROM merchants WHERE id = $1`, id).
			Scan(&m.ID, &m.Version, &m.Xpub, &m.ToleranceBps)
		if err == nil && ok(m) {
			return m
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("merchant %s was not projected", id)
	return invoice.Merchant{}
}
