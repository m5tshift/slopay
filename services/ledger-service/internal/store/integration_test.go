//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/outbox"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledger"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledgertest"
	"github.com/m5tshift/weigate/services/ledger-service/migrations"
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
		postgres.WithDatabase("ledger"), postgres.WithUsername("ledger"), postgres.WithPassword("pw"),
		postgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pc) }()
	host, err := pc.Host(ctx)
	if err != nil {
		return 1
	}
	port, err := pc.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return 1
	}
	dir, err := os.MkdirTemp("", "ledger-store")
	if err != nil {
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	pwFile := filepath.Join(dir, "password")
	if err := os.WriteFile(pwFile, []byte("pw"), 0o600); err != nil {
		return 1
	}
	pg, err = storage.OpenPostgres(ctx, storage.PostgresConfig{
		URL:          fmt.Sprintf("postgres://ledger@%s:%s/ledger?sslmode=disable", host, port.Port()),
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
	if codec, err = kafka.NewCodec(); err != nil {
		return 1
	}
	for _, topic := range []string{ledger.TopicInvoices, ledger.TopicLedger} {
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

var quiet = slog.New(slog.NewJSONHandler(io.Discard, nil))

func newService(t *testing.T) *ledger.Service {
	t.Helper()
	svc, err := ledger.NewService(New(pg.Pool(), codec), ledger.Config{FeeBps: 50}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// fixture is a fresh merchant with an ETH and a token invoice and unique block numbers, so tests sharing
// the database do not see each other.
type fixture struct {
	merchant   uuid.UUID
	eth, token ledgertest.Invoice
	base       uint64
}

func newFixture() fixture {
	m := uuid.New()
	n := rand.Uint64N(1 << 40) //nolint:gosec // test data
	return fixture{
		merchant: m,
		eth: ledgertest.Invoice{ID: uuid.New(), Merchant: m, Address: ledgertest.Address(n),
			Asset: ledger.Asset{Kind: ledger.KindETH, Decimals: 18}},
		token: ledgertest.Invoice{ID: uuid.New(), Merchant: m, Address: ledgertest.Address(n + 1),
			Asset: ledger.Asset{Kind: ledger.KindERC20, Contract: ledgertest.ERC20Contract, Decimals: 6}},
		base: n * 16,
	}
}

func (f fixture) ethTransfer(i uint64, amount int64) ledger.Transfer {
	return ledgertest.ETH(f.base+i, f.base+i, amount)
}

func (f fixture) tokenTransfer(i uint64, idx uint, amount int64) ledger.Transfer {
	return ledgertest.ERC20(f.base+i, f.base+i, idx, amount)
}

func outboxRows(t *testing.T, ctx context.Context, merchant uuid.UUID) []string {
	t.Helper()
	rows, err := pg.Pool().Query(ctx, `SELECT event_id FROM outbox WHERE key = $1 AND topic = $2 ORDER BY id`,
		merchant.String(), ledger.TopicLedger)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func countRows(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pg.Pool().QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMigrationsAreIdempotent(t *testing.T) {
	n, err := pg.Migrate(ctxT(t), migrations.FS)
	if err != nil || n != 0 {
		t.Errorf("second migration applied %d, %v", n, err)
	}
}

func TestRecordIsIdempotent(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	f := newFixture()
	a, b := f.ethTransfer(0, 6_000_000), f.ethTransfer(1, 4_000_000)
	paid := ledgertest.Event(t, codec, "invoice.paid", f.eth, 3, a, b)
	for range 3 {
		if err := svc.Apply(ctx, paid); err != nil {
			t.Fatal(err)
		}
	}
	late := ledgertest.Event(t, codec, "invoice.late_transfer", f.eth, 4, a, b, f.ethTransfer(2, 199))
	if err := svc.Apply(ctx, late); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM postings WHERE merchant_id = $1`, f.merchant); n != 3 {
		t.Errorf("%d postings, want 3", n)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM entries e JOIN postings p ON p.id = e.posting_id WHERE p.merchant_id = $1`, f.merchant); n != 12 {
		t.Errorf("%d entries, want 12", n)
	}
	ids := outboxRows(t, ctx, f.merchant)
	want := []string{"receipt:" + a.Key(), "fee:" + a.Key(), "receipt:" + b.Key(), "fee:" + b.Key()}
	if len(ids) != 6 {
		t.Fatalf("outbox %v", ids)
	}
	for i, id := range want {
		if ids[i] != id {
			t.Errorf("outbox[%d] = %s, want %s", i, ids[i], id)
		}
	}
	// Double entry over the whole journal of the merchant.
	var debits, credits string
	if err := pg.Pool().QueryRow(ctx, `SELECT
		coalesce(sum(e.amount) FILTER (WHERE side = 'D'), 0)::text, coalesce(sum(e.amount) FILTER (WHERE side = 'C'), 0)::text
		FROM entries e JOIN postings p ON p.id = e.posting_id WHERE p.merchant_id = $1`, f.merchant).Scan(&debits, &credits); err != nil {
		t.Fatal(err)
	}
	if debits != credits || debits != "10050199" { // 10 000 199 received + 50 000 fee (199 * 0.5 % rounds down to 0)
		t.Errorf("debits %s, credits %s", debits, credits)
	}
}

func TestConcurrentRepeatsPostOnce(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	f := newFixture()
	paid := ledgertest.Event(t, codec, "invoice.paid", f.token, 3, f.tokenTransfer(0, 0, 10_000_000), f.tokenTransfer(0, 1, 10_000_000))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- svc.Apply(ctx, paid)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM postings WHERE merchant_id = $1`, f.merchant); n != 2 {
		t.Errorf("%d postings, want 2", n)
	}
	if ids := outboxRows(t, ctx, f.merchant); len(ids) != 4 {
		t.Errorf("outbox %v", ids)
	}
}

func TestStatements(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	f := newFixture()
	// One event, several transfers: the postings share postedAt, so pages are ordered by ID.
	transfers := []ledger.Transfer{f.ethTransfer(3, 1_000_000), f.ethTransfer(1, 2_000_000), f.ethTransfer(2, 3_000_000), f.ethTransfer(0, 4_000_000)}
	if err := svc.Apply(ctx, ledgertest.Event(t, codec, "invoice.underpaid", f.eth, 3, transfers...)); err != nil {
		t.Fatal(err)
	}
	if err := svc.Apply(ctx, ledgertest.Event(t, codec, "invoice.paid", f.token, 3, f.tokenTransfer(8, 5, 20_000_000))); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(transfers))
	for _, tr := range transfers {
		ids = append(ids, ledger.PostingID(tr))
	}
	sort.Strings(ids)

	now := time.Now()
	from, to := now.Add(-time.Hour), now.Add(time.Hour)
	limit := 2
	var got []string
	var cursor *string
	for {
		st, err := svc.MerchantStatement(ctx, f.merchant, from, to, &limit, cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Totals) != 2 || st.Totals[0].Received.String() != "20000000" || st.Totals[0].Fee.String() != "100000" ||
			st.Totals[1].Received.String() != "10000000" || st.Totals[1].Fee.String() != "50000" || st.Totals[1].Postings != 4 {
			t.Fatalf("totals %+v", st.Totals)
		}
		for _, p := range st.Items {
			if p.MerchantID != f.merchant || p.Transfer.Amount.Sign() <= 0 || p.PostedAt.Location() != time.UTC {
				t.Errorf("posting %+v", p)
			}
			got = append(got, p.ID)
		}
		if st.Next == "" {
			break
		}
		cursor = &st.Next
	}
	if len(got) != 5 {
		t.Fatalf("pages returned %v", got)
	}
	for i, id := range ids {
		if got[i] != id {
			t.Errorf("item %d = %s, want %s", i, got[i], id)
		}
	}
	if got[4] != ledger.PostingID(f.tokenTransfer(8, 5, 0)) {
		t.Errorf("last item %s", got[4])
	}

	st, err := svc.MerchantStatement(ctx, f.merchant, to, to.Add(time.Hour), nil, nil)
	if err != nil || len(st.Items) != 0 || len(st.Totals) != 0 {
		t.Errorf("future period %+v, %v", st, err)
	}
	if _, err := svc.MerchantStatement(ctx, uuid.New(), from, to, nil, nil); !errors.Is(err, ledger.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}

	unknown := ledgertest.Address(rand.Uint64N(1 << 40)) //nolint:gosec // test data
	addrs, err := svc.AddressesStatement(ctx, []string{f.token.Address, unknown, f.eth.Address})
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 3 || addrs[0].Totals[0].Asset != f.token.Asset || len(addrs[1].Totals) != 0 ||
		addrs[2].Totals[0].Postings != 4 || addrs[2].Totals[0].Received.String() != "10000000" {
		t.Errorf("addresses %+v", addrs)
	}

	one := 3
	page, err := svc.AddressPostings(ctx, f.eth.Address, &one, nil)
	if err != nil || len(page.Items) != 3 || page.Next == "" {
		t.Fatalf("address page 1 %+v, %v", page, err)
	}
	rest, err := svc.AddressPostings(ctx, f.eth.Address, &one, &page.Next)
	if err != nil || len(rest.Items) != 1 || rest.Next != "" || rest.Items[0].ID != ids[3] {
		t.Errorf("address page 2 %+v, %v", rest, err)
	}
	tok, err := svc.AddressPostings(ctx, f.token.Address, nil, nil)
	if err != nil || len(tok.Items) != 1 || tok.Items[0].Transfer.LogIndex == nil || *tok.Items[0].Transfer.LogIndex != 5 ||
		tok.Items[0].Transfer.Asset != f.token.Asset || tok.Items[0].Fee.String() != "100000" {
		t.Errorf("token postings %+v, %v", tok, err)
	}
}

// TestSchemaEnforcesTheJournal checks the guards of the schema: stored postings never change (no reversing
// or edited postings) and a posting whose lines do not balance is rejected at commit.
func TestSchemaEnforcesTheJournal(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	f := newFixture()
	tr := f.ethTransfer(0, 1_000_000)
	if err := svc.Apply(ctx, ledgertest.Event(t, codec, "invoice.paid", f.eth, 3, tr)); err != nil {
		t.Fatal(err)
	}
	id := ledger.PostingID(tr)
	for _, stmt := range []string{
		`UPDATE postings SET amount = 1 WHERE id = $1`,
		`DELETE FROM postings WHERE id = $1`,
		`UPDATE entries SET amount = 1 WHERE posting_id = $1`,
		`DELETE FROM entries WHERE posting_id = $1`,
	} {
		if _, err := pg.Pool().Exec(ctx, stmt, id); err == nil {
			t.Errorf("%s succeeded", stmt)
		}
	}
	for _, table := range []string{"entries", "postings"} {
		if _, err := pg.Pool().Exec(ctx, `TRUNCATE `+table+` CASCADE`); err == nil {
			t.Errorf("TRUNCATE %s succeeded", table)
		}
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM entries WHERE posting_id = $1`, id); n != 4 {
		t.Errorf("%d entries left", n)
	}

	other := f.ethTransfer(1, 500)
	insert := func(tx pgx.Tx, lines [][3]string) error {
		if _, err := tx.Exec(ctx, insertPosting, ledger.PostingID(other), f.merchant, f.eth.ID, f.eth.Address,
			int64(other.BlockNumber), other.BlockHash, other.TxHash, nil, "ETH", "", 18, "500", 50, "2", time.Now()); err != nil { //nolint:gosec // small test block numbers
			return err
		}
		for i, l := range lines {
			if _, err := tx.Exec(ctx, `INSERT INTO entries (posting_id, line, account, side, amount) VALUES ($1, $2, $3, $4, $5::numeric)`,
				ledger.PostingID(other), i+1, l[0], l[1], l[2]); err != nil {
				return err
			}
		}
		return nil
	}
	for name, lines := range map[string][][3]string{
		"unbalanced":  {{"address:x", "D", "500"}, {"merchant:x:received", "C", "499"}},
		"single line": {{"address:x", "D", "0"}},
		"no lines":    nil,
	} {
		err := pgx.BeginFunc(ctx, pg.Pool(), func(tx pgx.Tx) error { return insert(tx, lines) })
		if err == nil {
			t.Errorf("%s posting committed", name)
		}
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM postings WHERE id = $1`, ledger.PostingID(other)); n != 0 {
		t.Error("a rejected posting is stored")
	}
}

func flushOutbox(t *testing.T, ctx context.Context) {
	t.Helper()
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
			return
		}
	}
}

// consume reads topic from the beginning until done returns true for an event of the merchant.
func consume(t *testing.T, ctx context.Context, topic string, merchant uuid.UUID, done func(kafka.Event) bool) {
	t.Helper()
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, err := kafka.NewConsumer(codec, nil, brokers, "g-"+uuid.NewString(), []string{topic})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	finished := false
	err = c.Run(cctx, func(_ context.Context, ev kafka.Event) error {
		if ev.Subject == merchant.String() && done(ev) {
			finished = true
			cancel()
		}
		return nil
	})
	if !finished {
		t.Fatalf("reading %s: %v", topic, err)
	}
}

// TestEventsReachKafka checks the path posting -> outbox in the same transaction -> relay -> ledger.events.
func TestEventsReachKafka(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	f := newFixture()
	tr := f.ethTransfer(0, 10_000_000_000_000_000)
	if err := svc.Apply(ctx, ledgertest.Event(t, codec, "invoice.paid", f.eth, 3, tr)); err != nil {
		t.Fatal(err)
	}
	flushOutbox(t, ctx)
	got := map[string]kafka.Event{}
	consume(t, ctx, ledger.TopicLedger, f.merchant, func(ev kafka.Event) bool {
		got[ev.Type] = ev
		return len(got) == 2
	})
	receipt, fee := got[ledger.TypeReceiptPosted], got[ledger.TypeFeeAccrued]
	if receipt.ID != "receipt:"+tr.Key() || fee.ID != "fee:"+tr.Key() || receipt.Source != ledger.Source {
		t.Errorf("events %+v", got)
	}
	var d struct {
		Amount string `json:"amount"`
		FeeBps int    `json:"feeBps"`
	}
	if err := json.Unmarshal(fee.Data, &d); err != nil || d.Amount != "50000000000000" || d.FeeBps != 50 {
		t.Errorf("fee %+v, %v", d, err)
	}
}

// TestConsumerPostsOnce runs the invoice.events consumer of the service: an invoice.paid delivered twice is
// posted once.
func TestConsumerPostsOnce(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	f := newFixture()
	tr := f.ethTransfer(0, 3_000_000)
	paid := ledgertest.Event(t, codec, "invoice.paid", f.eth, 3, tr)
	marker := ledgertest.Event(t, codec, "invoice.detected", f.eth, 4)
	p, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// Same key (the invoice), so the same partition and this order.
	for _, ev := range []kafka.Event{paid, paid, marker} {
		if err := p.Publish(ctx, ledger.TopicInvoices, ev); err != nil {
			t.Fatal(err)
		}
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, err := kafka.NewConsumer(codec, quiet, brokers, "ledger-"+uuid.NewString(), []string{ledger.TopicInvoices})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h := ledger.Handler(svc, ledger.Backoff{Min: 10 * time.Millisecond, Max: 100 * time.Millisecond}, quiet)
	seen := 0
	err = c.Run(cctx, func(ctx context.Context, ev kafka.Event) error {
		if ev.Subject != f.eth.ID.String() {
			return nil // events of other tests
		}
		if err := h(ctx, ev); err != nil {
			return err
		}
		seen++
		if ev.ID == marker.ID {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if seen != 3 {
		t.Fatalf("handled %d events, want 3", seen)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM postings WHERE merchant_id = $1`, f.merchant); n != 1 {
		t.Errorf("%d postings, want 1", n)
	}
	if ids := outboxRows(t, ctx, f.merchant); len(ids) != 2 || ids[0] != "receipt:"+tr.Key() || ids[1] != "fee:"+tr.Key() {
		t.Errorf("outbox %v", ids)
	}
}
