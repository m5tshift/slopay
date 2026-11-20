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
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/outbox"
	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/services/merchant-service/internal/merchant"
	"github.com/m5tshift/slopay/services/merchant-service/internal/xpubtest"
	"github.com/m5tshift/slopay/services/merchant-service/migrations"
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
		return 1
	}
	port, err := pc.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return 1
	}
	dir, err := os.MkdirTemp("", "merchant-store")
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
	if _, err := pg.Migrate(ctx, migrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	if codec, err = kafka.NewCodec(); err != nil {
		return 1
	}
	if err := createTopic(ctx, merchant.Topic); err != nil {
		fmt.Fprintln(os.Stderr, "topic:", err)
		return 1
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

var cfg = merchant.Config{KeyRotationGrace: 24 * time.Hour, MaxActiveKeys: 2, DefaultToleranceBps: 100, MaxToleranceBps: 500}

func newService() (*merchant.Service, *Postgres) {
	st := New(pg.Pool(), codec)
	return merchant.NewService(st, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))), st
}

func TestMigrationsAreIdempotent(t *testing.T) {
	n, err := pg.Migrate(ctxT(t), migrations.FS)
	if err != nil || n != 0 {
		t.Errorf("second run applied %d, err %v", n, err)
	}
}

func TestMerchantLifecycle(t *testing.T) {
	ctx := ctxT(t)
	svc, _ := newService()
	m, key, err := svc.Register(ctx, "Example Shop", "owner@merchant.example.com")
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get(ctx, m.ID)
	if err != nil || got.Name != m.Name || got.Email != m.Email || got.Version != 1 || got.ToleranceBps != 100 || !got.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("get = %+v, err %v", got, err)
	}
	if id, err := svc.Authenticate(ctx, key.Key); err != nil || id != m.ID {
		t.Fatalf("authenticate: %v %v", id, err)
	}
	// The key is stored only as its hash.
	var stored []byte
	if err := pg.Pool().QueryRow(ctx, `SELECT key_hash FROM api_keys WHERE id = $1`, key.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) == key.Key || string(stored) != string(merchant.HashKey(key.Key)) {
		t.Error("key is not stored as its hash")
	}

	tol := 250
	if _, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
		t.Fatal(err)
	}
	if n := outboxCount(t, m.ID); n != 0 {
		t.Errorf("event without xpub: %d", n)
	}
	after, first, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub)
	if err != nil || first == "" || after.Version != 3 {
		t.Fatalf("set xpub: %+v %s %v", after, first, err)
	}
	// Unchanged settings: no new version, no new event.
	if again, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil || again.Version != 3 {
		t.Errorf("no-op update: %+v %v", again, err)
	}
	if n := outboxCount(t, m.ID); n != 1 {
		t.Errorf("outbox events = %d, want 1", n)
	}
	if _, err := svc.Get(ctx, uuid.New()); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestUpdateRollsBack(t *testing.T) {
	ctx := ctxT(t)
	svc, st := newService()
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	_, err = st.Update(ctx, m.ID, func(mm *merchant.Merchant) error {
		x := xpubtest.Xpub
		mm.Xpub = &x
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// An event that does not match the schema rolls the whole change back.
	_, err = st.Update(ctx, m.ID, func(mm *merchant.Merchant) error {
		x := "xpub-invalid"
		mm.Xpub = &x
		return nil
	})
	if !errors.Is(err, kafka.ErrInvalidEvent) {
		t.Fatalf("invalid event: %v", err)
	}
	got, err := svc.Get(ctx, m.ID)
	if err != nil || got.Xpub != nil || got.Version != 1 || outboxCount(t, m.ID) != 0 {
		t.Errorf("not rolled back: %+v %v", got, err)
	}
	if _, err := st.Update(ctx, uuid.New(), func(*merchant.Merchant) error { return nil }); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestConcurrentUpdatesAreSerialised(t *testing.T) {
	ctx := ctxT(t)
	svc, _ := newService()
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tol := i
			if _, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	rows, err := pg.Pool().Query(ctx, `SELECT event->'data'->>'version' FROM outbox WHERE key = $1 ORDER BY id`, m.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	// Versions in outbox order are 2, 3, ... without gaps or repeats.
	for i, v := range versions {
		if v != fmt.Sprint(i+2) {
			t.Fatalf("versions = %v", versions)
		}
	}
	if len(versions) < 2 {
		t.Errorf("versions = %v", versions)
	}
}

func TestKeyLifecycle(t *testing.T) {
	ctx := ctxT(t)
	svc, st := newService()
	m, first, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.IssueKey(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := svc.ListKeys(ctx, m.ID)
	if err != nil || len(keys) != 2 || keys[0].ID != first.ID || keys[0].ExpiresAt == nil || keys[1].ID != second.ID || keys[1].ExpiresAt != nil {
		t.Fatalf("keys = %+v, err %v", keys, err)
	}
	if _, err := svc.IssueKey(ctx, m.ID); !errors.Is(err, merchant.ErrTooManyKeys) {
		t.Errorf("third key: %v", err)
	}
	// After the grace period the old key no longer works.
	later := keys[0].ExpiresAt.Add(time.Second)
	if _, err := st.KeyOwner(ctx, merchant.HashKey(first.Key), later); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("expired key: %v", err)
	}
	if id, err := st.KeyOwner(ctx, merchant.HashKey(second.Key), later); err != nil || id != m.ID {
		t.Errorf("new key: %v %v", id, err)
	}
	// Emergency revoke of the new key leaves the old one working without an expiry.
	if err := svc.RevokeKey(ctx, m.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Authenticate(ctx, second.Key); !errors.Is(err, merchant.ErrBadAPIKey) {
		t.Errorf("revoked key: %v", err)
	}
	if id, err := st.KeyOwner(ctx, merchant.HashKey(first.Key), later); err != nil || id != m.ID {
		t.Errorf("remaining key: %v %v", id, err)
	}
	if err := svc.RevokeKey(ctx, m.ID, first.ID); !errors.Is(err, merchant.ErrLastKey) {
		t.Errorf("last key: %v", err)
	}
	if err := svc.RevokeKey(ctx, uuid.New(), first.ID); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
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

// TestEventsReachKafka checks the whole path: change, outbox in the same transaction, relay, merchant.events.
func TestEventsReachKafka(t *testing.T) {
	ctx := ctxT(t)
	svc, _ := newService()
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	tol := 0
	if _, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
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
	c, err := kafka.NewConsumer(codec, nil, brokers, "g-"+uuid.NewString(), []string{merchant.Topic})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var got []merchant.UpdatedData
	err = c.Run(cctx, func(_ context.Context, ev kafka.Event) error {
		if ev.Subject != m.ID.String() {
			return nil
		}
		if ev.Type != merchant.EventType {
			t.Errorf("type %s", ev.Type)
		}
		var d merchant.UpdatedData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return err
		}
		got = append(got, d)
		if len(got) == 2 {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Version != 2 || got[1].Version != 3 || got[1].ToleranceBps != 0 ||
		got[1].WebhookURL == nil || *got[1].WebhookURL != "https://merchant.example.com/hooks" || got[1].Xpub != xpubtest.Xpub {
		t.Errorf("events = %+v", got)
	}
}
