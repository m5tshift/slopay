//go:build integration

package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/webhook-service/internal/consume"
	"github.com/m5tshift/weigate/services/webhook-service/internal/dedup"
	"github.com/m5tshift/weigate/services/webhook-service/internal/journal"
	"github.com/m5tshift/weigate/services/webhook-service/internal/merchants"
	"github.com/m5tshift/weigate/services/webhook-service/internal/replay"
	"github.com/m5tshift/weigate/services/webhook-service/internal/secrets"
)

const (
	waitFor   = 2 * time.Minute
	baoToken  = "dev-root-token" // #nosec G101 -- the root token of the throwaway dev-mode container
	mongoPass = "pw"
)

// stack is the service wired as in cmd/webhook-service against Kafka, MongoDB, Valkey and OpenBao in containers.
// The retry stages do not wait, so a failing endpoint reaches the DLQ within seconds.
type stack struct {
	t         *testing.T
	brokers   []string
	baoAddr   string
	producer  *kafka.Producer
	mongo     *storage.Mongo
	merchant  *merchants.Store
	codec     *kafka.Codec
	deliverer *Deliverer
}

func start(t *testing.T, ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) testcontainers.Container {
	t.Helper()
	c, err := testcontainers.Run(ctx, image, opts...)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func endpointOf(t *testing.T, ctx context.Context, c testcontainers.Container, port, proto string) string {
	t.Helper()
	ep, err := c.PortEndpoint(ctx, port, proto)
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

func newStack(t *testing.T) *stack {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &stack{t: t}

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	if err != nil {
		t.Fatal(err)
	}
	if s.brokers, err = kc.Brokers(ctx); err != nil {
		t.Fatal(err)
	}
	mc, err := mongodb.Run(ctx, "mongo:7", mongodb.WithUsername("app"), mongodb.WithPassword(mongoPass))
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(mc) })
	if err != nil {
		t.Fatal(err)
	}
	vc := start(t, ctx, "valkey/valkey:8.0", testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections").WithStartupTimeout(waitFor)))
	bc := start(t, ctx, "openbao/openbao:2.1.0", testcontainers.WithExposedPorts("8200/tcp"),
		testcontainers.WithCmd("server", "-dev", "-dev-root-token-id="+baoToken, "-dev-listen-address=0.0.0.0:8200"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/v1/sys/health").WithPort("8200/tcp").WithStartupTimeout(waitFor)))
	s.baoAddr = endpointOf(t, ctx, bc, "8200/tcp", "http")

	dir := t.TempDir()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	s.createTopics()
	if s.codec, err = kafka.NewCodec(); err != nil {
		t.Fatal(err)
	}
	if s.producer, err = kafka.NewProducer(s.codec, s.brokers); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.producer.Close)

	mongo, err := storage.OpenMongo(ctx, storage.MongoConfig{URI: endpointOf(t, ctx, mc, "27017/tcp", "mongodb"),
		Username: "app", PasswordFile: storage.SecretFile(write("mongo", mongoPass)), Database: "webhook_service"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mongo.Close(context.Background()) })
	s.mongo = mongo
	deliveries := journal.New(mongo, 720*time.Hour)
	if err := deliveries.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	projection := merchants.New(mongo)
	s.merchant = projection
	bao, err := secrets.New(secrets.Config{Addr: s.baoAddr, TokenFile: write("token", baoToken), Mount: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(Config{
		Merchants: projection, Secrets: bao, Journal: deliveries, Publisher: s.producer,
		HTTP: NewHTTPClient(2 * time.Second), Breaker: breaker.Config{FailureThreshold: 5, OpenTimeout: 30 * time.Second, HalfOpenMax: 1},
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	s.deliverer = d
	v, err := storage.OpenValkey(ctx, storage.ValkeyConfig{Addrs: []string{endpointOf(t, ctx, vc, "6379/tcp", "")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Close)
	dd := dedup.New(time.Second, nil)
	dd.Attach(storage.NewValkeyDedup(v, "webhook-service:dedup:", 24*time.Hour))

	handlers := map[string]kafka.Handler{
		merchants.Topic: merchants.Handler(projection),
		TopicInvoices:   dd.Wrap(d.Invoice, consume.RetryInitial, consume.RetryMax),
	}
	for _, st := range Stages {
		handlers[st.Topic] = dd.Wrap(d.Retry, consume.RetryInitial, consume.RetryMax)
	}
	for topic, h := range handlers {
		loop, err := consume.New(s.codec, nil, s.brokers, "webhook-service-"+topic, []string{topic}, 16)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(loop.Close)
		go func() { _ = loop.Run(ctx, h) }()
	}
	return s
}

func (s *stack) createTopics() {
	t := s.t
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	topics := []string{merchants.Topic, TopicInvoices, TopicDLQ}
	for _, st := range Stages {
		topics = append(topics, st.Topic)
	}
	for _, name := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 3, 1
		req.Topics = append(req.Topics, rt)
	}
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

// writeSecret writes a new version of the KV v2 secret webhook/<merchantId>, as merchant-service does.
func (s *stack) writeSecret(merchantID, secret string) {
	t := s.t
	body, _ := json.Marshal(map[string]any{"data": map[string]any{"secret": secret}})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.baoAddr+secrets.Path("secret", merchantID), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Vault-Token", baoToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write secret: %d", resp.StatusCode)
	}
}

func (s *stack) publish(topic string, ev kafka.Event) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.producer.Publish(ctx, topic, ev); err != nil {
		s.t.Fatal(err)
	}
}

// merchantUpdated publishes merchant.updated and waits until the projection has it, so the webhooks published
// after it find the merchant.
func (s *stack) merchantUpdated(id string, version int, url string, secretVersion int) {
	t := s.t
	ev, err := kafka.NewEvent("/weigate/merchant-service", merchants.EventUpdated, id, 1, map[string]any{
		"merchantId": id, "version": version, "xpub": "xpub" + strings.Repeat("1", 107), "webhookUrl": url,
		"toleranceBps": 100, "webhookSecretVersion": secretVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.publish(merchants.Topic, ev)
	s.eventually("projection of "+id, func() bool {
		m, ok, err := s.merchant.Get(context.Background(), id)
		return err == nil && ok && m.Version == version
	})
}

// invoice publishes invoice.created of a new invoice of the merchant.
func (s *stack) invoice(merchantID string) kafka.Event {
	t := s.t
	id := uuid.NewString()
	ev, err := kafka.NewEvent("/weigate/invoice-service", "invoice.created", id, 1, map[string]any{
		"invoiceId": id, "merchantId": merchantID, "sequence": 1, "status": "NEW",
		"address": "0x8ba1f109551bd432803012645ac136ddd64dba72", "asset": map[string]any{"kind": "ETH", "decimals": 18},
		"amount": "10000000000000000", "orderId": "order-" + id[:8], "amountUsdCents": 2500, "rate": "2500.00",
		"expiresAt": time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	s.publish(TopicInvoices, ev)
	return ev
}

func (s *stack) eventually(what string, ok func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// results lists the journal results of the event's attempts in attempt order.
func (s *stack) results(eventID string) []string {
	cur, err := s.mongo.Database().Collection(journal.Collection).Find(context.Background(), bson.M{"eventId": eventID})
	if err != nil {
		s.t.Fatal(err)
	}
	var docs []journal.Attempt
	if err := cur.All(context.Background(), &docs); err != nil {
		s.t.Fatal(err)
	}
	out := make([]string, len(docs))
	for _, a := range docs {
		if a.Attempt >= 1 && a.Attempt <= len(out) {
			out[a.Attempt-1] = a.Result
		}
	}
	return out
}

// dlq returns the dead-lettered event IDs read from webhook.dlq.
func (s *stack) dlq(want int) []string {
	t := s.t
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.brokers...), kgo.ConsumeTopics(TopicDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var ids []string
	for len(ids) < want {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("dlq: got %v, want %d events", ids, want)
		}
		fs.EachRecord(func(r *kgo.Record) {
			ev, err := s.codec.Decode(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, ev.ID)
		})
	}
	return ids
}

// One failing merchant endpoint goes through the retry stages to the DLQ, while the webhooks of another merchant
// are delivered meanwhile; a repeated event is delivered once (SRS 4.1.7, 4.1.9).
func TestIntegrationFailingEndpointDoesNotHoldOthers(t *testing.T) {
	s := newStack(t)
	failing, healthy := newEndpoint(t, http.StatusInternalServerError), newEndpoint(t, http.StatusNoContent)
	a, b := uuid.NewString(), uuid.NewString()
	s.writeSecret(a, "secret-a")
	s.writeSecret(b, "secret-b")
	s.merchantUpdated(a, 1, failing.srv.URL, 1)
	s.merchantUpdated(b, 1, healthy.srv.URL, 1)

	bad := s.invoice(a)
	var good []kafka.Event
	for range 3 {
		good = append(good, s.invoice(b))
	}
	s.publish(TopicInvoices, good[0]) // a redelivery of the same event.ID

	s.eventually("webhooks of the healthy merchant", func() bool { return healthy.count() >= 3 })
	if got := s.dlq(1); got[0] != bad.ID+":dlq" {
		t.Errorf("dlq %v", got)
	}
	if got := strings.Join(s.results(bad.ID), ","); got != "failed,failed,failed,failed" {
		t.Errorf("journal of the failing endpoint: %s", got)
	}
	if failing.count() != 4 {
		t.Errorf("failing endpoint got %d attempts, want 4", failing.count())
	}
	time.Sleep(time.Second)
	if healthy.count() != 3 {
		t.Errorf("healthy endpoint got %d webhooks, want 3 (the duplicate is skipped)", healthy.count())
	}
	for _, ev := range good {
		if got := s.results(ev.ID); len(got) != 1 || got[0] != journal.ResultDelivered {
			t.Errorf("journal of %s: %v", ev.ID, got)
		}
	}
}

// After the secret is rotated, webhooks are signed with the new version and carry it in the header (SRS 4.1.10).
func TestIntegrationSecretRotation(t *testing.T) {
	s := newStack(t)
	ep := newEndpoint(t, http.StatusOK)
	m := uuid.NewString()
	s.writeSecret(m, "secret-v1")
	s.merchantUpdated(m, 1, ep.srv.URL, 1)
	s.invoice(m)
	s.eventually("the first webhook", func() bool { return ep.count() == 1 })

	s.writeSecret(m, "secret-v2")
	s.merchantUpdated(m, 2, ep.srv.URL, 2)
	s.invoice(m)
	s.eventually("the second webhook", func() bool { return ep.count() == 2 })

	for i, secret := range []string{"secret-v1", "secret-v2"} {
		r, body := ep.request(i)
		if got := r.Header.Get(HeaderSecretVersion); got != strconv.Itoa(i+1) {
			t.Errorf("webhook %d: secret version %q", i+1, got)
		}
		verify(t, r, body, secret)
	}
}

// A delivery that reached the DLQ is replayed through POST /internal/dlq/replay once the endpoint works: it is
// delivered, journaled under <event id>:dlq, and the next call finds nothing to replay (FR-09).
func TestIntegrationDLQReplay(t *testing.T) {
	s := newStack(t)
	bad, healthy := newEndpoint(t, http.StatusInternalServerError), newEndpoint(t, http.StatusNoContent)
	a, b := uuid.NewString(), uuid.NewString()
	s.writeSecret(a, "secret-a")
	s.writeSecret(b, "secret-b")
	s.merchantUpdated(a, 1, bad.srv.URL, 1)
	s.merchantUpdated(b, 1, bad.srv.URL, 1)
	evA, evB := s.invoice(a), s.invoice(b)
	s.dlq(2)

	// Merchant a fixes its endpoint; only its delivery is replayed.
	s.merchantUpdated(a, 2, healthy.srv.URL, 1)
	h := &replay.Handler{
		DLQ:       &replay.Reader{Brokers: s.brokers, Topic: TopicDLQ, Codec: s.codec, Log: slog.Default()},
		Deliverer: s.deliverer, Journal: journal.New(s.mongo, time.Hour), Log: slog.Default(),
	}
	mux := http.NewServeMux()
	h.Register(mux)
	post := func(body string) string {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/internal/dlq/replay", strings.NewReader(body)))
		if w.Code != http.StatusOK {
			t.Fatalf("replay: %d %s", w.Code, w.Body)
		}
		return strings.TrimSpace(w.Body.String())
	}
	if got := post(`{"merchantId":"` + a + `"}`); got != `{"selected":1,"delivered":1,"failed":0}` {
		t.Errorf("replay of a: %s", got)
	}
	if healthy.count() != 1 {
		t.Fatalf("healthy endpoint got %d webhooks, want 1", healthy.count())
	}
	r, body := healthy.request(0)
	verify(t, r, body, "secret-a")
	if got := s.results(evA.ID + ":dlq"); len(got) != 1 || got[0] != journal.ResultDelivered {
		t.Errorf("journal of the replay: %v", got)
	}
	if got := post(`{"merchantId":"` + a + `"}`); got != `{"selected":0,"delivered":0,"failed":0}` {
		t.Errorf("repeated replay of a: %s", got)
	}
	// Merchant b still fails: the delivery stays in the DLQ and is tried again by the next call.
	if got := post(`{}`); got != `{"selected":1,"delivered":0,"failed":1}` {
		t.Errorf("replay of b: %s", got)
	}
	if got := s.results(evB.ID + ":dlq"); len(got) != 1 || got[0] != journal.ResultFailed {
		t.Errorf("journal of the failed replay: %v", got)
	}
}
