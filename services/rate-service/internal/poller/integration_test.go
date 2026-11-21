//go:build integration

package poller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/rate-service/internal/api"
	"github.com/m5tshift/weigate/services/rate-service/internal/events"
	"github.com/m5tshift/weigate/services/rate-service/internal/poller"
	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
	"github.com/m5tshift/weigate/services/rate-service/internal/source"
	"github.com/m5tshift/weigate/services/rate-service/internal/store"
)

var (
	brokers    []string
	valkeyAddr string
)

const waitFor = 60 * time.Second

func TestMain(m *testing.M) {
	ctx := context.Background()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		os.Exit(1)
	}
	vc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8.0",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err == nil {
		brokers, err = kc.Brokers(ctx)
	}
	if err == nil {
		var ep string
		ep, err = vc.PortEndpoint(ctx, "6379/tcp", "")
		valkeyAddr = ep
	}
	code := 1
	if err != nil {
		fmt.Fprintln(os.Stderr, "containers:", err)
	} else {
		code = m.Run()
	}
	_ = testcontainers.TerminateContainer(kc)
	if vc != nil {
		_ = testcontainers.TerminateContainer(vc)
	}
	os.Exit(code)
}

// createTopics creates rate.ticks and the compacted rate.latest of the contract.
func createTopics(t *testing.T) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	for _, name := range []string{events.TopicTicks, events.TopicLatest} {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, 1
		if name == events.TopicLatest {
			c := kmsg.NewCreateTopicsRequestTopicConfig()
			c.Name, c.Value = "cleanup.policy", kmsg.StringPtr("compact")
			rt.Configs = append(rt.Configs, c)
		}
		req.Topics = append(req.Topics, rt)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil && err != kerr.TopicAlreadyExists {
			t.Fatalf("create topic %s: %v", tr.Topic, err)
		}
	}
}

// rateMock is an httptest stand-in for rate-mock: GET /sources/{source}/quotes/{pair} with settable prices.
type rateMock struct {
	mu     sync.Mutex
	prices map[string]string // "<source>/<pair>" -> price, "" answers 503
}

func (m *rateMock) set(src rate.Source, pair rate.Pair, price string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prices[string(src)+"/"+string(pair)] = price
}

func (m *rateMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/sources/")
	src, pair, ok2 := strings.Cut(rest, "/quotes/")
	m.mu.Lock()
	price, known := m.prices[src+"/"+pair]
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case !ok || !ok2 || !known:
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"code":"not_found","message":"Source does not quote the pair"}`)
	case price == "":
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"code":"source_disabled","message":"Source is disabled"}`)
	default:
		_ = json.NewEncoder(w).Encode(map[string]string{
			"source": src, "pair": pair, "price": price, "quotedAt": time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
}

type harness struct {
	mock   *rateMock
	codec  *kafka.Codec
	leader *poller.Poller
	cache  *rate.Cache
	valkey *store.Valkey
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	createTopics(t)
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	mock := &rateMock{prices: map[string]string{}}
	mock.set(rate.Coinbase, rate.ETHUSD, "3700")
	mock.set(rate.Coinbase, rate.USDTUSD, "1")
	mock.set(rate.OKX, rate.ETHUSDT, "3710")
	srv := httptest.NewServer(mock)
	t.Cleanup(srv.Close)
	fetcher, err := source.NewMock(srv.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)

	v, err := storage.OpenValkey(ctx, storage.ValkeyConfig{Addrs: []string{valkeyAddr}})
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(time.Second)
	st.Attach(v.Client())
	t.Cleanup(st.Close)

	brs := map[rate.Source]*breaker.Breaker{}
	for _, src := range []rate.Source{rate.Coinbase, rate.OKX} {
		if brs[src], err = breaker.New(breaker.Config{Dependency: string(src), FailureThreshold: 3, OpenTimeout: 30 * time.Second, HalfOpenMax: 1}); err != nil {
			t.Fatal(err)
		}
	}
	cache := rate.NewCache()
	p, err := poller.New(poller.Config{
		Fetcher: fetcher, Breakers: brs, Publisher: events.NewPublisher(producer), Store: st, Cache: cache,
		Interval: 5 * time.Second, USDTMaxAge: time.Hour, DivergenceMaxBps: 100, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{mock: mock, codec: codec, leader: p, cache: cache, valkey: st}
}

// follower is a replica that does not lead: its memory is filled from rate.latest only.
func (h *harness) follower(t *testing.T) *rate.Cache {
	t.Helper()
	cache := rate.NewCache()
	r, err := events.NewReader(h.codec, cache, slog.New(slog.DiscardHandler), brokers)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done; r.Close() })
	return cache
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func serve(t *testing.T, rates api.Rates, pair string) (int, map[string]any) {
	t.Helper()
	mux := http.NewServeMux()
	api.Register(mux, api.Options{Rates: rates, MaxAge: 60 * time.Second, RetryAfter: 5 * time.Second})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/rates/"+pair, nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	return rec.Code, body
}

func TestLeaderPollsAndEveryReplicaServes(t *testing.T) {
	h := newHarness(t)
	follower := h.follower(t)
	ctx := context.Background()
	h.leader.Poll(ctx)

	// Valkey holds the rate written by the leader.
	s, ok, err := h.valkey.Get(ctx, rate.ETHUSD)
	if err != nil || !ok || s.Rate.Price.String() != "3705" || s.Diverged {
		t.Fatalf("valkey: %+v %v %v", s, ok, err)
	}
	// The follower restores both pairs from rate.latest.
	waitUntil(t, "rate.latest in the follower", func() bool {
		e, eok := follower.Get(rate.ETHUSD)
		u, uok := follower.Get(rate.USDTUSD)
		return eok && uok && e.UpdatedAt.Equal(s.UpdatedAt) && u.Rate.Price.String() == "1"
	})

	// The follower serves with Valkey and, when Valkey is unavailable, from memory.
	for name, latest := range map[string]*store.Latest{
		"valkey":    store.NewLatest(h.valkey, follower, slog.New(slog.DiscardHandler)),
		"no valkey": store.NewLatest(store.New(time.Second), follower, slog.New(slog.DiscardHandler)),
	} {
		code, body := serve(t, latest, "ETH-USD")
		if code != http.StatusOK || body["price"] != "3705" {
			t.Errorf("%s: %d %v", name, code, body)
		}
	}

	// Every quote reached rate.ticks with the key of the contract.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(events.TopicTicks),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	keys := map[string]bool{}
	pctx, cancel := context.WithTimeout(ctx, waitFor)
	defer cancel()
	for len(keys) < 3 && pctx.Err() == nil {
		cl.PollFetches(pctx).EachRecord(func(r *kgo.Record) {
			ev, err := h.codec.Decode(r.Value)
			if err != nil || ev.Type != events.TypeTick || ev.Subject != string(r.Key) {
				t.Errorf("tick %s: %v %+v", r.Key, err, ev)
			}
			keys[string(r.Key)] = true
		})
	}
	for _, k := range []string{"coinbase:ETH-USD", "coinbase:USDT-USD", "okx:ETH-USDT"} {
		if !keys[k] {
			t.Errorf("no tick %s in %v", k, keys)
		}
	}
}

func TestDivergenceReachesEveryReplicaThroughValkey(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.leader.Poll(ctx)
	follower := h.follower(t)
	waitUntil(t, "rate.latest in the follower", func() bool { _, ok := follower.Get(rate.ETHUSD); return ok })

	h.mock.set(rate.OKX, rate.ETHUSDT, "3800") // 270 bps above coinbase
	h.leader.Poll(ctx)
	code, body := serve(t, store.NewLatest(h.valkey, follower, slog.New(slog.DiscardHandler)), "ETH-USD")
	if code != http.StatusServiceUnavailable || body["reason"] != "diverged" {
		t.Fatalf("%d %v", code, body)
	}

	// OKX is disabled: the price of coinbase alone is the rate again.
	h.mock.set(rate.OKX, rate.ETHUSDT, "")
	h.leader.Poll(ctx)
	code, body = serve(t, store.NewLatest(h.valkey, follower, slog.New(slog.DiscardHandler)), "ETH-USD")
	if code != http.StatusOK || body["price"] != "3700" {
		t.Fatalf("%d %v", code, body)
	}
}

func TestRestartRestoresFromCompactedTopic(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.leader.Poll(ctx)
	h.mock.set(rate.Coinbase, rate.ETHUSD, "3720")
	h.mock.set(rate.OKX, rate.ETHUSDT, "3720")
	h.leader.Poll(ctx)
	want, _ := h.cache.Get(rate.ETHUSD)

	// A replica started later reads rate.latest from the start and keeps the newest snapshot.
	restarted := h.follower(t)
	waitUntil(t, "the newest rate after a restart", func() bool {
		s, ok := restarted.Get(rate.ETHUSD)
		return ok && s.UpdatedAt.Equal(want.UpdatedAt) && s.Rate.Price.String() == "3720"
	})
}
