//go:build integration

package watcher_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/slopay/pkg/breaker"
	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/events"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/node"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/projection"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/store"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/tracked"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/watcher"
)

const (
	anvilImage   = "ghcr.io/foundry-rs/foundry:v1.10.0"
	valkeyImage  = "valkey/valkey:8.0"
	anvilChainID = 31337
	waitFor      = 60 * time.Second
)

var (
	brokers       []string
	mongoURI      string
	mongoPassword string
	anvil         testcontainers.Container
	anvilURL      atomic.Value // string: the current endpoint, it may change after a restart
	proxyURL      string
	valkeyC       testcontainers.Container
	valkeyAddr    atomic.Value // string: host:port, it changes after a restart
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
		var ep string
		ep, err = mc.PortEndpoint(ctx, "27017/tcp", "mongodb")
		mongoURI, mongoPassword = ep, "pw"
	}
	if err == nil {
		// exec makes anvil receive SIGTERM; the state file keeps the chain, with the same hashes, across restarts.
		anvil, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        anvilImage,
				Entrypoint:   []string{"/bin/sh", "-c"},
				Cmd:          []string{"exec anvil --host 0.0.0.0 --state /tmp/anvil-state.json --state-interval 1"},
				ExposedPorts: []string{"8545/tcp"},
				WaitingFor:   wait.ForLog("Listening on").WithStartupTimeout(waitFor),
			},
			Started: true,
		})
		ctrs = append(ctrs, anvil)
	}
	if err == nil {
		err = refreshAnvilURL(ctx)
	}
	if err == nil {
		valkeyC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        valkeyImage,
				ExposedPorts: []string{"6379/tcp"},
				WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(waitFor),
			},
			Started: true,
		})
		ctrs = append(ctrs, valkeyC)
	}
	if err == nil {
		err = refreshValkeyAddr(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "containers:", err)
		return
	}
	// The watcher reaches Anvil through a proxy, so its URL stays the same when the container port changes.
	proxy := httptest.NewServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		target, _ := url.Parse(anvilURL.Load().(string))
		r.SetURL(target)
	}})
	defer proxy.Close()
	proxyURL = proxy.URL
	code = m.Run()
}

func refreshAnvilURL(ctx context.Context) error {
	ep, err := anvil.PortEndpoint(ctx, "8545/tcp", "http")
	if err != nil {
		return err
	}
	anvilURL.Store(ep)
	return nil
}

func refreshValkeyAddr(ctx context.Context) error {
	ep, err := valkeyC.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		return err
	}
	valkeyAddr.Store(ep)
	return nil
}

// rpcCall calls Anvil directly, bypassing the proxy.
func rpcCall(t *testing.T, result any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := rpc.DialContext(ctx, anvilURL.Load().(string))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CallContext(ctx, result, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

func mine(t *testing.T, n uint64) {
	t.Helper()
	rpcCall(t, nil, "anvil_mine", hexutil.Uint64(n))
}

type block struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       string         `json:"hash"`
	ParentHash string         `json:"parentHash"`
}

func blockAt(t *testing.T, n uint64) block {
	t.Helper()
	var b block
	rpcCall(t, &b, "eth_getBlockByNumber", hexutil.Uint64(n), false)
	return b
}

func headNumber(t *testing.T) uint64 {
	t.Helper()
	var n hexutil.Uint64
	rpcCall(t, &n, "eth_blockNumber")
	return uint64(n)
}

// A reorg of depth d: snapshot, mine d blocks, let the watcher process them, then revertAndMine d+1 blocks.
func snapshot(t *testing.T) string {
	t.Helper()
	var id string
	rpcCall(t, &id, "evm_snapshot")
	return id
}

func revertAndMine(t *testing.T, id string, n uint64) {
	t.Helper()
	var ok bool
	rpcCall(t, &ok, "evm_revert", id)
	if !ok {
		t.Fatal("evm_revert failed")
	}
	// Shift the time so that the new branch has other timestamps and therefore other hashes.
	rpcCall(t, nil, "evm_increaseTime", hexutil.Uint64(13))
	mine(t, n)
}

// received is a decoded event of the topic.
type received struct {
	key         string
	typ         string
	id          string
	Number      uint64 `json:"number"`
	Hash        string `json:"hash"`
	ParentHash  string `json:"parentHash"`
	FromBlock   uint64 `json:"fromBlock"`
	ChainID     uint64 `json:"chainId"`
	BlockNumber uint64 `json:"blockNumber"`
	BlockHash   string `json:"blockHash"`
	TxHash      string `json:"txHash"`
	To          string `json:"to"`
	Amount      string `json:"amount"`
}

// collector reads the topic of a test from the start.
type collector struct {
	mu  sync.Mutex
	evs []received
	err error
}

func collect(t *testing.T, topic string) *collector {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			fs.EachRecord(func(r *kgo.Record) {
				ev, err := codec.Decode(r.Value)
				var rc received
				if err == nil {
					err = json.Unmarshal(ev.Data, &rc)
				}
				rc.key, rc.typ, rc.id = string(r.Key), ev.Type, ev.ID
				c.mu.Lock()
				c.evs = append(c.evs, rc)
				if err != nil && c.err == nil {
					c.err = err
				}
				c.mu.Unlock()
			})
		}
	}()
	t.Cleanup(func() { cancel(); <-done; cl.Close() })
	return c
}

func (c *collector) all(t *testing.T) []received {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		t.Fatalf("event does not match the contract: %v", c.err)
	}
	return append([]received(nil), c.evs...)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitHead waits for chain.new_head of block n with the hash of the node.
func (c *collector) waitHead(t *testing.T, n uint64) {
	t.Helper()
	want := blockAt(t, n).Hash
	eventually(t, fmt.Sprintf("chain.new_head %d", n), func() bool {
		for _, e := range c.all(t) {
			if e.typ == events.TypeNewHead && e.Number == n && e.Hash == want {
				return true
			}
		}
		return false
	})
}

// replay applies the events in order and checks that every head follows the previous one without gaps or
// duplicates and links to it by parentHash, and that a reorg rolls back to a processed block. It returns the
// resulting chain view, number -> hash, and the reorg events.
func replay(t *testing.T, evs []received) (map[uint64]string, []received) {
	t.Helper()
	view := map[uint64]string{}
	var (
		reorgs []received
		last   uint64
		start  = true
	)
	for i, e := range evs {
		key := "31337"
		if e.typ == events.TypeTransfer {
			key = e.To
		}
		if e.key != key || e.ChainID != anvilChainID {
			t.Fatalf("event %d: key %q chainId %d", i, e.key, e.ChainID)
		}
		switch e.typ {
		case events.TypeNewHead:
			if !start && (e.Number != last+1 || e.ParentHash != view[last]) {
				t.Fatalf("event %d: head %d (parent %s) after %d (%s)", i, e.Number, e.ParentHash, last, view[last])
			}
			if e.id != fmt.Sprintf("%d:%s", anvilChainID, e.Hash) {
				t.Fatalf("event %d: id %s", i, e.id)
			}
			start, last = false, e.Number
			view[last] = e.Hash
		case events.TypeTransfer:
			// Checked by the transfer tests.
		case events.TypeReorg:
			if start || e.FromBlock > last || view[e.FromBlock-1] == "" {
				t.Fatalf("event %d: reorg from %d at cursor %d", i, e.FromBlock, last)
			}
			for n := e.FromBlock; n <= last; n++ {
				delete(view, n)
			}
			last = e.FromBlock - 1
			reorgs = append(reorgs, e)
		default:
			t.Fatalf("event %d: type %s", i, e.typ)
		}
	}
	return view, reorgs
}

// matchesNode checks that the view holds exactly the blocks from..to of the node.
func matchesNode(t *testing.T, view map[uint64]string, from, to uint64) {
	t.Helper()
	if len(view) != int(to-from+1) {
		t.Fatalf("view has %d blocks, want %d..%d", len(view), from, to)
	}
	for n := from; n <= to; n++ {
		if got, want := view[n], blockAt(t, n).Hash; got != want {
			t.Fatalf("block %d: published %s, node %s", n, got, want)
		}
	}
}

type harness struct {
	// topic gets both chain.blocks and chain.transfers events: one partition keeps the order of publication.
	topic     string
	invoices  string
	group     string
	mongo     *storage.Mongo
	store     *store.Store
	addresses *projection.Store
	events    *collector
	reader    *sdkmetric.ManualReader
	start     uint64

	cancel context.CancelFunc
	done   chan struct{}
}

var runs atomic.Int64

var nonName = regexp.MustCompile(`[^a-z0-9]+`)

func newHarness(t *testing.T) *harness {
	t.Helper()
	// The run number keeps topics, groups and databases apart when a test runs repeatedly (-count).
	name := fmt.Sprintf("%s_%d", strings.Trim(nonName.ReplaceAllString(strings.ToLower(t.Name()), "_"), "_"), runs.Add(1))
	h := &harness{topic: "chain.blocks." + name, invoices: "invoice.events." + name, group: "cw-" + name,
		start: headNumber(t) + 1}
	createTopic(t, h.topic)
	createTopic(t, h.invoices)

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	file := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(file, []byte(mongoPassword), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := storage.OpenMongo(ctx, storage.MongoConfig{URI: mongoURI, Username: "app",
		PasswordFile: storage.SecretFile(file), Database: "cw_" + name})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	h.mongo, h.store = m, store.New(m)
	h.addresses = projection.New(m)
	if err := h.addresses.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	h.events = collect(t, h.topic)
	return h
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

// run starts a watcher replica on the harness; stop ends it, as a lost leadership does.
func (h *harness) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	client, err := node.Dial(ctx, proxyURL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.reader = sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(h.reader)).Meter("test")
	br, err := breaker.New(breaker.Config{Dependency: "node", FailureThreshold: 3, OpenTimeout: 2 * time.Second,
		HalfOpenMax: 1, Meter: meter})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// The tracked addresses as in main: the projection, the cache connected in the background, the consumer.
	cache := tracked.NewValkey(anvilChainID, time.Second, log)
	book, err := tracked.NewBook(h.addresses, cache, 200*time.Millisecond, meter, log)
	if err != nil {
		t.Fatal(err)
	}
	invoices, err := kafka.NewConsumer(codec, log, brokers, h.group, []string{h.invoices},
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-26*time.Hour).UnixMilli())))
	if err != nil {
		t.Fatal(err)
	}
	handler := projection.Handler(h.addresses, 24*time.Hour, projection.Backoff{Min: 50 * time.Millisecond, Max: time.Second}, log)

	start := h.start
	w, err := watcher.New(watcher.Config{
		Node: client, Store: h.store, Publisher: events.NewPublisher(producer, h.topic, h.topic), Addresses: book,
		Breaker: br, ChainID: anvilChainID, StartBlock: &start, WindowSize: 64, MaxReorgDepth: 6,
		RescanClockSkew: 30 * time.Second, RescanLookback: 26 * time.Hour,
		PollInterval: 100 * time.Millisecond, BackoffInitial: 100 * time.Millisecond, BackoffMax: time.Second,
		Logger: log, Meter: meter,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.cancel, h.done = cancel, make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		cache.Connect(ctx, func(ctx context.Context) (valkey.Client, error) {
			v, err := storage.OpenValkey(ctx, storage.ValkeyConfig{Addrs: []string{valkeyAddr.Load().(string)}})
			if err != nil {
				return nil, err
			}
			return v.Client(), nil
		}, 200*time.Millisecond)
	})
	wg.Go(func() {
		rebuild := func(ctx context.Context) error {
			return projection.Rebuild(ctx, projection.RebuildConfig{Brokers: brokers, Topic: h.invoices,
				Since: time.Now().Add(-26 * time.Hour), Codec: codec, Handler: handler, Log: log, Idle: 5 * time.Second})
		}
		if projection.EnsureBuilt(ctx, h.addresses, rebuild, projection.Backoff{Min: 100 * time.Millisecond, Max: time.Second}, log) != nil {
			return
		}
		if err := invoices.Run(ctx, handler); err != nil && ctx.Err() == nil {
			t.Errorf("invoice.events consumer: %v", err)
		}
	})
	go func() {
		defer close(h.done)
		w.Run(ctx)
		wg.Wait()
		invoices.Close()
		cache.Close()
		client.Close()
		producer.Close()
	}()
	t.Cleanup(h.stop)
}

func (h *harness) stop() {
	if h.cancel != nil {
		h.cancel()
		<-h.done
		h.cancel = nil
	}
}

func (h *harness) metric(t *testing.T, name string, kv ...attribute.KeyValue) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			var dps []metricdata.DataPoint[int64]
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				dps = d.DataPoints
			case metricdata.Sum[int64]:
				dps = d.DataPoints
			}
			for _, dp := range dps {
				match := true
				for _, a := range kv {
					if v, ok := dp.Attributes.Value(a.Key); !ok || v != a.Value {
						match = false
					}
				}
				if match {
					sum += dp.Value
				}
			}
		}
	}
	return sum
}

func (h *harness) storedCursor(t *testing.T) uint64 {
	t.Helper()
	st, err := h.store.Load(context.Background(), anvilChainID)
	if err != nil {
		t.Fatal(err)
	}
	tip, ok := st.Tip()
	if !ok {
		t.Fatal("no stored cursor")
	}
	return tip.Number
}

// TestReorg is SRS 8.5: reorgs of depth 3 and 6 are handled, chain.reorg points at the first orphaned block,
// and the new branch is published in full.
func TestReorg(t *testing.T) {
	for _, depth := range []uint64{3, 6} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			h := newHarness(t)
			h.run(t)
			mine(t, 2)
			base := headNumber(t)
			h.events.waitHead(t, base)

			id := snapshot(t)
			mine(t, depth)
			h.events.waitHead(t, base+depth)
			orphan := blockAt(t, base+1).Hash
			revertAndMine(t, id, depth+1)
			h.events.waitHead(t, base+depth+1)

			view, reorgs := replay(t, h.events.all(t))
			if len(reorgs) != 1 || reorgs[0].FromBlock != base+1 {
				t.Fatalf("reorgs %+v, want one from %d", reorgs, base+1)
			}
			if want := fmt.Sprintf("%d:reorg:%d:%s", anvilChainID, base+1, orphan); reorgs[0].id != want {
				t.Fatalf("reorg id %s, want %s", reorgs[0].id, want)
			}
			matchesNode(t, view, h.start, base+depth+1)
			if got := h.metric(t, "slopay_chain_reorgs", attribute.String("depth", fmt.Sprint(depth))); got != 1 {
				t.Fatalf("reorgs{depth=%d} = %d", depth, got)
			}
			if got := h.storedCursor(t); got != base+depth+1 {
				t.Fatalf("stored cursor %d", got)
			}
		})
	}
}

// TestReorgTooDeep checks that a reorg of 7 blocks stops reading and raises the gauge of ChainReorgTooDeep.
func TestReorgTooDeep(t *testing.T) {
	h := newHarness(t)
	h.run(t)
	mine(t, 2)
	base := headNumber(t)
	h.events.waitHead(t, base)
	id := snapshot(t)
	mine(t, 7)
	h.events.waitHead(t, base+7)
	revertAndMine(t, id, 8)

	eventually(t, "slopay_chain_reorg_too_deep = 1", func() bool {
		return h.metric(t, "slopay_chain_reorg_too_deep") == 1
	})
	time.Sleep(500 * time.Millisecond)
	_, reorgs := replay(t, h.events.all(t))
	if len(reorgs) != 0 {
		t.Fatalf("reorg published: %+v", reorgs)
	}
	if got := h.storedCursor(t); got != base+7 {
		t.Fatalf("stored cursor %d, want %d", got, base+7)
	}
}

// TestRestartResumesFromCursor stops a replica and starts another one: it continues after the stored cursor.
func TestRestartResumesFromCursor(t *testing.T) {
	h := newHarness(t)
	h.run(t)
	mine(t, 3)
	h.events.waitHead(t, headNumber(t))
	h.stop()

	mine(t, 4)
	h.run(t)
	head := headNumber(t)
	h.events.waitHead(t, head)
	view, reorgs := replay(t, h.events.all(t))
	if len(reorgs) != 0 {
		t.Fatalf("reorgs %+v", reorgs)
	}
	matchesNode(t, view, h.start, head)
}

// TestNodeOutageCatchUp stops the node container: reading pauses, and after the node is back every block is
// published exactly once, in order.
func TestNodeOutageCatchUp(t *testing.T) {
	h := newHarness(t)
	h.run(t)
	mine(t, 3)
	before := headNumber(t)
	h.events.waitHead(t, before)
	time.Sleep(1500 * time.Millisecond) // let Anvil dump its state

	ctx := context.Background()
	stopTimeout := 10 * time.Second
	if err := anvil.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	eventually(t, "failed node calls", func() bool {
		return h.metric(t, "slopay_chain_node_requests", attribute.String("result", "error")) >= 3 &&
			h.metric(t, "slopay_chain_node_requests", attribute.String("result", "rejected")) >= 1
	})
	if err := anvil.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := refreshAnvilURL(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, "anvil after the restart", func() bool {
		c, err := node.Dial(ctx, anvilURL.Load().(string), time.Second)
		if err != nil {
			return false
		}
		defer c.Close()
		_, err = c.HeadNumber(ctx)
		return err == nil
	})
	if got := headNumber(t); got != before {
		t.Fatalf("anvil lost its state: head %d, want %d", got, before)
	}
	mine(t, 20)
	head := headNumber(t)
	h.events.waitHead(t, head)

	view, reorgs := replay(t, h.events.all(t))
	if len(reorgs) != 0 {
		t.Fatalf("reorgs %+v", reorgs)
	}
	matchesNode(t, view, h.start, head)
}
