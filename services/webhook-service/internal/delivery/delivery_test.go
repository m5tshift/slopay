package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/webhook-service/internal/journal"
	"github.com/m5tshift/weigate/services/webhook-service/internal/merchants"
)

const (
	merchantA = "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b"
	merchantB = "1c8e5f3b-8d4e-4f60-9b2c-7d3e4f5a6b7c"
	invoiceID = "5d7c1a52-3b1e-4f0a-9a43-2f6f0c9b8e11"
)

// --- fakes ---

type fakeMerchants struct {
	mu  sync.Mutex
	m   map[string]merchants.Merchant
	err error
}

func (f *fakeMerchants) Get(_ context.Context, id string) (merchants.Merchant, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.m[id]
	return m, ok, f.err
}

func (f *fakeMerchants) set(m merchants.Merchant) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[m.ID] = m
}

type fakeSecrets struct {
	m   map[string]string
	err error
}

func (f *fakeSecrets) Secret(_ context.Context, id string, version int) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	s, ok := f.m[id+":"+strconv.Itoa(version)]
	if !ok {
		return nil, errors.New("not found")
	}
	return []byte(s), nil
}

type fakeJournal struct {
	mu       sync.Mutex
	attempts []journal.Attempt
	err      error
}

func (f *fakeJournal) Record(_ context.Context, a journal.Attempt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.attempts = append(f.attempts, a)
	return nil
}

func (f *fakeJournal) FirstAttempt(_ context.Context, eventID string) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var first time.Time
	for _, a := range f.attempts {
		if a.EventID == eventID && (first.IsZero() || a.At.Before(first)) {
			first = a.At
		}
	}
	return first, !first.IsZero(), nil
}

func (f *fakeJournal) last() journal.Attempt {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[len(f.attempts)-1]
}

type published struct {
	topic string
	ev    kafka.Event
}

// codecPublisher validates every event against its schema (contract test of the retry and DLQ events).
type codecPublisher struct {
	t     *testing.T
	codec *kafka.Codec
	mu    sync.Mutex
	out   []published
	err   error
}

func (p *codecPublisher) Publish(_ context.Context, topic string, ev kafka.Event) error {
	if p.err != nil {
		return p.err
	}
	raw, err := p.codec.Encode(ev)
	if err != nil {
		p.t.Errorf("%s violates its schema: %v", ev.Type, err)
		return err
	}
	dec, err := p.codec.Decode(raw)
	if err != nil {
		p.t.Errorf("decode %s: %v", ev.Type, err)
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.out = append(p.out, published{topic, dec})
	return nil
}

func (p *codecPublisher) all() []published {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]published(nil), p.out...)
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// endpoint is a merchant webhook receiver that answers status and checks every request against the contract.
type endpoint struct {
	t      *testing.T
	srv    *httptest.Server
	mu     sync.Mutex
	status int
	reqs   []*http.Request
	bodies [][]byte
}

func newEndpoint(t *testing.T, status int) *endpoint {
	t.Helper()
	e := &endpoint{t: t, status: status}
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		validateWebhook(t, r, body)
		e.mu.Lock()
		e.reqs = append(e.reqs, r)
		e.bodies = append(e.bodies, body)
		status := e.status
		e.mu.Unlock()
		if status == http.StatusFound {
			http.Redirect(w, r, "/elsewhere", status)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *endpoint) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.reqs)
}

func (e *endpoint) request(i int) (*http.Request, []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.reqs[i], e.bodies[i]
}

var (
	contractOnce sync.Once
	contractDoc  *openapi3.T
	contractErr  error
)

// validateWebhook checks a webhook request against contracts/openapi/webhook.yaml.
func validateWebhook(t *testing.T, r *http.Request, body []byte) {
	t.Helper()
	contractOnce.Do(func() {
		contractDoc, contractErr = openapi3.NewLoader().LoadFromFile("../../../../contracts/openapi/webhook.yaml")
		if contractErr == nil {
			contractErr = contractDoc.Validate(context.Background())
		}
		if contractErr == nil {
			contractDoc.Servers = openapi3.Servers{{URL: "/"}}
		}
	})
	if contractErr != nil {
		t.Errorf("contract: %v", contractErr)
		return
	}
	router, err := legacy.NewRouter(contractDoc)
	if err != nil {
		t.Errorf("router: %v", err)
		return
	}
	req := r.Clone(context.Background())
	req.URL.Path = "/"
	req.Body = io.NopCloser(bytes.NewReader(body))
	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Errorf("webhook request is not in the contract: %v", err)
		return
	}
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route,
		Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}}
	if err := openapi3filter.ValidateRequest(context.Background(), in); err != nil {
		t.Errorf("webhook request violates the contract: %v", err)
	}
}

// --- environment ---

type env struct {
	t         *testing.T
	merchants *fakeMerchants
	secrets   *fakeSecrets
	journal   *fakeJournal
	pub       *codecPublisher
	clk       *clock
	slept     []time.Duration
	reader    *sdkmetric.ManualReader
	d         *Deliverer
}

func newEnv(t *testing.T, threshold int) *env {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, merchants: &fakeMerchants{m: map[string]merchants.Merchant{}},
		secrets: &fakeSecrets{m: map[string]string{}}, journal: &fakeJournal{},
		pub: &codecPublisher{t: t, codec: codec}, clk: &clock{t: time.Date(2026, 11, 15, 10, 0, 2, 0, time.UTC)},
		reader: sdkmetric.NewManualReader()}
	e.d, err = New(Config{
		Merchants: e.merchants, Secrets: e.secrets, Journal: e.journal, Publisher: e.pub,
		HTTP:    NewHTTPClient(time.Second),
		Breaker: breaker.Config{FailureThreshold: threshold, OpenTimeout: 30 * time.Second, HalfOpenMax: 1},
		Now:     e.clk.now,
		Sleep: func(_ context.Context, d time.Duration) error {
			e.slept = append(e.slept, d)
			e.clk.add(d)
			return nil
		},
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(e.reader)).Meter("test"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) merchant(id, url string, version int, secret string) {
	e.merchants.set(merchants.Merchant{ID: id, Version: 1, WebhookURL: url, SecretVersion: version})
	e.secrets.m[id+":"+strconv.Itoa(version)] = secret
}

func (e *env) sum(name string, attrs ...string) int64 {
	e.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(context.Background(), &rm); err != nil {
		e.t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if matches(dp.Attributes.ToSlice(), attrs) {
						total += dp.Value
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					if matches(dp.Attributes.ToSlice(), attrs) {
						total += int64(dp.Count) //nolint:gosec // test counts are small.
					}
				}
			}
		}
	}
	return total
}

func matches(kvs []attribute.KeyValue, want []string) bool {
	for i := 0; i+1 < len(want); i += 2 {
		found := false
		for _, kv := range kvs {
			if string(kv.Key) == want[i] && kv.Value.String() == want[i+1] {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func invoiceEvent(t *testing.T, merchantID, typ string, extra map[string]any) kafka.Event {
	t.Helper()
	data := map[string]any{
		"invoiceId": invoiceID, "merchantId": merchantID, "sequence": 1, "status": "NEW",
		"address": "0x8ba1f109551bd432803012645ac136ddd64dba72", "asset": map[string]any{"kind": "ETH", "decimals": 18},
		"amount": "10000000000000000", "orderId": "order-1001", "amountUsdCents": 2500, "rate": "2500.00",
		"expiresAt": "2026-11-15T10:15:00Z",
	}
	for k, v := range extra {
		data[k] = v
	}
	ev, err := kafka.NewEvent("/weigate/invoice-service", typ, invoiceID, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	ev.ID = "9f3b2c1d-8e7a-4b6c-9d5e-1f2a3b4c5d01"
	ev.Time = time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	return ev
}

func lastError(t *testing.T, ev kafka.Event) (*int, string, int) {
	t.Helper()
	var d struct {
		Attempt   int `json:"attempt"`
		Attempts  int `json:"attempts"`
		LastError struct {
			StatusCode *int   `json:"statusCode"`
			Reason     string `json:"reason"`
		} `json:"lastError"`
	}
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d.LastError.StatusCode, d.LastError.Reason, max(d.Attempt, d.Attempts)
}

func verify(t *testing.T, r *http.Request, body []byte, secret string) {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(r.Header.Get(HeaderTimestamp) + "." + string(body)))
	if want := "v1=" + hex.EncodeToString(mac.Sum(nil)); r.Header.Get(HeaderSignature) != want {
		t.Errorf("signature %q does not verify with the secret", r.Header.Get(HeaderSignature))
	}
}

// --- tests ---

func TestSign(t *testing.T) {
	mac := hmac.New(sha256.New, []byte("whsec_test"))
	mac.Write([]byte(`1795953600.{"a":1}`))
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if got := Sign([]byte("whsec_test"), 1795953600, []byte(`{"a":1}`)); got != want || len(got) != 67 {
		t.Errorf("Sign = %s, want %s", got, want)
	}
}

func TestDeliveredSignedPerContract(t *testing.T) {
	e := newEnv(t, 5)
	ep := newEndpoint(t, http.StatusNoContent)
	e.merchant(merchantA, ep.srv.URL+"/webhook/a", 2, "whsec_a2")
	ev := invoiceEvent(t, merchantA, "invoice.created", nil)

	if err := e.d.Invoice(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if ep.count() != 1 {
		t.Fatalf("requests = %d", ep.count())
	}
	r, body := ep.request(0)
	verify(t, r, body, "whsec_a2")
	want, _ := json.Marshal(ev)
	if !bytes.Equal(body, want) {
		t.Errorf("body is not the invoice event:\n%s\n%s", body, want)
	}
	if r.Header.Get(HeaderSecretVersion) != "2" || r.Header.Get(HeaderTimestamp) != strconv.FormatInt(e.clk.now().Unix(), 10) ||
		r.Header.Get("Content-Type") != "application/json" || r.URL.Path != "/webhook/a" {
		t.Errorf("headers: %v, path %s", r.Header, r.URL.Path)
	}
	a := e.journal.last()
	if a.Result != journal.ResultDelivered || a.Attempt != 1 || a.SecretVersion != 2 || *a.StatusCode != 204 ||
		a.EventID != ev.ID || a.InvoiceID != invoiceID || a.MerchantID != merchantA {
		t.Errorf("journal: %+v", a)
	}
	if len(e.pub.all()) != 0 {
		t.Errorf("published %+v", e.pub.all())
	}
	if e.sum("weigate_webhook_deliveries", "result", "delivered", "event_type", "invoice.created") != 1 {
		t.Error("deliveries metric")
	}
}

// Failed attempts go through 1m, 5m, 30m, then the DLQ; each retry waits until nextAttemptAt and is signed anew.
func TestRetryStagesThenDLQ(t *testing.T) {
	e := newEnv(t, 100)
	ep := newEndpoint(t, http.StatusServiceUnavailable)
	e.merchant(merchantA, ep.srv.URL, 1, "whsec_a1")
	ev := invoiceEvent(t, merchantA, "invoice.created", nil)
	first := e.clk.now()

	if err := e.d.Invoice(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	wantTopics := []string{"webhook.retry.1m", "webhook.retry.5m", "webhook.retry.30m", "webhook.dlq"}
	for i := range 3 {
		out := e.pub.all()
		if len(out) != i+1 {
			t.Fatalf("published %d events, want %d", len(out), i+1)
		}
		p := out[i]
		if p.topic != wantTopics[i] || p.ev.Type != EventRetry || p.ev.ID != ev.ID+":"+strconv.Itoa(i+1) ||
			p.ev.Subject != invoiceID || p.ev.Source != Source {
			t.Fatalf("event %d: %s %+v", i, p.topic, p.ev)
		}
		code, reason, attempt := lastError(t, p.ev)
		if code == nil || *code != 503 || reason != "merchant endpoint answered 503" || attempt != i+1 {
			t.Errorf("lastError %v %q attempt %d", code, reason, attempt)
		}
		var rd retryData
		if err := json.Unmarshal(p.ev.Data, &rd); err != nil {
			t.Fatal(err)
		}
		if want, _ := json.Marshal(ev); !bytes.Equal(rd.Event, want) {
			t.Errorf("retry carries %s", rd.Event)
		}
		if err := e.d.Retry(context.Background(), p.ev); err != nil {
			t.Fatal(err)
		}
		if got := e.slept[len(e.slept)-1]; got != Stages[i].Delay {
			t.Errorf("waited %s before attempt %d, want %s", got, i+2, Stages[i].Delay)
		}
	}
	out := e.pub.all()
	if len(out) != 4 {
		t.Fatalf("published %d events", len(out))
	}
	dlq := out[3]
	if dlq.topic != TopicDLQ || dlq.ev.Type != EventDeadLettered || dlq.ev.ID != ev.ID+":dlq" {
		t.Fatalf("dlq: %s %+v", dlq.topic, dlq.ev)
	}
	var dd deadLetteredData
	if err := json.Unmarshal(dlq.ev.Data, &dd); err != nil {
		t.Fatal(err)
	}
	if dd.Attempts != 4 || !dd.FirstAttemptAt.Equal(first) || !dd.FailedAt.Equal(e.clk.now()) || *dd.LastError.StatusCode != 503 {
		t.Errorf("dlq data: %+v", dd)
	}
	if ep.count() != 4 {
		t.Errorf("attempts = %d, want 4", ep.count())
	}
	// The timestamp and the signature are made anew for every attempt, the body stays the same.
	r0, b0 := ep.request(0)
	r3, b3 := ep.request(3)
	if r0.Header.Get(HeaderSignature) == r3.Header.Get(HeaderSignature) || !bytes.Equal(b0, b3) {
		t.Error("signature must change between attempts, body must not")
	}
	verify(t, r3, b3, "whsec_a1")
	if e.sum("weigate_webhook_scheduled", "topic", TopicDLQ) != 1 {
		t.Error("scheduled metric")
	}
}

// A retry already due is attempted without waiting.
func TestRetryDueNoWait(t *testing.T) {
	e := newEnv(t, 5)
	ep := newEndpoint(t, http.StatusOK)
	e.merchant(merchantA, ep.srv.URL, 1, "s")
	ev := retryEvent(t, invoiceEvent(t, merchantA, "invoice.created", nil), 1, e.clk.now().Add(-time.Second))
	if err := e.d.Retry(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if len(e.slept) != 0 || ep.count() != 1 || e.journal.last().Attempt != 2 {
		t.Errorf("slept %v, requests %d, journal %+v", e.slept, ep.count(), e.journal.last())
	}
}

func retryEvent(t *testing.T, inv kafka.Event, attempt int, next time.Time) kafka.Event {
	t.Helper()
	body, _ := json.Marshal(inv)
	ev, err := kafka.NewEvent(Source, EventRetry, invoiceID, 1, retryData{InvoiceID: invoiceID, MerchantID: merchantA,
		Attempt: attempt, NextAttemptAt: next, LastError: failure{Reason: "x"}, Event: body})
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func TestRetryWaitCancelled(t *testing.T) {
	e := newEnv(t, 5)
	e.d.cfg.Sleep = sleep
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ev := retryEvent(t, invoiceEvent(t, merchantA, "invoice.created", nil), 1, time.Now().Add(time.Hour))
	e.d.cfg.Now = time.Now
	if err := e.d.Retry(ctx, ev); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

func TestRetrySkipsOtherAndInvalid(t *testing.T) {
	e := newEnv(t, 5)
	other, _ := kafka.NewEvent(Source, "webhook.other", invoiceID, 1, map[string]any{"a": 1})
	bad, _ := kafka.NewEvent(Source, EventRetry, invoiceID, 1, map[string]any{"event": "nope"})
	noIDs, _ := kafka.NewEvent(Source, EventRetry, invoiceID, 1, map[string]any{"event": map[string]any{"data": map[string]any{}}})
	for _, ev := range []kafka.Event{other, bad, noIDs} {
		if err := e.d.Retry(context.Background(), ev); err != nil {
			t.Errorf("%s: %v", ev.Type, err)
		}
	}
	if len(e.journal.attempts) != 0 || len(e.pub.all()) != 0 {
		t.Error("nothing must be attempted")
	}
}

func TestInvoiceWithoutIDsSkipped(t *testing.T) {
	e := newEnv(t, 5)
	ev, _ := kafka.NewEvent("/weigate/invoice-service", "invoice.created", invoiceID, 1, map[string]any{"sequence": 1})
	if err := e.d.Invoice(context.Background(), ev); err != nil || len(e.journal.attempts) != 0 {
		t.Errorf("err %v, journal %+v", err, e.journal.attempts)
	}
}

func TestNoResponse(t *testing.T) {
	for name, url := range map[string]func(t *testing.T) string{
		"unreachable": func(t *testing.T) string {
			srv := httptest.NewServer(http.NotFoundHandler())
			srv.Close()
			return srv.URL
		},
		"timeout": func(t *testing.T) string {
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			// Cleanups run in reverse: the handler is released before Close waits for it.
			t.Cleanup(srv.Close)
			t.Cleanup(func() { close(release) })
			return srv.URL
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 5)
			e.d.cfg.HTTP = NewHTTPClient(100 * time.Millisecond)
			e.merchant(merchantA, url(t), 1, "s")
			if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
				t.Fatal(err)
			}
			code, reason, _ := lastError(t, e.pub.all()[0].ev)
			if code != nil || !strings.HasPrefix(reason, "merchant endpoint") {
				t.Errorf("lastError %v %q", code, reason)
			}
			if a := e.journal.last(); a.Result != journal.ResultFailed || a.StatusCode != nil {
				t.Errorf("journal %+v", a)
			}
		})
	}
}

func TestRedirectIsFailure(t *testing.T) {
	e := newEnv(t, 5)
	ep := newEndpoint(t, http.StatusFound)
	e.merchant(merchantA, ep.srv.URL, 1, "s")
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := lastError(t, e.pub.all()[0].ev); ep.count() != 1 || code == nil || *code != http.StatusFound {
		t.Errorf("requests %d, status %v", ep.count(), code)
	}
}

// The breaker of a failing endpoint opens after the threshold; another merchant is delivered meanwhile.
func TestBreakerPerEndpoint(t *testing.T) {
	e := newEnv(t, 2)
	bad := newEndpoint(t, http.StatusInternalServerError)
	good := newEndpoint(t, http.StatusOK)
	e.merchant(merchantA, bad.srv.URL, 1, "a")
	e.merchant(merchantB, good.srv.URL, 1, "b")
	for range 3 {
		if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if bad.count() != 2 {
		t.Errorf("failing endpoint got %d requests, want 2 (then the breaker is open)", bad.count())
	}
	code, reason, _ := lastError(t, e.pub.all()[2].ev)
	if code != nil || reason != "circuit breaker is open" {
		t.Errorf("lastError %v %q", code, reason)
	}
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantB, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	if good.count() != 1 || e.journal.last().Result != journal.ResultDelivered {
		t.Error("the other merchant must be delivered")
	}
	if e.sum("weigate_webhook_deliveries", "result", "breaker_open") != 1 {
		t.Error("breaker_open metric")
	}
}

func TestUnknownMerchantRetried(t *testing.T) {
	e := newEnv(t, 5)
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	out := e.pub.all()
	if len(out) != 1 || out[0].topic != "webhook.retry.1m" {
		t.Fatalf("published %+v", out)
	}
	if _, reason, _ := lastError(t, out[0].ev); reason != "merchant is unknown" {
		t.Errorf("reason %q", reason)
	}
}

func TestNoWebhookURLSkipped(t *testing.T) {
	e := newEnv(t, 5)
	e.merchant(merchantA, "", 1, "s")
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	if len(e.pub.all()) != 0 || e.journal.last().Result != journal.ResultSkipped {
		t.Errorf("published %+v, journal %+v", e.pub.all(), e.journal.attempts)
	}
}

// A missing secret is repeated later and does not count against the endpoint breaker.
func TestSecretUnavailableRetried(t *testing.T) {
	e := newEnv(t, 1)
	ep := newEndpoint(t, http.StatusOK)
	e.merchant(merchantA, ep.srv.URL, 1, "s")
	e.secrets.err = errors.New("sealed")
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	if _, reason, _ := lastError(t, e.pub.all()[0].ev); reason != "signing secret unavailable" || ep.count() != 0 {
		t.Errorf("reason %q, requests %d", reason, ep.count())
	}
	e.secrets.err = nil
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	if ep.count() != 1 {
		t.Error("the breaker must stay closed after a secret failure")
	}
}

// After rotation the next webhook is signed with the new secret and carries the new version (SRS 4.1.10).
func TestSecretRotation(t *testing.T) {
	e := newEnv(t, 5)
	ep := newEndpoint(t, http.StatusOK)
	e.merchant(merchantA, ep.srv.URL, 1, "whsec_old")
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	e.merchant(merchantA, ep.srv.URL, 2, "whsec_new")
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.detected", nil)); err != nil {
		t.Fatal(err)
	}
	r0, b0 := ep.request(0)
	r1, b1 := ep.request(1)
	verify(t, r0, b0, "whsec_old")
	verify(t, r1, b1, "whsec_new")
	if r0.Header.Get(HeaderSecretVersion) != "1" || r1.Header.Get(HeaderSecretVersion) != "2" {
		t.Error("secret version header")
	}
}

func TestPaidDelayMetric(t *testing.T) {
	e := newEnv(t, 5)
	ep := newEndpoint(t, http.StatusOK)
	e.merchant(merchantA, ep.srv.URL, 1, "s")
	paid := invoiceEvent(t, merchantA, "invoice.paid", map[string]any{"status": "PAID",
		"paidAt": e.clk.now().Add(-2 * time.Second).Format(time.RFC3339)})
	if err := e.d.Invoice(context.Background(), paid); err != nil {
		t.Fatal(err)
	}
	if e.sum("weigate_webhook_paid_delay") != 1 {
		t.Error("paid delay not recorded")
	}
	if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err != nil {
		t.Fatal(err)
	}
	if e.sum("weigate_webhook_paid_delay") != 1 {
		t.Error("paid delay recorded for another type")
	}
}

func TestInfrastructureErrorsReturned(t *testing.T) {
	cases := map[string]func(e *env){
		"projection": func(e *env) { e.merchants.err = errors.New("mongo down") },
		"journal":    func(e *env) { e.journal.err = errors.New("mongo down") },
		"publish":    func(e *env) { e.pub.err = errors.New("kafka down") },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 5)
			ep := newEndpoint(t, http.StatusServiceUnavailable)
			e.merchant(merchantA, ep.srv.URL, 1, "s")
			breakIt(e)
			if err := e.d.Invoice(context.Background(), invoiceEvent(t, merchantA, "invoice.created", nil)); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestReasonTruncated(t *testing.T) {
	e := newEnv(t, 5)
	inv, err := parseInvoice(invoiceEvent(t, merchantA, "invoice.created", nil))
	if err != nil {
		t.Fatal(err)
	}
	rec := journal.Attempt{EventID: inv.ev.ID, Attempt: 1, At: e.clk.now()}
	if err := e.d.fail(context.Background(), inv, rec, failure{Reason: strings.Repeat("x", 300)}); err != nil {
		t.Fatal(err)
	}
	if _, reason, _ := lastError(t, e.pub.all()[0].ev); len(reason) != 256 {
		t.Errorf("reason length %d", len(reason))
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("expected an error")
	}
}

// dlqEvent runs a delivery to the DLQ and returns the dead-lettered event.
func dlqEvent(t *testing.T, e *env, ev kafka.Event) kafka.Event {
	t.Helper()
	if err := e.d.Invoice(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := e.d.Retry(context.Background(), e.pub.all()[i].ev); err != nil {
			t.Fatal(err)
		}
	}
	out := e.pub.all()
	if len(out) != 4 || out[3].topic != TopicDLQ {
		t.Fatalf("published %d events", len(out))
	}
	return out[3].ev
}

func TestReplayDelivered(t *testing.T) {
	e := newEnv(t, 100)
	bad := newEndpoint(t, http.StatusServiceUnavailable)
	e.merchant(merchantA, bad.srv.URL, 1, "whsec_a1")
	ev := invoiceEvent(t, merchantA, "invoice.created", nil)
	dlq := dlqEvent(t, e, ev)

	good := newEndpoint(t, http.StatusNoContent)
	e.merchant(merchantA, good.srv.URL, 1, "whsec_a1")
	ok, err := e.d.Replay(context.Background(), dlq)
	if err != nil || !ok {
		t.Fatalf("replay: %v %v", ok, err)
	}
	r, body := good.request(0)
	verify(t, r, body, "whsec_a1")
	if want, _ := json.Marshal(ev); !bytes.Equal(body, want) {
		t.Errorf("replayed body %s", body)
	}
	if a := e.journal.last(); a.EventID != ev.ID+":dlq" || a.Attempt != 1 || a.Result != journal.ResultDelivered {
		t.Errorf("journal: %+v", a)
	}
	if len(e.pub.all()) != 4 {
		t.Error("a delivered replay must publish nothing")
	}
}

func TestReplayFailedStaysInDLQ(t *testing.T) {
	e := newEnv(t, 100)
	bad := newEndpoint(t, http.StatusServiceUnavailable)
	e.merchant(merchantA, bad.srv.URL, 1, "whsec_a1")
	dlq := dlqEvent(t, e, invoiceEvent(t, merchantA, "invoice.created", nil))

	ok, err := e.d.Replay(context.Background(), dlq)
	if err != nil || ok {
		t.Fatalf("replay: %v %v", ok, err)
	}
	if bad.count() != 5 {
		t.Errorf("attempts = %d, want 5", bad.count())
	}
	if a := e.journal.last(); a.Result != journal.ResultFailed || a.EventID != dlq.ID {
		t.Errorf("journal: %+v", a)
	}
	if len(e.pub.all()) != 4 {
		t.Error("a failed replay must publish nothing")
	}
}

func TestReplayErrors(t *testing.T) {
	e := newEnv(t, 100)
	ctx := context.Background()
	bad := kafka.Event{ID: "x:dlq", Data: []byte(`{"event":"nope"}`)}
	for _, ev := range []kafka.Event{{ID: "x:dlq", Data: []byte(`nope`)}, bad, {ID: "x:dlq", Data: []byte(`{"event":{"data":{}}}`)}} {
		if ok, err := e.d.Replay(ctx, ev); err == nil || ok {
			t.Errorf("replay of %s: %v %v", ev.Data, ok, err)
		}
	}
}

func TestReplaySkippedWithoutURL(t *testing.T) {
	e := newEnv(t, 100)
	bad := newEndpoint(t, http.StatusServiceUnavailable)
	e.merchant(merchantA, bad.srv.URL, 1, "whsec_a1")
	dlq := dlqEvent(t, e, invoiceEvent(t, merchantA, "invoice.created", nil))

	e.merchant(merchantA, "", 1, "whsec_a1")
	if ok, err := e.d.Replay(context.Background(), dlq); err != nil || ok {
		t.Fatalf("replay: %v %v", ok, err)
	}
	if a := e.journal.last(); a.Result != journal.ResultSkipped {
		t.Errorf("journal: %+v", a)
	}
}
