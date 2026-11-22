// Package delivery sends webhooks to merchants (SRS 4.1.3, 4.1.9, 4.1.10; FR-07): every invoice event is POSTed to
// the webhook URL of its merchant, signed anew for every attempt with the current secret version. A failed attempt
// is repeated through webhook.retry.1m, .5m and .30m, then the delivery goes to webhook.dlq. Every merchant endpoint
// has its own Circuit Breaker, so a failing merchant does not hold up the others.
package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/webhook-service/internal/journal"
	"github.com/m5tshift/weigate/services/webhook-service/internal/merchants"
)

// Topics and event types (contracts/asyncapi/webhook-service.yaml).
const (
	Source            = "/weigate/webhook-service"
	TopicInvoices     = "invoice.events"
	TopicDLQ          = "webhook.dlq"
	EventRetry        = "webhook.retry"
	EventDeadLettered = "webhook.dead_lettered"
	eventPaid         = "invoice.paid"
)

// Headers of the webhook (contracts/openapi/webhook.yaml).
const (
	HeaderTimestamp     = "X-Weigate-Timestamp"
	HeaderSignature     = "X-Weigate-Signature"
	HeaderSecretVersion = "X-Weigate-Secret-Version"
)

// Stage is a delayed repeat: a delivery that failed n times goes to Stages[n-1]; after the last stage it goes to
// the DLQ. The delays are fixed by the topic names.
type Stage struct {
	Topic string
	Delay time.Duration
}

// Stages are the retry topics in order.
var Stages = []Stage{
	{Topic: "webhook.retry.1m", Delay: time.Minute},
	{Topic: "webhook.retry.5m", Delay: 5 * time.Minute},
	{Topic: "webhook.retry.30m", Delay: 30 * time.Minute},
}

// Results of an attempt in the deliveries metric.
const (
	resultDelivered       = "delivered"
	resultHTTPError       = "http_error"
	resultNoResponse      = "no_response"
	resultBreakerOpen     = "breaker_open"
	resultUnknownMerchant = "unknown_merchant"
	resultNoSecret        = "secret_unavailable"
	resultSkipped         = "skipped"
)

// Merchants is the merchant projection.
type Merchants interface {
	Get(ctx context.Context, id string) (merchants.Merchant, bool, error)
}

// Secrets reads a signing secret by version.
type Secrets interface {
	Secret(ctx context.Context, merchantID string, version int) ([]byte, error)
}

// Journal records the attempts.
type Journal interface {
	Record(ctx context.Context, a journal.Attempt) error
	FirstAttempt(ctx context.Context, eventID string) (time.Time, bool, error)
}

// Publisher publishes retry and DLQ events; kafka.Producer implements it.
type Publisher interface {
	Publish(ctx context.Context, topic string, ev kafka.Event) error
}

// Config are the dependencies of a Deliverer.
type Config struct {
	Merchants Merchants
	Secrets   Secrets
	Journal   Journal
	Publisher Publisher
	// HTTP sends the webhooks; NewHTTPClient builds it.
	HTTP *http.Client
	// Breaker are the thresholds of the breaker of every endpoint; Dependency is set per merchant.
	Breaker breaker.Config
	// Now and Sleep replace the clock in tests.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	Meter metric.Meter
	// Logger never receives secrets or signatures.
	Logger *slog.Logger
}

// Deliverer handles invoice events and retries. It is safe for concurrent use.
type Deliverer struct {
	cfg Config
	log *slog.Logger

	mu       sync.Mutex
	breakers map[string]*breaker.Breaker

	deliveries metric.Int64Counter
	duration   metric.Float64Histogram
	paidDelay  metric.Float64Histogram
	scheduled  metric.Int64Counter
}

// NewHTTPClient returns the client for webhooks: timeout per attempt, redirects are not followed (a 3xx is a
// failed attempt), so a webhook goes only to the URL of the merchant settings.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// New checks cfg and registers the metrics.
func New(cfg Config) (*Deliverer, error) {
	if cfg.Merchants == nil || cfg.Secrets == nil || cfg.Journal == nil || cfg.Publisher == nil || cfg.HTTP == nil {
		return nil, errors.New("delivery: merchants, secrets, journal, publisher and http client are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleep
	}
	if cfg.Meter == nil {
		cfg.Meter = otel.Meter("github.com/m5tshift/weigate/services/webhook-service/internal/delivery")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	d := &Deliverer{cfg: cfg, log: cfg.Logger, breakers: map[string]*breaker.Breaker{}}
	m := cfg.Meter
	var err error
	if d.deliveries, err = m.Int64Counter("weigate_webhook_deliveries",
		metric.WithDescription("Webhook delivery attempts by result and event type")); err != nil {
		return nil, err
	}
	if d.duration, err = m.Float64Histogram("weigate_webhook_delivery_duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
		metric.WithDescription("Duration of a webhook request to the merchant endpoint, by result")); err != nil {
		return nil, err
	}
	if d.paidDelay, err = m.Float64Histogram("weigate_webhook_paid_delay", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.25, 0.5, 1, 1.5, 2, 3, 5, 10, 30, 60, 300),
		metric.WithDescription("Time from paidAt (the block of the N-th confirmation) to the delivered invoice.paid webhook (NFR-04)")); err != nil {
		return nil, err
	}
	if d.scheduled, err = m.Int64Counter("weigate_webhook_scheduled",
		metric.WithDescription("Failed deliveries published to a retry topic or the DLQ, by topic")); err != nil {
		return nil, err
	}
	return d, nil
}

// failure is the last error of an attempt as in lastError of the retry and DLQ events.
type failure struct {
	StatusCode *int   `json:"statusCode"`
	Reason     string `json:"reason"`
	result     string
}

// invoice is what a delivery needs from the invoice event.
type invoice struct {
	ev         kafka.Event
	body       []byte
	invoiceID  string
	merchantID string
	paidAt     *time.Time
	// replay marks a delivery taken from the DLQ: it is journaled under <event id>:dlq and is not scheduled again.
	replay bool
}

func parseInvoice(ev kafka.Event) (invoice, error) {
	var data struct {
		InvoiceID  string     `json:"invoiceId"`
		MerchantID string     `json:"merchantId"`
		PaidAt     *time.Time `json:"paidAt"`
	}
	if err := json.Unmarshal(ev.Data, &data); err != nil || data.InvoiceID == "" || data.MerchantID == "" {
		return invoice{}, fmt.Errorf("delivery: invoice event %s has no invoiceId or merchantId", ev.ID)
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return invoice{}, fmt.Errorf("delivery: encode %s: %w", ev.ID, err)
	}
	return invoice{ev: ev, body: body, invoiceID: data.InvoiceID, merchantID: data.MerchantID, paidAt: data.PaidAt}, nil
}

// Invoice is the handler of invoice.events: the first attempt of the webhook of the event.
func (d *Deliverer) Invoice(ctx context.Context, ev kafka.Event) error {
	inv, err := parseInvoice(ev)
	if err != nil {
		// The schema requires both ids, so this event can never be delivered.
		d.log.ErrorContext(ctx, "skipping invoice event", "event_id", ev.ID, "error", err)
		return nil
	}
	_, err = d.attempt(ctx, inv, 0)
	return err
}

// retryData is the data of webhook.retry (contracts/schemas/webhook.retry.v1.json).
type retryData struct {
	InvoiceID     string          `json:"invoiceId"`
	MerchantID    string          `json:"merchantId"`
	Attempt       int             `json:"attempt"`
	NextAttemptAt time.Time       `json:"nextAttemptAt"`
	LastError     failure         `json:"lastError"`
	Event         json.RawMessage `json:"event"`
}

// deadLetteredData is the data of webhook.dead_lettered (contracts/schemas/webhook.dead_lettered.v1.json).
type deadLetteredData struct {
	InvoiceID      string          `json:"invoiceId"`
	MerchantID     string          `json:"merchantId"`
	Attempts       int             `json:"attempts"`
	FirstAttemptAt time.Time       `json:"firstAttemptAt"`
	FailedAt       time.Time       `json:"failedAt"`
	LastError      failure         `json:"lastError"`
	Event          json.RawMessage `json:"event"`
}

// Retry is the handler of the retry topics: it waits until nextAttemptAt and makes the next attempt. The records of
// a retry topic come in the order they were delayed, so waiting for the first one delays none of the others.
func (d *Deliverer) Retry(ctx context.Context, ev kafka.Event) error {
	if ev.Type != EventRetry {
		return nil
	}
	var r retryData
	if err := json.Unmarshal(ev.Data, &r); err != nil {
		d.log.ErrorContext(ctx, "skipping retry event", "event_id", ev.ID, "error", err)
		return nil
	}
	var inner kafka.Event
	if err := json.Unmarshal(r.Event, &inner); err != nil {
		d.log.ErrorContext(ctx, "skipping retry event", "event_id", ev.ID, "error", err)
		return nil
	}
	inv, err := parseInvoice(inner)
	if err != nil {
		d.log.ErrorContext(ctx, "skipping retry event", "event_id", ev.ID, "error", err)
		return nil
	}
	if wait := r.NextAttemptAt.Sub(d.cfg.Now()); wait > 0 {
		if err := d.cfg.Sleep(ctx, wait); err != nil {
			return err
		}
	}
	_, err = d.attempt(ctx, inv, r.Attempt)
	return err
}

// Replay is one new attempt of a webhook taken from webhook.dlq (POST /internal/dlq/replay). It reports whether
// the merchant answered 2xx. A failed replay is not scheduled again: the delivery stays in the DLQ. The attempt is
// journaled under <event id>:dlq, so a repeated replay overwrites it.
func (d *Deliverer) Replay(ctx context.Context, ev kafka.Event) (bool, error) {
	var dl deadLetteredData
	if err := json.Unmarshal(ev.Data, &dl); err != nil {
		return false, fmt.Errorf("delivery: dead-lettered event %s: %w", ev.ID, err)
	}
	var inner kafka.Event
	if err := json.Unmarshal(dl.Event, &inner); err != nil {
		return false, fmt.Errorf("delivery: dead-lettered event %s: %w", ev.ID, err)
	}
	inv, err := parseInvoice(inner)
	if err != nil {
		return false, err
	}
	inv.replay = true
	return d.attempt(ctx, inv, 0)
}

// attempt makes attempt number failed+1 and reports whether it was delivered. Errors are those of the journal, the
// projection and Kafka: the caller repeats the event. A failed delivery itself is not an error: it is scheduled.
func (d *Deliverer) attempt(ctx context.Context, inv invoice, failed int) (bool, error) {
	no := failed + 1
	at := d.cfg.Now()
	rec := journal.Attempt{EventID: inv.ev.ID, EventType: inv.ev.Type, InvoiceID: inv.invoiceID,
		MerchantID: inv.merchantID, Attempt: no, At: at.UTC()}
	if inv.replay {
		rec.EventID += ":dlq"
	}

	m, ok, err := d.cfg.Merchants.Get(ctx, inv.merchantID)
	if err != nil {
		return false, err
	}
	if !ok {
		// merchant.updated may not be read yet: repeat later.
		return false, d.fail(ctx, inv, rec, failure{Reason: "merchant is unknown", result: resultUnknownMerchant})
	}
	if m.WebhookURL == "" {
		rec.Result, rec.Reason = journal.ResultSkipped, "merchant has no webhook URL"
		d.count(ctx, inv, resultSkipped)
		return false, d.cfg.Journal.Record(ctx, rec)
	}
	secret, err := d.cfg.Secrets.Secret(ctx, m.ID, m.SecretVersion)
	if err != nil {
		d.log.WarnContext(ctx, "webhook secret unavailable", "merchant_id", m.ID, "secret_version", m.SecretVersion, "error", err)
		return false, d.fail(ctx, inv, rec, failure{Reason: "signing secret unavailable", result: resultNoSecret})
	}
	br, err := d.breaker(m.ID)
	if err != nil {
		return false, err
	}
	done, err := br.Allow()
	if err != nil {
		return false, d.fail(ctx, inv, rec, failure{Reason: "circuit breaker is open", result: resultBreakerOpen})
	}
	rec.SecretVersion = m.SecretVersion
	status, perr := d.post(ctx, m.WebhookURL, inv.body, secret, m.SecretVersion)
	elapsed := d.cfg.Now().Sub(at)
	rec.DurationMs = elapsed.Milliseconds()
	delivered := perr == nil && status/100 == 2
	done(delivered)

	var f failure
	switch {
	case delivered:
		f.result = resultDelivered
	case perr != nil:
		f = failure{Reason: perr.Error(), result: resultNoResponse}
	default:
		code := status
		f = failure{StatusCode: &code, Reason: fmt.Sprintf("merchant endpoint answered %d", status), result: resultHTTPError}
	}
	d.duration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attribute.String("result", f.result)))
	if !delivered {
		return false, d.fail(ctx, inv, rec, f)
	}
	rec.Result, rec.StatusCode = journal.ResultDelivered, &status
	d.count(ctx, inv, resultDelivered)
	if inv.ev.Type == eventPaid && inv.paidAt != nil && !inv.replay {
		d.paidDelay.Record(ctx, d.cfg.Now().Sub(*inv.paidAt).Seconds())
	}
	d.log.InfoContext(ctx, "webhook delivered", "event_id", inv.ev.ID, "event_type", inv.ev.Type,
		"invoice_id", inv.invoiceID, "merchant_id", m.ID, "attempt", no, "status", status)
	return true, d.cfg.Journal.Record(ctx, rec)
}

// fail records the failed attempt and publishes the delivery to the next retry topic or, after the last one, to
// the DLQ.
func (d *Deliverer) fail(ctx context.Context, inv invoice, rec journal.Attempt, f failure) error {
	if len(f.Reason) > 256 {
		f.Reason = f.Reason[:256]
	}
	rec.Result, rec.StatusCode, rec.Reason = journal.ResultFailed, f.StatusCode, f.Reason
	d.count(ctx, inv, f.result)
	if err := d.cfg.Journal.Record(ctx, rec); err != nil {
		return err
	}
	if inv.replay {
		d.log.WarnContext(ctx, "webhook replay failed", "event_id", inv.ev.ID, "invoice_id", inv.invoiceID,
			"merchant_id", inv.merchantID, "reason", f.Reason)
		return nil
	}
	now := d.cfg.Now().UTC()
	var (
		topic string
		ev    kafka.Event
		err   error
	)
	if rec.Attempt <= len(Stages) {
		stage := Stages[rec.Attempt-1]
		topic = stage.Topic
		ev, err = kafka.NewEvent(Source, EventRetry, inv.invoiceID, 1, retryData{
			InvoiceID: inv.invoiceID, MerchantID: inv.merchantID, Attempt: rec.Attempt,
			NextAttemptAt: now.Add(stage.Delay), LastError: f, Event: inv.body,
		})
		ev.ID = fmt.Sprintf("%s:%d", inv.ev.ID, rec.Attempt)
	} else {
		topic = TopicDLQ
		first, ok, ferr := d.cfg.Journal.FirstAttempt(ctx, inv.ev.ID)
		if ferr != nil {
			return ferr
		}
		if !ok {
			first = inv.ev.Time
		}
		ev, err = kafka.NewEvent(Source, EventDeadLettered, inv.invoiceID, 1, deadLetteredData{
			InvoiceID: inv.invoiceID, MerchantID: inv.merchantID, Attempts: rec.Attempt,
			FirstAttemptAt: first.UTC(), FailedAt: now, LastError: f, Event: inv.body,
		})
		ev.ID = inv.ev.ID + ":dlq"
	}
	if err != nil {
		return err
	}
	ev.Time = now
	if err := d.cfg.Publisher.Publish(ctx, topic, ev); err != nil {
		return err
	}
	d.scheduled.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
	d.log.WarnContext(ctx, "webhook delivery failed", "event_id", inv.ev.ID, "event_type", inv.ev.Type,
		"invoice_id", inv.invoiceID, "merchant_id", inv.merchantID, "attempt", rec.Attempt, "reason", f.Reason,
		"next_topic", topic)
	return nil
}

func (d *Deliverer) count(ctx context.Context, inv invoice, result string) {
	d.deliveries.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result), attribute.String("event_type", inv.ev.Type)))
}

// breaker returns the breaker of the endpoint of a merchant; a merchant has one webhook URL.
func (d *Deliverer) breaker(merchantID string) (*breaker.Breaker, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if b, ok := d.breakers[merchantID]; ok {
		return b, nil
	}
	cfg := d.cfg.Breaker
	cfg.Dependency = "merchant:" + merchantID
	if cfg.Meter == nil {
		cfg.Meter = d.cfg.Meter
	}
	b, err := breaker.New(cfg)
	if err != nil {
		return nil, err
	}
	d.breakers[merchantID] = b
	return b, nil
}

// Sign returns the X-Weigate-Signature value: v1= and the lower-case hex HMAC-SHA256 of "<timestamp>.<body>"
// with the secret bytes as the key (contracts/openapi/webhook.yaml).
func Sign(secret []byte, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// post sends one signed attempt and returns the status. The error is a short reason without the URL, the secret or
// the signature.
func (d *Deliverer) post(ctx context.Context, url string, body, secret []byte, version int) (int, error) {
	ctx, span := otel.Tracer("github.com/m5tshift/weigate/services/webhook-service").Start(ctx, "webhook POST",
		trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("http.request.method", http.MethodPost)))
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		span.SetStatus(codes.Error, "invalid url")
		return 0, errors.New("invalid webhook URL")
	}
	ts := d.cfg.Now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, Sign(secret, ts, body))
	req.Header.Set(HeaderSecretVersion, strconv.Itoa(version))
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := d.cfg.HTTP.Do(req)
	if err != nil {
		span.SetStatus(codes.Error, "no response")
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return 0, errors.New("merchant endpoint timed out")
		}
		return 0, errors.New("merchant endpoint unreachable")
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode/100 != 2 {
		span.SetStatus(codes.Error, "non-2xx")
	}
	return resp.StatusCode, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
