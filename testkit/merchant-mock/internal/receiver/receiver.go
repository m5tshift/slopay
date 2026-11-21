// Package receiver is the merchant side of the webhook contract (contracts/openapi/webhook.yaml): it verifies
// the signature and the timestamp against the current and the previous secret version, drops duplicates and
// stale webhooks, keeps a journal and counts the outcomes (SRS 4.1.10, 8.2б).
package receiver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Outcome is what happened to a webhook.
type Outcome string

// The outcomes; the same names are the values of the metric label and of the journal field.
const (
	Accepted             Outcome = "accepted"
	Duplicate            Outcome = "duplicate"
	Stale                Outcome = "stale"
	InvalidSignature     Outcome = "invalid_signature"
	InvalidTimestamp     Outcome = "invalid_timestamp"
	UnknownSecretVersion Outcome = "unknown_secret_version"
	Malformed            Outcome = "malformed"
	InjectedError        Outcome = "injected_error"
)

// Outcomes lists all outcomes.
var Outcomes = []Outcome{Accepted, Duplicate, Stale, InvalidSignature, InvalidTimestamp, UnknownSecretVersion, Malformed, InjectedError}

// MaxDelay is the largest injectable delay.
const MaxDelay = 60 * time.Second

var (
	// ErrUnknownEndpoint means no command has created the endpoint.
	ErrUnknownEndpoint = errors.New("unknown endpoint")
	// ErrInvalid means a command has invalid arguments.
	ErrInvalid = errors.New("invalid argument")

	nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	sigRe  = regexp.MustCompile(`^v1=[0-9a-f]{64}$`)
	tsRe   = regexp.MustCompile(`^(0|[1-9][0-9]{0,15})$`)
)

// Options configures a Receiver.
type Options struct {
	// TimestampTolerance is the largest difference between the timestamp header and the clock.
	TimestampTolerance time.Duration
	// PreviousSecretTTL is how long the previous secret version is accepted after a rotation.
	PreviousSecretTTL time.Duration
	// JournalSize and SeenEvents bound the memory per endpoint.
	JournalSize, SeenEvents int
	// Now and Sleep are replaceable in tests; Sleep returns early with the context error.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	// Registerer receives the metric; nil keeps it unregistered.
	Registerer prometheus.Registerer
}

// Headers are the X-SLOPay-* headers of a webhook as received.
type Headers struct {
	Timestamp, Signature, SecretVersion string
}

// Result is the answer to a webhook.
type Result struct {
	Status  int
	Outcome Outcome
	Message string
}

// Entry is a journal record. It never holds a secret or a signature.
type Entry struct {
	ReceivedAt    time.Time `json:"receivedAt"`
	Outcome       Outcome   `json:"outcome"`
	EventID       string    `json:"eventId,omitempty"`
	InvoiceID     string    `json:"invoiceId,omitempty"`
	Sequence      int64     `json:"sequence,omitempty"`
	Type          string    `json:"type,omitempty"`
	SecretVersion int       `json:"secretVersion,omitempty"`
}

// Fault is an injected failure of an endpoint.
type Fault struct {
	// Status is the status to answer with, 0 for none.
	Status int `json:"status,omitempty"`
	// DelayMs is the delay before the answer.
	DelayMs int `json:"delayMs"`
}

// View is the state of an endpoint without secrets.
type View struct {
	Name                  string           `json:"name"`
	SecretVersion         *int             `json:"secretVersion,omitempty"`
	PreviousSecretVersion *int             `json:"previousSecretVersion,omitempty"`
	Fault                 Fault            `json:"fault"`
	Counters              map[string]int64 `json:"counters"`
}

type secret struct {
	version int
	key     []byte
}

type endpoint struct {
	cur, prev *secret
	prevUntil time.Time
	fault     Fault
	counters  map[Outcome]int64
	journal   ring[Entry]
	seenQ     ring[string]
	seen      map[string]struct{}
	highest   map[string]int64
}

// Receiver holds the endpoints.
type Receiver struct {
	o       Options
	metric  *prometheus.CounterVec
	mu      sync.Mutex
	targets map[string]*endpoint
}

// New builds a Receiver.
func New(o Options) *Receiver {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = sleep
	}
	r := &Receiver{o: o, targets: map[string]*endpoint{}}
	r.metric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "merchant_mock_webhooks_total", Help: "Webhooks received by outcome and endpoint.",
	}, []string{"endpoint", "outcome"})
	if o.Registerer != nil {
		o.Registerer.MustRegister(r.metric)
	}
	return r
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ValidName reports whether name is a valid endpoint name.
func ValidName(name string) bool { return nameRe.MatchString(name) }

// get returns the endpoint, creating it when create is set; the caller holds r.mu.
func (r *Receiver) get(name string, create bool) (*endpoint, error) {
	if !ValidName(name) {
		return nil, ErrInvalid
	}
	e, ok := r.targets[name]
	if !ok {
		if !create {
			return nil, ErrUnknownEndpoint
		}
		e = &endpoint{
			counters: map[Outcome]int64{}, journal: newRing[Entry](r.o.JournalSize), seenQ: newRing[string](r.o.SeenEvents),
			seen: map[string]struct{}{}, highest: map[string]int64{},
		}
		r.targets[name] = e
	}
	return e, nil
}

// SetSecret sets the secret of an endpoint, creating it. A higher version becomes current and the former
// current one the previous version; the same version replaces the secret; a lower one is rejected.
func (r *Receiver) SetSecret(name string, version int, key string) (View, error) {
	if version < 1 || key == "" {
		return View{}, ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, err := r.get(name, true)
	if err != nil {
		return View{}, err
	}
	next := &secret{version: version, key: []byte(key)}
	switch {
	case e.cur == nil || e.cur.version == version:
		e.cur = next
	case version > e.cur.version:
		e.prev, e.prevUntil = e.cur, r.o.Now().Add(r.o.PreviousSecretTTL)
		e.cur = next
	default:
		return View{}, ErrInvalid
	}
	return r.view(name, e), nil
}

// SetFault sets the injected failure of an endpoint, creating it; the zero Fault removes it.
func (r *Receiver) SetFault(name string, f Fault) (View, error) {
	if (f.Status != 0 && (f.Status < 500 || f.Status > 599)) || f.DelayMs < 0 || time.Duration(f.DelayMs)*time.Millisecond > MaxDelay {
		return View{}, ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, err := r.get(name, true)
	if err != nil {
		return View{}, err
	}
	e.fault = f
	return r.view(name, e), nil
}

// Endpoint returns the state of an endpoint.
func (r *Receiver) Endpoint(name string) (View, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, err := r.get(name, false)
	if err != nil {
		return View{}, err
	}
	return r.view(name, e), nil
}

// Journal returns the last limit entries of an endpoint, oldest first.
func (r *Receiver) Journal(name string, limit int) ([]Entry, error) {
	if limit < 1 {
		return nil, ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, err := r.get(name, false)
	if err != nil {
		return nil, err
	}
	all := e.journal.items()
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all, nil
}

// Reset forgets all endpoints. The Prometheus counters stay: they are monotonic.
func (r *Receiver) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = map[string]*endpoint{}
}

func (r *Receiver) view(name string, e *endpoint) View {
	v := View{Name: name, Fault: e.fault, Counters: map[string]int64{}}
	for _, o := range Outcomes {
		v.Counters[string(o)] = e.counters[o]
	}
	if e.cur != nil {
		n := e.cur.version
		v.SecretVersion = &n
	}
	if e.prev != nil && r.o.Now().Before(e.prevUntil) {
		n := e.prev.version
		v.PreviousSecretVersion = &n
	}
	return v
}

// record counts and journals a webhook; the caller holds r.mu.
func (r *Receiver) record(name string, e *endpoint, entry Entry) {
	e.counters[entry.Outcome]++
	e.journal.push(entry)
	r.metric.WithLabelValues(name, string(entry.Outcome)).Inc()
}

// Sign returns the value of X-SLOPay-Signature for the timestamp and the raw body.
func Sign(key []byte, timestamp string, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(timestamp))
	m.Write([]byte{'.'})
	m.Write(body)
	return "v1=" + hex.EncodeToString(m.Sum(nil))
}

type event struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		InvoiceID string `json:"invoiceId"`
		Sequence  int64  `json:"sequence"`
	} `json:"data"`
}

// Handle processes a webhook of an endpoint. The error is ErrUnknownEndpoint or the context error.
func (r *Receiver) Handle(ctx context.Context, name string, h Headers, body []byte) (Result, error) {
	r.mu.Lock()
	e, err := r.get(name, false)
	var delay time.Duration
	if err == nil {
		delay = time.Duration(e.fault.DelayMs) * time.Millisecond
	}
	r.mu.Unlock()
	if err != nil {
		return Result{}, ErrUnknownEndpoint
	}
	if delay > 0 {
		if err := r.o.Sleep(ctx, delay); err != nil {
			return Result{}, err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// A reset may have happened during the delay.
	if e, err = r.get(name, false); err != nil {
		return Result{}, ErrUnknownEndpoint
	}
	now := r.o.Now()
	entry := Entry{ReceivedAt: now.UTC()}
	reject := func(o Outcome, status int, msg string) (Result, error) {
		entry.Outcome = o
		r.record(name, e, entry)
		return Result{Status: status, Outcome: o, Message: msg}, nil
	}

	if e.fault.Status != 0 {
		return reject(InjectedError, e.fault.Status, "Failure injected by a command")
	}
	version, verr := strconv.Atoi(h.SecretVersion)
	if verr != nil || version < 1 || !tsRe.MatchString(h.Timestamp) || !sigRe.MatchString(h.Signature) {
		return reject(Malformed, 400, "Missing or malformed X-SLOPay headers")
	}
	entry.SecretVersion = version
	var key []byte
	switch {
	case e.cur != nil && e.cur.version == version:
		key = e.cur.key
	case e.prev != nil && e.prev.version == version && now.Before(e.prevUntil):
		key = e.prev.key
	default:
		return reject(UnknownSecretVersion, 401, "Secret version is not accepted")
	}
	ts, _ := strconv.ParseInt(h.Timestamp, 10, 64) // matched tsRe: at most 16 digits
	if d := now.Sub(time.Unix(ts, 0)); d > r.o.TimestampTolerance || -d > r.o.TimestampTolerance {
		return reject(InvalidTimestamp, 401, "Timestamp is out of tolerance")
	}
	if !hmac.Equal([]byte(Sign(key, h.Timestamp, body)), []byte(strings.ToLower(h.Signature))) {
		return reject(InvalidSignature, 401, "Signature does not match")
	}

	var ev event
	if json.Unmarshal(body, &ev) != nil || ev.ID == "" || ev.Data.InvoiceID == "" || ev.Data.Sequence < 1 {
		return reject(Malformed, 400, "Body is not an invoice event")
	}
	entry.EventID, entry.InvoiceID, entry.Sequence, entry.Type = ev.ID, ev.Data.InvoiceID, ev.Data.Sequence, ev.Type

	if _, dup := e.seen[ev.ID]; dup {
		entry.Outcome = Duplicate
		r.record(name, e, entry)
		return Result{Status: 200, Outcome: Duplicate}, nil
	}
	if old, ok := e.seenQ.push(ev.ID); ok {
		delete(e.seen, old)
	}
	e.seen[ev.ID] = struct{}{}
	if ev.Data.Sequence < e.highest[ev.Data.InvoiceID] {
		entry.Outcome = Stale
		r.record(name, e, entry)
		return Result{Status: 200, Outcome: Stale}, nil
	}
	e.highest[ev.Data.InvoiceID] = ev.Data.Sequence
	entry.Outcome = Accepted
	r.record(name, e, entry)
	return Result{Status: 200, Outcome: Accepted}, nil
}
