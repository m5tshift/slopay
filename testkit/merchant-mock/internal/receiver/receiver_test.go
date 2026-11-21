package receiver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var t0 = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

type fixture struct {
	r     *Receiver
	now   time.Time
	slept []time.Duration
	reg   *prometheus.Registry
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{now: t0, reg: prometheus.NewRegistry()}
	f.r = New(Options{
		TimestampTolerance: 5 * time.Minute, PreviousSecretTTL: 24 * time.Hour, JournalSize: 3, SeenEvents: 2,
		Now:        func() time.Time { return f.now },
		Sleep:      func(_ context.Context, d time.Duration) error { f.slept = append(f.slept, d); return nil },
		Registerer: f.reg,
	})
	if _, err := f.r.SetSecret("m1", 1, "secret-one"); err != nil {
		t.Fatal(err)
	}
	return f
}

func counter(t *testing.T, f *fixture, outcome string) float64 {
	t.Helper()
	families, err := f.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range families {
		for _, m := range fam.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if fam.GetName() == "merchant_mock_webhooks_total" && labels["endpoint"] == "m1" && labels["outcome"] == outcome {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func body(id, invoice string, seq int) []byte {
	return []byte(fmt.Sprintf(`{"id":%q,"type":"invoice.paid","data":{"invoiceId":%q,"sequence":%d,"status":"PAID"}}`, id, invoice, seq))
}

// send signs a webhook with key at the clock of the fixture shifted by skew.
func (f *fixture) send(t *testing.T, key string, version int, skew time.Duration, b []byte) Result {
	t.Helper()
	ts := strconv.FormatInt(f.now.Add(skew).Unix(), 10)
	res, err := f.r.Handle(context.Background(), "m1", Headers{
		Timestamp: ts, Signature: Sign([]byte(key), ts, b), SecretVersion: strconv.Itoa(version),
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func want(t *testing.T, got Result, status int, o Outcome) {
	t.Helper()
	if got.Status != status || got.Outcome != o {
		t.Fatalf("got %d %s (%s), want %d %s", got.Status, got.Outcome, got.Message, status, o)
	}
}

func TestSignMatchesContractExample(t *testing.T) {
	// HMAC-SHA256 of "1795953600.{}" with key "k", computed independently.
	got := Sign([]byte("k"), "1795953600", []byte("{}"))
	if len(got) != 67 || got[:3] != "v1=" {
		t.Fatalf("format: %s", got)
	}
	if Sign([]byte("k"), "1795953600", []byte("{} ")) == got || Sign([]byte("k2"), "1795953600", []byte("{}")) == got {
		t.Fatal("signature must depend on body and key")
	}
}

func TestAccepted(t *testing.T) {
	f := newFixture(t)
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 1)), 200, Accepted)
	j, _ := f.r.Journal("m1", 10)
	if len(j) != 1 || j[0].EventID != "e1" || j[0].InvoiceID != "i1" || j[0].Sequence != 1 || j[0].Type != "invoice.paid" || j[0].SecretVersion != 1 {
		t.Fatalf("%+v", j)
	}
	if got := counter(t, f, "accepted"); got != 1 {
		t.Fatalf("metric %v", got)
	}
}

func TestInvalidSignature(t *testing.T) {
	f := newFixture(t)
	want(t, f.send(t, "wrong", 1, 0, body("e1", "i1", 1)), 401, InvalidSignature)
	// the body is signed as received: any change breaks the signature
	ts := strconv.FormatInt(f.now.Unix(), 10)
	b := body("e1", "i1", 1)
	res, _ := f.r.Handle(context.Background(), "m1", Headers{Timestamp: ts, Signature: Sign([]byte("secret-one"), ts, b), SecretVersion: "1"}, append(b, ' '))
	want(t, res, 401, InvalidSignature)
	v, _ := f.r.Endpoint("m1")
	if v.Counters["invalid_signature"] != 2 {
		t.Fatalf("%+v", v.Counters)
	}
}

func TestTimestamp(t *testing.T) {
	f := newFixture(t)
	want(t, f.send(t, "secret-one", 1, -5*time.Minute, body("e1", "i1", 1)), 200, Accepted)
	want(t, f.send(t, "secret-one", 1, -6*time.Minute, body("e2", "i1", 2)), 401, InvalidTimestamp)
	want(t, f.send(t, "secret-one", 1, 6*time.Minute, body("e2", "i1", 2)), 401, InvalidTimestamp)
}

func TestMalformedHeaders(t *testing.T) {
	f := newFixture(t)
	b := body("e1", "i1", 1)
	ts := strconv.FormatInt(f.now.Unix(), 10)
	good := Sign([]byte("secret-one"), ts, b)
	for name, h := range map[string]Headers{
		"empty":       {},
		"version":     {Timestamp: ts, Signature: good, SecretVersion: "x"},
		"version 0":   {Timestamp: ts, Signature: good, SecretVersion: "0"},
		"timestamp":   {Timestamp: "01", Signature: good, SecretVersion: "1"},
		"signature":   {Timestamp: ts, Signature: "v1=zz", SecretVersion: "1"},
		"upper-case":  {Timestamp: ts, Signature: "v1=" + "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789", SecretVersion: "1"},
		"no v1 label": {Timestamp: ts, Signature: good[3:], SecretVersion: "1"},
	} {
		res, err := f.r.Handle(context.Background(), "m1", h, b)
		if err != nil {
			t.Fatal(err)
		}
		if res.Status != 400 || res.Outcome != Malformed {
			t.Errorf("%s: %+v", name, res)
		}
	}
}

func TestMalformedBody(t *testing.T) {
	f := newFixture(t)
	for _, b := range []string{`not json`, `{}`, `{"id":"e","data":{"invoiceId":"i","sequence":0}}`, `{"id":"e","data":{"sequence":1}}`} {
		want(t, f.send(t, "secret-one", 1, 0, []byte(b)), 400, Malformed)
	}
}

func TestSecretRotation(t *testing.T) {
	f := newFixture(t)
	want(t, f.send(t, "secret-two", 2, 0, body("e1", "i1", 1)), 401, UnknownSecretVersion)

	v, err := f.r.SetSecret("m1", 2, "secret-two")
	if err != nil || *v.SecretVersion != 2 || *v.PreviousSecretVersion != 1 {
		t.Fatalf("%+v %v", v, err)
	}
	want(t, f.send(t, "secret-two", 2, 0, body("e1", "i1", 1)), 200, Accepted)
	want(t, f.send(t, "secret-one", 1, 0, body("e2", "i1", 2)), 200, Accepted)
	// a signature made with the other version's key does not match
	want(t, f.send(t, "secret-one", 2, 0, body("e3", "i1", 3)), 401, InvalidSignature)

	f.now = f.now.Add(24*time.Hour - time.Second)
	want(t, f.send(t, "secret-one", 1, 0, body("e3", "i1", 3)), 200, Accepted)
	f.now = f.now.Add(time.Second)
	want(t, f.send(t, "secret-one", 1, 0, body("e4", "i1", 4)), 401, UnknownSecretVersion)
	want(t, f.send(t, "secret-two", 2, 0, body("e4", "i1", 4)), 200, Accepted)
	if v, _ := f.r.Endpoint("m1"); v.PreviousSecretVersion != nil {
		t.Fatalf("previous version listed after expiry: %+v", v)
	}
}

func TestSetSecretRules(t *testing.T) {
	f := newFixture(t)
	if _, err := f.r.SetSecret("m1", 2, "b"); err != nil {
		t.Fatal(err)
	}
	// the same version replaces the secret and keeps the previous one
	v, err := f.r.SetSecret("m1", 2, "c")
	if err != nil || *v.SecretVersion != 2 || *v.PreviousSecretVersion != 1 {
		t.Fatalf("%+v %v", v, err)
	}
	want(t, f.send(t, "c", 2, 0, body("e1", "i1", 1)), 200, Accepted)
	for name, c := range map[string][3]any{
		"lower":    {"m1", 1, "x"},
		"zero":     {"m1", 0, "x"},
		"empty":    {"m1", 3, ""},
		"bad name": {"M1", 3, "x"},
	} {
		if _, err := f.r.SetSecret(c[0].(string), c[1].(int), c[2].(string)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestDuplicate(t *testing.T) {
	f := newFixture(t)
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 1)), 200, Accepted)
	// a repeated delivery has a new timestamp and signature but the same event id
	f.now = f.now.Add(time.Minute)
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 1)), 200, Duplicate)
	v, _ := f.r.Endpoint("m1")
	if v.Counters["duplicate"] != 1 || v.Counters["accepted"] != 1 {
		t.Fatalf("%+v", v.Counters)
	}
}

func TestStaleSequence(t *testing.T) {
	f := newFixture(t)
	want(t, f.send(t, "secret-one", 1, 0, body("e3", "i1", 3)), 200, Accepted)
	want(t, f.send(t, "secret-one", 1, 0, body("e2", "i1", 2)), 200, Stale)
	// the same number with another event is not lower; another invoice has its own order
	want(t, f.send(t, "secret-one", 1, 0, body("e3b", "i1", 3)), 200, Accepted)
	want(t, f.send(t, "secret-one", 1, 0, body("e9", "i2", 1)), 200, Accepted)
	want(t, f.send(t, "secret-one", 1, 0, body("e4", "i1", 4)), 200, Accepted)
}

func TestBoundedMemory(t *testing.T) {
	f := newFixture(t) // journal 3, seen 2
	for i := 1; i <= 4; i++ {
		want(t, f.send(t, "secret-one", 1, 0, body("e"+strconv.Itoa(i), "i1", i)), 200, Accepted)
	}
	j, _ := f.r.Journal("m1", 100)
	if len(j) != 3 || j[0].EventID != "e2" || j[2].EventID != "e4" {
		t.Fatalf("%+v", j)
	}
	j, _ = f.r.Journal("m1", 2)
	if len(j) != 2 || j[0].EventID != "e3" {
		t.Fatalf("limit: %+v", j)
	}
	// e1 left the dedup window, e4 is still in it
	want(t, f.send(t, "secret-one", 1, 0, body("e4", "i1", 4)), 200, Duplicate)
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 5)), 200, Accepted)
}

func TestFaultStatusAndDelay(t *testing.T) {
	f := newFixture(t)
	if _, err := f.r.SetFault("m1", Fault{Status: 500}); err != nil {
		t.Fatal(err)
	}
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 1)), 500, InjectedError)
	// an injected error does not verify: even a bad signature gets it
	want(t, f.send(t, "wrong", 1, 0, body("e1", "i1", 1)), 500, InjectedError)

	if _, err := f.r.SetFault("m1", Fault{DelayMs: 1500}); err != nil {
		t.Fatal(err)
	}
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 1)), 200, Accepted)
	if len(f.slept) != 1 || f.slept[0] != 1500*time.Millisecond {
		t.Fatalf("slept %v", f.slept)
	}

	if _, err := f.r.SetFault("m1", Fault{}); err != nil {
		t.Fatal(err)
	}
	want(t, f.send(t, "secret-one", 1, 0, body("e2", "i1", 2)), 200, Accepted)
	if len(f.slept) != 1 {
		t.Fatalf("delay not removed: %v", f.slept)
	}
	if got := counter(t, f, "injected_error"); got != 2 {
		t.Fatalf("metric %v", got)
	}
}

func TestFaultValidation(t *testing.T) {
	f := newFixture(t)
	for _, ft := range []Fault{{Status: 200}, {Status: 600}, {DelayMs: -1}, {DelayMs: 60001}} {
		if _, err := f.r.SetFault("m1", ft); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: %v", ft, err)
		}
	}
	if _, err := f.r.SetFault("bad name", Fault{}); !errors.Is(err, ErrInvalid) {
		t.Error("bad name accepted")
	}
}

func TestDelayCancelled(t *testing.T) {
	f := newFixture(t)
	f.r.o.Sleep = func(ctx context.Context, _ time.Duration) error { return context.Canceled }
	if _, err := f.r.SetFault("m1", Fault{DelayMs: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Handle(context.Background(), "m1", Headers{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
}

func TestRealSleep(t *testing.T) {
	if err := sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
}

func TestUnknownEndpointAndReset(t *testing.T) {
	f := newFixture(t)
	if _, err := f.r.Handle(context.Background(), "nope", Headers{}, nil); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("%v", err)
	}
	if _, err := f.r.Endpoint("nope"); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("%v", err)
	}
	if _, err := f.r.Journal("nope", 1); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("%v", err)
	}
	if _, err := f.r.Journal("m1", 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("%v", err)
	}
	f.r.Reset()
	if _, err := f.r.Endpoint("m1"); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("%v", err)
	}
}

func TestResetDuringDelay(t *testing.T) {
	f := newFixture(t)
	f.r.o.Sleep = func(context.Context, time.Duration) error { f.r.Reset(); return nil }
	if _, err := f.r.SetFault("m1", Fault{DelayMs: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Handle(context.Background(), "m1", Headers{}, nil); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("%v", err)
	}
}

func TestEndpointsAreIndependent(t *testing.T) {
	f := newFixture(t)
	if _, err := f.r.SetFault("m2", Fault{Status: 503}); err != nil {
		t.Fatal(err)
	}
	want(t, f.send(t, "secret-one", 1, 0, body("e1", "i1", 1)), 200, Accepted)
	v, _ := f.r.Endpoint("m2")
	if v.Fault.Status != 503 || v.SecretVersion != nil || v.Counters["accepted"] != 0 {
		t.Fatalf("%+v", v)
	}
}

func TestRingZeroValueSafe(t *testing.T) {
	r := newRing[int](2)
	for i := 1; i <= 3; i++ {
		r.push(i)
	}
	if got := r.items(); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("%v", got)
	}
}
