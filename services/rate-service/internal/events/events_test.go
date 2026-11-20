package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
)

var t0 = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

func codec(t *testing.T) *kafka.Codec {
	t.Helper()
	c, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestEventsMatchSchemas encodes every kind of event through the codec, which validates it against
// contracts/schemas (contract test of the producer side).
func TestEventsMatchSchemas(t *testing.T) {
	c := codec(t)
	for _, qt := range []rate.Quote{
		{Source: rate.Coinbase, Pair: rate.ETHUSD, Price: rate.MustPrice("3694.250000"), QuotedAt: t0},
		{Source: rate.Coinbase, Pair: rate.USDTUSD, Price: rate.MustPrice("0.999800"), QuotedAt: t0},
		{Source: rate.OKX, Pair: rate.ETHUSDT, Price: rate.MustPrice("3695"), QuotedAt: t0},
	} {
		ev, err := Tick(qt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Encode(ev); err != nil {
			t.Errorf("tick %s:%s: %v", qt.Source, qt.Pair, err)
		}
		if ev.Subject != string(qt.Source)+":"+string(qt.Pair) {
			t.Errorf("subject %q", ev.Subject)
		}
	}
	// OKX never quotes ETH-USD: the schema rejects it.
	ev, _ := Tick(rate.Quote{Source: rate.OKX, Pair: rate.ETHUSD, Price: rate.MustPrice("1"), QuotedAt: t0})
	if _, err := c.Encode(ev); !errors.Is(err, kafka.ErrInvalidEvent) {
		t.Errorf("okx ETH-USD tick accepted: %v", err)
	}

	r := rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("3694.6375"), QuotedAt: t0, Sources: []rate.Source{rate.Coinbase, rate.OKX}}
	at := t0.Add(2 * time.Second)
	ev, err := Latest(r, at)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := c.Encode(ev)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	back, err := c.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Snapshot(back)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rate.Price.String() != "3694.6375" || !s.UpdatedAt.Equal(at) || !s.Rate.QuotedAt.Equal(t0) || len(s.Rate.Sources) != 2 || s.Diverged || s.Rate.Pair != rate.ETHUSD {
		t.Errorf("round trip: %+v", s)
	}
}

func TestSnapshotErrors(t *testing.T) {
	good, _ := Latest(rate.Rate{Pair: rate.USDTUSD, Price: rate.MustPrice("1"), QuotedAt: t0, Sources: []rate.Source{rate.Coinbase}}, t0)
	cases := map[string]func(ev *kafka.Event){
		"type": func(ev *kafka.Event) { ev.Type = TypeTick },
		"data": func(ev *kafka.Event) { ev.Data = json.RawMessage(`[]`) },
		"price": func(ev *kafka.Event) {
			ev.Data = json.RawMessage(`{"pair":"ETH-USD","price":"x","quotedAt":"2026-11-28T12:00:00Z","sources":["okx"]}`)
		},
		"pair": func(ev *kafka.Event) {
			ev.Data = json.RawMessage(`{"pair":"ETH-USDT","price":"1","quotedAt":"2026-11-28T12:00:00Z","sources":["okx"]}`)
		},
		"no sources": func(ev *kafka.Event) {
			ev.Data = json.RawMessage(`{"pair":"ETH-USD","price":"1","quotedAt":"2026-11-28T12:00:00Z","sources":[]}`)
		},
	}
	for name, mut := range cases {
		ev := good
		mut(&ev)
		if _, err := Snapshot(ev); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := Latest(rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("1"), QuotedAt: t0}, t0); err != nil {
		t.Errorf("building an event never validates: %v", err)
	}
}

type fakeProducer struct {
	topics []string
	events []kafka.Event
	err    error
}

func (f *fakeProducer) Publish(_ context.Context, topic string, ev kafka.Event) error {
	f.topics = append(f.topics, topic)
	f.events = append(f.events, ev)
	return f.err
}

func TestPublisher(t *testing.T) {
	fp := &fakeProducer{}
	p := NewPublisher(fp)
	ctx := context.Background()
	if err := p.PublishTick(ctx, rate.Quote{Source: rate.OKX, Pair: rate.ETHUSDT, Price: rate.MustPrice("1"), QuotedAt: t0}); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishLatest(ctx, rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("2"), QuotedAt: t0, Sources: []rate.Source{rate.OKX}}, t0); err != nil {
		t.Fatal(err)
	}
	if strings.Join(fp.topics, ",") != "rate.ticks,rate.latest" || fp.events[0].Subject != "okx:ETH-USDT" || fp.events[1].Subject != "ETH-USD" {
		t.Errorf("published %v %+v", fp.topics, fp.events)
	}
	fp.err = errors.New("broker down")
	if err := p.PublishLatest(ctx, rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("2"), QuotedAt: t0, Sources: []rate.Source{rate.OKX}}, t0); err == nil {
		t.Error("publish error is lost")
	}
	if err := p.PublishTick(ctx, rate.Quote{}); err != nil && !errors.Is(err, fp.err) {
		t.Errorf("tick error: %v", err)
	}
}

func TestReaderApply(t *testing.T) {
	c := codec(t)
	cache := rate.NewCache()
	var logs bytes.Buffer
	r, err := NewReader(c, cache, telemetry.NewLogger(&logs, -4), []string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	record := func(rt rate.Rate, at time.Time) *kgo.Record {
		ev, err := Latest(rt, at)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := c.Encode(ev)
		if err != nil {
			t.Fatal(err)
		}
		return &kgo.Record{Topic: TopicLatest, Value: raw, Headers: []kgo.RecordHeader{
			{Key: "traceparent", Value: []byte("00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01")},
		}}
	}
	ctx := context.Background()
	newer := rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("3700"), QuotedAt: t0, Sources: []rate.Source{rate.Coinbase}}
	older := rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("3600"), QuotedAt: t0, Sources: []rate.Source{rate.Coinbase}}
	r.Apply(ctx, record(newer, t0.Add(time.Second)))
	r.Apply(ctx, record(older, t0)) // a replayed older event
	r.Apply(ctx, &kgo.Record{Topic: TopicLatest, Value: []byte(`{"not":"an event"}`)})
	s, ok := cache.Get(rate.ETHUSD)
	if !ok || s.Rate.Price.String() != "3700" {
		t.Errorf("cache: %+v %v", s, ok)
	}
	if !strings.Contains(logs.String(), "skipping invalid rate.latest record") {
		t.Errorf("invalid record not logged: %s", logs.String())
	}
	if !strings.Contains(logs.String(), `"trace_id":"0102030405060708090a0b0c0d0e0f10"`) {
		t.Errorf("trace context of the record not continued: %s", logs.String())
	}

	// Run stops with the context.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	r.Run(cctx)

	if _, err := NewReader(c, cache, nil, nil, kgo.ConsumerGroup("")); err == nil {
		// franz-go rejects a client without seed brokers only when it dials; either way NewReader returns.
		t.Log("reader without brokers created")
	}
}
