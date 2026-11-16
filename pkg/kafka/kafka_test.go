package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestNewRecord(t *testing.T) {
	c := newTestCodec(t)
	ev := validEvent(t)
	rec, err := newRecord(ctxWithSpan(t), c, "invoice.events", ev)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != "invoice.events" || string(rec.Key) != ev.Subject {
		t.Fatalf("topic/key: %s %s", rec.Topic, rec.Key)
	}
	if headerValue(rec, "content-type") != ContentType {
		t.Fatalf("content-type: %q", headerValue(rec, "content-type"))
	}
	if headerValue(rec, "traceparent") != "00-"+testTraceID+"-"+testSpanID+"-01" {
		t.Fatalf("traceparent: %q", headerValue(rec, "traceparent"))
	}
	if got, err := c.Decode(rec.Value); err != nil || got.ID != ev.ID {
		t.Fatalf("value: %v %v", got, err)
	}

	ev.Type = "invoice.paid"
	if _, err := newRecord(context.Background(), c, "t", ev); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("got %v", err)
	}
}

func TestProducerConstructorAndInvalidPublish(t *testing.T) {
	c := newTestCodec(t)
	if _, err := NewProducer(c, nil); !errors.Is(err, errNoBrokers) {
		t.Fatalf("got %v", err)
	}
	if _, err := NewProducer(c, []string{"x:1"}, kgo.ProduceRequestTimeout(0)); err == nil {
		t.Fatal("invalid franz-go option must fail")
	}
	p, err := NewProducer(c, []string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ev := validEvent(t)
	ev.DataSchema = "unknown"
	if err := p.Publish(context.Background(), "t", ev); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumerConstructor(t *testing.T) {
	c := newTestCodec(t)
	b := []string{"127.0.0.1:1"}
	if _, err := NewConsumer(c, nil, nil, "g", []string{"t"}); !errors.Is(err, errNoBrokers) {
		t.Fatalf("got %v", err)
	}
	if _, err := NewConsumer(c, nil, b, "", []string{"t"}); err == nil {
		t.Fatal("empty group must fail")
	}
	if _, err := NewConsumer(c, nil, b, "g", nil); err == nil {
		t.Fatal("no topics must fail")
	}
	if _, err := NewConsumer(c, nil, b, "g", []string{"t"}, kgo.ProduceRequestTimeout(0)); err == nil {
		t.Fatal("invalid franz-go option must fail")
	}
}

func TestRunReturnsNilWhenStopped(t *testing.T) {
	c := newTestCodec(t)
	handler := func(context.Context, Event) error { t.Fatal("unexpected event"); return nil }

	cons, err := NewConsumer(c, nil, []string{"127.0.0.1:1"}, "g", []string{"t"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cons.Run(ctx, handler); err != nil {
		t.Fatalf("cancelled: %v", err)
	}
	cons.Close()
	if err := cons.Run(context.Background(), handler); err != nil {
		t.Fatalf("closed: %v", err)
	}
}

func testConsumer(t *testing.T) (*Consumer, *bytes.Buffer) {
	var buf bytes.Buffer
	return &Consumer{codec: newTestCodec(t), log: slog.New(slog.NewTextHandler(&buf, nil))}, &buf
}

func TestProcessDeliversEventWithRemoteTrace(t *testing.T) {
	cons, _ := testConsumer(t)
	rec, err := newRecord(ctxWithSpan(t), cons.codec, "invoice.events", validEvent(t))
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	var sc trace.SpanContext
	err = cons.process(context.Background(), rec, func(ctx context.Context, ev Event) error {
		got, sc = ev, trace.SpanContextFromContext(ctx)
		return nil
	})
	if err != nil || got.Type != "invoice.created" {
		t.Fatalf("event %+v, err %v", got, err)
	}
	// Without an SDK the consumer span is non-recording and passes the remote context through.
	if sc.TraceID().String() != testTraceID {
		t.Fatalf("handler context lost the trace: %+v", sc)
	}
}

func TestProcessHandlerError(t *testing.T) {
	cons, _ := testConsumer(t)
	rec, err := newRecord(context.Background(), cons.codec, "t", validEvent(t))
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if err := cons.process(context.Background(), rec, func(context.Context, Event) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}

func TestProcessSkipsInvalidEvent(t *testing.T) {
	cons, buf := testConsumer(t)
	rec := &kgo.Record{Topic: "t", Value: []byte("garbage")}
	err := cons.process(context.Background(), rec, func(context.Context, Event) error { t.Fatal("handler called"); return nil })
	if err != nil {
		t.Fatalf("invalid event must be skipped, got %v", err)
	}
	if !strings.Contains(buf.String(), "skipping invalid event") {
		t.Fatalf("not logged: %s", buf.String())
	}
}

func TestHandleBatchStopsAtFirstError(t *testing.T) {
	cons, buf := testConsumer(t)
	mk := func(offset int64) *kgo.Record {
		rec, err := newRecord(context.Background(), cons.codec, "t", validEvent(t))
		if err != nil {
			t.Fatal(err)
		}
		rec.Offset = offset
		return rec
	}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "t", Partitions: []kgo.FetchPartition{
		{Partition: 0, Records: []*kgo.Record{mk(0), mk(1), mk(2)}},
	}}}}, {Topics: []kgo.FetchTopic{{Topic: "t", Partitions: []kgo.FetchPartition{
		{Partition: 1, Err: errors.New("fetch broke")},
	}}}}}
	boom := errors.New("boom")
	var seen int
	done, err := cons.handleBatch(context.Background(), fetches, func(context.Context, Event) error {
		seen++
		if seen == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || len(done) != 1 || done[0].Offset != 0 || seen != 2 {
		t.Fatalf("done=%d seen=%d err=%v", len(done), seen, err)
	}
	if !strings.Contains(buf.String(), "fetch broke") {
		t.Fatalf("fetch error not logged: %s", buf.String())
	}

	done, err = cons.handleBatch(context.Background(), fetches, func(context.Context, Event) error { return nil })
	if err != nil || len(done) != 3 {
		t.Fatalf("done=%d err=%v", len(done), err)
	}
}
