//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var brokers []string

func TestMain(m *testing.M) {
	ctx := context.Background()
	ctr, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		os.Exit(1)
	}
	if brokers, err = ctr.Brokers(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "brokers:", err)
		_ = testcontainers.TerminateContainer(ctr)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(ctr)
	os.Exit(code)
}

const eventTimeout = 60 * time.Second

func createTopic(t *testing.T, name string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, 1
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil {
			t.Fatalf("create topic %s: %v", name, err)
		}
	}
}

func traceIDOf(ctx context.Context) string {
	return trace.SpanContextFromContext(ctx).TraceID().String()
}

func newProducer(t *testing.T) *Producer {
	t.Helper()
	p, err := NewProducer(newTestCodec(t), brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func newConsumer(t *testing.T, group, topic string) *Consumer {
	t.Helper()
	c, err := NewConsumer(newTestCodec(t), nil, brokers, group, []string{topic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// runConsumer runs the consumer in the background; the result of Run arrives on the channel.
func runConsumer(ctx context.Context, c *Consumer, h Handler) <-chan error {
	out := make(chan error, 1)
	go func() { out <- c.Run(ctx, h) }()
	return out
}

func waitEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

func TestRedeliveryAfterHandlerFailure(t *testing.T) {
	topic, group := "redelivery.events", "redelivery-group"
	createTopic(t, topic)
	ev := validEvent(t)
	if err := newProducer(t).Publish(context.Background(), topic, ev); err != nil {
		t.Fatal(err)
	}

	// The first member fails: the offset must stay uncommitted.
	boom := errors.New("boom")
	first := newConsumer(t, group, topic)
	select {
	case err := <-runConsumer(context.Background(), first, func(context.Context, Event) error { return boom }):
		if !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("first consumer did not fail")
	}
	first.Close()

	// The second member of the same group gets the event again and commits it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan Event, 1)
	second := newConsumer(t, group, topic)
	done := runConsumer(ctx, second, func(_ context.Context, e Event) error { got <- e; return nil })
	if redelivered := waitEvent(t, got); redelivered.ID != ev.ID {
		t.Fatalf("redelivered %s, want %s", redelivered.ID, ev.ID)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	second.Close()

	// After the commit the group does not see the event again.
	third := newConsumer(t, group, topic)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()
	var extra atomic.Int32
	err := <-runConsumer(ctx3, third, func(context.Context, Event) error { extra.Add(1); return nil })
	if err != nil || extra.Load() != 0 {
		t.Fatalf("committed event redelivered: n=%d err=%v", extra.Load(), err)
	}
}

func TestDuplicatePublishIsHandledOnce(t *testing.T) {
	topic := "dedup.events"
	createTopic(t, topic)
	p := newProducer(t)
	ev := validEvent(t)
	last := validEvent(t)
	// The same fact published twice (outbox retry) and a marker event after it.
	for _, e := range []Event{ev, ev, last} {
		if err := p.Publish(context.Background(), topic, e); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handled atomic.Int32
	marker := make(chan Event, 1)
	h := Deduper{Store: &MemoryStore{}}.Wrap(func(_ context.Context, e Event) error {
		handled.Add(1)
		if e.ID == last.ID {
			marker <- e
		}
		return nil
	})
	done := runConsumer(ctx, newConsumer(t, "dedup-group", topic), h)
	waitEvent(t, marker)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if handled.Load() != 2 {
		t.Fatalf("handled %d events, want 2 (duplicate skipped)", handled.Load())
	}
}

func TestTraceContextThroughBroker(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	topic := "trace.events"
	createTopic(t, topic)

	ctx, root := tp.Tracer("test").Start(context.Background(), "create invoice")
	if err := newProducer(t).Publish(ctx, topic, validEvent(t)); err != nil {
		t.Fatal(err)
	}
	root.End()

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	traceIDs := make(chan string, 1)
	done := runConsumer(cctx, newConsumer(t, "trace-group", topic), func(ctx context.Context, _ Event) error {
		traceIDs <- traceIDOf(ctx)
		return nil
	})
	select {
	case id := <-traceIDs:
		if id != root.SpanContext().TraceID().String() {
			t.Fatalf("handler trace %s, producer trace %s", id, root.SpanContext().TraceID())
		}
	case <-time.After(eventTimeout):
		t.Fatal("timed out")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	var publish, process bool
	for _, s := range rec.Ended() {
		switch {
		case strings.HasSuffix(s.Name(), " publish"):
			publish = s.Parent().SpanID() == root.SpanContext().SpanID()
		case strings.HasSuffix(s.Name(), " process"):
			process = s.Parent().IsRemote() && s.SpanContext().TraceID() == root.SpanContext().TraceID()
		}
	}
	if !publish || !process {
		t.Fatalf("span chain broken: publish child of root=%v, process child of remote=%v", publish, process)
	}
}
