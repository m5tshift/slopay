package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceRoundTrip(t *testing.T) {
	var headers []kgo.RecordHeader
	InjectTrace(ctxWithSpan(t), &headers)
	if len(headers) != 1 || headers[0].Key != "traceparent" ||
		string(headers[0].Value) != "00-"+testTraceID+"-"+testSpanID+"-01" {
		t.Fatalf("headers: %+v", headers)
	}
	sc := trace.SpanContextFromContext(ExtractTrace(context.Background(), headers))
	if !sc.IsValid() || !sc.IsRemote() || sc.TraceID().String() != testTraceID || sc.SpanID().String() != testSpanID || !sc.IsSampled() {
		t.Fatalf("extracted: %+v", sc)
	}
}

func TestTraceState(t *testing.T) {
	ts, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext(t).WithTraceState(ts))
	var headers []kgo.RecordHeader
	InjectTrace(ctx, &headers)
	got := trace.SpanContextFromContext(ExtractTrace(context.Background(), headers))
	if got.TraceState().String() != "vendor=value" {
		t.Fatalf("tracestate: %q", got.TraceState())
	}
}

func TestInjectWithoutSpanWritesNothing(t *testing.T) {
	var headers []kgo.RecordHeader
	InjectTrace(context.Background(), &headers)
	if len(headers) != 0 {
		t.Fatalf("headers: %+v", headers)
	}
}

func TestInjectOverwritesExistingHeader(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: "traceparent", Value: []byte("stale")}, {Key: "x", Value: []byte("1")}}
	InjectTrace(ctxWithSpan(t), &headers)
	if len(headers) != 2 || string(headers[0].Value) == "stale" {
		t.Fatalf("headers: %+v", headers)
	}
	if got := (headerCarrier{&headers}).Keys(); len(got) != 2 || got[1] != "x" {
		t.Fatalf("keys: %v", got)
	}
}

func TestExtractIgnoresMissingAndMalformed(t *testing.T) {
	for _, headers := range [][]kgo.RecordHeader{
		nil,
		{{Key: "traceparent", Value: []byte("garbage")}},
	} {
		if sc := trace.SpanContextFromContext(ExtractTrace(context.Background(), headers)); sc.IsValid() {
			t.Fatalf("unexpected span context %+v", sc)
		}
	}
}
