package kafka

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

const testSource = "/slopay/invoice-service"

// createdData is the data of the invoice.created example of contracts/asyncapi/invoice-service.yaml.
func createdData() map[string]any {
	return map[string]any{
		"invoiceId":      "5d7c1a52-3b1e-4f0a-9a43-2f6f0c9b8e11",
		"merchantId":     "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b",
		"sequence":       1,
		"status":         "NEW",
		"address":        "0x8ba1f109551bd432803012645ac136ddd64dba72",
		"asset":          map[string]any{"kind": "ETH", "decimals": 18},
		"amount":         "10000000000000000",
		"orderId":        "order-1001",
		"amountUsdCents": 2500,
		"rate":           "2500.00",
		"expiresAt":      "2026-11-15T10:15:00Z",
	}
}

func validEvent(t testing.TB) Event {
	t.Helper()
	ev, err := NewEvent(testSource, "invoice.created", "5d7c1a52-3b1e-4f0a-9a43-2f6f0c9b8e11", 1, createdData())
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func newTestCodec(t testing.TB) *Codec {
	t.Helper()
	c, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func spanContext(t testing.TB) trace.SpanContext {
	t.Helper()
	tid, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatal(err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
}

func ctxWithSpan(t testing.TB) context.Context {
	return trace.ContextWithSpanContext(context.Background(), spanContext(t))
}
