package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// W3C trace context is always used for message headers, whatever the global propagator is.
var propagator = propagation.TraceContext{}

// headerCarrier adapts Kafka record headers to propagation.TextMapCarrier.
type headerCarrier struct{ headers *[]kgo.RecordHeader }

func (h headerCarrier) Get(key string) string {
	for _, rh := range *h.headers {
		if rh.Key == key {
			return string(rh.Value)
		}
	}
	return ""
}

func (h headerCarrier) Set(key, value string) {
	for i, rh := range *h.headers {
		if rh.Key == key {
			(*h.headers)[i].Value = []byte(value)
			return
		}
	}
	*h.headers = append(*h.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*h.headers))
	for _, rh := range *h.headers {
		keys = append(keys, rh.Key)
	}
	return keys
}

// InjectTrace writes the trace context of ctx (traceparent, tracestate) into the headers.
// Without a valid span in ctx nothing is written.
func InjectTrace(ctx context.Context, headers *[]kgo.RecordHeader) {
	propagator.Inject(ctx, headerCarrier{headers})
}

// ExtractTrace returns ctx carrying the remote span context read from the headers.
// Missing or malformed headers leave ctx unchanged.
func ExtractTrace(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	return propagator.Extract(ctx, headerCarrier{&headers})
}
