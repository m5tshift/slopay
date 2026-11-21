// Package kafka wraps franz-go for Weigate services (SRS 4.1.5, 4.5.1).
//
// Events are CloudEvents 1.0 in structured JSON mode (contracts/schemas/cloudevent.v1.json): the Kafka value
// is the whole envelope, the Kafka key equals subject. Every event is validated against the schema named by
// its dataschema on encode and on decode. The W3C trace context travels in the traceparent and tracestate
// message headers. The consumer commits offsets manually, after the handler has finished, so delivery is
// at-least-once; Deduper makes handlers idempotent by event.id.
package kafka
