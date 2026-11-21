package outbox

import (
	"fmt"
	"testing"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// invoiceID returns a valid invoice UUID for n (the schemas require UUIDs).
func invoiceID(n int) string { return fmt.Sprintf("5d7c1a52-3b1e-4f0a-9a43-2f6f0c9b8e%02x", n) }

// event builds a valid invoice.created for the invoice (the Kafka key) whose amountUsdCents carries seq.
func event(t *testing.T, invoice string, seq int) kafka.Event {
	t.Helper()
	ev, err := kafka.NewEvent("/weigate/invoice-service", "invoice.created", invoice, 1, map[string]any{
		"invoiceId":      invoice,
		"merchantId":     "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b",
		"sequence":       1, // invoice.created is always sequence 1
		"status":         "NEW",
		"address":        "0x8ba1f109551bd432803012645ac136ddd64dba72",
		"asset":          map[string]any{"kind": "ETH", "decimals": 18},
		"amount":         "10000000000000000",
		"orderId":        "order-1001",
		"amountUsdCents": seq, // carries the ordering counter of the tests
		"rate":           "2500.00",
		"expiresAt":      "2026-11-15T10:15:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	return ev
}
