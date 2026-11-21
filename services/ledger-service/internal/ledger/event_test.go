package ledger_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledgertest"
)

type asyncAPI struct {
	Channels map[string]struct {
		Address string `yaml:"address"`
	} `yaml:"channels"`
	Components struct {
		Messages map[string]struct {
			Name     string `yaml:"name"`
			Examples []struct {
				Name    string         `yaml:"name"`
				Payload map[string]any `yaml:"payload"`
			} `yaml:"examples"`
		} `yaml:"messages"`
	} `yaml:"components"`
}

func readAsyncAPI(t *testing.T, name string) asyncAPI {
	t.Helper()
	raw, err := os.ReadFile("../../../../contracts/asyncapi/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var doc asyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func toJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func normalize(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestTopicsMatchAsyncAPI(t *testing.T) {
	if got := readAsyncAPI(t, "ledger-service.yaml").Channels["ledgerEvents"].Address; got != ledger.TopicLedger {
		t.Errorf("ledger topic %q, contract %q", ledger.TopicLedger, got)
	}
	if got := readAsyncAPI(t, "invoice-service.yaml").Channels["invoiceEvents"].Address; got != ledger.TopicInvoices {
		t.Errorf("invoice topic %q, contract %q", ledger.TopicInvoices, got)
	}
}

// TestEventsMatchAsyncAPIExamples rebuilds every example of contracts/asyncapi/ledger-service.yaml from the
// transfer it describes and compares the whole CloudEvent: ids, accounts, fee and payload.
func TestEventsMatchAsyncAPIExamples(t *testing.T) {
	codec := newCodec(t)
	doc := readAsyncAPI(t, "ledger-service.yaml")
	byID := map[string]kafka.Event{}
	for _, ex := range doc.Components.Messages["receiptPosted"].Examples {
		ev, err := codec.Decode(toJSON(t, ex.Payload))
		if err != nil {
			t.Fatalf("example %s: %v", ex.Name, err)
		}
		var d struct {
			MerchantID uuid.UUID       `json:"merchantId"`
			InvoiceID  uuid.UUID       `json:"invoiceId"`
			Address    string          `json:"address"`
			Transfer   ledger.Transfer `json:"transfer"`
			PostedAt   time.Time       `json:"postedAt"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		p, err := ledger.NewPosting(ledger.Receipt{MerchantID: d.MerchantID, InvoiceID: d.InvoiceID, Address: d.Address, Transfer: d.Transfer}, 50, d.PostedAt)
		if err != nil {
			t.Fatal(err)
		}
		events, err := p.Events()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			byID[e.ID] = e
		}
	}
	checked := 0
	for _, msg := range []string{"receiptPosted", "feeAccrued"} {
		for _, ex := range doc.Components.Messages[msg].Examples {
			want := toJSON(t, ex.Payload)
			id, _ := ex.Payload["id"].(string)
			ev, ok := byID[id]
			if !ok {
				t.Errorf("%s/%s: no event with id %s", msg, ex.Name, id)
				continue
			}
			got, err := codec.Encode(ev)
			if err != nil {
				t.Fatalf("%s/%s: %v", msg, ex.Name, err)
			}
			if !reflect.DeepEqual(normalize(t, got), normalize(t, want)) {
				t.Errorf("%s/%s:\n got %s\nwant %s", msg, ex.Name, got, want)
			}
			checked++
		}
	}
	if checked < 3 {
		t.Errorf("only %d examples checked", checked)
	}
}

func TestEventsMatchSchemas(t *testing.T) {
	codec := newCodec(t)
	for _, tr := range []ledger.Transfer{ledgertest.ETH(1, 1, 199), ledgertest.ERC20(2, 2, 5, 0)} {
		p, err := ledger.NewPosting(receipt(tr), 50, t0)
		if err != nil {
			t.Fatal(err)
		}
		events, err := p.Events()
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 || events[0].Type != ledger.TypeReceiptPosted || events[1].Type != ledger.TypeFeeAccrued {
			t.Fatalf("events %+v", events)
		}
		for _, ev := range events {
			raw, err := codec.Encode(ev)
			if err != nil {
				t.Fatalf("%s does not match its schema: %v", ev.Type, err)
			}
			back, err := codec.Decode(raw)
			if err != nil || back.Subject != merchantA.String() || back.Source != ledger.Source || !back.Time.Equal(t0) {
				t.Errorf("decoded %+v, %v", back, err)
			}
		}
		// A zero fee is still accrued, so the fee events match the receipts one to one.
		var fee struct {
			Amount string `json:"amount"`
		}
		if err := json.Unmarshal(events[1].Data, &fee); err != nil || fee.Amount != "0" {
			t.Errorf("fee %q, %v", fee.Amount, err)
		}
	}
}

// TestInvoiceExamplesArePosted feeds every example of contracts/asyncapi/invoice-service.yaml to the service:
// PAID, UNDERPAID, LATE_PAID and invoice.late_transfer post their transfers, the other events post nothing.
func TestInvoiceExamplesArePosted(t *testing.T) {
	codec := newCodec(t)
	doc := readAsyncAPI(t, "invoice-service.yaml")
	posting := map[string]bool{}
	for name, msg := range doc.Components.Messages {
		if len(msg.Examples) == 0 {
			t.Errorf("%s has no examples", name)
		}
		for _, ex := range msg.Examples {
			ev, err := codec.Decode(toJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("%s/%s: %v", name, ex.Name, err)
			}
			var d struct {
				Transfers []json.RawMessage `json:"transfers"`
			}
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				t.Fatal(err)
			}
			store := ledger.NewMemStore(codec)
			s := newService(t, store)
			if err := s.Apply(context.Background(), ev); err != nil {
				t.Fatalf("%s/%s: %v", name, ex.Name, err)
			}
			got := len(store.Postings())
			switch msg.Name {
			case "invoice.paid", "invoice.underpaid", "invoice.late_paid", "invoice.late_transfer":
				posting[msg.Name] = true
				if got == 0 || got > len(d.Transfers) {
					t.Errorf("%s/%s: %d postings for %d transfers", name, ex.Name, got, len(d.Transfers))
				}
				if len(store.Events()) != 2*got {
					t.Errorf("%s/%s: %d events for %d postings", name, ex.Name, len(store.Events()), got)
				}
			default:
				if got != 0 {
					t.Errorf("%s/%s posted %d receipts", name, ex.Name, got)
				}
			}
		}
	}
	if len(posting) != 4 {
		t.Errorf("posting events with examples: %v", posting)
	}
}
