package kafka

import (
	"strings"
	"testing"
	"time"
)

func TestNewEvent(t *testing.T) {
	ev := validEvent(t)
	if ev.SpecVersion != "1.0" || ev.DataContentType != "application/json" || ev.Source != testSource {
		t.Fatalf("envelope: %+v", ev)
	}
	if ev.DataSchema != SchemaBaseURL+"invoice.created.v1.json" {
		t.Fatalf("dataschema: %s", ev.DataSchema)
	}
	if ev.ID == "" || time.Since(ev.Time) > time.Minute || ev.Time.Location() != time.UTC {
		t.Fatalf("id/time: %+v", ev)
	}
	if other := validEvent(t); other.ID == ev.ID {
		t.Fatal("event IDs must be unique")
	}
}

func TestNewEventRejectsBadData(t *testing.T) {
	if _, err := NewEvent(testSource, "invoice.created", "s", 1, make(chan int)); err == nil || !strings.Contains(err.Error(), "marshal") {
		t.Fatalf("unmarshalable data: %v", err)
	}
	if _, err := NewEvent(testSource, "invoice.created", "s", 1, []int{1}); err == nil {
		t.Fatal("non-object data must be rejected")
	}
	if _, err := NewEvent(testSource, "invoice.created", "s", 1, nil); err == nil {
		t.Fatal("null data must be rejected")
	}
}
