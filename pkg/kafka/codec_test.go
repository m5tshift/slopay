package kafka

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/fstest"
)

func TestCodecRoundTrip(t *testing.T) {
	c := newTestCodec(t)
	ev := validEvent(t)
	raw, err := c.Encode(ev)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != ev.ID || got.Type != ev.Type || got.Subject != ev.Subject || !got.Time.Equal(ev.Time) {
		t.Fatalf("round trip: %+v != %+v", got, ev)
	}
	var data map[string]any
	if err := json.Unmarshal(got.Data, &data); err != nil || data["status"] != "NEW" {
		t.Fatalf("data: %v %v", data, err)
	}
}

func TestEncodeRejects(t *testing.T) {
	c := newTestCodec(t)
	tests := map[string]func(*Event){
		"empty data":        func(e *Event) { e.Data = nil },
		"unknown schema":    func(e *Event) { e.DataSchema = SchemaURL("no.such", 1) },
		"empty schema":      func(e *Event) { e.DataSchema = "" },
		"wrong type":        func(e *Event) { e.Type = "invoice.paid" },
		"wrong source":      func(e *Event) { e.Source = "/slopay/other-service" },
		"bad spec version":  func(e *Event) { e.SpecVersion = "0.3" },
		"missing id":        func(e *Event) { e.ID = "" },
		"bad data field":    func(e *Event) { e.Data = json.RawMessage(`{"invoiceId":"x"}`) },
		"subject not uuid":  func(e *Event) { e.Subject = "not-a-uuid" },
		"invalid data json": func(e *Event) { e.Data = json.RawMessage(`{`) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ev := validEvent(t)
			mutate(&ev)
			if _, err := c.Encode(ev); !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("want ErrInvalidEvent, got %v", err)
			}
		})
	}
}

func TestDecodeRejects(t *testing.T) {
	c := newTestCodec(t)
	for name, raw := range map[string]string{
		"not json":       `nope`,
		"no dataschema":  `{"specversion":"1.0"}`,
		"unknown schema": `{"dataschema":"https://example.com/x.json"}`,
		"invalid body":   `{"dataschema":"` + SchemaURL("invoice.created", 1) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := c.Decode([]byte(raw)); !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("want ErrInvalidEvent, got %v", err)
			}
		})
	}
}

func TestEveryCommittedSchemaCompiles(t *testing.T) {
	c := newTestCodec(t)
	if len(c.schemas) < 20 {
		t.Fatalf("only %d schemas loaded", len(c.schemas))
	}
	if _, ok := c.schemas[SchemaURL("invoice.paid", 1)]; !ok {
		t.Fatal("invoice.paid schema not registered under its $id")
	}
}

func TestNewCodecErrors(t *testing.T) {
	if _, err := newCodec(fstest.MapFS{}, "schemas"); err == nil {
		t.Fatal("missing directory must fail")
	}
	cases := map[string]fstest.MapFS{
		"bad json":      {"s/a.json": {Data: []byte(`{`)}},
		"no id":         {"s/a.json": {Data: []byte(`{"type":"object"}`)}},
		"not an object": {"s/a.json": {Data: []byte(`[]`)}},
		"bad ref":       {"s/a.json": {Data: []byte(`{"$id":"https://x/a.json","$ref":"https://x/missing.json"}`)}},
		"bad schema":    {"s/a.json": {Data: []byte(`{"$id":"https://x/a.json","type":5}`)}},
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newCodec(fsys, "s"); err == nil {
				t.Fatal("want error")
			}
		})
	}
	// Non-JSON files and subdirectories are ignored.
	ok := fstest.MapFS{
		"s/README.md":  {Data: []byte("x")},
		"s/sub/b.json": {Data: []byte(`{`)},
		"s/a.json":     {Data: []byte(`{"$id":"https://x/a.json","type":"object"}`)},
	}
	c, err := newCodec(ok, "s")
	if err != nil || len(c.schemas) != 1 {
		t.Fatalf("got %v, %v", c, err)
	}
	if !strings.HasPrefix(SchemaBaseURL, "https://") {
		t.Fatal("unexpected base URL")
	}
}
