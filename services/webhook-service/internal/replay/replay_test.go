package replay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
)

const (
	inv1 = "0d6f1c1e-6a0b-4d44-9a8e-3a8f4f0f5b21"
	inv2 = "1d6f1c1e-6a0b-4d44-9a8e-3a8f4f0f5b22"
	mer1 = "2d6f1c1e-6a0b-4d44-9a8e-3a8f4f0f5b23"
	mer2 = "3d6f1c1e-6a0b-4d44-9a8e-3a8f4f0f5b24"
)

type fakeDLQ struct {
	events []kafka.Event
	err    error
}

func (f fakeDLQ) Events(context.Context) ([]kafka.Event, error) { return f.events, f.err }

type fakeDeliverer struct {
	ok    map[string]bool
	err   map[string]error
	calls []string
}

func (f *fakeDeliverer) Replay(_ context.Context, ev kafka.Event) (bool, error) {
	f.calls = append(f.calls, ev.ID)
	return f.ok[ev.ID], f.err[ev.ID]
}

type fakeJournal struct {
	done map[string]bool
	err  error
}

func (f fakeJournal) Delivered(_ context.Context, id string) (bool, error) { return f.done[id], f.err }

func dead(id, invoice, merchant string, at time.Duration) kafka.Event {
	data, _ := json.Marshal(map[string]string{"invoiceId": invoice, "merchantId": merchant})
	return kafka.Event{ID: id, Data: data, Time: time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC).Add(at)}
}

func call(t *testing.T, h *Handler, body string) (int, map[string]any, http.Header) {
	t.Helper()
	mux := http.NewServeMux()
	h.Register(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/dlq/replay", strings.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out, w.Header()
}

func newHandler(events []kafka.Event, d *fakeDeliverer, j fakeJournal) *Handler {
	return &Handler{DLQ: fakeDLQ{events: events}, Deliverer: d, Journal: j, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestReplayFilters(t *testing.T) {
	events := []kafka.Event{
		dead("c:dlq", inv2, mer2, 2*time.Second),
		dead("a:dlq", inv1, mer1, 0),
		dead("b:dlq", inv2, mer1, time.Second),
	}
	tests := []struct {
		name, body string
		want       []string
	}{
		{"no filter, oldest first", `{}`, []string{"a:dlq", "b:dlq", "c:dlq"}},
		{"empty body", ``, []string{"a:dlq", "b:dlq", "c:dlq"}},
		{"invoice", `{"invoiceId":"` + inv2 + `"}`, []string{"b:dlq", "c:dlq"}},
		{"merchant", `{"merchantId":"` + mer1 + `"}`, []string{"a:dlq", "b:dlq"}},
		{"intersection", `{"invoiceId":"` + inv2 + `","merchantId":"` + mer1 + `"}`, []string{"b:dlq"}},
		{"limit", `{"limit":2}`, []string{"a:dlq", "b:dlq"}},
		{"nothing matches", `{"invoiceId":"` + mer1 + `"}`, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &fakeDeliverer{ok: map[string]bool{"a:dlq": true, "b:dlq": true, "c:dlq": true}}
			code, out, _ := call(t, newHandler(events, d, fakeJournal{}), tc.body)
			if code != 200 || strings.Join(d.calls, ",") != strings.Join(tc.want, ",") ||
				out["selected"] != float64(len(tc.want)) || out["delivered"] != float64(len(tc.want)) || out["failed"] != 0.0 {
				t.Errorf("%d %v calls %v", code, out, d.calls)
			}
		})
	}
}

func TestReplayCountsAndSkipsReplayed(t *testing.T) {
	events := []kafka.Event{dead("a:dlq", inv1, mer1, 0), dead("b:dlq", inv1, mer1, time.Second),
		dead("c:dlq", inv1, mer1, 2*time.Second), dead("d:dlq", inv1, mer1, 3*time.Second)}
	d := &fakeDeliverer{ok: map[string]bool{"b:dlq": true}, err: map[string]error{"c:dlq": errors.New("mongo down")}}
	code, out, _ := call(t, newHandler(events, d, fakeJournal{done: map[string]bool{"a:dlq": true}}), `{}`)
	if code != 200 || out["selected"] != 3.0 || out["delivered"] != 1.0 || out["failed"] != 2.0 {
		t.Errorf("%d %v", code, out)
	}
	if strings.Join(d.calls, ",") != "b:dlq,c:dlq,d:dlq" {
		t.Errorf("calls %v", d.calls)
	}
}

func TestReplayInvalidRequest(t *testing.T) {
	for _, body := range []string{`{`, `{"limit":0.5}`, `{"limit":-1}`, `{"limit":1001}`, `{"invoiceId":"x"}`,
		`{"merchantId":"x"}`, `{"other":1}`} {
		d := &fakeDeliverer{}
		code, out, _ := call(t, newHandler(nil, d, fakeJournal{}), body)
		if code != 400 || out["code"] != "validation_failed" || len(d.calls) != 0 {
			t.Errorf("%s: %d %v", body, code, out)
		}
	}
}

func TestReplayUnavailable(t *testing.T) {
	h := newHandler(nil, &fakeDeliverer{}, fakeJournal{})
	h.DLQ = fakeDLQ{err: errors.New("kafka down")}
	code, out, hdr := call(t, h, `{}`)
	if code != 503 || hdr.Get("Retry-After") == "" || out["code"] != "dlq_unavailable" {
		t.Errorf("dlq: %d %v %v", code, out, hdr)
	}
	h = newHandler([]kafka.Event{dead("a:dlq", inv1, mer1, 0)}, &fakeDeliverer{}, fakeJournal{err: errors.New("mongo down")})
	if code, _, hdr = call(t, h, `{}`); code != 503 || hdr.Get("Retry-After") == "" {
		t.Errorf("journal: %d", code)
	}
}

func TestReplaySkipsUnreadableData(t *testing.T) {
	d := &fakeDeliverer{}
	events := []kafka.Event{{ID: "x:dlq", Data: []byte(`nope`)}}
	if code, out, _ := call(t, newHandler(events, d, fakeJournal{}), `{}`); code != 200 || out["selected"] != 0.0 {
		t.Errorf("%d %v", code, out)
	}
}
