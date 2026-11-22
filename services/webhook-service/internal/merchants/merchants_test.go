package merchants

import (
	"context"
	"errors"
	"testing"

	"github.com/m5tshift/weigate/pkg/kafka"
)

type writer struct {
	got []Merchant
	err error
}

func (w *writer) Apply(_ context.Context, m Merchant) error {
	w.got = append(w.got, m)
	return w.err
}

func event(t *testing.T, typ string, data map[string]any) kafka.Event {
	t.Helper()
	ev, err := kafka.NewEvent("/weigate/merchant-service", typ, "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b", 1, data)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func TestHandlerApplies(t *testing.T) {
	w := &writer{}
	url := "http://merchant-mock/webhook/a"
	ev := event(t, EventUpdated, map[string]any{
		"merchantId": "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b", "version": 3, "webhookUrl": url,
		"webhookSecretVersion": 2, "xpub": "x", "toleranceBps": 100,
	})
	if err := Handler(w)(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	want := Merchant{ID: "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b", Version: 3, WebhookURL: url, SecretVersion: 2}
	if len(w.got) != 1 || w.got[0] != want {
		t.Errorf("got %+v, want %+v", w.got, want)
	}
}

func TestHandlerNullURL(t *testing.T) {
	w := &writer{}
	ev := event(t, EventUpdated, map[string]any{
		"merchantId": "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b", "version": 1, "webhookUrl": nil, "webhookSecretVersion": 1,
	})
	if err := Handler(w)(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if len(w.got) != 1 || w.got[0].WebhookURL != "" {
		t.Errorf("got %+v", w.got)
	}
}

func TestHandlerIgnoresOtherTypes(t *testing.T) {
	w := &writer{}
	if err := Handler(w)(context.Background(), event(t, "merchant.other", map[string]any{"a": 1})); err != nil {
		t.Fatal(err)
	}
	if len(w.got) != 0 {
		t.Errorf("applied %+v", w.got)
	}
}

func TestHandlerReturnsWriteError(t *testing.T) {
	w := &writer{err: errors.New("down")}
	ev := event(t, EventUpdated, map[string]any{"merchantId": "m", "version": 1, "webhookSecretVersion": 1})
	if err := Handler(w)(context.Background(), ev); err == nil {
		t.Fatal("expected the write error")
	}
}

func TestHandlerBadData(t *testing.T) {
	ev := event(t, EventUpdated, map[string]any{"version": "one"})
	if err := Handler(&writer{})(context.Background(), ev); err == nil {
		t.Fatal("expected a decode error")
	}
}
