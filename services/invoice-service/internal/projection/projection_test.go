package projection

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoicetest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type applier struct {
	fails int
	got   []invoice.Merchant
}

func (a *applier) ApplyMerchant(_ context.Context, m invoice.Merchant) error {
	if a.fails > 0 {
		a.fails--
		return errors.New("postgres down")
	}
	a.got = append(a.got, m)
	return nil
}

func merchantEvent(t *testing.T, id uuid.UUID, version int) kafka.Event {
	t.Helper()
	ev, err := kafka.NewEvent("/slopay/merchant-service", "merchant.updated", id.String(), 1, map[string]any{
		"merchantId": id, "version": version, "xpub": invoicetest.Xpub, "webhookUrl": "https://shop.example/hook",
		"toleranceBps": 100, "webhookSecretVersion": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(ev); err != nil {
		t.Fatalf("fixture does not match merchant.updated v1: %v", err)
	}
	return ev
}

var fast = Backoff{Min: time.Millisecond, Max: 2 * time.Millisecond}

func TestHandlerApplies(t *testing.T) {
	a := &applier{}
	id := uuid.New()
	if err := Handler(a, fast, quiet)(context.Background(), merchantEvent(t, id, 2)); err != nil {
		t.Fatal(err)
	}
	if len(a.got) != 1 || a.got[0].ID != id || a.got[0].Version != 2 || a.got[0].Xpub != invoicetest.Xpub {
		t.Errorf("applied %+v", a.got)
	}
}

func TestHandlerSkipsOtherTypes(t *testing.T) {
	a := &applier{}
	ev := merchantEvent(t, uuid.New(), 1)
	ev.Type = "merchant.something"
	if err := Handler(a, fast, quiet)(context.Background(), ev); err != nil || len(a.got) != 0 {
		t.Errorf("%v %+v", err, a.got)
	}
	ev.Type, ev.Data = "merchant.updated", []byte(`[`)
	if err := Handler(a, fast, quiet)(context.Background(), ev); err == nil {
		t.Error("broken data accepted")
	}
}

func TestHandlerRetriesUntilApplied(t *testing.T) {
	a := &applier{fails: 3}
	if err := Handler(a, fast, quiet)(context.Background(), merchantEvent(t, uuid.New(), 1)); err != nil || len(a.got) != 1 {
		t.Errorf("%v %+v", err, a.got)
	}
}

func TestHandlerStopsOnCancel(t *testing.T) {
	a := &applier{fails: 1 << 30}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := Handler(a, fast, quiet)(ctx, merchantEvent(t, uuid.New(), 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("%v", err)
	}
}

func TestNewConsumerNeedsBrokers(t *testing.T) {
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewConsumer(codec, &applier{}, quiet, nil, "invoice-service"); err == nil {
		t.Error("consumer without brokers")
	}
}
