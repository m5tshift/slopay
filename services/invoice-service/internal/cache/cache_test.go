package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"

	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func page() invoice.Page {
	return invoice.Page{
		InvoiceID: uuid.MustParse("0d6f1c1e-6a0b-4d44-9a8e-3a8f4f0f5b21"), Status: invoice.StatusNew,
		Address: "0x71c7656ec7ab88b098defb751b7401b5f6d8976f", Amount: "5412000000000000", Asset: invoice.ETH,
		OrderID: "order-1", ExpiresAt: time.Date(2026, 11, 20, 12, 15, 0, 0, time.UTC),
	}
}

func TestWithoutClient(t *testing.T) {
	c := New(30*time.Second, time.Second, quiet)
	c.Set(context.Background(), page())
	if _, ok := c.Get(context.Background(), page().InvoiceID); ok {
		t.Error("hit without Valkey")
	}
	c.Close()
}

func TestGetSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	c := New(30*time.Second, time.Second, quiet)
	c.Attach(client)
	ctx := context.Background()
	p := page()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	key := "invoice:status:" + p.InvoiceID.String()
	if Key(p.InvoiceID) != key {
		t.Fatalf("key %s", Key(p.InvoiceID))
	}

	// SRS 4.1.7: a STRING with the page JSON and a TTL of 30 s.
	client.EXPECT().Do(gomock.Any(), mock.Match("SET", key, string(b), "PX", "30000")).Return(mock.Result(mock.ValkeyString("OK")))
	c.Set(ctx, p)
	client.EXPECT().Do(gomock.Any(), mock.Match("SET", key, string(b), "PX", "30000")).Return(mock.ErrorResult(errors.New("down")))
	c.Set(ctx, p) // errors are only logged

	client.EXPECT().Do(gomock.Any(), mock.Match("GET", key)).Return(mock.Result(mock.ValkeyBlobString(string(b))))
	if got, ok := c.Get(ctx, p.InvoiceID); !ok || got != p {
		t.Errorf("hit: %+v %v", got, ok)
	}
	misses := map[string]valkey.ValkeyResult{
		"nil":           mock.Result(mock.ValkeyNil()),
		"error":         mock.ErrorResult(errors.New("down")),
		"not json":      mock.Result(mock.ValkeyBlobString("nope")),
		"other invoice": mock.Result(mock.ValkeyBlobString(`{"invoiceId":"` + uuid.NewString() + `"}`)),
	}
	for name, res := range misses {
		client.EXPECT().Do(gomock.Any(), mock.Match("GET", key)).Return(res)
		if _, ok := c.Get(ctx, p.InvoiceID); ok {
			t.Errorf("%s: hit", name)
		}
	}
	client.EXPECT().Close()
	c.Close()
}

func TestConnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	c := New(time.Second, time.Second, quiet)
	attempts := 0
	open := func(context.Context) (valkey.Client, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("connection refused")
		}
		return client, nil
	}
	c.Connect(context.Background(), open, time.Millisecond)
	if attempts != 3 || c.get() != client {
		t.Errorf("attempts %d", attempts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	other := New(time.Second, time.Second, quiet)
	other.Connect(ctx, func(context.Context) (valkey.Client, error) { return nil, errors.New("down") }, time.Hour)
	if other.get() != nil {
		t.Error("attached after cancel")
	}
}
