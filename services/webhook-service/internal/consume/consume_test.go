package consume

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/m5tshift/weigate/pkg/kafka"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

var subjects = map[string]string{"a": "0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b", "b": "1c8e5f3b-8d4e-4f60-9b2c-7d3e4f5a6b7c"}

// record is a valid merchant.updated of the merchant behind key with event ID <key>:<n>.
func record(t *testing.T, codec *kafka.Codec, key string, n int) *kgo.Record {
	t.Helper()
	ev, err := kafka.NewEvent("/weigate/merchant-service", "merchant.updated", subjects[key], 1, map[string]any{
		"merchantId": subjects[key], "version": n, "xpub": "xpub" + strings.Repeat("1", 107), "webhookUrl": nil,
		"toleranceBps": 100, "webhookSecretVersion": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ev.ID = key + ":" + strconv.Itoa(n)
	val, err := codec.Encode(ev)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "t", Partition: 0, Key: []byte(key), Value: val}
}

func newCodec(t *testing.T) *kafka.Codec {
	t.Helper()
	c, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Records of one key are handled in order; another key runs at the same time, so a key blocked on a slow
// endpoint does not hold up the others.
func TestHandleOrderPerKeyConcurrentAcrossKeys(t *testing.T) {
	codec := newCodec(t)
	recs := []*kgo.Record{record(t, codec, "a", 1), record(t, codec, "b", 1), record(t, codec, "a", 2), record(t, codec, "b", 2)}
	bDone := make(chan struct{})
	var mu sync.Mutex
	var order []string
	h := func(_ context.Context, ev kafka.Event) error {
		if ev.ID == "a:1" {
			select {
			case <-bDone:
			case <-time.After(5 * time.Second):
				return errors.New("key b did not run while key a was blocked")
			}
		}
		mu.Lock()
		order = append(order, ev.ID)
		mu.Unlock()
		if ev.ID == "b:2" {
			close(bDone)
		}
		return nil
	}
	if !Handle(context.Background(), quiet, codec, recs, 4, h) {
		t.Fatal("not complete")
	}
	want := []string{"b:1", "b:2", "a:1", "a:2"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order %v, want %v", order, want)
		}
	}
}

func TestHandleSkipsInvalidAndRepeatsFailures(t *testing.T) {
	codec := newCodec(t)
	recs := []*kgo.Record{{Topic: "t", Key: []byte("x"), Value: []byte("not an event")}, record(t, codec, "a", 1)}
	var calls atomic.Int32
	h := func(context.Context, kafka.Event) error {
		if calls.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	}
	if !Handle(context.Background(), quiet, codec, recs, 1, h) {
		t.Fatal("not complete")
	}
	if calls.Load() != 2 {
		t.Errorf("handler calls %d, want 2", calls.Load())
	}
}

func TestHandleStopsOnCancel(t *testing.T) {
	codec := newCodec(t)
	ctx, cancel := context.WithCancel(context.Background())
	h := func(ctx context.Context, _ kafka.Event) error {
		cancel()
		return ctx.Err()
	}
	if Handle(ctx, quiet, codec, []*kgo.Record{record(t, codec, "a", 1), record(t, codec, "b", 1)}, 1, h) {
		t.Error("a cancelled batch must not be complete")
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(newCodec(t), quiet, nil, "g", []string{"t"}, 1); err == nil {
		t.Error("expected an error without brokers")
	}
	if _, err := New(newCodec(t), quiet, []string{"k:9092"}, "g", []string{"t"}, 0); err == nil {
		t.Error("expected an error with limit 0")
	}
}
