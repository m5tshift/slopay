package limiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newMem(t *testing.T, limits map[string]Config) (*Limiter, *clock) {
	t.Helper()
	c := &clock{t: time.Unix(1000, 0)}
	l, err := New(limits, nil, time.Second, WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	return l, c
}

func TestMemoryBucket(t *testing.T) {
	ctx := context.Background()
	l, c := newMem(t, map[string]Config{"a": {Capacity: 3, Window: 3 * time.Second}, "b": {Capacity: 1, Window: time.Second}})

	for i := range 3 {
		if !l.Allow(ctx, "a") {
			t.Fatalf("request %d of the full bucket refused", i)
		}
	}
	if l.Allow(ctx, "a") {
		t.Fatal("empty bucket allowed a request")
	}
	if !l.Allow(ctx, "b") {
		t.Fatal("buckets of sources must be independent")
	}

	c.t = c.t.Add(999 * time.Millisecond)
	if l.Allow(ctx, "a") {
		t.Fatal("token returned before the refill period")
	}
	c.t = c.t.Add(time.Millisecond)
	if !l.Allow(ctx, "a") {
		t.Fatal("token not returned after the refill period")
	}
	if l.Allow(ctx, "a") {
		t.Fatal("only one token was refilled")
	}

	c.t = c.t.Add(time.Hour)
	for i := range 3 {
		if !l.Allow(ctx, "a") {
			t.Fatalf("request %d after a long pause refused", i)
		}
	}
	if l.Allow(ctx, "a") {
		t.Fatal("bucket must not hold more than its capacity")
	}
}

func TestUnknownKeyRefused(t *testing.T) {
	l, _ := newMem(t, map[string]Config{"a": {Capacity: 1, Window: time.Second}})
	if l.Allow(context.Background(), "zzz") {
		t.Fatal("a source without a limit must be refused")
	}
}

func TestNewValidates(t *testing.T) {
	good := Config{Capacity: 1, Window: time.Second}
	cases := map[string]map[string]Config{
		"empty":         {},
		"zero capacity": {"a": {Capacity: 0, Window: time.Second}},
		"zero window":   {"a": {Capacity: 1}},
		"too fine":      {"a": {Capacity: 1000, Window: 100 * time.Millisecond}},
	}
	for name, limits := range cases {
		if _, err := New(limits, nil, time.Second); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	if _, err := New(map[string]Config{"a": good}, nil, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestNoValkeyClientFallsBackToMemory(t *testing.T) {
	c := &clock{t: time.Unix(1000, 0)}
	l, err := New(map[string]Config{"a": {Capacity: 1, Window: time.Second}},
		func() valkey.Client { return nil }, time.Second, WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	if !l.Allow(context.Background(), "a") || l.Allow(context.Background(), "a") {
		t.Fatal("memory bucket must apply while Valkey is not connected")
	}
}

func TestValkeyAnswerDecides(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	l, err := New(map[string]Config{"a": {Capacity: 1, Window: time.Second}},
		func() valkey.Client { return client }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gomock.InOrder(
		client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyInt64(1))),
		client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyInt64(0))),
	)
	if !l.Allow(context.Background(), "a") {
		t.Fatal("Valkey granted a token")
	}
	if l.Allow(context.Background(), "a") {
		t.Fatal("Valkey refused the token, memory must not override it")
	}
}

func TestValkeyErrorFallsBackToMemory(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.ErrorResult(errors.New("down"))).AnyTimes()
	c := &clock{t: time.Unix(1000, 0)}
	l, err := New(map[string]Config{"a": {Capacity: 2, Window: 2 * time.Second}},
		func() valkey.Client { return client }, time.Second, WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for range 2 {
		if !l.Allow(ctx, "a") {
			t.Fatal("memory bucket must grant its capacity after a Valkey error")
		}
	}
	if l.Allow(ctx, "a") {
		t.Fatal("memory bucket must keep the limit after a Valkey error")
	}
}
