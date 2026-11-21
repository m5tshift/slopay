//go:build integration

package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/valkey-io/valkey-go"
)

func startValkey(t *testing.T) valkey.Client {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8.0",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })
	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "6379/tcp")
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{fmt.Sprintf("%s:%s", host, port.Port())}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestValkeyTokenBucket(t *testing.T) {
	c := startValkey(t)
	ctx := context.Background()
	limits := map[string]Config{
		"coinbase": {Capacity: 5, Window: 2 * time.Second},
		"okx":      {Capacity: 2, Window: 2 * time.Second},
	}
	// Two limiters on the same Valkey are two replicas: the bucket is shared.
	a, err := New(limits, func() valkey.Client { return c }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(limits, func() valkey.Client { return c }, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var granted atomic.Int32
	var wg sync.WaitGroup
	for i := range 20 {
		l := a
		if i%2 == 1 {
			l = b
		}
		wg.Go(func() {
			if l.Allow(ctx, "coinbase") {
				granted.Add(1)
			}
		})
	}
	wg.Wait()
	if granted.Load() != 5 {
		t.Fatalf("granted %d requests, the bucket holds 5", granted.Load())
	}
	if !a.Allow(ctx, "okx") {
		t.Fatal("the bucket of another source must be independent")
	}

	// One token comes back every 400 ms.
	time.Sleep(500 * time.Millisecond)
	if !b.Allow(ctx, "coinbase") {
		t.Fatal("no token after the refill period")
	}
	if a.Allow(ctx, "coinbase") {
		t.Fatal("only one token was refilled")
	}

	ttl, err := c.Do(ctx, c.B().Pttl().Key(KeyPrefix+"coinbase").Build()).AsInt64()
	if err != nil || ttl <= 0 {
		t.Fatalf("bucket must expire: pttl = %d, %v", ttl, err)
	}
}

func TestValkeyLostFallsBackToMemory(t *testing.T) {
	c := startValkey(t)
	ctx := context.Background()
	l, err := New(map[string]Config{"okx": {Capacity: 2, Window: time.Minute}}, func() valkey.Client { return c }, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Allow(ctx, "okx") {
		t.Fatal("first request refused")
	}
	c.Close() // Valkey is gone: the leader counts in memory from a full bucket
	if !l.Allow(ctx, "okx") || !l.Allow(ctx, "okx") {
		t.Fatal("memory bucket must grant its capacity")
	}
	if l.Allow(ctx, "okx") {
		t.Fatal("memory bucket must keep the limit")
	}
}
