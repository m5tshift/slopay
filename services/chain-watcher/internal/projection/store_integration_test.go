//go:build integration

package projection_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"

	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/projection"
)

func openMongo(t *testing.T) *storage.Mongo {
	t.Helper()
	ctx := context.Background()
	ctr, err := mongodb.Run(ctx, "mongo:7", mongodb.WithUsername("app"), mongodb.WithPassword("pw"))
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "27017/tcp")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(file, []byte("pw"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := storage.OpenMongo(ctx, storage.MongoConfig{
		URI: fmt.Sprintf("mongodb://%s:%s", host, port.Port()), Username: "app",
		PasswordFile: storage.SecretFile(file), Database: "chain_watcher_test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return m
}

// TestStoreLifecycle walks an address through the projection: tracked, searched, extended, expired.
func TestStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	s := projection.New(openMongo(t))
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if built, err := s.Built(ctx); err != nil || built {
		t.Fatalf("built before the rebuild: %v %v", built, err)
	}
	if err := s.MarkBuilt(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if built, err := s.Built(ctx); err != nil || !built {
		t.Fatalf("built after the rebuild: %v %v", built, err)
	}

	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	const a, b = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.Track(ctx, a, "inv-a", t0, t0.Add(15*time.Minute), t0.Add(time.Hour)))
	must(s.Track(ctx, b, "inv-b", t0.Add(time.Minute), t0.Add(16*time.Minute), t0.Add(2*time.Hour)))
	// A repeated invoice.created keeps the document and does not move the window back.
	must(s.Track(ctx, a, "inv-a", t0, t0.Add(15*time.Minute), t0.Add(30*time.Minute)))
	must(s.Extend(ctx, "0xcccccccccccccccccccccccccccccccccccccccc", t0.Add(time.Hour)))

	pending, err := s.Pending(ctx, t0, 10)
	must(err)
	slices.SortFunc(pending, func(x, y projection.Pending) int { return x.CreatedAt.Compare(y.CreatedAt) })
	if len(pending) != 2 || pending[0].Address != a || !pending[0].CreatedAt.Equal(t0) || pending[1].Address != b {
		t.Fatalf("pending %+v", pending)
	}
	if got, err := s.Lookup(ctx, []string{a, b}); err != nil || len(got) != 0 {
		t.Fatalf("not searched addresses looked up: %v %v", got, err)
	}

	must(s.MarkRescanned(ctx, []string{a}))
	if got, err := s.Lookup(ctx, []string{a, b}); err != nil || !got[a] || got[b] {
		t.Fatalf("lookup %v %v", got, err)
	}
	if pending, err := s.Pending(ctx, t0, 10); err != nil || len(pending) != 1 || pending[0].Address != b {
		t.Fatalf("pending after the search %+v %v", pending, err)
	}
	// A window that has ended is not searched.
	if pending, err := s.Pending(ctx, t0.Add(3*time.Hour), 10); err != nil || len(pending) != 0 {
		t.Fatalf("pending after the window %+v %v", pending, err)
	}
	var searched []string
	must(s.Searched(ctx, 1, func(batch []string) error { searched = append(searched, batch...); return nil }))
	if !slices.Equal(searched, []string{a}) {
		t.Fatalf("searched %v", searched)
	}

	now := t0.Add(90 * time.Minute)
	expired, err := s.Expired(ctx, now, 10)
	must(err)
	if !slices.Equal(expired, []string{a}) {
		t.Fatalf("expired %v", expired)
	}
	// invoice.paid extends the window between Expired and Delete: the address stays.
	must(s.Extend(ctx, a, t0.Add(3*time.Hour)))
	must(s.Delete(ctx, expired, now))
	if got, err := s.Lookup(ctx, []string{a}); err != nil || !got[a] {
		t.Fatalf("extended address deleted: %v %v", got, err)
	}
	now = t0.Add(4 * time.Hour)
	expired, err = s.Expired(ctx, now, 10)
	must(err)
	slices.Sort(expired)
	must(s.Delete(ctx, expired, now))
	if !slices.Equal(expired, []string{a, b}) {
		t.Fatalf("expired %v", expired)
	}
	if got, err := s.Lookup(ctx, []string{a, b}); err != nil || len(got) != 0 {
		t.Fatalf("lookup after the window %v %v", got, err)
	}
}
