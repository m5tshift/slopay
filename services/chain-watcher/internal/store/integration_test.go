//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/chain"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/store"
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

func TestStoreRoundTripAndConflict(t *testing.T) {
	ctx := context.Background()
	m := openMongo(t)
	s := store.New(m)

	st, err := s.Load(ctx, 31337)
	if err != nil || st.Version != 0 || len(st.Window) != 0 || st.ChainID != 31337 {
		t.Fatalf("empty load = %+v, %v", st, err)
	}
	st.Append(chain.Header{Number: 7, Hash: "0x07", ParentHash: "0x06", Timestamp: 1794736932}, 64)
	if err := s.Save(ctx, &st); err != nil || st.Version != 1 {
		t.Fatalf("insert: version %d, %v", st.Version, err)
	}
	// A second writer that also started from an empty state loses.
	other := chain.State{ChainID: 31337}
	if err := s.Save(ctx, &other); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second insert: %v", err)
	}

	st.Append(chain.Header{Number: 8, Hash: "0x08", ParentHash: "0x07", Timestamp: 1794736944}, 64)
	if err := s.Save(ctx, &st); err != nil || st.Version != 2 {
		t.Fatalf("replace: version %d, %v", st.Version, err)
	}
	stale := st
	stale.Version = 1
	if err := s.Save(ctx, &stale); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale replace: %v", err)
	}

	got, err := s.Load(ctx, 31337)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 || len(got.Window) != 2 || got.Window[1] != st.Window[1] {
		t.Fatalf("load = %+v", got)
	}
	var doc struct {
		Cursor int64 `bson:"cursor"`
	}
	if err := m.Database().Collection(store.Collection).FindOne(ctx, bson.D{{Key: "_id", Value: int64(31337)}}).Decode(&doc); err != nil || doc.Cursor != 8 {
		t.Fatalf("cursor = %d, %v", doc.Cursor, err)
	}

	// Another chain has its own document.
	if st2, err := s.Load(ctx, 11155111); err != nil || st2.Version != 0 {
		t.Fatalf("other chain = %+v, %v", st2, err)
	}
}
