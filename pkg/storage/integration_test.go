//go:build integration

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const startTimeout = 3 * time.Minute

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	t.Cleanup(cancel)
	return ctx
}

func setSecret(t *testing.T, f SecretFile, v string) {
	t.Helper()
	// Atomic replace like the kubelet does for mounted secrets.
	tmp := string(f) + ".tmp"
	if err := os.WriteFile(tmp, []byte(v), 0o600); err != nil {
		t.Error(err)
		return
	}
	if err := os.Rename(tmp, string(f)); err != nil {
		t.Error(err)
	}
}

func startPostgres(t *testing.T) (host, port string) {
	t.Helper()
	ctx := ctxT(t)
	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("pw1"),
		postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })
	h, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return h, p.Port()
}

// TestPostgresPasswordRotation: the password changes in the database before the mounted file is
// updated (INF-2.4). Open connections keep working, a new connection retries and succeeds once
// the file has the new password.
func TestPostgresPasswordRotation(t *testing.T) {
	host, port := startPostgres(t)
	ctx := ctxT(t)
	file := SecretFile(filepath.Join(t.TempDir(), "password"))
	setSecret(t, file, "pw1\n")

	pg, err := OpenPostgres(ctx, PostgresConfig{
		URL:          fmt.Sprintf("postgres://app@%s:%s/app?sslmode=disable", host, port),
		PasswordFile: file, Backoff: Backoff{Initial: 200 * time.Millisecond, Total: 30 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	if err := pg.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	held, err := pg.Pool().Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	// OpenBao changes the password in the database; the file still has the old one.
	if _, err := held.Exec(ctx, "ALTER ROLE app PASSWORD 'pw2'"); err != nil {
		t.Fatal(err)
	}
	pg.Pool().Reset() // drop idle connections, the next ones are new
	if _, err := held.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("open connection must keep working: %v", err)
	}

	// The file is synced a bit later.
	time.AfterFunc(time.Second, func() { setSecret(t, file, "pw2\n") })

	var attempts atomic.Int32
	err = pg.Do(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		return pg.Pool().Ping(ctx)
	})
	if err != nil {
		t.Fatalf("new connection after the file update: %v", err)
	}
	if attempts.Load() < 2 {
		t.Fatalf("attempts = %d, want a retry after the authentication failure", attempts.Load())
	}
}

func TestPostgresMigrate(t *testing.T) {
	host, port := startPostgres(t)
	ctx := ctxT(t)
	file := SecretFile(filepath.Join(t.TempDir(), "password"))
	setSecret(t, file, "pw1")
	pg, err := OpenPostgres(ctx, PostgresConfig{URL: fmt.Sprintf("postgres://app@%s:%s/app?sslmode=disable", host, port), PasswordFile: file})
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	fsys := fstest.MapFS{"00001_t.sql": {Data: []byte("-- +goose Up\nCREATE TABLE t (id int PRIMARY KEY);\n-- +goose Down\nDROP TABLE t;\n")}}
	if n, err := pg.Migrate(ctx, fsys); err != nil || n != 1 {
		t.Fatalf("first run: n=%d err=%v", n, err)
	}
	if n, err := pg.Migrate(ctx, fsys); err != nil || n != 0 {
		t.Fatalf("second run: n=%d err=%v", n, err)
	}
	if _, err := pg.Pool().Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	bad := fstest.MapFS{
		"00001_t.sql":   fsys["00001_t.sql"],
		"00002_bad.sql": {Data: []byte("-- +goose Up\nSELEKT;\n-- +goose Down\nSELECT 1;\n")},
	}
	if _, err := pg.Migrate(ctx, bad); err == nil {
		t.Fatal("broken migration must fail")
	}
}

func TestMongoOpenRetriesUntilPasswordFileIsFixed(t *testing.T) {
	ctx := ctxT(t)
	ctr, err := mongodb.Run(ctx, "mongo:7", mongodb.WithUsername("app"), mongodb.WithPassword("pw1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })
	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "27017/tcp")

	file := SecretFile(filepath.Join(t.TempDir(), "password"))
	setSecret(t, file, "stale")
	time.AfterFunc(time.Second, func() { setSecret(t, file, "pw1") })

	m, err := OpenMongo(ctx, MongoConfig{
		URI: fmt.Sprintf("mongodb://%s:%s", host, port.Port()), Username: "app", PasswordFile: file,
		Database: "app", Backoff: Backoff{Initial: 200 * time.Millisecond, Total: 30 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close(ctx) }()
	if err := m.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Database().Collection("c").InsertOne(ctx, map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
}

func startValkey(t *testing.T) (addr string) {
	t.Helper()
	ctx := ctxT(t)
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8.0",
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          []string{"valkey-server", "--requirepass", "pw1"},
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
	return fmt.Sprintf("%s:%s", host, port.Port())
}

func TestValkeyOpenRetriesAndDedup(t *testing.T) {
	addr := startValkey(t)
	ctx := ctxT(t)
	file := SecretFile(filepath.Join(t.TempDir(), "password"))
	setSecret(t, file, "stale")
	time.AfterFunc(time.Second, func() { setSecret(t, file, "pw1") })

	v, err := OpenValkey(ctx, ValkeyConfig{Addrs: []string{addr}, PasswordFile: file, Backoff: Backoff{Initial: 200 * time.Millisecond, Total: 30 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if err := v.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	d := NewValkeyDedup(v, "t:", time.Minute)
	if ok, err := d.Claim(ctx, "e1"); err != nil || !ok {
		t.Fatalf("first claim = %v, %v", ok, err)
	}
	if ok, err := d.Claim(ctx, "e1"); err != nil || ok {
		t.Fatalf("second claim = %v, %v", ok, err)
	}
	if err := d.Release(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
	if ok, err := d.Claim(ctx, "e1"); err != nil || !ok {
		t.Fatalf("claim after release = %v, %v", ok, err)
	}
}
