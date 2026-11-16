package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/valkey-io/valkey-go/mock"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/m5tshift/slopay/pkg/kafka"
)

var _ kafka.DedupStore = (*ValkeyDedup)(nil)

func writeSecret(t *testing.T, content string) SecretFile {
	t.Helper()
	p := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return SecretFile(p)
}

func TestSecretFile(t *testing.T) {
	f := writeSecret(t, "s3cret\n")
	if got, err := f.Read(); err != nil || got != "s3cret" {
		t.Fatalf("Read = %q, %v", got, err)
	}
	if err := os.WriteFile(string(f), []byte("rotated \r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.Read(); got != "rotated" {
		t.Fatalf("file is not re-read: %q", got)
	}
	if _, err := SecretFile("").Read(); err == nil {
		t.Fatal("empty path must fail")
	}
	_, err := SecretFile(filepath.Join(t.TempDir(), "missing")).Read()
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
}

// fakeClock drives Backoff without real waiting.
type fakeClock struct {
	t      time.Time
	sleeps []time.Duration
}

func (c *fakeClock) backoff(b Backoff) Backoff {
	b.now = func() time.Time { return c.t }
	b.sleep = func(_ context.Context, d time.Duration) error {
		c.sleeps = append(c.sleeps, d)
		c.t = c.t.Add(d)
		return nil
	}
	return b
}

var errAuth = errors.New("auth")

func isAuth(err error) bool { return errors.Is(err, errAuth) }

func TestRetrySuccessAfterFailures(t *testing.T) {
	c := &fakeClock{}
	calls, notified := 0, 0
	err := Retry(context.Background(), c.backoff(Backoff{Initial: time.Second, Max: 4 * time.Second, Total: time.Minute}), isAuth,
		func(int, error, time.Duration) { notified++ },
		func(context.Context) error {
			calls++
			if calls < 5 {
				return errAuth
			}
			return nil
		})
	if err != nil || calls != 5 || notified != 4 {
		t.Fatalf("err=%v calls=%d notified=%d", err, calls, notified)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	if fmt.Sprint(c.sleeps) != fmt.Sprint(want) {
		t.Fatalf("sleeps = %v, want %v", c.sleeps, want)
	}
}

func TestRetryNonRetryableReturnsAtOnce(t *testing.T) {
	c := &fakeClock{}
	boom := errors.New("boom")
	calls := 0
	err := Retry(context.Background(), c.backoff(Backoff{}), isAuth, nil, func(context.Context) error { calls++; return boom })
	if !errors.Is(err, boom) || calls != 1 || len(c.sleeps) != 0 {
		t.Fatalf("err=%v calls=%d sleeps=%v", err, calls, c.sleeps)
	}
}

func TestRetryStopsAfterTotal(t *testing.T) {
	c := &fakeClock{}
	calls := 0
	err := Retry(context.Background(), c.backoff(Backoff{Initial: time.Second, Max: 30 * time.Second, Total: 10 * time.Second}), isAuth, nil,
		func(context.Context) error { calls++; return errAuth })
	if !errors.Is(err, errAuth) {
		t.Fatalf("err = %v", err)
	}
	var total time.Duration
	for _, s := range c.sleeps {
		total += s
	}
	if total > 10*time.Second || calls != len(c.sleeps)+1 {
		t.Fatalf("slept %v in %v, calls=%d", total, c.sleeps, calls)
	}
}

func TestRetryDefaults(t *testing.T) {
	b := Backoff{}.withDefaults()
	if b.Initial != 500*time.Millisecond || b.Max != 15*time.Second || b.Total != 2*time.Minute || b.sleep == nil || b.now == nil {
		t.Fatalf("defaults: %+v", b)
	}
}

func TestRetryContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := Retry(ctx, Backoff{}, isAuth, nil, func(context.Context) error { cancel(); return errAuth })
	if !errors.Is(err, errAuth) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestRetrySleepInterrupted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := Retry(ctx, Backoff{Initial: time.Hour, Total: 2 * time.Hour}, isAuth, nil, func(context.Context) error { return errAuth })
	if !errors.Is(err, errAuth) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestRetryAuthCountsAndRetries(t *testing.T) {
	c := &fakeClock{}
	calls := 0
	err := retryAuth(context.Background(), "postgresql", c.backoff(Backoff{}), isAuth, func(context.Context) error {
		calls++
		if calls == 1 {
			return errAuth
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if err := retryAuth(context.Background(), "postgresql", c.backoff(Backoff{}), isAuth, func(context.Context) error { return os.ErrClosed }); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("err = %v", err)
	}
}

func TestIsAuthErrors(t *testing.T) {
	cases := []struct {
		name string
		got  bool
		want bool
	}{
		{"pg 28P01", IsPostgresAuthError(fmt.Errorf("w: %w", &pgconn.PgError{Code: "28P01"})), true},
		{"pg 28000", IsPostgresAuthError(&pgconn.PgError{Code: "28000"}), true},
		{"pg other", IsPostgresAuthError(&pgconn.PgError{Code: "42P01"}), false},
		{"pg plain", IsPostgresAuthError(errors.New("x")), false},
		{"mongo nil", IsMongoAuthError(nil), false},
		{"mongo code", IsMongoAuthError(mongo.CommandError{Code: 18}), true},
		{"mongo text", IsMongoAuthError(errors.New("server selection error: AuthenticationFailed: x")), true},
		{"mongo other", IsMongoAuthError(mongo.CommandError{Code: 11000}), false},
		{"valkey nil", IsValkeyAuthError(nil), false},
		{"valkey wrongpass", IsValkeyAuthError(errors.New("WRONGPASS invalid username-password pair")), true},
		{"valkey noauth", IsValkeyAuthError(errors.New("NOAUTH Authentication required.")), true},
		{"valkey other", IsValkeyAuthError(errors.New("LOADING")), false},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %v", c.name, c.got)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("PG_URL", "postgres://u@h/db")
	t.Setenv("PG_PASSWORD_FILE", "/secrets/pg")
	t.Setenv("PG_MAX_CONNS", "7")
	pg, err := PostgresConfigFromEnv("PG")
	if err != nil || pg.URL != "postgres://u@h/db" || pg.PasswordFile != "/secrets/pg" || pg.MaxConns != 7 {
		t.Fatalf("pg = %+v, %v", pg, err)
	}
	t.Setenv("PG_MAX_CONNS", "x")
	if _, err := PostgresConfigFromEnv("PG"); err == nil {
		t.Fatal("bad max conns must fail")
	}
	if _, err := PostgresConfigFromEnv("NOPE"); err == nil {
		t.Fatal("missing vars must fail")
	}

	t.Setenv("MG_URI", "mongodb://h")
	t.Setenv("MG_USERNAME", "u")
	t.Setenv("MG_PASSWORD_FILE", "/secrets/mg")
	t.Setenv("MG_DATABASE", "d")
	mg, err := MongoConfigFromEnv("MG")
	if err != nil || mg.URI != "mongodb://h" || mg.Database != "d" || mg.AuthSource != "" {
		t.Fatalf("mg = %+v, %v", mg, err)
	}
	if _, err := MongoConfigFromEnv("NOPE"); err == nil {
		t.Fatal("missing vars must fail")
	}

	t.Setenv("VK_ADDRS", "a:6379, b:6379,,")
	t.Setenv("VK_USERNAME", "u")
	vk, err := ValkeyConfigFromEnv("VK")
	if err != nil || len(vk.Addrs) != 2 || vk.Addrs[1] != "b:6379" || vk.Username != "u" {
		t.Fatalf("vk = %+v, %v", vk, err)
	}
	if _, err := ValkeyConfigFromEnv("NOPE"); err == nil {
		t.Fatal("missing addrs must fail")
	}
}

func TestMigrationsDir(t *testing.T) {
	if got := MigrationsDir("invoice-service"); got != "services/invoice-service/migrations" {
		t.Fatalf("got %q", got)
	}
	if MigrationsFS("x") == nil {
		t.Fatal("nil FS")
	}
}

func TestOpenPostgresBadURL(t *testing.T) {
	_, err := OpenPostgres(context.Background(), PostgresConfig{URL: "://bad"})
	if err == nil {
		t.Fatal("bad url must fail")
	}
}

func TestOpenPostgresMissingPasswordFile(t *testing.T) {
	_, err := OpenPostgres(context.Background(), PostgresConfig{
		URL: "postgres://u@127.0.0.1:1/db", PasswordFile: SecretFile(filepath.Join(t.TempDir(), "none")),
		Backoff: Backoff{Total: time.Millisecond},
	})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v", err)
	}
}

func TestValkeyDedup(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	d := newValkeyDedup(client, "", 0)
	ctx := context.Background()

	set := []string{"SET", "dedup:e1", "1", "NX", "EX", "86400"}
	client.EXPECT().Do(ctx, mock.Match(set...)).Return(mock.Result(mock.ValkeyString("OK")))
	if ok, err := d.Claim(ctx, "e1"); err != nil || !ok {
		t.Fatalf("first claim = %v, %v", ok, err)
	}
	client.EXPECT().Do(ctx, mock.Match(set...)).Return(mock.Result(mock.ValkeyNil()))
	if ok, err := d.Claim(ctx, "e1"); err != nil || ok {
		t.Fatalf("second claim = %v, %v", ok, err)
	}
	client.EXPECT().Do(ctx, mock.Match(set...)).Return(mock.ErrorResult(errors.New("down")))
	if ok, err := d.Claim(ctx, "e1"); err == nil || ok {
		t.Fatalf("claim on error = %v, %v", ok, err)
	}

	client.EXPECT().Do(ctx, mock.Match("DEL", "dedup:e1")).Return(mock.Result(mock.ValkeyInt64(1)))
	if err := d.Release(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
	client.EXPECT().Do(ctx, mock.Match("DEL", "dedup:e1")).Return(mock.ErrorResult(errors.New("down")))
	if err := d.Release(ctx, "e1"); err == nil {
		t.Fatal("release must fail")
	}
}

func TestValkeyDedupOptions(t *testing.T) {
	d := newValkeyDedup(nil, "w:", time.Hour)
	if d.prefix != "w:" || d.ttl != time.Hour {
		t.Fatalf("%+v", d)
	}
}
