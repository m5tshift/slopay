package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresConfig configures a pgx pool. The password is not part of URL.
type PostgresConfig struct {
	// URL is a connection URL or DSN without the password, e.g. postgres://user@host:5432/db.
	URL string
	// PasswordFile is the mounted secret with the password.
	PasswordFile SecretFile
	// MaxConns is the pool size; 0 keeps the pgx default.
	MaxConns int32
	Backoff  Backoff
}

// PostgresConfigFromEnv reads <prefix>_URL, <prefix>_PASSWORD_FILE and <prefix>_MAX_CONNS.
func PostgresConfigFromEnv(prefix string) (PostgresConfig, error) {
	c := PostgresConfig{URL: os.Getenv(prefix + "_URL"), PasswordFile: SecretFile(os.Getenv(prefix + "_PASSWORD_FILE"))}
	if c.URL == "" || c.PasswordFile == "" {
		return c, fmt.Errorf("storage: %s_URL and %s_PASSWORD_FILE are required", prefix, prefix)
	}
	n, err := envInt32(prefix + "_MAX_CONNS")
	if err != nil {
		return c, err
	}
	c.MaxConns = n
	return c, nil
}

// Postgres is a pgx pool whose password is re-read from the file for each new connection.
type Postgres struct {
	pool *pgxpool.Pool
	cfg  PostgresConfig
}

// OpenPostgres creates the pool and checks it with a ping. Authentication failures are retried
// with backoff, re-reading the password file each time.
func OpenPostgres(ctx context.Context, cfg PostgresConfig) (*Postgres, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("storage: parse postgres url: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pc.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error {
		pw, err := cfg.PasswordFile.Read()
		if err != nil {
			return err
		}
		cc.Password = pw
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("storage: postgres pool: %w", err)
	}
	p := &Postgres{pool: pool, cfg: cfg}
	if err := p.Do(ctx, pool.Ping); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage: postgres: %w", err)
	}
	return p, nil
}

// Pool returns the underlying pool.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// Do runs op and repeats it with backoff while PostgreSQL rejects the credentials. Use it around
// the first operation that may need a new connection after a password rotation.
func (p *Postgres) Do(ctx context.Context, op func(ctx context.Context) error) error {
	return retryAuth(ctx, "postgresql", p.cfg.Backoff, IsPostgresAuthError, op)
}

// Ready is a readiness check for /health/ready. It does not retry: the probe repeats itself.
func (p *Postgres) Ready(ctx context.Context) error { return p.pool.Ping(ctx) }

// Close closes the pool.
func (p *Postgres) Close() { p.pool.Close() }
