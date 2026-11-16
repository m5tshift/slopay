package storage

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// MigrationsDir is the migrations directory of a service relative to the repository root.
func MigrationsDir(service string) string { return path.Join("services", service, "migrations") }

// MigrationsFS opens the migrations directory of a service on disk.
func MigrationsFS(service string) fs.FS { return os.DirFS(MigrationsDir(service)) }

// Migrate applies all pending goose migrations from fsys. A PostgreSQL advisory lock makes
// concurrent starts of several replicas safe: one applies, the others wait and find nothing to do.
// It returns the number of applied migrations.
func (p *Postgres) Migrate(ctx context.Context, fsys fs.FS) (int, error) {
	ctx, span := otel.Tracer(instrumentationName).Start(ctx, "postgresql migrate")
	defer span.End()
	n, err := p.migrate(ctx, fsys)
	span.SetAttributes(attribute.Int("migrations.applied", n))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "migrate failed")
	}
	return n, err
}

func (p *Postgres) migrate(ctx context.Context, fsys fs.FS) (int, error) {
	db := stdlib.OpenDBFromPool(p.pool)
	defer func() { _ = db.Close() }()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return 0, fmt.Errorf("storage: migrate: %w", err)
	}
	prov, err := goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		return 0, fmt.Errorf("storage: migrate: %w", err)
	}
	res, err := prov.Up(ctx)
	if err != nil {
		return len(res), fmt.Errorf("storage: migrate: %w", err)
	}
	return len(res), nil
}
