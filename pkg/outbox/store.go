package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// store is the relay's view of PostgreSQL.
type store interface {
	// withBatch takes the relay lock and up to limit pending rows, calls fn and marks the ids it returns
	// as published, all in one transaction. locked is false when another relay holds the lock.
	withBatch(ctx context.Context, limit int, fn func(rows []row) (published []int64, err error)) (locked bool, err error)
	// stats returns the number of pending rows and the age of the oldest one.
	stats(ctx context.Context) (pending int64, oldest time.Duration, err error)
	// purge deletes rows published longer than olderThan ago.
	purge(ctx context.Context, olderThan time.Duration) (int64, error)
}

// lockID is the key of the advisory lock that makes a single relay active.
const lockID int64 = 0x534c4f4f42 // "SLOOB"

type pgStore struct{ pool *pgxpool.Pool }

func (s pgStore) withBatch(ctx context.Context, limit int, fn func([]row) ([]int64, error)) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var got bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, lockID).Scan(&got); err != nil {
		return false, fmt.Errorf("outbox: lock: %w", err)
	}
	if !got {
		return false, nil
	}
	rows, err := loadPending(ctx, tx, limit)
	if err != nil {
		return true, err
	}
	if len(rows) == 0 {
		return true, nil
	}
	published, ferr := fn(rows)
	if len(published) > 0 {
		// Commit what was acknowledged even when fn failed later in the batch. The context is detached
		// so that a shutdown does not lose the marks of events that already reached Kafka.
		cctx := context.WithoutCancel(ctx)
		if _, err := tx.Exec(cctx, `UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY($1)`, published); err != nil {
			return true, fmt.Errorf("outbox: mark published: %w", err)
		}
		if err := tx.Commit(cctx); err != nil {
			return true, fmt.Errorf("outbox: commit: %w", err)
		}
	}
	return true, ferr
}

func loadPending(ctx context.Context, tx pgx.Tx, limit int) ([]row, error) {
	rs, err := tx.Query(ctx, `SELECT id, topic, event, traceparent, tracestate FROM outbox
WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: select pending: %w", err)
	}
	defer rs.Close()
	var out []row
	for rs.Next() {
		var r row
		var raw []byte
		if err := rs.Scan(&r.id, &r.topic, &raw, &r.traceparent, &r.tracestate); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if err := json.Unmarshal(raw, &r.event); err != nil {
			return nil, fmt.Errorf("outbox: row %d: decode event: %w", r.id, err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("outbox: select pending: %w", err)
	}
	return out, nil
}

func (s pgStore) stats(ctx context.Context) (int64, time.Duration, error) {
	var pending int64
	var oldest float64
	err := s.pool.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)), 0)
FROM outbox WHERE published_at IS NULL`).Scan(&pending, &oldest)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox: stats: %w", err)
	}
	return pending, time.Duration(oldest * float64(time.Second)), nil
}

func (s pgStore) purge(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM outbox WHERE published_at IS NOT NULL
AND published_at < clock_timestamp() - make_interval(secs => $1)`, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("outbox: purge: %w", err)
	}
	return tag.RowsAffected(), nil
}
