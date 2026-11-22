// Package store reads the report views of ClickHouse (weigate.merchant_turnover_daily and
// weigate.merchant_conversion_daily). The service only reads: the ClickHouse user `reporting` has a read-only profile.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/storage"
)

// authFailed is the ClickHouse error code AUTHENTICATION_FAILED.
const authFailed = 516

// TurnoverDay is one row of merchant_turnover_daily.
type TurnoverDay struct {
	Day              time.Time
	PaidInvoices     int64
	TurnoverUSDCents int64
}

// ConversionDay is one row of merchant_conversion_daily.
type ConversionDay struct {
	Day             time.Time
	CreatedInvoices int64
	PaidInvoices    int64
	Conversion      float64
}

// Config is the ClickHouse connection. The password is read from the file, never from a variable.
type Config struct {
	// Addr is host:port of the native protocol (CLICKHOUSE_ADDR).
	Addr string
	// Database is the database with the views (CLICKHOUSE_DATABASE).
	Database string
	Username string
	// PasswordFile is the mounted secret with the password.
	PasswordFile storage.SecretFile
	Backoff      storage.Backoff
}

// ConfigFromEnv reads CLICKHOUSE_ADDR, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD_FILE and CLICKHOUSE_DATABASE.
func ConfigFromEnv() (Config, error) {
	c := Config{
		Addr: os.Getenv("CLICKHOUSE_ADDR"), Username: os.Getenv("CLICKHOUSE_USERNAME"),
		PasswordFile: storage.SecretFile(os.Getenv("CLICKHOUSE_PASSWORD_FILE")), Database: os.Getenv("CLICKHOUSE_DATABASE"),
	}
	if c.Addr == "" || c.Username == "" || c.PasswordFile == "" || c.Database == "" {
		return c, errors.New("CLICKHOUSE_ADDR, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD_FILE and CLICKHOUSE_DATABASE are required")
	}
	return c, nil
}

// Store is a ClickHouse client. The driver reads the password only when the connection is created, so after an
// authentication failure the connection is rebuilt with the current file content (SRS INF-2.4).
type Store struct {
	cfg Config

	mu   sync.RWMutex
	conn driver.Conn
}

// Open connects to ClickHouse and pings it. Authentication failures are retried with backoff.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	s := &Store{cfg: cfg}
	if err := s.retry(ctx, func(ctx context.Context) error { return s.current().Ping(ctx) }, true); err != nil {
		return nil, fmt.Errorf("store: clickhouse: %w", err)
	}
	return s, nil
}

func (s *Store) dial() (driver.Conn, error) {
	pw, err := s.cfg.PasswordFile.Read()
	if err != nil {
		return nil, err
	}
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{s.cfg.Addr},
		Auth: clickhouse.Auth{Database: s.cfg.Database, Username: s.cfg.Username, Password: pw},
	})
}

func (s *Store) reconnect() error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	s.mu.Lock()
	old := s.conn
	s.conn = c
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func isAuthError(err error) bool {
	var ex *clickhouse.Exception
	return errors.As(err, &ex) && ex.Code == authFailed
}

// retry runs op and repeats it with backoff, on a connection rebuilt from the password file, while ClickHouse
// rejects the credentials. first creates the connection before the first attempt.
func (s *Store) retry(ctx context.Context, op func(ctx context.Context) error, first bool) error {
	if first {
		if err := s.reconnect(); err != nil {
			return err
		}
	}
	return storage.Retry(ctx, s.cfg.Backoff, isAuthError, func(int, error, time.Duration) { _ = s.reconnect() }, op)
}

// noDeadline returns a context that ends with ctx but carries no deadline. The driver turns a deadline into the
// max_execution_time setting of the query, and the read-only profile of the reporting user may not change settings.
func noDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	c, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancel)
	return c, func() { stop(); cancel() }
}

func (s *Store) current() driver.Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn
}

// Ready is a readiness check. It does not retry: the probe repeats itself.
func (s *Store) Ready(ctx context.Context) error { return s.current().Ping(ctx) }

// Close closes the connection.
func (s *Store) Close(context.Context) error { return s.current().Close() }

// Turnover returns the turnover of the merchant for the days from..to inclusive, ordered by day.
func (s *Store) Turnover(ctx context.Context, merchant uuid.UUID, from, to time.Time) ([]TurnoverDay, error) {
	var out []TurnoverDay
	err := s.retry(ctx, func(ctx context.Context) error {
		out = nil
		qctx, release := noDeadline(ctx)
		defer release()
		rows, err := s.current().Query(qctx,
			`SELECT day, paid_invoices, turnover_usd_cents FROM merchant_turnover_daily
			 WHERE merchant_id = toUUID(?) AND day BETWEEN toDate(?) AND toDate(?) ORDER BY day`,
			merchant.String(), from.Format(time.DateOnly), to.Format(time.DateOnly))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d TurnoverDay
			var paid, turnover uint64
			if err := rows.Scan(&d.Day, &paid, &turnover); err != nil {
				return err
			}
			d.Day = utcDay(d.Day)
			d.PaidInvoices, d.TurnoverUSDCents = int64(paid), int64(turnover) //nolint:gosec // bounded by the number of invoices and their USD cents
			out = append(out, d)
		}
		return rows.Err()
	}, false)
	return out, err
}

// Conversion returns the conversion of the merchant for the days from..to inclusive, ordered by day.
func (s *Store) Conversion(ctx context.Context, merchant uuid.UUID, from, to time.Time) ([]ConversionDay, error) {
	var out []ConversionDay
	err := s.retry(ctx, func(ctx context.Context) error {
		out = nil
		qctx, release := noDeadline(ctx)
		defer release()
		rows, err := s.current().Query(qctx,
			`SELECT day, created_invoices, paid_invoices, conversion FROM merchant_conversion_daily
			 WHERE merchant_id = toUUID(?) AND day BETWEEN toDate(?) AND toDate(?) ORDER BY day`,
			merchant.String(), from.Format(time.DateOnly), to.Format(time.DateOnly))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d ConversionDay
			var created, paid uint64
			if err := rows.Scan(&d.Day, &created, &paid, &d.Conversion); err != nil {
				return err
			}
			d.Day = utcDay(d.Day)
			d.CreatedInvoices, d.PaidInvoices = int64(created), int64(paid) //nolint:gosec // bounded by the number of invoices
			out = append(out, d)
		}
		return rows.Err()
	}, false)
	return out, err
}

// utcDay is the date of t as midnight UTC, whatever location the driver put on a Date column.
func utcDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
