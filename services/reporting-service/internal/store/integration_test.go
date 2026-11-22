//go:build integration

package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/m5tshift/weigate/pkg/storage"
)

const reportingPassword = "reporting-pw" // #nosec G101 -- the password of a throwaway container

// ddl is the part of weigate-gitops platform/clickhouse/full/ddl (02-invoice-events.sql, 05-reports.sql) the
// reports read: the Kafka queue and its materialized view are left out, the events are inserted directly.
var ddl = []string{
	`CREATE TABLE weigate.invoice_events (
		event_id String, event_type LowCardinality(String), event_time DateTime64(3, 'UTC'),
		invoice_id UUID, merchant_id UUID, sequence UInt32, status LowCardinality(String), amount_usd_cents UInt64
	) ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (merchant_id, invoice_id, event_id)`,
	`CREATE VIEW weigate.invoice_state AS SELECT invoice_id, any(merchant_id) AS merchant_id, min(event_time) AS created_at,
		max(event_type IN ('invoice.paid', 'invoice.late_paid')) AS is_paid,
		minIf(event_time, event_type IN ('invoice.paid', 'invoice.late_paid')) AS paid_at, max(amount_usd_cents) AS amount_usd_cents
		FROM weigate.invoice_events FINAL GROUP BY invoice_id`,
	`CREATE VIEW weigate.merchant_turnover_daily AS SELECT toDate(paid_at) AS day, merchant_id, count() AS paid_invoices,
		sum(amount_usd_cents) AS turnover_usd_cents FROM weigate.invoice_state WHERE is_paid = 1 GROUP BY day, merchant_id`,
	`CREATE VIEW weigate.merchant_conversion_daily AS SELECT toDate(created_at) AS day, merchant_id, count() AS created_invoices,
		sum(is_paid) AS paid_invoices, sum(is_paid) / count() AS conversion FROM weigate.invoice_state GROUP BY day, merchant_id`,
	// The user of the service: a read-only profile, as in the CHI of weigate-gitops.
	`CREATE USER reporting IDENTIFIED BY '` + reportingPassword + `' SETTINGS readonly = 1`,
	`GRANT SELECT ON weigate.* TO reporting`,
}

type event struct {
	id, typ, at string
	invoice     uuid.UUID
	merchant    uuid.UUID
	cents       int
}

func newAdmin(addr string) (driver.Conn, error) {
	return ch.Open(&ch.Options{Addr: []string{addr}, Auth: ch.Auth{Database: "weigate", Username: "admin", Password: "admin-pw"}})
}

func startClickHouse(t *testing.T) (addr string, admin func(query string, args ...any)) {
	t.Helper()
	ctx := context.Background()
	c, err := clickhouse.Run(ctx, "clickhouse/clickhouse-server:25.3-alpine",
		clickhouse.WithUsername("admin"), clickhouse.WithPassword("admin-pw"), clickhouse.WithDatabase("weigate"),
		testcontainers.WithEnv(map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}))
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.WithoutCancel(ctx)) })
	host, err := c.ConnectionHost(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admin = func(query string, args ...any) {
		t.Helper()
		db, err := newAdmin(host)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	return host, admin
}

func insert(admin func(string, ...any), events []event) {
	for _, e := range events {
		admin(`INSERT INTO weigate.invoice_events (event_id, event_type, event_time, invoice_id, merchant_id, sequence, status, amount_usd_cents)
			VALUES (?, ?, parseDateTime64BestEffort(?, 3, 'UTC'), toUUID(?), toUUID(?), 1, '', ?)`,
			e.id, e.typ, e.at, e.invoice.String(), e.merchant.String(), uint64(e.cents)) //nolint:gosec // positive test value
	}
}

func openStore(t *testing.T, addr, passwordFile string) *Store {
	t.Helper()
	s, err := Open(context.Background(), Config{
		Addr: addr, Database: "weigate", Username: "reporting", PasswordFile: storage.SecretFile(passwordFile),
		Backoff: storage.Backoff{Initial: 100 * time.Millisecond, Max: 200 * time.Millisecond, Total: 30 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func TestReports(t *testing.T) {
	addr, admin := startClickHouse(t)
	for _, q := range ddl {
		admin(q)
	}
	m, other := uuid.New(), uuid.New()
	inv := func() uuid.UUID { return uuid.New() }
	paid1, paid2, late, open, foreign := inv(), inv(), inv(), inv(), inv()
	insert(admin, []event{
		// paid1: created and paid on 11-01; the paid event is delivered twice (at-least-once) and counted once.
		{"e1", "invoice.created", "2026-11-01T10:00:00Z", paid1, m, 1000},
		{"e2", "invoice.paid", "2026-11-01T10:05:00Z", paid1, m, 1000},
		{"e2", "invoice.paid", "2026-11-01T10:05:00Z", paid1, m, 1000},
		// paid2: created on 11-01, paid on 11-02: conversion counts it on 11-01, turnover on 11-02.
		{"e3", "invoice.created", "2026-11-01T23:50:00Z", paid2, m, 2500},
		{"e4", "invoice.paid", "2026-11-02T00:05:00Z", paid2, m, 2500},
		// late: expired and paid late on 11-03.
		{"e5", "invoice.created", "2026-11-03T08:00:00Z", late, m, 500},
		{"e6", "invoice.expired", "2026-11-03T08:15:00Z", late, m, 500},
		{"e7", "invoice.late_paid", "2026-11-03T09:00:00Z", late, m, 500},
		// open: never paid.
		{"e8", "invoice.created", "2026-11-03T12:00:00Z", open, m, 700},
		// another merchant, and a day outside the period.
		{"e9", "invoice.created", "2026-11-01T10:00:00Z", foreign, other, 9999},
		{"e10", "invoice.paid", "2026-11-01T10:01:00Z", foreign, other, 9999},
		{"e11", "invoice.created", "2026-12-15T10:00:00Z", inv(), m, 100},
	})

	dir := t.TempDir()
	pwFile := filepath.Join(dir, "password")
	if err := os.WriteFile(pwFile, []byte(reportingPassword+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := openStore(t, addr, pwFile)
	// A deadline must not reach ClickHouse as max_execution_time: the read-only profile forbids it.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	from, to := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 30, 0, 0, 0, 0, time.UTC)

	turnover, err := s.Turnover(ctx, m, from, to)
	if err != nil {
		t.Fatal(err)
	}
	wantTurnover := []TurnoverDay{
		{day(2026, 11, 1), 1, 1000}, {day(2026, 11, 2), 1, 2500}, {day(2026, 11, 3), 1, 500},
	}
	if len(turnover) != len(wantTurnover) {
		t.Fatalf("turnover = %+v", turnover)
	}
	for i, w := range wantTurnover {
		if !turnover[i].Day.Equal(w.Day) || turnover[i].PaidInvoices != w.PaidInvoices || turnover[i].TurnoverUSDCents != w.TurnoverUSDCents {
			t.Errorf("turnover[%d] = %+v, want %+v", i, turnover[i], w)
		}
	}

	conv, err := s.Conversion(ctx, m, from, to)
	if err != nil {
		t.Fatal(err)
	}
	wantConv := []ConversionDay{{day(2026, 11, 1), 2, 2, 1}, {day(2026, 11, 3), 2, 1, 0.5}}
	if len(conv) != len(wantConv) {
		t.Fatalf("conversion = %+v", conv)
	}
	for i, w := range wantConv {
		if !conv[i].Day.Equal(w.Day) || conv[i].CreatedInvoices != w.CreatedInvoices || conv[i].PaidInvoices != w.PaidInvoices || conv[i].Conversion != w.Conversion {
			t.Errorf("conversion[%d] = %+v, want %+v", i, conv[i], w)
		}
	}

	// The period is inclusive on both ends and other merchants are never visible.
	one, err := s.Turnover(ctx, m, day(2026, 11, 2), day(2026, 11, 2))
	if err != nil || len(one) != 1 || one[0].TurnoverUSDCents != 2500 {
		t.Errorf("one day: %+v %v", one, err)
	}
	none, err := s.Turnover(ctx, uuid.New(), from, to)
	if err != nil || len(none) != 0 {
		t.Errorf("unknown merchant: %+v %v", none, err)
	}

	// The service user is read-only.
	if err := s.current().Exec(ctx, `INSERT INTO weigate.invoice_events (event_id) VALUES ('x')`); err == nil {
		t.Error("the reporting user must not write")
	}
	if err := s.Ready(ctx); err != nil {
		t.Errorf("ready: %v", err)
	}
}

// A password rotated after the start is read from the file again and the connection is retried (SRS INF-2.4).
func TestPasswordFileIsReread(t *testing.T) {
	addr, admin := startClickHouse(t)
	for _, q := range ddl {
		admin(q)
	}
	pwFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(pwFile, []byte("stale-password"), 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = os.WriteFile(pwFile, []byte(reportingPassword), 0o600)
	}()
	s := openStore(t, addr, pwFile)
	if _, err := s.Turnover(context.Background(), uuid.New(), day(2026, 11, 1), day(2026, 11, 2)); err != nil {
		t.Fatal(err)
	}
}

func day(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
