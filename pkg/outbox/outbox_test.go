package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/slopay/pkg/kafka"
)

type fakeExec struct {
	sql  string
	args []any
	err  error
}

func (f *fakeExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.sql, f.args = sql, args
	return pgconn.CommandTag{}, f.err
}

func testEvent(t *testing.T) kafka.Event { return event(t, invoiceID(1), 1) }

func testCodec(t *testing.T) *kafka.Codec {
	t.Helper()
	c, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAddInserts(t *testing.T) {
	ev := testEvent(t)
	tx := &fakeExec{}
	if err := Add(context.Background(), tx, testCodec(t), "invoice.events", ev); err != nil {
		t.Fatal(err)
	}
	if tx.sql != insertSQL {
		t.Fatalf("sql = %q", tx.sql)
	}
	if tx.args[0] != ev.ID || tx.args[1] != "invoice.events" || tx.args[2] != invoiceID(1) {
		t.Fatalf("args = %v", tx.args[:3])
	}
	if tx.args[4] != "" || tx.args[5] != "" {
		t.Fatalf("no span in ctx, trace columns must be empty: %v", tx.args[4:])
	}
}

func TestAddSavesTraceContext(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5},
		TraceFlags: trace.FlagsSampled,
		TraceState: mustState(t),
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	tx := &fakeExec{}
	if err := Add(ctx, tx, testCodec(t), "t", testEvent(t)); err != nil {
		t.Fatal(err)
	}
	parent, state := tx.args[4].(string), tx.args[5].(string)
	if !strings.Contains(parent, sc.TraceID().String()) || state != "k=v" {
		t.Fatalf("traceparent=%q tracestate=%q", parent, state)
	}
	// And the relay restores it.
	got := trace.SpanContextFromContext(row{traceparent: parent, tracestate: state}.traceContext(context.Background()))
	if got.TraceID() != sc.TraceID() || !got.IsRemote() || got.TraceState().String() != "k=v" {
		t.Fatalf("restored %v", got)
	}
}

func mustState(t *testing.T) trace.TraceState {
	t.Helper()
	ts, err := trace.ParseTraceState("k=v")
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestTraceContextWithoutTrace(t *testing.T) {
	got := trace.SpanContextFromContext(row{}.traceContext(context.Background()))
	if got.IsValid() {
		t.Fatal("no trace saved, none expected")
	}
}

func TestAddValidation(t *testing.T) {
	good := testEvent(t)
	noID, noSubject := good, good
	noID.ID = ""
	noSubject.Subject = ""
	tests := []struct {
		name  string
		topic string
		ev    kafka.Event
	}{
		{"no topic", "", good},
		{"no id", "t", noID},
		{"no subject", "t", noSubject},
		{"bad json", "t", func() kafka.Event { e := good; e.Data = []byte("{"); return e }()},
		{"schema violation", "t", func() kafka.Event { e := good; e.Data = []byte(`{"a":1}`); return e }()},
		{"unknown schema", "t", func() kafka.Event { e := good; e.DataSchema = "https://example.com/x.json"; return e }()},
		{"empty data", "t", func() kafka.Event { e := good; e.Data = nil; return e }()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeExec{}
			if err := Add(context.Background(), tx, testCodec(t), tc.topic, tc.ev); err == nil {
				t.Fatal("expected error")
			}
			if tx.sql != "" {
				t.Fatal("nothing must be written")
			}
		})
	}
}

func TestAddExecError(t *testing.T) {
	boom := errors.New("boom")
	err := Add(context.Background(), &fakeExec{err: boom}, testCodec(t), "t", testEvent(t))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestMigrationsEmbedded(t *testing.T) {
	m := Migrations()
	f, err := m.Open("00001_outbox.sql")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}

func TestAddInvalidSchemaWrapsErrInvalidEvent(t *testing.T) {
	ev := testEvent(t)
	ev.Data = []byte(`{"a":1}`)
	tx := &fakeExec{}
	err := Add(context.Background(), tx, testCodec(t), "t", ev)
	if !errors.Is(err, kafka.ErrInvalidEvent) || tx.sql != "" {
		t.Fatalf("err=%v sql=%q", err, tx.sql)
	}
}
