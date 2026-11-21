package outbox

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/m5tshift/weigate/pkg/kafka"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the goose migrations of the outbox table, for tests and for copying into a service.
func Migrations() fs.FS {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil { // the directory is embedded, so this cannot happen
		panic(err)
	}
	return sub
}

// Execer is what Add needs from a pgx transaction (pgx.Tx satisfies it).
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

const insertSQL = `INSERT INTO outbox (event_id, topic, key, event, traceparent, tracestate)
VALUES ($1, $2, $3, $4, $5, $6)`

// Add stores the event for publication to topic. Call it with the transaction that changes the data:
// the event becomes visible to the relay only when that transaction commits, and disappears on rollback.
// The event is validated by codec (envelope and the schema named by dataschema) before anything is written;
// the error wraps kafka.ErrInvalidEvent. The Kafka key is ev.Subject. The trace context of ctx is saved and continues in the relay's span.
func Add(ctx context.Context, tx Execer, codec *kafka.Codec, topic string, ev kafka.Event) error {
	switch {
	case topic == "":
		return errors.New("outbox: topic is empty")
	case ev.ID == "":
		return errors.New("outbox: event id is empty")
	case ev.Subject == "":
		return errors.New("outbox: event subject is empty, it is the Kafka key")
	}
	// The same validation as at publication: an event that the relay could never publish is refused here,
	// so the caller's transaction fails at once instead of blocking the relay later.
	raw, err := codec.Encode(ev)
	if err != nil {
		return fmt.Errorf("outbox: %s: %w", ev.ID, err)
	}
	var headers []kgo.RecordHeader
	kafka.InjectTrace(ctx, &headers)
	var parent, state string
	for _, h := range headers {
		switch h.Key {
		case "traceparent":
			parent = string(h.Value)
		case "tracestate":
			state = string(h.Value)
		}
	}
	if _, err := tx.Exec(ctx, insertSQL, ev.ID, topic, ev.Subject, raw, parent, state); err != nil {
		return fmt.Errorf("outbox: insert %s: %w", ev.ID, err)
	}
	return nil
}

// row is a stored event waiting for publication.
type row struct {
	id          int64
	topic       string
	event       kafka.Event
	traceparent string
	tracestate  string
}

// traceContext returns ctx carrying the trace saved by Add.
func (r row) traceContext(ctx context.Context) context.Context {
	var headers []kgo.RecordHeader
	if r.traceparent != "" {
		headers = append(headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(r.traceparent)})
	}
	if r.tracestate != "" {
		headers = append(headers, kgo.RecordHeader{Key: "tracestate", Value: []byte(r.tracestate)})
	}
	return kafka.ExtractTrace(ctx, headers)
}
