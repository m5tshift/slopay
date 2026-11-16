// Package outbox implements the transactional outbox of SRS 4.1.5.
//
// A handler calls Add inside the pgx transaction that changes the data, so the event is stored if and
// only if the transaction commits. A Relay reads the stored events and publishes them to Kafka.
//
// Guarantees of the relay:
//   - at-least-once: a row is marked published only after the brokers acknowledged it, in the same
//     transaction that holds the batch. A crash between the two repeats the event with the same
//     event.id, consumers deduplicate by it;
//   - order per key: events are published in id order, one at a time, and the batch stops at the first
//     failure. Only one replica relays at a time, guarded by a transaction-level advisory lock; the
//     others skip the cycle and take over as soon as the holder's transaction ends (also on a crash).
//     The writer must serialise events of one key (for example by locking the aggregate row), so that
//     ids of one key grow in commit order;
//   - restart: all state is in PostgreSQL.
//
// The table is created by migrations/00001_outbox.sql (embedded, see Migrations): copy it into the
// service's goose directory.
package outbox
