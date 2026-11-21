// Package events maps quotes and rates to the events of contracts/asyncapi/rate-service.yaml: rate.tick on
// rate.ticks and rate.latest on the compacted rate.latest, and reads rate.latest back into the replica cache.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

// Event source and types of the contract.
const (
	EventSource = "/weigate/rate-service"
	TypeTick    = "rate.tick"
	TypeLatest  = "rate.latest"
	// TopicTicks and TopicLatest are the topic names of the contract.
	TopicTicks  = "rate.ticks"
	TopicLatest = "rate.latest"
)

type tickData struct {
	Source   rate.Source `json:"source"`
	Pair     rate.Pair   `json:"pair"`
	Price    string      `json:"price"`
	QuotedAt time.Time   `json:"quotedAt"`
}

type latestData struct {
	Pair     rate.Pair     `json:"pair"`
	Price    string        `json:"price"`
	QuotedAt time.Time     `json:"quotedAt"`
	Sources  []rate.Source `json:"sources"`
}

// Tick builds the rate.tick event of a quote. Key and subject are <source>:<pair>.
func Tick(q rate.Quote) (kafka.Event, error) {
	return kafka.NewEvent(EventSource, TypeTick, string(q.Source)+":"+string(q.Pair), 1, tickData{
		Source: q.Source, Pair: q.Pair, Price: q.Price.String(), QuotedAt: q.QuotedAt.UTC(),
	})
}

// Latest builds the rate.latest event of a rate. Key and subject are the pair; the event time is the time
// of the snapshot (at), which orders snapshots in every replica.
func Latest(r rate.Rate, at time.Time) (kafka.Event, error) {
	ev, err := kafka.NewEvent(EventSource, TypeLatest, string(r.Pair), 1, latestData{
		Pair: r.Pair, Price: r.Price.String(), QuotedAt: r.QuotedAt.UTC(), Sources: r.Sources,
	})
	ev.Time = at.UTC()
	return ev, err
}

// Snapshot reads a rate.latest event: the rate with the event time as UpdatedAt.
func Snapshot(ev kafka.Event) (rate.Snapshot, error) {
	if ev.Type != TypeLatest {
		return rate.Snapshot{}, fmt.Errorf("events: unexpected type %q", ev.Type)
	}
	var d latestData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return rate.Snapshot{}, fmt.Errorf("events: rate.latest data: %w", err)
	}
	p, err := rate.ParsePrice(d.Price)
	if err != nil {
		return rate.Snapshot{}, fmt.Errorf("events: rate.latest price: %w", err)
	}
	if !rate.IsServed(d.Pair) || len(d.Sources) == 0 {
		return rate.Snapshot{}, fmt.Errorf("events: rate.latest of %q without sources", d.Pair)
	}
	return rate.Snapshot{
		Rate:      rate.Rate{Pair: d.Pair, Price: p, QuotedAt: d.QuotedAt, Sources: d.Sources},
		UpdatedAt: ev.Time,
	}, nil
}

// Producer is the part of kafka.Producer the publisher needs.
type Producer interface {
	Publish(ctx context.Context, topic string, ev kafka.Event) error
}

// Publisher publishes ticks and latest rates directly, without an outbox: rate-service keeps no state in
// PostgreSQL, and the producer is idempotent (AGENTS.md, SRS 4.1.5).
type Publisher struct {
	p Producer
}

// NewPublisher wraps a producer.
func NewPublisher(p Producer) *Publisher { return &Publisher{p: p} }

// PublishTick publishes a quote to rate.ticks.
func (pb *Publisher) PublishTick(ctx context.Context, q rate.Quote) error {
	ev, err := Tick(q)
	if err != nil {
		return err
	}
	return pb.p.Publish(ctx, TopicTicks, ev)
}

// PublishLatest publishes a rate to rate.latest.
func (pb *Publisher) PublishLatest(ctx context.Context, r rate.Rate, at time.Time) error {
	ev, err := Latest(r, at)
	if err != nil {
		return err
	}
	return pb.p.Publish(ctx, TopicLatest, ev)
}
