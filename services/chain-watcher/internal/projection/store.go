// Package projection keeps the tracked addresses of chain-watcher (SRS 4.1.3, 4.1.7): a projection of
// invoice.events in MongoDB, one document per payment address, from invoice.created until the tracking window
// ends (SRS 4.1.4: 24 h after the later of expiresAt and paidAt, by chain time). A lost projection is rebuilt by
// reading invoice.events of the last 26 h.
//
// Every write is an idempotent upsert, so an event delivered again, or read both by the rebuild and by the
// consumer group, changes nothing.
package projection

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collections of the projection.
const (
	Collection      = "tracked_addresses"
	StateCollection = "projection_state"
	stateID         = "invoice.events"
)

// Database returns the current service database; storage.Mongo implements it and may replace its client.
type Database interface {
	Database() *mongo.Database
}

// Pending is a tracked address whose earlier blocks have not been searched yet.
type Pending struct {
	Address string
	// CreatedAt is the creation time of the invoice: the blocks from it on are searched.
	CreatedAt time.Time
}

// Store is the projection in MongoDB.
type Store struct {
	db Database
}

// New returns a store on db.
func New(db Database) *Store { return &Store{db: db} }

// addressDoc is a tracked address. Rescanned is false until the leader has searched the blocks processed before
// the address was known; TrackUntil is the end of the tracking window.
type addressDoc struct {
	Address    string    `bson:"_id"`
	InvoiceID  string    `bson:"invoiceId"`
	CreatedAt  time.Time `bson:"createdAt"`
	ExpiresAt  time.Time `bson:"expiresAt"`
	TrackUntil time.Time `bson:"trackUntil"`
	Rescanned  bool      `bson:"rescanned"`
}

func (s *Store) coll() *mongo.Collection { return s.db.Database().Collection(Collection) }

// EnsureIndexes creates the indexes of the leader queries; it is idempotent.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "rescanned", Value: 1}, {Key: "trackUntil", Value: 1}}},
		{Keys: bson.D{{Key: "trackUntil", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("projection: indexes: %w", err)
	}
	return nil
}

// Track adds the address of a new invoice. A repeated call keeps the stored document and only moves the end of
// the window forward, so the order of invoice.created and invoice.paid does not matter.
func (s *Store) Track(ctx context.Context, address, invoiceID string, createdAt, expiresAt, trackUntil time.Time) error {
	_, err := s.coll().UpdateOne(ctx, bson.D{{Key: "_id", Value: address}}, bson.D{
		{Key: "$setOnInsert", Value: bson.D{
			{Key: "invoiceId", Value: invoiceID}, {Key: "createdAt", Value: createdAt.UTC()},
			{Key: "expiresAt", Value: expiresAt.UTC()}, {Key: "rescanned", Value: false},
		}},
		{Key: "$max", Value: bson.D{{Key: "trackUntil", Value: trackUntil.UTC()}}},
	}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("projection: track %s: %w", address, err)
	}
	return nil
}

// Extend moves the end of the window of a tracked address forward (invoice.paid). An unknown address is left
// alone: its window has ended, or invoice.created arrives later with the whole history.
func (s *Store) Extend(ctx context.Context, address string, trackUntil time.Time) error {
	_, err := s.coll().UpdateOne(ctx, bson.D{{Key: "_id", Value: address}},
		bson.D{{Key: "$max", Value: bson.D{{Key: "trackUntil", Value: trackUntil.UTC()}}}})
	if err != nil {
		return fmt.Errorf("projection: extend %s: %w", address, err)
	}
	return nil
}

// Lookup returns which of addrs are tracked and searched: the same answer as the cache in Valkey. An address not
// searched yet is found in new blocks by the search of the earlier ones, which covers them too.
func (s *Store) Lookup(ctx context.Context, addrs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(addrs))
	if len(addrs) == 0 {
		return out, nil
	}
	cur, err := s.coll().Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: addrs}}}, {Key: "rescanned", Value: true},
	},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("projection: lookup: %w", err)
	}
	var docs []struct {
		Address string `bson:"_id"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("projection: lookup: %w", err)
	}
	for _, d := range docs {
		out[d.Address] = true
	}
	return out, nil
}

// Pending returns up to limit addresses not searched yet whose window has not ended at chain time now.
func (s *Store) Pending(ctx context.Context, now time.Time, limit int) ([]Pending, error) {
	cur, err := s.coll().Find(ctx, bson.D{
		{Key: "rescanned", Value: false}, {Key: "trackUntil", Value: bson.D{{Key: "$gte", Value: now.UTC()}}},
	}, options.Find().SetLimit(int64(limit)).SetProjection(bson.D{{Key: "createdAt", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("projection: pending: %w", err)
	}
	var docs []addressDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("projection: pending: %w", err)
	}
	out := make([]Pending, 0, len(docs))
	for _, d := range docs {
		out = append(out, Pending{Address: d.Address, CreatedAt: d.CreatedAt})
	}
	return out, nil
}

// MarkRescanned records that the earlier blocks of addrs have been searched.
func (s *Store) MarkRescanned(ctx context.Context, addrs []string) error {
	if len(addrs) == 0 {
		return nil
	}
	_, err := s.coll().UpdateMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: addrs}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "rescanned", Value: true}}}})
	if err != nil {
		return fmt.Errorf("projection: mark rescanned: %w", err)
	}
	return nil
}

// Expired returns up to limit addresses whose window ended before chain time now.
func (s *Store) Expired(ctx context.Context, now time.Time, limit int) ([]string, error) {
	cur, err := s.coll().Find(ctx, bson.D{{Key: "trackUntil", Value: bson.D{{Key: "$lt", Value: now.UTC()}}}},
		options.Find().SetLimit(int64(limit)).SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("projection: expired: %w", err)
	}
	return ids(ctx, cur)
}

// Delete removes addrs whose window still ends before now; an address whose window was extended meanwhile stays.
func (s *Store) Delete(ctx context.Context, addrs []string, now time.Time) error {
	if len(addrs) == 0 {
		return nil
	}
	_, err := s.coll().DeleteMany(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: addrs}}},
		{Key: "trackUntil", Value: bson.D{{Key: "$lt", Value: now.UTC()}}},
	})
	if err != nil {
		return fmt.Errorf("projection: delete: %w", err)
	}
	return nil
}

// Searched calls fn with batches of the addresses whose earlier blocks have been searched: the content of the
// cache in Valkey.
func (s *Store) Searched(ctx context.Context, batch int, fn func([]string) error) error {
	cur, err := s.coll().Find(ctx, bson.D{{Key: "rescanned", Value: true}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(min(batch, math.MaxInt32)))) //nolint:gosec // clamped to int32.
	if err != nil {
		return fmt.Errorf("projection: searched: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	out := make([]string, 0, batch)
	for cur.Next(ctx) {
		var d struct {
			Address string `bson:"_id"`
		}
		if err := cur.Decode(&d); err != nil {
			return fmt.Errorf("projection: searched: %w", err)
		}
		if out = append(out, d.Address); len(out) == batch {
			if err := fn(out); err != nil {
				return err
			}
			out = out[:0]
		}
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("projection: searched: %w", err)
	}
	if len(out) > 0 {
		return fn(out)
	}
	return nil
}

// Built reports whether the projection exists: false on the first start and after the data was lost.
func (s *Store) Built(ctx context.Context) (bool, error) {
	err := s.db.Database().Collection(StateCollection).FindOne(ctx, bson.D{{Key: "_id", Value: stateID}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("projection: state: %w", err)
	}
	return true, nil
}

// MarkBuilt records that the projection was rebuilt from invoice.events.
func (s *Store) MarkBuilt(ctx context.Context, at time.Time) error {
	_, err := s.db.Database().Collection(StateCollection).UpdateOne(ctx, bson.D{{Key: "_id", Value: stateID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "rebuiltAt", Value: at.UTC()}}}}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("projection: state: %w", err)
	}
	return nil
}

func ids(ctx context.Context, cur *mongo.Cursor) ([]string, error) {
	var docs []struct {
		Address string `bson:"_id"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("projection: %w", err)
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.Address)
	}
	return out, nil
}
