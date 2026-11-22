// Package journal is the delivery journal of webhook-service in MongoDB (SRS 4.1.6): one document per delivery
// attempt, removed by a TTL index 30 days after the attempt.
package journal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection of the journal.
const Collection = "deliveries"

// Results of an attempt.
const (
	ResultDelivered = "delivered"
	ResultFailed    = "failed"
	// ResultSkipped is an event of a merchant without a webhook URL: nothing is sent.
	ResultSkipped = "skipped"
)

// Attempt is one delivery attempt. Its document ID is <event id>:<attempt>, so a repeated attempt after a crash
// overwrites the record instead of adding one.
type Attempt struct {
	EventID    string `bson:"eventId"`
	EventType  string `bson:"eventType"`
	InvoiceID  string `bson:"invoiceId"`
	MerchantID string `bson:"merchantId"`
	Attempt    int    `bson:"attempt"`
	Result     string `bson:"result"`
	StatusCode *int   `bson:"statusCode,omitempty"`
	Reason     string `bson:"reason,omitempty"`
	// SecretVersion is the version the webhook was signed with; 0 when it was not sent.
	SecretVersion int       `bson:"secretVersion,omitempty"`
	At            time.Time `bson:"at"`
	DurationMs    int64     `bson:"durationMs"`
}

// Database returns the current service database; storage.Mongo implements it and may replace its client.
type Database interface {
	Database() *mongo.Database
}

// Store is the journal.
type Store struct {
	db  Database
	ttl time.Duration
}

// New returns a store on db that keeps attempts for ttl.
func New(db Database, ttl time.Duration) *Store { return &Store{db: db, ttl: ttl} }

func (s *Store) coll() *mongo.Collection { return s.db.Database().Collection(Collection) }

// EnsureIndexes creates the index of the attempts of an event and the TTL index; it is idempotent.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "eventId", Value: 1}, {Key: "at", Value: 1}}},
		{Keys: bson.D{{Key: "expireAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
	})
	if err != nil {
		return fmt.Errorf("journal: indexes: %w", err)
	}
	return nil
}

// Record writes an attempt.
func (s *Store) Record(ctx context.Context, a Attempt) error {
	doc, err := bson.Marshal(a)
	if err != nil {
		return fmt.Errorf("journal: encode: %w", err)
	}
	var set bson.D
	if err := bson.Unmarshal(doc, &set); err != nil {
		return fmt.Errorf("journal: encode: %w", err)
	}
	set = append(set, bson.E{Key: "expireAt", Value: a.At.Add(s.ttl).UTC()})
	_, err = s.coll().ReplaceOne(ctx, bson.D{{Key: "_id", Value: fmt.Sprintf("%s:%d", a.EventID, a.Attempt)}}, set,
		options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("journal: record %s:%d: %w", a.EventID, a.Attempt, err)
	}
	return nil
}

// FirstAttempt returns the time of the earliest recorded attempt of an event.
func (s *Store) FirstAttempt(ctx context.Context, eventID string) (time.Time, bool, error) {
	var a Attempt
	err := s.coll().FindOne(ctx, bson.D{{Key: "eventId", Value: eventID}},
		options.FindOne().SetSort(bson.D{{Key: "at", Value: 1}})).Decode(&a)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("journal: first attempt %s: %w", eventID, err)
	}
	return a.At, true, nil
}
