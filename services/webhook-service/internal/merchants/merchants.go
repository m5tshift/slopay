// Package merchants keeps the projection of merchant.events that webhook-service needs: the webhook URL and the
// version of the webhook signing secret of every merchant (SRS 4.1.2, event-carried state transfer). The URL is
// taken only from here, never from an invoice (SRS 4.1.10).
package merchants

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// Topic and event type of merchant-service (contracts/asyncapi/merchant-service.yaml).
const (
	Topic        = "merchant.events"
	EventUpdated = "merchant.updated"
	// Collection of the projection.
	Collection = "merchants"
)

// Merchant is the delivery settings of a merchant.
type Merchant struct {
	ID      string `bson:"_id"`
	Version int    `bson:"version"`
	// WebhookURL is empty until the merchant sets it.
	WebhookURL    string `bson:"webhookUrl"`
	SecretVersion int    `bson:"secretVersion"`
}

// Database returns the current service database; storage.Mongo implements it and may replace its client.
type Database interface {
	Database() *mongo.Database
}

// Store is the projection in MongoDB, one document per merchant.
type Store struct {
	db  Database
	now func() time.Time
}

// New returns a store on db.
func New(db Database) *Store { return &Store{db: db, now: time.Now} }

func (s *Store) coll() *mongo.Collection { return s.db.Database().Collection(Collection) }

// Get returns the merchant; ok is false when no merchant.updated of it was read yet.
func (s *Store) Get(ctx context.Context, id string) (Merchant, bool, error) {
	var m Merchant
	err := s.coll().FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&m)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Merchant{}, false, nil
	}
	if err != nil {
		return Merchant{}, false, fmt.Errorf("merchants: get %s: %w", id, err)
	}
	return m, true, nil
}

// Apply stores m unless the projection already holds the same or a newer version, so a replayed or reordered
// event never rolls the settings back.
func (s *Store) Apply(ctx context.Context, m Merchant) error {
	_, err := s.coll().UpdateOne(ctx,
		bson.D{{Key: "_id", Value: m.ID}, {Key: "version", Value: bson.D{{Key: "$lt", Value: m.Version}}}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "version", Value: m.Version}, {Key: "webhookUrl", Value: m.WebhookURL},
			{Key: "secretVersion", Value: m.SecretVersion}, {Key: "updatedAt", Value: s.now().UTC()},
		}}},
		options.UpdateOne().SetUpsert(true))
	// The upsert of a merchant that holds a newer version collides with its _id: nothing to do.
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("merchants: apply %s: %w", m.ID, err)
	}
	return nil
}

// Writer is the part of Store the handler needs.
type Writer interface {
	Apply(ctx context.Context, m Merchant) error
}

type updatedData struct {
	MerchantID           string  `json:"merchantId"`
	Version              int     `json:"version"`
	WebhookURL           *string `json:"webhookUrl"`
	WebhookSecretVersion int     `json:"webhookSecretVersion"`
}

// Handler applies merchant.updated to the projection; other event types are ignored. The codec has validated the
// event against its schema.
func Handler(w Writer) kafka.Handler {
	return func(ctx context.Context, ev kafka.Event) error {
		if ev.Type != EventUpdated {
			return nil
		}
		var d updatedData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return fmt.Errorf("merchants: decode %s: %w", ev.ID, err)
		}
		m := Merchant{ID: d.MerchantID, Version: d.Version, SecretVersion: d.WebhookSecretVersion}
		if d.WebhookURL != nil {
			m.WebhookURL = *d.WebhookURL
		}
		return w.Apply(ctx, m)
	}
}
