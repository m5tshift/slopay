package projection

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"
)

// Collection of the projection.
const Collection = "invoices"

// Database returns the current service database; storage.Mongo implements it and may replace its client.
type Database interface {
	Database() *mongo.Database
}

// Store is the projection in MongoDB. A document is removed by its TTL index ttl after the last event of the
// invoice, which is longer than the lookback of the runs.
type Store struct {
	db  Database
	ttl time.Duration
	now func() time.Time
}

var _ recon.Projection = (*Store)(nil)

// New returns a store on db.
func New(db Database, ttl time.Duration) *Store { return &Store{db: db, ttl: ttl, now: time.Now} }

func (s *Store) coll() *mongo.Collection { return s.db.Database().Collection(Collection) }

// EnsureIndexes creates the index of the run query and the TTL index; it is idempotent.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "createdAt", Value: 1}}},
		{Keys: bson.D{{Key: "expireAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
	})
	if err != nil {
		return fmt.Errorf("projection: indexes: %w", err)
	}
	return nil
}

type assetDoc struct {
	Kind     string `bson:"kind"`
	Contract string `bson:"contract,omitempty"`
	Decimals int    `bson:"decimals"`
}

type transferDoc struct {
	BlockNumber int64  `bson:"blockNumber"`
	Amount      string `bson:"amount"`
}

type invoiceDoc struct {
	InvoiceID  string                 `bson:"_id"`
	MerchantID string                 `bson:"merchantId"`
	Address    string                 `bson:"address"`
	Asset      assetDoc               `bson:"asset"`
	CreatedAt  time.Time              `bson:"createdAt"`
	ExpiresAt  time.Time              `bson:"expiresAt"`
	PaidAt     time.Time              `bson:"paidAt"`
	Transfers  map[string]transferDoc `bson:"transfers"`
}

// Apply stores an Update with one idempotent upsert: the transfers are set or unset by key, the times are set,
// sequence keeps the largest value. The events of an invoice come in order from its partition.
func (s *Store) Apply(ctx context.Context, u Update) error {
	set := bson.D{
		{Key: "merchantId", Value: u.MerchantID}, {Key: "address", Value: u.Address},
		{Key: "asset", Value: assetDoc{Kind: u.Asset.Kind, Contract: u.Asset.Contract, Decimals: u.Asset.Decimals}},
		{Key: "updatedAt", Value: s.now().UTC()}, {Key: "expireAt", Value: s.now().Add(s.ttl).UTC()},
	}
	for name, t := range map[string]*time.Time{"createdAt": u.CreatedAt, "expiresAt": u.ExpiresAt, "paidAt": u.PaidAt} {
		if t != nil {
			set = append(set, bson.E{Key: name, Value: t.UTC()})
		}
	}
	for _, t := range u.Add {
		set = append(set, bson.E{Key: "transfers." + t.Key, Value: transferDoc{
			BlockNumber: int64(min(t.BlockNumber, 1<<62)), Amount: t.Amount.String(), //nolint:gosec // clamped.
		}})
	}
	update := bson.D{{Key: "$set", Value: set}, {Key: "$max", Value: bson.D{{Key: "sequence", Value: u.Sequence}}}}
	if len(u.Remove) > 0 {
		unset := bson.D{}
		for _, k := range u.Remove {
			unset = append(unset, bson.E{Key: "transfers." + k, Value: ""})
		}
		update = append(update, bson.E{Key: "$unset", Value: unset})
	}
	if _, err := s.coll().UpdateOne(ctx, bson.D{{Key: "_id", Value: u.InvoiceID}}, update,
		options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("projection: apply %s: %w", u.InvoiceID, err)
	}
	return nil
}

// Invoices returns the invoices created at or after since. An invoice whose invoice.created was not read has no
// creation time and is not returned.
func (s *Store) Invoices(ctx context.Context, since time.Time) ([]recon.Invoice, error) {
	cur, err := s.coll().Find(ctx, bson.D{{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: since.UTC()}}}})
	if err != nil {
		return nil, fmt.Errorf("projection: invoices: %w", err)
	}
	var docs []invoiceDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("projection: invoices: %w", err)
	}
	out := make([]recon.Invoice, 0, len(docs))
	for _, d := range docs {
		inv := recon.Invoice{
			InvoiceID: d.InvoiceID, MerchantID: d.MerchantID, Address: d.Address,
			Asset:     recon.Asset{Kind: d.Asset.Kind, Contract: d.Asset.Contract, Decimals: d.Asset.Decimals},
			CreatedAt: d.CreatedAt, ExpiresAt: d.ExpiresAt, PaidAt: d.PaidAt,
		}
		for key, t := range d.Transfers {
			amount, ok := new(big.Int).SetString(t.Amount, 10)
			if !ok || t.BlockNumber < 0 {
				return nil, fmt.Errorf("projection: invoice %s: invalid transfer %s", d.InvoiceID, key)
			}
			inv.Transfers = append(inv.Transfers, recon.Transfer{Key: key, BlockNumber: uint64(t.BlockNumber), Amount: amount})
		}
		out = append(out, inv)
	}
	return out, nil
}
