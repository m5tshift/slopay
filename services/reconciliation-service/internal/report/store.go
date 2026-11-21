// Package report stores the reconciliation reports in MongoDB (SRS 4.1.3): one document per run with its block,
// counters and rows (discrepancies and addresses with transfers after the tracking window), kept for a TTL.
package report

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"
)

// Collection of the reports.
const Collection = "reports"

// Database returns the current service database; storage.Mongo implements it and may replace its client.
type Database interface {
	Database() *mongo.Database
}

// Store is the reports in MongoDB.
type Store struct {
	db  Database
	ttl time.Duration
}

var _ recon.Reports = (*Store)(nil)

// New returns a store on db; a report is removed ttl after its run started.
func New(db Database, ttl time.Duration) *Store { return &Store{db: db, ttl: ttl} }

func (s *Store) coll() *mongo.Collection { return s.db.Database().Collection(Collection) }

// EnsureIndexes creates the indexes of the queries and the TTL index; it is idempotent.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "startedAt", Value: -1}}},
		{Keys: bson.D{{Key: "published", Value: 1}, {Key: "startedAt", Value: 1}}},
		{Keys: bson.D{{Key: "expireAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
	})
	if err != nil {
		return fmt.Errorf("report: indexes: %w", err)
	}
	return nil
}

type assetDoc struct {
	Kind     string `bson:"kind"`
	Contract string `bson:"contract,omitempty"`
	Decimals int    `bson:"decimals"`
}

type rowDoc struct {
	Kind          string    `bson:"kind"`
	Address       string    `bson:"address"`
	MerchantID    string    `bson:"merchantId"`
	InvoiceIDs    []string  `bson:"invoiceIds"`
	Asset         assetDoc  `bson:"asset"`
	ChainBalance  string    `bson:"chainBalance"`
	InvoicesTotal string    `bson:"invoicesTotal"`
	LedgerTotal   string    `bson:"ledgerTotal"`
	Excess        string    `bson:"excess,omitempty"`
	TrackUntil    time.Time `bson:"trackUntil"`
}

type reportDoc struct {
	RunID            string    `bson:"_id"`
	ChainID          int64     `bson:"chainId"`
	BlockNumber      int64     `bson:"blockNumber"`
	BlockTime        time.Time `bson:"blockTime"`
	StartedAt        time.Time `bson:"startedAt"`
	FinishedAt       time.Time `bson:"finishedAt"`
	AddressesChecked int       `bson:"addressesChecked"`
	Mismatches       int       `bson:"mismatches"`
	AfterWindow      int       `bson:"afterWindow"`
	Result           string    `bson:"result"`
	Rows             []rowDoc  `bson:"rows"`
	Published        bool      `bson:"published"`
	ExpireAt         time.Time `bson:"expireAt"`
}

func clamp(n uint64) int64 { return int64(min(n, 1<<62)) } //nolint:gosec // clamped.

func str(n *big.Int) string {
	if n == nil {
		return ""
	}
	return n.String()
}

func toDoc(r recon.Report, ttl time.Duration) reportDoc {
	d := reportDoc{
		RunID: r.RunID, ChainID: clamp(r.ChainID), BlockNumber: clamp(r.BlockNumber), BlockTime: r.BlockTime.UTC(),
		StartedAt: r.StartedAt.UTC(), FinishedAt: r.FinishedAt.UTC(), AddressesChecked: r.AddressesChecked,
		Mismatches: r.Mismatches, AfterWindow: r.AfterWindow, Result: r.Result(), Rows: []rowDoc{},
		Published: r.Published, ExpireAt: r.StartedAt.Add(ttl).UTC(),
	}
	for _, row := range r.Rows {
		d.Rows = append(d.Rows, rowDoc{
			Kind: row.Kind, Address: row.Address, MerchantID: row.MerchantID, InvoiceIDs: row.InvoiceIDs,
			Asset:        assetDoc{Kind: row.Asset.Kind, Contract: row.Asset.Contract, Decimals: row.Asset.Decimals},
			ChainBalance: str(row.ChainBalance), InvoicesTotal: str(row.InvoicesTotal), LedgerTotal: str(row.LedgerTotal),
			Excess: str(row.Excess), TrackUntil: row.TrackUntil.UTC(),
		})
	}
	return d
}

func parse(s string) (*big.Int, error) {
	if s == "" {
		return nil, nil
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount %q", s)
	}
	return n, nil
}

func fromDoc(d reportDoc) (recon.Report, error) {
	r := recon.Report{
		RunID: d.RunID, ChainID: uint64(max(d.ChainID, 0)), BlockNumber: uint64(max(d.BlockNumber, 0)),
		BlockTime: d.BlockTime, StartedAt: d.StartedAt, FinishedAt: d.FinishedAt, AddressesChecked: d.AddressesChecked,
		Mismatches: d.Mismatches, AfterWindow: d.AfterWindow, Published: d.Published,
	}
	for _, rd := range d.Rows {
		row := recon.Row{Kind: rd.Kind, Address: rd.Address, MerchantID: rd.MerchantID, InvoiceIDs: rd.InvoiceIDs,
			Asset:      recon.Asset{Kind: rd.Asset.Kind, Contract: rd.Asset.Contract, Decimals: rd.Asset.Decimals},
			TrackUntil: rd.TrackUntil}
		var errs [4]error
		row.ChainBalance, errs[0] = parse(rd.ChainBalance)
		row.InvoicesTotal, errs[1] = parse(rd.InvoicesTotal)
		row.LedgerTotal, errs[2] = parse(rd.LedgerTotal)
		row.Excess, errs[3] = parse(rd.Excess)
		if err := errors.Join(errs[:]...); err != nil {
			return recon.Report{}, fmt.Errorf("report %s: %w", d.RunID, err)
		}
		r.Rows = append(r.Rows, row)
	}
	return r, nil
}

// Save stores a report; saving it again replaces it.
func (s *Store) Save(ctx context.Context, r recon.Report) error {
	_, err := s.coll().ReplaceOne(ctx, bson.D{{Key: "_id", Value: r.RunID}}, toDoc(r, s.ttl), options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("report: save %s: %w", r.RunID, err)
	}
	return nil
}

// MarkPublished records that the events of the run were published.
func (s *Store) MarkPublished(ctx context.Context, runID string) error {
	if _, err := s.coll().UpdateOne(ctx, bson.D{{Key: "_id", Value: runID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "published", Value: true}}}}); err != nil {
		return fmt.Errorf("report: mark %s published: %w", runID, err)
	}
	return nil
}

func (s *Store) find(ctx context.Context, filter bson.D, opts *options.FindOptionsBuilder) ([]recon.Report, error) {
	cur, err := s.coll().Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("report: find: %w", err)
	}
	var docs []reportDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("report: find: %w", err)
	}
	out := make([]recon.Report, 0, len(docs))
	for _, d := range docs {
		r, err := fromDoc(d)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// Unpublished returns the reports whose events were not published, oldest first.
func (s *Store) Unpublished(ctx context.Context) ([]recon.Report, error) {
	return s.find(ctx, bson.D{{Key: "published", Value: false}},
		options.Find().SetSort(bson.D{{Key: "startedAt", Value: 1}}).SetLimit(100))
}

// Latest returns the latest report or nil when there is none.
func (s *Store) Latest(ctx context.Context) (*recon.Report, error) {
	rs, err := s.find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "startedAt", Value: -1}}).SetLimit(1))
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	return &rs[0], nil
}
