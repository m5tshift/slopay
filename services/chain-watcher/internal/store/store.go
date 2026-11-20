// Package store keeps the reading state of chain-watcher in MongoDB (SRS 4.1.6): one document per chain in
// the collection chain_state with the cursor and the window of the last processed headers. A single-document
// write is atomic, so the cursor and the window never disagree. Writes are optimistic: a write based on a
// stale version fails with ErrConflict, which protects the state when two replicas lead for a moment.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/m5tshift/slopay/services/chain-watcher/internal/chain"
)

// Collection is the name of the state collection.
const Collection = "chain_state"

// ErrConflict means the stored state changed since it was loaded.
var ErrConflict = errors.New("store: state changed by another writer")

// Database returns the current service database; storage.Mongo implements it and may replace its client.
type Database interface {
	Database() *mongo.Database
}

// Store reads and writes the state document.
type Store struct {
	db Database
}

// New returns a store on db.
func New(db Database) *Store { return &Store{db: db} }

type headerDoc struct {
	Number     int64  `bson:"number"`
	Hash       string `bson:"hash"`
	ParentHash string `bson:"parentHash"`
	Timestamp  int64  `bson:"timestamp"`
}

type stateDoc struct {
	ChainID int64       `bson:"_id"`
	Cursor  *int64      `bson:"cursor"`
	Window  []headerDoc `bson:"window"`
	Version int64       `bson:"version"`
	// UpdatedAt is for operators only; the service never reads it.
	UpdatedAt time.Time `bson:"updatedAt"`
}

func (s *Store) coll() *mongo.Collection { return s.db.Database().Collection(Collection) }

// Load returns the state of the chain, or an empty state with Version 0 when there is none.
func (s *Store) Load(ctx context.Context, chainID uint64) (chain.State, error) {
	var d stateDoc
	err := s.coll().FindOne(ctx, bson.D{{Key: "_id", Value: chain.Int64(chainID)}}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return chain.State{ChainID: chainID}, nil
	}
	if err != nil {
		return chain.State{}, fmt.Errorf("store: load: %w", err)
	}
	st := chain.State{ChainID: chainID, Version: d.Version, Window: make([]chain.Header, 0, len(d.Window))}
	for _, h := range d.Window {
		st.Window = append(st.Window, chain.Header{
			Number: chain.Uint64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: chain.Uint64(h.Timestamp),
		})
	}
	return st, nil
}

// Save writes st if the stored version still equals st.Version and increments st.Version.
func (s *Store) Save(ctx context.Context, st *chain.State) error {
	d := stateDoc{ChainID: chain.Int64(st.ChainID), Version: st.Version + 1, UpdatedAt: time.Now().UTC(),
		Window: make([]headerDoc, 0, len(st.Window))}
	for _, h := range st.Window {
		d.Window = append(d.Window, headerDoc{
			Number: chain.Int64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: chain.Int64(h.Timestamp),
		})
	}
	if tip, ok := st.Tip(); ok {
		c := chain.Int64(tip.Number)
		d.Cursor = &c
	}
	if st.Version == 0 {
		if _, err := s.coll().InsertOne(ctx, d); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return ErrConflict
			}
			return fmt.Errorf("store: insert: %w", err)
		}
	} else {
		res, err := s.coll().ReplaceOne(ctx, bson.D{{Key: "_id", Value: d.ChainID}, {Key: "version", Value: st.Version}}, d)
		if err != nil {
			return fmt.Errorf("store: replace: %w", err)
		}
		if res.MatchedCount == 0 {
			return ErrConflict
		}
	}
	st.Version = d.Version
	return nil
}
