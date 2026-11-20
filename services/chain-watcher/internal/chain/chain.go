// Package chain holds the block headers chain-watcher reads and the reading state: the cursor and the
// window of the last processed headers (SRS 4.1.6, 4.1.9).
package chain

import (
	"errors"
	"math"
	"slices"
)

// MaxValue bounds block numbers, timestamps and chain IDs: MongoDB, the contract (JSON numbers) and the
// metrics keep them as signed 64-bit integers. The node client and the configuration reject larger values.
const MaxValue = math.MaxInt64

// Int64 converts a value bounded by MaxValue; a larger one saturates.
func Int64(v uint64) int64 {
	if v > MaxValue {
		return MaxValue
	}
	return int64(v)
}

// Uint64 converts a stored value; a negative one, which chain-watcher never writes, becomes 0.
func Uint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// ErrNotFound is returned by a node when it has no block with the requested number.
var ErrNotFound = errors.New("chain: block not found")

// Header is the part of a block header chain-watcher needs. Hashes are lower-case hex with 0x.
type Header struct {
	Number     uint64
	Hash       string
	ParentHash string
	// Timestamp is the block time, Unix seconds.
	Timestamp uint64
}

// State is the reading state of one chain. Window holds the last processed headers in ascending,
// contiguous order; its last header is the cursor. An empty window means nothing was processed yet.
type State struct {
	ChainID uint64
	Window  []Header
	// Version is the optimistic-concurrency version of the stored state; 0 means not stored yet.
	Version int64
}

// Tip returns the last processed header.
func (s State) Tip() (Header, bool) {
	if len(s.Window) == 0 {
		return Header{}, false
	}
	return s.Window[len(s.Window)-1], true
}

// At returns the processed header with number n if it is still in the window.
func (s State) At(n uint64) (Header, bool) {
	if len(s.Window) == 0 {
		return Header{}, false
	}
	first := s.Window[0].Number
	if n < first || n-first >= uint64(len(s.Window)) {
		return Header{}, false
	}
	return s.Window[n-first], true
}

// Append adds the next header and keeps at most size headers. The caller checks that h follows the tip.
func (s *State) Append(h Header, size int) {
	w := append(slices.Clone(s.Window), h)
	if size > 0 && len(w) > size {
		w = w[len(w)-size:]
	}
	s.Window = w
}

// TruncateAfter drops the headers with numbers greater than n.
func (s *State) TruncateAfter(n uint64) {
	i := len(s.Window)
	for i > 0 && s.Window[i-1].Number > n {
		i--
	}
	s.Window = slices.Clone(s.Window[:i])
}
