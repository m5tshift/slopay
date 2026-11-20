// Package watcher is the work of the leader replica of chain-watcher (SRS 4.1.3, 4.1.9): it reads blocks one by
// one from the cursor, checks the parentHash of every block against the window of processed headers, publishes
// the native ETH transfers of the block to tracked addresses and then chain.new_head, and on a mismatch rolls
// back to the common ancestor and publishes chain.reorg. When an address starts being tracked, the blocks
// processed since its invoice was created are searched again before the next block, so a transfer made before
// the address was known is published before the next chain.new_head too. A reorg deeper
// than the limit stops reading. Node calls go through a Circuit Breaker; any failure pauses reading with an
// exponential backoff, after which reading resumes from the cursor, so no block is skipped.
//
// Every step publishes first and then saves the state: after a crash in between the step is repeated, and
// the event, whose ID is deterministic, is deduplicated by consumers.
package watcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/slopay/pkg/breaker"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/chain"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/projection"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/store"
)

const instrumentation = "github.com/m5tshift/slopay/services/chain-watcher/internal/watcher"

// Node reads the chain (node.Client).
type Node interface {
	ChainID(ctx context.Context) (uint64, error)
	HeadNumber(ctx context.Context) (uint64, error)
	HeaderByNumber(ctx context.Context, n uint64) (chain.Header, error)
	BlockByNumber(ctx context.Context, n uint64) (chain.Block, error)
	Receipt(ctx context.Context, txHash string) (chain.Receipt, error)
}

// Store keeps the reading state (store.Store). Save fails with store.ErrConflict when the stored version
// differs from st.Version.
type Store interface {
	Load(ctx context.Context, chainID uint64) (chain.State, error)
	Save(ctx context.Context, st *chain.State) error
}

// Publisher publishes block and transfer events (events.Publisher).
type Publisher interface {
	PublishNewHead(ctx context.Context, chainID uint64, h chain.Header) error
	PublishReorg(ctx context.Context, chainID, fromBlock uint64, orphan string) error
	PublishTransfer(ctx context.Context, chainID uint64, t chain.Transfer) error
}

// Addresses are the tracked addresses (tracked.Book). Times are chain times.
type Addresses interface {
	// Tracked returns which of addrs are tracked and already searched in the processed blocks.
	Tracked(ctx context.Context, addrs []string) (map[string]bool, error)
	// Pending returns the tracked addresses not searched yet in the processed blocks.
	Pending(ctx context.Context, now time.Time) ([]projection.Pending, error)
	// Rescanned records that the processed blocks were searched for addrs.
	Rescanned(ctx context.Context, addrs []string) error
	// Expire stops tracking the addresses whose window ended before now.
	Expire(ctx context.Context, now time.Time) error
}

// Config holds the dependencies and the numbers of SRS 4.1.6 and 4.1.9.
type Config struct {
	Node      Node
	Store     Store
	Publisher Publisher
	Addresses Addresses
	// Breaker protects the node calls.
	Breaker *breaker.Breaker
	// ChainID is the expected chain; a node of another chain is not read.
	ChainID uint64
	// StartBlock is the first block when there is no stored state; nil means the node head at that moment.
	StartBlock *uint64
	// WindowSize is the number of processed headers kept (64).
	WindowSize int
	// MaxReorgDepth is the deepest reorg handled (6); it must be smaller than WindowSize.
	MaxReorgDepth int
	// RescanClockSkew is subtracted from the creation time of an invoice when the processed blocks are searched
	// for its address: the clocks of the node and of invoice-service differ.
	RescanClockSkew time.Duration
	// RescanLookback bounds the search of the processed blocks by chain time: the oldest invoice in the
	// projection is 26 h old.
	RescanLookback time.Duration
	// PollInterval is the pause when the cursor has reached the node head.
	PollInterval time.Duration
	// BackoffInitial and BackoffMax bound the pause after a failure; it doubles with every failure in a row.
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	Logger         *slog.Logger
	// Meter and Tracer default to the global OTel providers.
	Meter  metric.Meter
	Tracer trace.Tracer
	// sleep substitutes waiting in tests.
	sleep func(ctx context.Context, d time.Duration) error
	// now substitutes the clock in tests.
	now func() time.Time
}

// Results of a node call in slopay_chain_node_requests_total.
const (
	resultOK       = "ok"
	resultError    = "error"
	resultRejected = "rejected" // the breaker is open
)

// TooDeepError is a reorg deeper than MaxReorgDepth or below the window: reading stops (alert ChainReorgTooDeep).
type TooDeepError struct {
	Cursor   uint64
	MaxDepth int
}

func (e *TooDeepError) Error() string {
	return fmt.Sprintf("watcher: reorg below block %d is deeper than %d blocks", e.Cursor, e.MaxDepth)
}

// Watcher reads the chain. Run it only on the leader.
type Watcher struct {
	cfg    Config
	log    *slog.Logger
	tracer trace.Tracer
	chain  attribute.KeyValue

	leading atomic.Bool
	head    atomic.Int64 // -1 while unknown
	cursor  atomic.Int64 // -1 while nothing is processed
	tooDeep atomic.Bool

	recent  map[uint64]chain.Block // processed blocks of the window, for the search of new addresses
	expired uint64                 // tip at the last expiry, +1; 0 before the first

	blocks    metric.Int64Counter
	reorgs    metric.Int64Counter
	transfers metric.Int64Counter
	delay     metric.Float64Histogram
	rescanned metric.Int64Counter
	requests  metric.Int64Counter
	duration  metric.Float64Histogram
}

// New validates cfg and registers the metrics.
func New(cfg Config) (*Watcher, error) {
	switch {
	case cfg.Node == nil || cfg.Store == nil || cfg.Publisher == nil || cfg.Addresses == nil || cfg.Breaker == nil:
		return nil, errors.New("watcher: node, store, publisher, addresses and breaker are required")
	case cfg.ChainID == 0:
		return nil, errors.New("watcher: chain id is required")
	case cfg.MaxReorgDepth < 1 || cfg.WindowSize <= cfg.MaxReorgDepth:
		return nil, errors.New("watcher: max reorg depth must be >= 1 and smaller than the window size")
	case cfg.PollInterval <= 0 || cfg.BackoffInitial <= 0 || cfg.BackoffMax < cfg.BackoffInitial:
		return nil, errors.New("watcher: poll interval and backoff must be positive, backoff max >= initial")
	case cfg.RescanClockSkew < 0 || cfg.RescanLookback <= 0:
		return nil, errors.New("watcher: rescan clock skew must be >= 0 and rescan lookback positive")
	}
	if cfg.sleep == nil {
		cfg.sleep = sleepCtx
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	w := &Watcher{cfg: cfg, log: cfg.Logger, tracer: cfg.Tracer, recent: map[uint64]chain.Block{},
		chain: attribute.String("chain_id", strconv.FormatUint(cfg.ChainID, 10))}
	if w.log == nil {
		w.log = slog.Default()
	}
	if w.tracer == nil {
		w.tracer = otel.Tracer(instrumentation)
	}
	w.head.Store(-1)
	w.cursor.Store(-1)
	if err := w.registerMetrics(cfg.Meter); err != nil {
		return nil, fmt.Errorf("watcher: metrics: %w", err)
	}
	return w, nil
}

func (w *Watcher) registerMetrics(m metric.Meter) error {
	if m == nil {
		m = otel.Meter(instrumentation)
	}
	var err error
	if w.blocks, err = m.Int64Counter("slopay_chain_blocks_processed",
		metric.WithDescription("Blocks processed and published as chain.new_head")); err != nil {
		return err
	}
	if w.reorgs, err = m.Int64Counter("slopay_chain_reorgs",
		metric.WithDescription("Reorgs handled, by depth in blocks")); err != nil {
		return err
	}
	if w.transfers, err = m.Int64Counter("slopay_chain_transfers_published",
		metric.WithDescription("Transfers to tracked addresses published to chain.transfers, by mode: live (a new block) or rescan (a processed block searched for a new address)")); err != nil {
		return err
	}
	if w.delay, err = m.Float64Histogram("slopay_chain_transfer_delay", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.25, 0.5, 1, 1.5, 2, 3, 5, 10, 30, 60, 300),
		metric.WithDescription("Time from the block timestamp to the publication of a transfer to chain.transfers (NFR-04), by mode")); err != nil {
		return err
	}
	if w.rescanned, err = m.Int64Counter("slopay_chain_rescan_blocks",
		metric.WithDescription("Processed blocks searched again for newly tracked addresses")); err != nil {
		return err
	}
	if w.requests, err = m.Int64Counter("slopay_chain_node_requests",
		metric.WithDescription("Node calls by method and result: ok, error, rejected (breaker open)")); err != nil {
		return err
	}
	if w.duration, err = m.Float64Histogram("slopay_chain_node_request_duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
		metric.WithDescription("Node call duration")); err != nil {
		return err
	}
	gauge := func(name, desc string, value func() (int64, bool)) error {
		_, err := m.Int64ObservableGauge(name, metric.WithDescription(desc),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				if v, ok := value(); ok && w.leading.Load() {
					o.Observe(v, metric.WithAttributes(w.chain))
				}
				return nil
			}))
		return err
	}
	known := func(a *atomic.Int64) func() (int64, bool) {
		return func() (int64, bool) { v := a.Load(); return v, v >= 0 }
	}
	if err = gauge("slopay_chain_head_block", "Head block number of the node; reported by the leader",
		known(&w.head)); err != nil {
		return err
	}
	if err = gauge("slopay_chain_cursor_block", "Last processed block number; reported by the leader",
		known(&w.cursor)); err != nil {
		return err
	}
	return gauge("slopay_chain_reorg_too_deep",
		"1 while reading is stopped by a reorg deeper than the limit (alert ChainReorgTooDeep); reported by the leader",
		func() (int64, bool) {
			if w.tooDeep.Load() {
				return 1, true
			}
			return 0, true
		})
}

// Run reads the chain until ctx is done or a reorg is too deep; it is the work of leader.Run. The state is
// loaded from the store every time the replica starts leading.
func (w *Watcher) Run(ctx context.Context) {
	w.leading.Store(true)
	defer func() {
		w.leading.Store(false)
		w.tooDeep.Store(false)
		w.head.Store(-1)
		w.cursor.Store(-1)
		w.recent, w.expired = map[uint64]chain.Block{}, 0
	}()
	delay := w.cfg.BackoffInitial
	var (
		st     chain.State
		loaded bool
	)
	for ctx.Err() == nil {
		var (
			progressed bool
			err        error
		)
		if !loaded {
			st, err = w.prepare(ctx)
			loaded, progressed = err == nil, err == nil
		} else {
			progressed, err = w.Step(ctx, &st)
		}
		var deep *TooDeepError
		switch {
		case ctx.Err() != nil:
			return
		case errors.As(err, &deep):
			w.halt(ctx, deep)
			return
		case err != nil:
			if errors.Is(err, store.ErrConflict) {
				loaded = false // another replica wrote the state: read it again
				w.recent = map[uint64]chain.Block{}
			}
			w.log.WarnContext(ctx, "chain reading paused", "error", err, "retry_in", delay.String())
			if w.cfg.sleep(ctx, delay) != nil {
				return
			}
			delay = min(delay*2, w.cfg.BackoffMax)
			continue
		}
		delay = w.cfg.BackoffInitial
		if !progressed && w.cfg.sleep(ctx, w.cfg.PollInterval) != nil {
			return
		}
	}
}

// prepare checks the chain of the node and loads the state.
func (w *Watcher) prepare(ctx context.Context) (chain.State, error) {
	var id uint64
	if err := w.call(ctx, "eth_chainId", func(ctx context.Context) (err error) {
		id, err = w.cfg.Node.ChainID(ctx)
		return err
	}); err != nil {
		return chain.State{}, err
	}
	if id != w.cfg.ChainID {
		return chain.State{}, fmt.Errorf("watcher: node chain id %d, expected %d", id, w.cfg.ChainID)
	}
	st, err := w.cfg.Store.Load(ctx, w.cfg.ChainID)
	if err != nil {
		return chain.State{}, err
	}
	if tip, ok := st.Tip(); ok {
		w.cursor.Store(chain.Int64(tip.Number))
		w.log.InfoContext(ctx, "reading the chain from the cursor", "chain_id", w.cfg.ChainID, "cursor", tip.Number)
	} else {
		w.log.InfoContext(ctx, "reading the chain without a cursor", "chain_id", w.cfg.ChainID)
	}
	return st, nil
}

// Step searches the processed blocks for new addresses, then processes the next block or handles a reorg found at
// it. It reports whether it made progress; false means the cursor is at the node head. st is changed only after
// the state was saved.
func (w *Watcher) Step(ctx context.Context, st *chain.State) (bool, error) {
	if err := w.maintain(ctx, st); err != nil {
		return false, err
	}
	var head uint64
	if err := w.call(ctx, "eth_blockNumber", func(ctx context.Context) (err error) {
		head, err = w.cfg.Node.HeadNumber(ctx)
		return err
	}); err != nil {
		return false, err
	}
	w.head.Store(chain.Int64(head))

	tip, started := st.Tip()
	next := tip.Number + 1
	switch {
	case started:
	case w.cfg.StartBlock != nil:
		next = *w.cfg.StartBlock
	default:
		next = head
	}
	if head < next {
		return false, nil
	}
	b, err := w.block(ctx, next)
	if errors.Is(err, chain.ErrNotFound) {
		return false, nil // the head moved back between the calls
	}
	if err != nil {
		return false, err
	}
	if started && b.ParentHash != tip.Hash {
		return w.reorg(ctx, st)
	}
	return true, w.process(ctx, st, b)
}

func (w *Watcher) header(ctx context.Context, n uint64) (chain.Header, error) {
	var h chain.Header
	err := w.call(ctx, "eth_getBlockByNumber", func(ctx context.Context) (err error) {
		h, err = w.cfg.Node.HeaderByNumber(ctx, n)
		return err
	})
	return h, err
}

func (w *Watcher) block(ctx context.Context, n uint64) (chain.Block, error) {
	var b chain.Block
	err := w.call(ctx, "eth_getBlockByNumber", func(ctx context.Context) (err error) {
		b, err = w.cfg.Node.BlockByNumber(ctx, n)
		return err
	})
	return b, err
}

// process publishes the transfers of b to tracked addresses, then chain.new_head of b, and moves the cursor to it.
func (w *Watcher) process(ctx context.Context, st *chain.State, b chain.Block) (err error) {
	h := b.Header
	ctx, span := w.tracer.Start(ctx, "chain-watcher process block", trace.WithAttributes(
		w.chain, attribute.Int64("block.number", chain.Int64(h.Number)), attribute.String("block.hash", h.Hash)))
	defer func() { endSpan(span, err) }()

	recipients := b.Recipients()
	tracked, err := w.cfg.Addresses.Tracked(ctx, recipients)
	if err != nil {
		return err
	}
	if err := w.publishTransfers(ctx, b, func(tx chain.Tx) bool { return tracked[tx.To] }, modeLive); err != nil {
		return err
	}
	if err := w.cfg.Publisher.PublishNewHead(ctx, w.cfg.ChainID, h); err != nil {
		return err
	}
	next := *st
	next.Append(h, w.cfg.WindowSize)
	if err := w.cfg.Store.Save(ctx, &next); err != nil {
		return err
	}
	*st = next
	w.remember(b, st)
	w.cursor.Store(chain.Int64(h.Number))
	w.blocks.Add(ctx, 1, metric.WithAttributes(w.chain))
	w.log.DebugContext(ctx, "block processed", "number", h.Number, "hash", h.Hash)
	return nil
}

// reorg finds the common ancestor of the window and the node, publishes chain.reorg from the block after it
// and moves the cursor back to it. The next steps read the new branch.
func (w *Watcher) reorg(ctx context.Context, st *chain.State) (progressed bool, err error) {
	tip, _ := st.Tip()
	ctx, span := w.tracer.Start(ctx, "chain-watcher reorg", trace.WithAttributes(
		w.chain, attribute.Int64("block.cursor", chain.Int64(tip.Number))))
	defer func() { endSpan(span, err) }()

	ancestor, found := uint64(0), false
	for depth := 0; depth <= w.cfg.MaxReorgDepth && uint64(depth) <= tip.Number; depth++ {
		n := tip.Number - uint64(depth)
		local, ok := st.At(n)
		if !ok {
			break // the window does not reach this block
		}
		remote, err := w.header(ctx, n)
		if err != nil && !errors.Is(err, chain.ErrNotFound) {
			return false, err
		}
		if err == nil && remote.Hash == local.Hash {
			ancestor, found = n, true
			break
		}
	}
	switch {
	case !found:
		return false, &TooDeepError{Cursor: tip.Number, MaxDepth: w.cfg.MaxReorgDepth}
	case ancestor == tip.Number:
		// The node switched branches between the calls and the tip is canonical again: read on.
		return false, nil
	}
	depth := tip.Number - ancestor
	orphan, _ := st.At(ancestor + 1)
	span.SetAttributes(attribute.Int64("reorg.depth", chain.Int64(depth)), attribute.Int64("reorg.from_block", chain.Int64(ancestor+1)))
	if err := w.cfg.Publisher.PublishReorg(ctx, w.cfg.ChainID, ancestor+1, orphan.Hash); err != nil {
		return false, err
	}
	next := *st
	next.TruncateAfter(ancestor)
	if err := w.cfg.Store.Save(ctx, &next); err != nil {
		return false, err
	}
	*st = next
	for n := range w.recent {
		if n > ancestor {
			delete(w.recent, n)
		}
	}
	w.cursor.Store(chain.Int64(ancestor))
	w.reorgs.Add(ctx, 1, metric.WithAttributes(w.chain, attribute.String("depth", strconv.FormatUint(depth, 10))))
	w.log.WarnContext(ctx, "reorg handled, reading the new branch", "from_block", ancestor+1, "depth", depth,
		"orphaned_tip", tip.Hash)
	return true, nil
}

// halt stops reading until leadership is lost or the service stops; the gauge raises ChainReorgTooDeep.
func (w *Watcher) halt(ctx context.Context, e *TooDeepError) {
	w.tooDeep.Store(true)
	w.log.ErrorContext(ctx, "reorg deeper than the limit, chain reading stopped", "cursor", e.Cursor,
		"max_depth", e.MaxDepth, "alert", "ChainReorgTooDeep")
	<-ctx.Done()
}

// call runs a node call through the breaker and records its metrics. chain.ErrNotFound is an answer of a
// healthy node, so it does not count against the breaker.
func (w *Watcher) call(ctx context.Context, method string, fn func(ctx context.Context) error) error {
	start := time.Now()
	var callErr error
	err := w.cfg.Breaker.Do(func() error {
		callErr = fn(ctx)
		if errors.Is(callErr, chain.ErrNotFound) {
			return nil
		}
		return callErr
	})
	result := resultOK
	switch {
	case errors.Is(err, breaker.ErrOpen):
		result = resultRejected
	case err != nil:
		result = resultError
	}
	attrs := metric.WithAttributes(attribute.String("method", method), attribute.String("result", result))
	w.requests.Add(ctx, 1, attrs)
	if result != resultRejected {
		w.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("method", method)))
	}
	if err != nil {
		return fmt.Errorf("watcher: node %s: %w", method, err)
	}
	return callErr
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
