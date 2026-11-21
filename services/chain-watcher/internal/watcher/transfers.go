package watcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/projection"
)

// Modes of weigate_chain_transfers_published and weigate_chain_transfer_delay.
const (
	modeLive   = "live"
	modeRescan = "rescan"
)

// errReplaced means that a block read from the node is no longer canonical.
var errReplaced = errors.New("watcher: block replaced on the node")

// publishTransfers publishes the transfers of b selected by match, in block order. A transfer is published only
// when its receipt is in b and the transaction succeeded; a receipt missing or in another block means b was
// replaced (errReplaced).
func (w *Watcher) publishTransfers(ctx context.Context, b chain.Block, match func(chain.Tx) bool, mode string) error {
	for _, tx := range b.Txs {
		if !match(tx) {
			continue
		}
		var r chain.Receipt
		err := w.call(ctx, "eth_getTransactionReceipt", func(ctx context.Context) (err error) {
			r, err = w.cfg.Node.Receipt(ctx, tx.Hash)
			return err
		})
		switch {
		case errors.Is(err, chain.ErrNotFound):
			return fmt.Errorf("%w: no receipt of transaction %s of block %d", errReplaced, tx.Hash, b.Number)
		case err != nil:
			return err
		case r.BlockHash != b.Hash:
			return fmt.Errorf("%w: receipt of transaction %s is in block %s, not %s", errReplaced, tx.Hash, r.BlockHash, b.Hash)
		case !r.Success:
			w.log.InfoContext(ctx, "failed transaction to a tracked address skipped", "tx", tx.Hash, "to", tx.To,
				"block", b.Number)
			continue
		}
		if err := w.cfg.Publisher.PublishTransfer(ctx, w.cfg.ChainID, chain.Transfer{Block: b.Header, Tx: tx}); err != nil {
			return err
		}
		attrs := metric.WithAttributes(w.chain, attribute.String("mode", mode))
		w.transfers.Add(ctx, 1, attrs)
		w.delay.Record(ctx, w.cfg.now().Sub(time.Unix(chain.Int64(b.Timestamp), 0)).Seconds(), attrs)
		w.log.InfoContext(ctx, "transfer published", "mode", mode, "tx", tx.Hash, "to", tx.To, "block", b.Number)
	}
	return nil
}

// maintain stops tracking the addresses whose window ended at the tip and searches the processed blocks for the
// addresses tracked since the last step. Their transfers are published before the next block, so before the next
// chain.new_head.
func (w *Watcher) maintain(ctx context.Context, st *chain.State) error {
	tip, started := st.Tip()
	if !started {
		// Nothing is processed: there is nothing to search.
		pending, err := w.cfg.Addresses.Pending(ctx, w.cfg.now())
		if err != nil || len(pending) == 0 {
			return err
		}
		return w.cfg.Addresses.Rescanned(ctx, addresses(pending))
	}
	now := time.Unix(chain.Int64(tip.Timestamp), 0)
	if w.expired != tip.Number+1 {
		if err := w.cfg.Addresses.Expire(ctx, now); err != nil {
			return err
		}
		w.expired = tip.Number + 1
	}
	pending, err := w.cfg.Addresses.Pending(ctx, now)
	if err != nil || len(pending) == 0 {
		return err
	}
	done, err := w.rescan(ctx, st, pending)
	if err != nil || !done {
		return err
	}
	return w.cfg.Addresses.Rescanned(ctx, addresses(pending))
}

// rescan searches the processed blocks from the creation of the oldest pending invoice up to the tip. It
// reports false when the window no longer matches the node: the next step handles the reorg and searches again.
func (w *Watcher) rescan(ctx context.Context, st *chain.State, pending []projection.Pending) (done bool, err error) {
	tip, _ := st.Tip()
	from := make(map[string]time.Time, len(pending))
	oldest := pending[0].CreatedAt
	for _, p := range pending {
		since := p.CreatedAt.Add(-w.cfg.RescanClockSkew)
		from[p.Address] = since
		oldest = minTime(oldest, since)
	}
	tipTime := time.Unix(chain.Int64(tip.Timestamp), 0)
	oldest = maxTime(oldest, tipTime.Add(-w.cfg.RescanLookback))
	if oldest.After(tipTime) {
		return true, nil // created after the tip: no processed block is that new
	}
	ctx, span := w.tracer.Start(ctx, "chain-watcher rescan", trace.WithAttributes(
		w.chain, attribute.Int("rescan.addresses", len(pending)), attribute.Int64("block.cursor", chain.Int64(tip.Number))))
	defer func() { endSpan(span, err) }()

	first, err := w.firstAtOrAfter(ctx, st, chain.Uint64(oldest.Unix()))
	if err != nil {
		return false, err
	}
	span.SetAttributes(attribute.Int64("rescan.from_block", chain.Int64(first)))
	match := func(b chain.Block) func(chain.Tx) bool {
		t := time.Unix(chain.Int64(b.Timestamp), 0)
		return func(tx chain.Tx) bool {
			since, ok := from[tx.To]
			return ok && !t.Before(since)
		}
	}
	for n := first; n <= tip.Number; n++ {
		b, ok, err := w.processed(ctx, st, n)
		if err != nil || !ok {
			return false, err
		}
		w.rescanned.Add(ctx, 1, metric.WithAttributes(w.chain))
		err = w.publishTransfers(ctx, b, match(b), modeRescan)
		if errors.Is(err, errReplaced) {
			// The reorg is handled by the next step, and the new branch is searched after it.
			w.log.InfoContext(ctx, "processed block replaced on the node, search postponed", "number", n, "error", err)
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	w.log.InfoContext(ctx, "processed blocks searched for new addresses", "addresses", len(pending),
		"from_block", first, "to_block", tip.Number)
	return true, nil
}

// processed returns the processed block n. A block of the window is checked against its hash; false means the
// node has replaced it.
func (w *Watcher) processed(ctx context.Context, st *chain.State, n uint64) (chain.Block, bool, error) {
	h, inWindow := st.At(n)
	if b, ok := w.recent[n]; ok && inWindow && b.Hash == h.Hash {
		return b, true, nil
	}
	b, err := w.block(ctx, n)
	if errors.Is(err, chain.ErrNotFound) {
		return chain.Block{}, false, nil
	}
	if err != nil {
		return chain.Block{}, false, err
	}
	if inWindow && b.Hash != h.Hash {
		w.log.InfoContext(ctx, "processed block replaced on the node, search postponed", "number", n)
		return chain.Block{}, false, nil
	}
	return b, true, nil
}

// firstAtOrAfter returns the first block with a timestamp >= ts, at most the tip (ts <= tip timestamp). The window
// is searched first, the older blocks by a binary search on the node headers.
func (w *Watcher) firstAtOrAfter(ctx context.Context, st *chain.State, ts uint64) (uint64, error) {
	if st.Window[0].Timestamp < ts {
		for _, h := range st.Window {
			if h.Timestamp >= ts {
				return h.Number, nil
			}
		}
	}
	lo, hi := uint64(0), st.Window[0].Number // the answer is in [lo, hi]
	for lo < hi {
		mid := lo + (hi-lo)/2
		h, err := w.header(ctx, mid)
		if err != nil {
			return 0, err
		}
		if h.Timestamp >= ts {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

// remember keeps the processed block b for the search of new addresses, as long as it is in the window.
func (w *Watcher) remember(b chain.Block, st *chain.State) {
	w.recent[b.Number] = b
	for n := range w.recent {
		if _, ok := st.At(n); !ok {
			delete(w.recent, n)
		}
	}
}

func addresses(p []projection.Pending) []string {
	out := make([]string, 0, len(p))
	for _, a := range p {
		out = append(out, a.Address)
	}
	return out
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
