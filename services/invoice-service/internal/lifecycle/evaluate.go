// Package lifecycle is the invoice state machine of SRS 4.1.4 driven by chain.blocks and chain.transfers
// (SRS 4.1.5; FR-04, FR-05, FR-06, NFR-04, NFR-06, NFR-07).
//
// invoice-service keeps the canonical chain (block number -> hash) from chain.blocks and stores every transfer
// to an invoice address with a canonical flag. Evaluate is a pure function: from an invoice, its transfers,
// the canonical head and the chain time it computes the next state, the roles of the transfers and the
// transitions, each of which becomes an invoice.* event with the next sequence. The Engine runs it in one
// transaction with the chain lock and writes the events to the outbox.
//
// All deadlines use chain time: the largest timestamp of the processed blocks, never the service clock.
package lifecycle

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/money"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

// Config holds the numbers of SRS 4.1.4. They come from the service configuration.
type Config struct {
	// Confirmations is N: a payment is settled when its last transfer has N confirmations (7 on Anvil).
	// It must be at least 2 and greater than the deepest handled reorg (6, SRS 4.1.9): settled statuses
	// are final and are not revisited by a reorg.
	Confirmations int
	// RevertWait is how long an invoice whose transfer was reverted after expiresAt waits for the same
	// txHash to be included again (10 min).
	RevertWait time.Duration
	// Window is the address tracking window after the later of expiresAt and paidAt (24 h).
	Window time.Duration
}

// Validate checks the configuration.
func (c Config) Validate() error {
	if c.Confirmations < 2 {
		return errors.New("lifecycle: Confirmations must be at least 2")
	}
	if c.RevertWait <= 0 || c.Window <= 0 {
		return errors.New("lifecycle: RevertWait and Window must be positive")
	}
	return nil
}

// Role is what a transfer means for its invoice.
type Role string

// Roles of a transfer.
const (
	// RoleNone: not counted: its block is not canonical yet, it is a late transfer that is not reported yet,
	// or it arrived after the tracking window.
	RoleNone Role = ""
	// RoleCounted: part of the invoice sum (sent before expiresAt, or the same txHash included again in the
	// wait after a revert).
	RoleCounted Role = "counted"
	// RoleLate: reported to the merchant by invoice.late_paid or invoice.late_transfer.
	RoleLate Role = "late"
)

// Transfer is a transfer to an invoice address (chain.transfer) with what invoice-service knows of it.
type Transfer struct {
	BlockNumber uint64
	BlockHash   string
	// BlockTime is the timestamp of the block, chain time.
	BlockTime time.Time
	TxHash    string
	// LogIndex is the index of the ERC-20 Transfer log; nil for native ETH.
	LogIndex *int64
	Amount   *big.Int
	Asset    invoice.Asset
	// Canonical: the block number -> hash of the transfer is in the canonical chain.
	Canonical bool
	Role      Role
	// GraceUntil is set on a counted transfer that a reorg removed after expiresAt: the same txHash
	// included in a block with a timestamp up to GraceUntil is still on time. Zero: none.
	GraceUntil time.Time
}

// Key is the idempotency key of the transfer: blockHash + txHash + logIndex (SRS 4.1.5).
func (t Transfer) Key() string {
	li := ""
	if t.LogIndex != nil {
		li = strconv.FormatInt(*t.LogIndex, 10)
	}
	return t.BlockHash + ":" + t.TxHash + ":" + li
}

// Record is an invoice with the state the lifecycle keeps.
type Record struct {
	invoice.Invoice
	// ToleranceBps is the tolerance of the merchant: paid when received >= amount - amount*bps/10000.
	ToleranceBps int
	// PaidAt is the chain time of the PAID transition; zero before it.
	PaidAt time.Time
	// Watch: the invoice must be evaluated on every block (confirmations are being counted).
	Watch bool
	// Deadline: the invoice must be evaluated when the chain time passes it; zero: none.
	Deadline time.Time
}

// Chain is the view of the canonical chain an evaluation uses.
type Chain struct {
	// Head is the number of the canonical head.
	Head uint64
	// Time is the chain time: the largest timestamp of the processed blocks. It never decreases, also when
	// a reorg drops blocks, so a passed deadline stays passed.
	Time time.Time
	// FromBlock is fromBlock of the reorg being handled, 0 outside a reorg.
	FromBlock uint64
}

// Transition is one status change (or a late transfer report) of an invoice; it becomes an invoice.* event.
type Transition struct {
	Type     string
	From, To invoice.Status
	// Sequence is the invoice version after the transition.
	Sequence      int
	Confirmations int
	Received      *big.Int
	Overpaid      bool
	PaidAt        time.Time
	FromBlock     uint64
	// Transfers are the accounted transfers (paid, underpaid), the late ones (late_paid, late_transfer)
	// or the reverted ones (reverted).
	Transfers []Transfer
}

// Event types of invoice.events (SRS 4.1.5).
const (
	EventDetected     = "invoice.detected"
	EventConfirming   = "invoice.confirming"
	EventPaid         = "invoice.paid"
	EventUnderpaid    = "invoice.underpaid"
	EventExpired      = "invoice.expired"
	EventLatePaid     = "invoice.late_paid"
	EventReverted     = "invoice.reverted"
	EventLateTransfer = "invoice.late_transfer"
)

// Result is the outcome of Evaluate.
type Result struct {
	Record    Record
	Transfers []Transfer
	// Changed lists the indexes of Transfers whose Role or GraceUntil changed.
	Changed     []int
	Transitions []Transition
}

// Final reports whether the status of rec cannot change any more at chain time now: PAID, LATE_PAID,
// EXPIRED (only LATE_PAID follows) and UNDERPAID after expiresAt.
func Final(rec Record, now time.Time) bool {
	switch rec.Status {
	case invoice.StatusPaid, invoice.StatusLatePaid, invoice.StatusExpired:
		return true
	case invoice.StatusUnderpaid:
		return now.After(rec.ExpiresAt)
	}
	return false
}

// WindowEnd is the end of the address tracking window: Window after the later of expiresAt and paidAt.
func WindowEnd(rec Record, window time.Duration) time.Time {
	end := rec.ExpiresAt
	if rec.PaidAt.After(end) {
		end = rec.PaidAt
	}
	return end.Add(window)
}

type evaluation struct {
	cfg   Config
	chain Chain
	rec   Record
	ts    []Transfer
	out   []Transition
}

// Evaluate computes the next state of an invoice. It is deterministic and has no side effects; the order of
// transfers does not matter. The rules are SRS 4.1.4:
//
//   - the sum is the sum of the canonical transfers sent before expiresAt (or included again within RevertWait
//     after a revert past expiresAt); confirmations are those of the last of them;
//   - NEW -> DETECTED -> CONFIRMING -> PAID or UNDERPAID by the confirmations; UNDERPAID -> CONFIRMING on an
//     additional transfer before expiresAt; a skipped step is still published, so every event follows the
//     state diagram;
//   - a reorg that removed counted transfers is invoice.reverted with the status recalculated from the
//     remaining ones (NEW when none is left);
//   - NEW -> EXPIRED when the chain time passes expiresAt and the revert wait;
//   - in a final status a transfer within the tracking window is reported after N confirmations:
//     EXPIRED -> LATE_PAID, otherwise invoice.late_transfer with the status unchanged.
func Evaluate(cfg Config, rec Record, transfers []Transfer, chain Chain) Result {
	e := &evaluation{cfg: cfg, chain: chain, rec: rec,
		ts: append([]Transfer(nil), transfers...)}
	if rec.Received == nil {
		e.rec.Received = new(big.Int)
	}
	if !Final(e.rec, chain.Time) {
		e.recount()
		e.expire()
	}
	if Final(e.rec, chain.Time) {
		e.late()
	}
	e.schedule()
	res := Result{Record: e.rec, Transfers: e.ts, Transitions: e.out}
	for i := range transfers {
		if e.ts[i].Role != transfers[i].Role || !e.ts[i].GraceUntil.Equal(transfers[i].GraceUntil) {
			res.Changed = append(res.Changed, i)
		}
	}
	return res
}

// confirmations of a canonical transfer: 1 when its block is the head.
func (e *evaluation) confirmations(t Transfer) int {
	if !t.Canonical || t.BlockNumber > e.chain.Head {
		return 0
	}
	d := e.chain.Head - t.BlockNumber + 1
	if d > uint64(1<<31-1) {
		return 1<<31 - 1
	}
	return int(d)
}

// grace returns the latest GraceUntil of the transfers with txHash.
func (e *evaluation) grace(txHash string) time.Time {
	var g time.Time
	for _, t := range e.ts {
		if t.TxHash == txHash && t.GraceUntil.After(g) {
			g = t.GraceUntil
		}
	}
	return g
}

func (e *evaluation) onTime(t Transfer) bool {
	return !t.BlockTime.After(e.rec.ExpiresAt) || !t.BlockTime.After(e.grace(t.TxHash))
}

func (e *evaluation) emit(tr Transition) {
	e.rec.Sequence++
	tr.Sequence = e.rec.Sequence
	tr.From = e.rec.Status
	if tr.To == "" {
		tr.To = e.rec.Status
	}
	e.rec.Status = tr.To
	e.out = append(e.out, tr)
}

// recount handles a non-final invoice: reverted transfers, the sum, the confirmations and the forward steps.
func (e *evaluation) recount() {
	var reverted []Transfer
	for i := range e.ts {
		t := &e.ts[i]
		if t.Role == RoleCounted && !t.Canonical {
			t.Role = RoleNone
			if e.chain.Time.After(e.rec.ExpiresAt) {
				t.GraceUntil = e.chain.Time.Add(e.cfg.RevertWait)
			}
			reverted = append(reverted, *t)
		}
	}
	received := new(big.Int)
	var last *Transfer
	var counted []Transfer
	added := false
	for i := range e.ts {
		t := &e.ts[i]
		if t.Canonical && t.Role != RoleLate && e.onTime(*t) {
			added = added || t.Role != RoleCounted
			t.Role = RoleCounted
			received.Add(received, t.Amount)
			counted = append(counted, *t)
			if last == nil || t.BlockNumber > last.BlockNumber {
				last = t
			}
		}
	}
	conf := 0
	if last != nil {
		conf = e.confirmations(*last)
	}
	e.rec.Received, e.rec.Confirmations = received, conf
	n := e.cfg.Confirmations

	if len(reverted) > 0 && (e.rec.Status == invoice.StatusDetected || e.rec.Status == invoice.StatusConfirming) {
		to := invoice.StatusNew
		switch {
		case last == nil:
		case conf == 1:
			to = invoice.StatusDetected
		default:
			to = invoice.StatusConfirming
		}
		from := e.chain.FromBlock
		if from == 0 {
			from = reverted[0].BlockNumber
			for _, t := range reverted {
				from = min(from, t.BlockNumber)
			}
		}
		e.emit(Transition{Type: EventReverted, To: to, Received: received, FromBlock: from,
			Transfers: sortTransfers(reverted)})
	}
	if last == nil {
		return
	}
	snapshot := func(typ string, to invoice.Status) Transition {
		return Transition{Type: typ, To: to, Confirmations: conf, Received: received}
	}
	if e.rec.Status == invoice.StatusNew {
		e.emit(snapshot(EventDetected, invoice.StatusDetected))
	}
	// A top-up: a transfer counted now for the first time. Fewer confirmations after a reorg are not one.
	if e.rec.Status == invoice.StatusUnderpaid && added {
		e.emit(snapshot(EventConfirming, invoice.StatusConfirming))
	}
	if e.rec.Status == invoice.StatusDetected && conf >= 2 {
		e.emit(snapshot(EventConfirming, invoice.StatusConfirming))
	}
	if e.rec.Status == invoice.StatusConfirming && conf >= n {
		res, err := money.Check(e.rec.Amount, received, e.rec.ToleranceBps)
		if err != nil {
			// The tolerance is 0..500 by the projection schema; an invalid one never pays an invoice.
			res = money.Result{}
		}
		tr := snapshot(EventUnderpaid, invoice.StatusUnderpaid)
		tr.Transfers = sortTransfers(counted)
		if res.Paid {
			tr.Type, tr.To, tr.Overpaid, tr.PaidAt = EventPaid, invoice.StatusPaid, res.Overpaid, e.chain.Time
			e.rec.Overpaid, e.rec.PaidAt = res.Overpaid, e.chain.Time
		}
		e.emit(tr)
	}
}

// deadline returns when a NEW invoice expires: expiresAt, or later while a reverted transfer may come back.
func (e *evaluation) deadline() time.Time {
	d := e.rec.ExpiresAt
	for _, t := range e.ts {
		if t.GraceUntil.After(d) {
			d = t.GraceUntil
		}
	}
	return d
}

func (e *evaluation) expire() {
	if e.rec.Status == invoice.StatusNew && e.chain.Time.After(e.deadline()) {
		e.emit(Transition{Type: EventExpired, To: invoice.StatusExpired})
	}
}

// late reports the transfers of a final invoice that reached N confirmations within the tracking window.
func (e *evaluation) late() {
	end := WindowEnd(e.rec, e.cfg.Window)
	var late []Transfer
	received := new(big.Int)
	lastConf := 0
	for i := range e.ts {
		t := &e.ts[i]
		if t.Canonical && t.Role == RoleNone && !t.BlockTime.After(end) && e.confirmations(*t) >= e.cfg.Confirmations {
			t.Role = RoleLate
			late = append(late, *t)
			received.Add(received, t.Amount)
			if c := e.confirmations(*t); lastConf == 0 || c < lastConf {
				lastConf = c
			}
		}
	}
	if len(late) == 0 {
		return
	}
	if e.rec.Status == invoice.StatusExpired {
		e.rec.Received, e.rec.Confirmations = received, lastConf
		e.emit(Transition{Type: EventLatePaid, To: invoice.StatusLatePaid, Confirmations: lastConf,
			Received: received, Transfers: sortTransfers(late)})
		return
	}
	e.emit(Transition{Type: EventLateTransfer, Confirmations: lastConf, Transfers: sortTransfers(late)})
}

// schedule sets when the invoice must be evaluated again without a new transfer.
func (e *evaluation) schedule() {
	e.rec.Watch, e.rec.Deadline = false, time.Time{}
	switch e.rec.Status {
	case invoice.StatusDetected, invoice.StatusConfirming:
		e.rec.Watch = true
	case invoice.StatusNew:
		e.rec.Deadline = e.deadline()
	case invoice.StatusUnderpaid:
		if !Final(e.rec, e.chain.Time) {
			e.rec.Deadline = e.rec.ExpiresAt
		}
	}
	if !Final(e.rec, e.chain.Time) {
		return
	}
	end := WindowEnd(e.rec, e.cfg.Window)
	for _, t := range e.ts {
		if t.Canonical && t.Role == RoleNone && !t.BlockTime.After(end) {
			e.rec.Watch = true
		}
	}
}

// sortTransfers orders transfers by block, transaction and log for stable events.
func sortTransfers(ts []Transfer) []Transfer {
	out := append([]Transfer(nil), ts...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && lessTransfer(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func lessTransfer(a, b Transfer) bool {
	if a.BlockNumber != b.BlockNumber {
		return a.BlockNumber < b.BlockNumber
	}
	return a.Key() < b.Key()
}

// EventID is the deterministic id of the event of an invoice version: a repeated evaluation of the same
// version yields the same id, so consumers deduplicate it.
func EventID(invoiceID uuid.UUID, sequence int) string {
	return uuid.NewSHA1(invoiceID, []byte(fmt.Sprintf("sequence:%d", sequence))).String()
}
