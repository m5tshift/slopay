package lifecycle

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

var (
	testCfg  = Config{Confirmations: 3, RevertWait: 10 * time.Minute, Window: 24 * time.Hour}
	base     = time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	expires  = base.Add(15 * time.Minute)
	testID   = uuid.MustParse("0b1c2d3e-4f50-4617-8899-aabbccddeeff")
	merchant = uuid.MustParse("1a2b3c4d-5e6f-4071-8293-a4b5c6d7e8f9")
)

func blockHash(fork, n uint64) string { return fmt.Sprintf("0x%016x%048x", fork, n) }

func txHash(n int) string { return fmt.Sprintf("0x%064x", 0xabc000+n) }

func newRecord(amount int64) Record {
	return Record{Invoice: invoice.Invoice{ID: testID, MerchantID: merchant, Amount: big.NewInt(amount),
		Address: "0x8ba1f109551bd432803012645ac136ddd64dba72", Asset: invoice.ETH, Status: invoice.StatusNew,
		Sequence: 1, Received: new(big.Int), ExpiresAt: expires}, ToleranceBps: 100, Deadline: expires}
}

// tr is a canonical transfer in block n of fork 0 at base+at.
func tr(n uint64, at time.Duration, amount int64, tx int) Transfer {
	return Transfer{BlockNumber: n, BlockHash: blockHash(0, n), BlockTime: base.Add(at), TxHash: txHash(tx),
		Amount: big.NewInt(amount), Asset: invoice.ETH, Canonical: true}
}

func chainAt(head uint64, at time.Duration) Chain { return Chain{Head: head, Time: base.Add(at)} }

// step evaluates and returns the result, applying it to rec and ts.
func step(t *testing.T, rec *Record, ts []Transfer, c Chain) Result {
	t.Helper()
	res := Evaluate(testCfg, *rec, ts, c)
	*rec = res.Record
	copy(ts, res.Transfers)
	for i, tr := range res.Transitions {
		if tr.Sequence != res.Record.Sequence-len(res.Transitions)+i+1 {
			t.Fatalf("transition %d: sequence %d, record %d", i, tr.Sequence, res.Record.Sequence)
		}
	}
	return res
}

func types(res Result) []string {
	out := make([]string, len(res.Transitions))
	for i, tr := range res.Transitions {
		out[i] = tr.Type
	}
	return out
}

func wantTypes(t *testing.T, res Result, want ...string) {
	t.Helper()
	got := types(res)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("transitions %v, want %v", got, want)
	}
}

func wantStatus(t *testing.T, rec Record, status invoice.Status, received int64) {
	t.Helper()
	if rec.Status != status || rec.Received.Cmp(big.NewInt(received)) != 0 {
		t.Fatalf("status %s received %s, want %s %d", rec.Status, rec.Received, status, received)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := testCfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []Config{
		{Confirmations: 1, RevertWait: time.Minute, Window: time.Hour},
		{Confirmations: 7, Window: time.Hour},
		{Confirmations: 7, RevertWait: time.Minute},
	} {
		if c.Validate() == nil {
			t.Fatalf("%+v accepted", c)
		}
	}
}

// NEW -> DETECTED (1 confirmation) -> CONFIRMING (2..N-1) -> PAID (N, within the tolerance).
func TestHappyPath(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	res := step(t, &rec, ts, chainAt(100, time.Minute))
	wantTypes(t, res, EventDetected)
	wantStatus(t, rec, invoice.StatusDetected, 1000)
	if tr := res.Transitions[0]; tr.Confirmations != 1 || tr.Sequence != 2 || tr.From != invoice.StatusNew {
		t.Fatalf("detected %+v", tr)
	}
	if !rec.Watch || !rec.Deadline.IsZero() || ts[0].Role != RoleCounted || len(res.Changed) != 1 {
		t.Fatalf("schedule %+v, transfer %+v", rec, ts[0])
	}
	res = step(t, &rec, ts, chainAt(101, 2*time.Minute))
	wantTypes(t, res, EventConfirming)
	if rec.Confirmations != 2 || len(res.Changed) != 0 {
		t.Fatalf("confirmations %d, changed %v", rec.Confirmations, res.Changed)
	}
	res = step(t, &rec, ts, chainAt(102, 3*time.Minute))
	wantTypes(t, res, EventPaid)
	wantStatus(t, rec, invoice.StatusPaid, 1000)
	paid := res.Transitions[0]
	if paid.Overpaid || !paid.PaidAt.Equal(base.Add(3*time.Minute)) || len(paid.Transfers) != 1 || paid.Confirmations != 3 {
		t.Fatalf("paid %+v", paid)
	}
	if !rec.PaidAt.Equal(paid.PaidAt) || rec.Sequence != 4 || rec.Watch {
		t.Fatalf("record %+v", rec)
	}
	wantTypes(t, step(t, &rec, ts, chainAt(110, time.Hour)))
}

// A CONFIRMING invoice with more confirmations below N publishes nothing but keeps the count.
func TestConfirmingCountsWithoutEvents(t *testing.T) {
	cfg := testCfg
	cfg.Confirmations = 5
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	for head, want := range map[uint64]int{101: 2, 102: 3, 103: 4} {
		r := rec
		r.Status = invoice.StatusConfirming
		res := Evaluate(cfg, r, ts, chainAt(head, time.Minute))
		if len(res.Transitions) != 0 || res.Record.Confirmations != want || !res.Record.Watch {
			t.Fatalf("head %d: %v, confirmations %d", head, types(res), res.Record.Confirmations)
		}
	}
}

// A transfer seen with N confirmations at once still publishes every step of the diagram.
func TestSkippedStepsArePublished(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	res := step(t, &rec, ts, chainAt(105, 2*time.Minute))
	wantTypes(t, res, EventDetected, EventConfirming, EventPaid)
	if res.Transitions[0].Confirmations != 6 || rec.Sequence != 4 {
		t.Fatalf("%+v", res.Transitions)
	}
}

func TestTolerance(t *testing.T) {
	for _, c := range []struct {
		bps      int
		received int64
		status   invoice.Status
		overpaid bool
	}{
		{100, 990, invoice.StatusPaid, false},
		{100, 989, invoice.StatusUnderpaid, false},
		{0, 999, invoice.StatusUnderpaid, false},
		{0, 1000, invoice.StatusPaid, false},
		{500, 950, invoice.StatusPaid, false},
		{100, 1001, invoice.StatusPaid, true},
	} {
		rec := newRecord(1000)
		rec.ToleranceBps = c.bps
		ts := []Transfer{tr(100, time.Minute, c.received, 1)}
		step(t, &rec, ts, chainAt(102, 3*time.Minute))
		if rec.Status != c.status || rec.Overpaid != c.overpaid {
			t.Fatalf("%+v: status %s overpaid %v", c, rec.Status, rec.Overpaid)
		}
	}
}

// An invalid tolerance never pays.
func TestInvalidToleranceUnderpays(t *testing.T) {
	rec := newRecord(1000)
	rec.ToleranceBps = -1
	step(t, &rec, []Transfer{tr(100, time.Minute, 1000, 1)}, chainAt(102, 3*time.Minute))
	wantStatus(t, rec, invoice.StatusUnderpaid, 1000)
}

// TTL limits the sending, not the confirmations: an invoice in DETECTED or CONFIRMING does not expire.
func TestTTLDoesNotLimitConfirmations(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, 14*time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(100, 14*time.Minute))
	wantTypes(t, step(t, &rec, ts, chainAt(101, time.Hour)), EventConfirming)
	wantTypes(t, step(t, &rec, ts, chainAt(102, 2*time.Hour)), EventPaid)
}

// A transfer counts by the timestamp of its block: at expiresAt it is on time, after it is not.
func TestTransferDeadlineIsBlockTimestamp(t *testing.T) {
	rec := newRecord(1000)
	step(t, &rec, []Transfer{tr(100, 15*time.Minute, 1000, 1)}, chainAt(100, 15*time.Minute))
	wantStatus(t, rec, invoice.StatusDetected, 1000)

	rec = newRecord(1000)
	ts := []Transfer{tr(100, 15*time.Minute+time.Second, 1000, 1)}
	res := step(t, &rec, ts, chainAt(100, 15*time.Minute+time.Second))
	wantTypes(t, res, EventExpired)
	if ts[0].Role != RoleNone || !rec.Watch {
		t.Fatalf("late transfer %+v, record %+v", ts[0], rec)
	}
}

// NEW -> EXPIRED only after a block with a timestamp later than expiresAt.
func TestExpiry(t *testing.T) {
	rec := newRecord(1000)
	wantTypes(t, step(t, &rec, nil, chainAt(100, 15*time.Minute)))
	if !rec.Deadline.Equal(expires) {
		t.Fatalf("deadline %v", rec.Deadline)
	}
	res := step(t, &rec, nil, chainAt(101, 15*time.Minute+time.Second))
	wantTypes(t, res, EventExpired)
	if rec.Status != invoice.StatusExpired || rec.Sequence != 2 || !rec.Deadline.IsZero() || rec.Watch {
		t.Fatalf("record %+v", rec)
	}
	wantTypes(t, step(t, &rec, nil, chainAt(102, time.Hour)))
}

// Several transfers are summed; confirmations count from the last one.
func TestTransfersAreSummed(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 400, 1), tr(101, 2*time.Minute, 600, 2)}
	ts[1].Canonical = false
	step(t, &rec, ts, chainAt(100, time.Minute))
	wantStatus(t, rec, invoice.StatusDetected, 400)
	step(t, &rec, ts, chainAt(101, 2*time.Minute))
	wantStatus(t, rec, invoice.StatusConfirming, 400)
	ts[1].Canonical = true
	step(t, &rec, ts, chainAt(101, 2*time.Minute))
	if rec.Confirmations != 1 || rec.Status != invoice.StatusConfirming {
		t.Fatalf("record %+v", rec)
	}
	wantTypes(t, step(t, &rec, ts, chainAt(102, 3*time.Minute)))
	res := step(t, &rec, ts, chainAt(103, 4*time.Minute))
	wantTypes(t, res, EventPaid)
	wantStatus(t, rec, invoice.StatusPaid, 1000)
	if len(res.Transitions[0].Transfers) != 2 {
		t.Fatalf("transfers %+v", res.Transitions[0].Transfers)
	}
}

// UNDERPAID -> CONFIRMING on an additional transfer before expiresAt; the sums add up.
func TestTopUp(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 500, 1), tr(104, 5*time.Minute, 500, 2)}
	ts[1].Canonical = false
	step(t, &rec, ts, chainAt(102, 3*time.Minute))
	wantStatus(t, rec, invoice.StatusUnderpaid, 500)
	if !rec.Deadline.Equal(expires) || rec.Watch {
		t.Fatalf("underpaid schedule %+v", rec)
	}
	ts[1].Canonical = true
	res := step(t, &rec, ts, chainAt(104, 5*time.Minute))
	wantTypes(t, res, EventConfirming)
	if res.Transitions[0].From != invoice.StatusUnderpaid || rec.Confirmations != 1 {
		t.Fatalf("%+v", res.Transitions[0])
	}
	wantTypes(t, step(t, &rec, ts, chainAt(106, 7*time.Minute)), EventPaid)
	wantStatus(t, rec, invoice.StatusPaid, 1000)
}

// After expiresAt UNDERPAID is final: a later transfer is invoice.late_transfer after N confirmations.
func TestUnderpaidFinality(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 500, 1), tr(110, 20*time.Minute, 500, 2)}
	ts[1].Canonical = false
	step(t, &rec, ts, chainAt(102, 3*time.Minute))
	wantTypes(t, step(t, &rec, ts, chainAt(109, 16*time.Minute)))
	if !rec.Deadline.IsZero() || rec.Watch {
		t.Fatalf("final underpaid schedule %+v", rec)
	}
	ts[1].Canonical = true
	wantTypes(t, step(t, &rec, ts, chainAt(110, 20*time.Minute)))
	if !rec.Watch || rec.Status != invoice.StatusUnderpaid {
		t.Fatalf("record %+v", rec)
	}
	res := step(t, &rec, ts, chainAt(112, 22*time.Minute))
	wantTypes(t, res, EventLateTransfer)
	lt := res.Transitions[0]
	if lt.To != invoice.StatusUnderpaid || len(lt.Transfers) != 1 || lt.Transfers[0].TxHash != txHash(2) {
		t.Fatalf("late transfer %+v", lt)
	}
	wantStatus(t, rec, invoice.StatusUnderpaid, 500)
	if ts[1].Role != RoleLate || rec.Watch {
		t.Fatalf("transfer %+v, record %+v", ts[1], rec)
	}
	wantTypes(t, step(t, &rec, ts, chainAt(113, 23*time.Minute)))
}

// A reorg that removed every counted transfer returns the invoice to NEW.
func TestRevertToNew(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(101, 2*time.Minute))
	ts[0].Canonical = false
	res := step(t, &rec, ts, Chain{Head: 99, Time: base.Add(2 * time.Minute), FromBlock: 100})
	wantTypes(t, res, EventReverted)
	rv := res.Transitions[0]
	if rv.From != invoice.StatusConfirming || rv.To != invoice.StatusNew || rv.FromBlock != 100 ||
		len(rv.Transfers) != 1 || rv.Received.Sign() != 0 {
		t.Fatalf("reverted %+v", rv)
	}
	if ts[0].Role != RoleNone || !ts[0].GraceUntil.IsZero() || !rec.Deadline.Equal(expires) || rec.Watch {
		t.Fatalf("before expiresAt there is no grace: %+v %+v", ts[0], rec)
	}
	wantStatus(t, rec, invoice.StatusNew, 0)
}

// A reorg that removed part of the transfers recalculates the sum and the status from the others.
func TestPartialRevert(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 400, 1), tr(101, 2*time.Minute, 600, 2)}
	step(t, &rec, ts, chainAt(101, 2*time.Minute))
	ts[1].Canonical = false
	res := step(t, &rec, ts, Chain{Head: 100, Time: base.Add(2 * time.Minute), FromBlock: 101})
	wantTypes(t, res, EventReverted)
	if res.Transitions[0].To != invoice.StatusDetected || res.Transitions[0].Received.Int64() != 400 {
		t.Fatalf("reverted %+v", res.Transitions[0])
	}

	// The remaining transfer is already deep (a top-up of UNDERPAID was reverted): CONFIRMING, then the
	// settlement with the remaining sum.
	rec = newRecord(1000)
	ts = []Transfer{tr(100, time.Minute, 400, 1), tr(103, 4*time.Minute, 600, 2)}
	ts[1].Canonical = false
	step(t, &rec, ts, chainAt(102, 3*time.Minute))
	wantStatus(t, rec, invoice.StatusUnderpaid, 400)
	ts[1].Canonical = true
	step(t, &rec, ts, chainAt(103, 4*time.Minute))
	wantStatus(t, rec, invoice.StatusConfirming, 1000)
	ts[1].Canonical = false
	res = step(t, &rec, ts, Chain{Head: 102, Time: base.Add(4 * time.Minute), FromBlock: 103})
	wantTypes(t, res, EventReverted, EventUnderpaid)
	if res.Transitions[0].To != invoice.StatusConfirming || res.Transitions[1].Received.Int64() != 400 {
		t.Fatalf("%+v", res.Transitions)
	}
}

// Without FromBlock (a block replaced in place) the reverted event names the lowest removed block.
func TestRevertFromBlockFallback(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(100, time.Minute))
	ts[0].Canonical = false
	res := step(t, &rec, ts, chainAt(100, time.Minute))
	if res.Transitions[0].FromBlock != 100 {
		t.Fatalf("fromBlock %d", res.Transitions[0].FromBlock)
	}
}

// A revert after expiresAt waits RevertWait for the same txHash; included again it is on time.
func TestRevertAfterExpiryReinclusion(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, 14*time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(101, 16*time.Minute))
	ts[0].Canonical = false
	res := step(t, &rec, ts, Chain{Head: 99, Time: base.Add(16 * time.Minute), FromBlock: 100})
	wantTypes(t, res, EventReverted)
	grace := base.Add(26 * time.Minute)
	if !ts[0].GraceUntil.Equal(grace) || !rec.Deadline.Equal(grace) || len(res.Changed) != 1 {
		t.Fatalf("grace %v, deadline %v", ts[0].GraceUntil, rec.Deadline)
	}
	// Not expired within the wait.
	wantTypes(t, step(t, &rec, ts, Chain{Head: 100, Time: base.Add(20 * time.Minute)}))
	// Another transaction after expiresAt does not count; the same one does.
	other := Transfer{BlockNumber: 101, BlockHash: blockHash(1, 101), BlockTime: base.Add(21 * time.Minute),
		TxHash: txHash(9), Amount: big.NewInt(1000), Asset: invoice.ETH, Canonical: true}
	again := Transfer{BlockNumber: 102, BlockHash: blockHash(1, 102), BlockTime: base.Add(25 * time.Minute),
		TxHash: txHash(1), Amount: big.NewInt(1000), Asset: invoice.ETH, Canonical: true}
	ts = append(ts, other, again)
	res = step(t, &rec, ts, Chain{Head: 102, Time: base.Add(25 * time.Minute)})
	wantTypes(t, res, EventDetected)
	wantStatus(t, rec, invoice.StatusDetected, 1000)
	if ts[1].Role != RoleNone || ts[2].Role != RoleCounted {
		t.Fatalf("roles %v %v", ts[1].Role, ts[2].Role)
	}
	// The other transaction is a transfer to a paid invoice within the window.
	wantTypes(t, step(t, &rec, ts, Chain{Head: 104, Time: base.Add(30 * time.Minute)}),
		EventConfirming, EventPaid, EventLateTransfer)
}

// Included again after the wait the transaction is late; the invoice expires when the wait is over.
func TestRevertAfterExpiryTimeout(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, 14*time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(100, 16*time.Minute))
	ts[0].Canonical = false
	step(t, &rec, ts, Chain{Head: 99, Time: base.Add(16 * time.Minute), FromBlock: 100})
	wantTypes(t, step(t, &rec, ts, Chain{Head: 100, Time: base.Add(26 * time.Minute)}))
	wantTypes(t, step(t, &rec, ts, Chain{Head: 101, Time: base.Add(26*time.Minute + time.Second)}), EventExpired)
}

// A revert before expiresAt gives no wait: the invoice expires at expiresAt.
func TestRevertBeforeExpiryHasNoGrace(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(100, time.Minute))
	ts[0].Canonical = false
	step(t, &rec, ts, Chain{Head: 99, Time: base.Add(time.Minute), FromBlock: 100})
	again := Transfer{BlockNumber: 120, BlockHash: blockHash(1, 120), BlockTime: base.Add(16 * time.Minute),
		TxHash: txHash(1), Amount: big.NewInt(1000), Asset: invoice.ETH, Canonical: true}
	ts = append(ts, again)
	wantTypes(t, step(t, &rec, ts, Chain{Head: 120, Time: base.Add(16 * time.Minute)}), EventExpired)
}

// EXPIRED -> LATE_PAID for a transfer within the window after N confirmations; the late sum is reported.
func TestLatePaid(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, 20*time.Minute, 700, 1)}
	step(t, &rec, ts, chainAt(100, 20*time.Minute))
	wantStatus(t, rec, invoice.StatusExpired, 0)
	wantTypes(t, step(t, &rec, ts, chainAt(101, 21*time.Minute)))
	res := step(t, &rec, ts, chainAt(102, 22*time.Minute))
	wantTypes(t, res, EventLatePaid)
	wantStatus(t, rec, invoice.StatusLatePaid, 700)
	if res.Transitions[0].Confirmations != 3 || rec.Watch || !rec.PaidAt.IsZero() {
		t.Fatalf("%+v %+v", res.Transitions[0], rec)
	}
	// LATE_PAID does not extend the window: within it a further transfer is late_transfer, after it nothing.
	ts = append(ts, tr(103, 15*time.Minute+24*time.Hour, 50, 2), tr(104, 15*time.Minute+24*time.Hour+time.Second, 50, 3))
	res = step(t, &rec, ts, chainAt(106, 25*time.Hour))
	wantTypes(t, res, EventLateTransfer)
	if len(res.Transitions[0].Transfers) != 1 || ts[2].Role != RoleNone || rec.Watch {
		t.Fatalf("%+v, %+v", res.Transitions[0], ts[2])
	}
	wantStatus(t, rec, invoice.StatusLatePaid, 700)
}

// The window starts at the later of expiresAt and paidAt.
func TestWindowAfterPaidAt(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, 14*time.Minute, 1000, 1)}
	step(t, &rec, ts, chainAt(102, time.Hour))
	wantStatus(t, rec, invoice.StatusPaid, 1000)
	if got := WindowEnd(rec, testCfg.Window); !got.Equal(base.Add(25 * time.Hour)) {
		t.Fatalf("window end %v", got)
	}
	ts = append(ts, tr(103, 24*time.Hour+50*time.Minute, 10, 2))
	wantTypes(t, step(t, &rec, ts, chainAt(105, 25*time.Hour)), EventLateTransfer)
}

// A transfer outside the window is never processed.
func TestAfterWindowIgnored(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, 15*time.Minute+24*time.Hour+time.Second, 1000, 1)}
	res := step(t, &rec, ts, chainAt(110, 30*time.Hour))
	wantTypes(t, res, EventExpired)
	if rec.Watch || ts[0].Role != RoleNone {
		t.Fatalf("%+v %+v", rec, ts[0])
	}
}

// A transfer whose block is not canonical is not counted.
func TestNonCanonicalIgnored(t *testing.T) {
	rec := newRecord(1000)
	ts := []Transfer{tr(100, time.Minute, 1000, 1)}
	ts[0].Canonical = false
	res := step(t, &rec, ts, chainAt(105, 2*time.Minute))
	wantTypes(t, res)
	wantStatus(t, rec, invoice.StatusNew, 0)
	if Evaluate(testCfg, rec, ts, chainAt(99, time.Minute)).Record.Confirmations != 0 {
		t.Fatal("confirmations of a missing transfer")
	}
}

// Confirmations of a transfer above the head (during a reorg) are zero.
func TestConfirmationsAboveHead(t *testing.T) {
	e := &evaluation{chain: Chain{Head: 99}}
	if c := e.confirmations(tr(100, 0, 1, 1)); c != 0 {
		t.Fatalf("confirmations %d", c)
	}
}

func TestFinal(t *testing.T) {
	rec := newRecord(1)
	for status, want := range map[invoice.Status][2]bool{
		invoice.StatusNew: {false, false}, invoice.StatusDetected: {false, false},
		invoice.StatusConfirming: {false, false}, invoice.StatusUnderpaid: {false, true},
		invoice.StatusPaid: {true, true}, invoice.StatusExpired: {true, true}, invoice.StatusLatePaid: {true, true},
	} {
		rec.Status = status
		if Final(rec, expires) != want[0] || Final(rec, expires.Add(time.Second)) != want[1] {
			t.Fatalf("%s: %v", status, want)
		}
	}
}

func TestEventID(t *testing.T) {
	a, b := EventID(testID, 2), EventID(testID, 3)
	if a != EventID(testID, 2) || a == b || uuid.MustParse(a).Version() != 5 {
		t.Fatalf("%s %s", a, b)
	}
}

func TestTransferKeyAndOrder(t *testing.T) {
	li := int64(3)
	a := tr(100, 0, 1, 1)
	b := a
	b.LogIndex = &li
	if a.Key() == b.Key() || a.Key() != blockHash(0, 100)+":"+txHash(1)+":" {
		t.Fatalf("keys %s %s", a.Key(), b.Key())
	}
	got := sortTransfers([]Transfer{tr(101, 0, 1, 1), b, a})
	if got[0].Key() != a.Key() || got[1].Key() != b.Key() || got[2].BlockNumber != 101 {
		t.Fatalf("order %+v", got)
	}
}
