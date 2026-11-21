package ledger_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/google/uuid"
	"pgregory.net/rapid"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledger"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledgertest"
)

var postingEventTypes = []string{"invoice.paid", "invoice.underpaid", "invoice.late_paid", "invoice.late_transfer"}

// genEvents draws invoice events of a few invoices whose transfers overlap: the same transfer arrives in
// several events (PAID, then invoice.late_transfer), events repeat (at-least-once delivery) and come in any
// order. It returns the events and the expected amount of every unique transfer.
func genEvents(t *rapid.T, codec *kafka.Codec) ([]kafka.Event, map[string]*big.Int) {
	type invoice struct {
		inv       ledgertest.Invoice
		transfers []ledger.Transfer
	}
	merchants := []uuid.UUID{merchantA, uuid.MustParse("7f3e2d1c-0b9a-4876-9543-210fedcba987")}
	var invoices []invoice
	unique := map[string]*big.Int{}
	for i := range rapid.IntRange(1, 4).Draw(t, "invoices") {
		n := uint64(i + 1)
		inv := ethInvoice(n)
		inv.Merchant = merchants[rapid.IntRange(0, 1).Draw(t, "merchant")]
		erc20 := rapid.Bool().Draw(t, "erc20")
		if erc20 {
			inv.Asset = tokenInvoice(n).Asset
		}
		var trs []ledger.Transfer
		for j := range rapid.IntRange(1, 4).Draw(t, "transfers") {
			amount := rapid.Int64Range(0, 1<<62).Draw(t, "amount")
			block := rapid.Uint64Range(1, 3).Draw(t, "block")
			tx := n*100 + uint64(j)
			var tr ledger.Transfer
			if erc20 {
				// Two logs of one transaction are different transfers.
				tr = ledgertest.ERC20(block, tx/2, uint(tx%2), amount)
			} else {
				tr = ledgertest.ETH(block, tx, amount)
			}
			// The same transfer always has the same amount; the first draw wins.
			if prev, ok := unique[tr.Key()]; ok {
				tr.Amount = new(big.Int).Set(prev)
			} else {
				unique[tr.Key()] = new(big.Int).Set(tr.Amount)
			}
			trs = append(trs, tr)
		}
		invoices = append(invoices, invoice{inv: inv, transfers: trs})
	}

	var events []kafka.Event
	for i, inv := range invoices {
		// Every transfer reaches at least one posting event, possibly together with others.
		for j := range inv.transfers {
			k := rapid.IntRange(j, len(inv.transfers)-1).Draw(t, "upto")
			typ := rapid.SampledFrom(postingEventTypes).Draw(t, "type")
			events = append(events, ledgertest.Event(t, codec, typ, inv.inv, i*10+j+2, inv.transfers[:k+1]...))
		}
		events = append(events, ledgertest.Event(t, codec, "invoice.detected", inv.inv, 1, inv.transfers[0]))
	}
	// Repeats and permutations.
	for range rapid.IntRange(0, len(events)).Draw(t, "repeats") {
		events = append(events, events[rapid.IntRange(0, len(events)-1).Draw(t, "repeat")])
	}
	perm := rapid.Permutation(events).Draw(t, "order")
	return perm, unique
}

type snapshot struct {
	postings map[string]string // id -> amount:fee
	events   map[string]bool
}

func snap(store *ledger.MemStore) snapshot {
	s := snapshot{postings: map[string]string{}, events: map[string]bool{}}
	for _, p := range store.Postings() {
		s.postings[p.ID] = p.Transfer.Amount.String() + ":" + p.Fee.String()
	}
	for _, ev := range store.Events() {
		s.events[ev.ID] = true
	}
	return s
}

func TestPropertyDoubleEntryAndIdempotency(t *testing.T) {
	codec := newCodec(t)
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		feeBps := rapid.IntRange(0, 10000).Draw(t, "feeBps")
		events, unique := genEvents(t, codec)

		apply := func(events []kafka.Event) *ledger.MemStore {
			store := ledger.NewMemStore(codec)
			s, err := ledger.NewService(store, ledger.Config{FeeBps: feeBps}, quiet)
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range events {
				if err := s.Apply(ctx, ev); err != nil {
					t.Fatal(err)
				}
			}
			return store
		}
		store := apply(events)

		// One posting per unique transfer, each with the fee rounded down.
		postings := store.Postings()
		if len(postings) != len(unique) {
			t.Fatalf("%d postings for %d transfers", len(postings), len(unique))
		}
		debits, credits := new(big.Int), new(big.Int)
		for _, p := range postings {
			amount, ok := unique[p.Transfer.Key()]
			if !ok || p.Transfer.Amount.Cmp(amount) != 0 {
				t.Fatalf("posting %s of an unknown transfer or amount", p.ID)
			}
			fee := new(big.Int).Mul(amount, big.NewInt(int64(feeBps)))
			fee.Quo(fee, big.NewInt(10000))
			if p.Fee.Cmp(fee) != 0 {
				t.Fatalf("%s: fee %s, want %s", p.ID, p.Fee, fee)
			}
			if !p.Balanced() {
				t.Fatalf("%s is not balanced", p.ID)
			}
			for _, e := range p.Entries {
				if e.Amount.Sign() < 0 {
					t.Fatalf("%s: negative line", p.ID)
				}
				if e.Side == ledger.Debit {
					debits.Add(debits, e.Amount)
				} else {
					credits.Add(credits, e.Amount)
				}
			}
		}
		if debits.Cmp(credits) != 0 {
			t.Fatalf("debits %s, credits %s", debits, credits)
		}

		// The outbox holds exactly a receipt and a fee event per posting.
		got := snap(store)
		if len(store.Events()) != 2*len(postings) || len(got.events) != 2*len(postings) {
			t.Fatalf("%d events (%d unique) for %d postings", len(store.Events()), len(got.events), len(postings))
		}

		// Applying every distinct event once in the original order gives the same ledger.
		seen := map[string]bool{}
		var once []kafka.Event
		for _, ev := range events {
			if !seen[ev.ID] {
				seen[ev.ID] = true
				once = append(once, ev)
			}
		}
		want := snap(apply(once))
		if len(want.postings) != len(got.postings) || len(want.events) != len(got.events) {
			t.Fatalf("repeats changed the ledger: %d/%d postings", len(got.postings), len(want.postings))
		}
		for id, v := range want.postings {
			if got.postings[id] != v {
				t.Fatalf("%s: %q with repeats, %q without", id, got.postings[id], v)
			}
		}
	})
}
