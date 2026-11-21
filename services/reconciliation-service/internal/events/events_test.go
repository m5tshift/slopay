package events

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/recon"
)

const (
	runID      = "3f2b8c1d-4e5a-4b6c-8d7e-9f0a1b2c3d4e"
	merchantID = "7c0b6a51-5f0e-4f57-9d35-3f7f2d2b1a10"
	address    = "0x00000000000000000000000000000000000000aa"
	erc20Addr  = "0x5fbdb2315678afecb367f032d93f642f64180aa3"
)

// codecProducer encodes every event with the contract codec, as kafka.Producer does.
type codecProducer struct {
	t      *testing.T
	codec  *kafka.Codec
	events []kafka.Event
	fail   bool
}

func (p *codecProducer) Publish(_ context.Context, topic string, ev kafka.Event) error {
	if p.fail {
		return errors.New("kafka down")
	}
	if topic != Topic {
		p.t.Errorf("topic %s", topic)
	}
	if _, err := p.codec.Encode(ev); err != nil {
		p.t.Errorf("%s does not match the contract: %v", ev.Type, err)
	}
	p.events = append(p.events, ev)
	return nil
}

func report() recon.Report {
	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	eth := recon.Asset{Kind: recon.KindETH, Decimals: 18}
	tok := recon.Asset{Kind: recon.KindERC20, Contract: erc20Addr, Decimals: 6}
	return recon.Report{RunID: runID, ChainID: 31337, BlockNumber: 93, StartedAt: at, FinishedAt: at.Add(20 * time.Second),
		AddressesChecked: 3, Mismatches: 2, AfterWindow: 1, Rows: []recon.Row{
			{Kind: recon.KindInvoicesVsLedger, Address: address, MerchantID: merchantID, Asset: eth,
				ChainBalance: big.NewInt(1000), InvoicesTotal: big.NewInt(1000), LedgerTotal: big.NewInt(400)},
			{Kind: recon.KindAfterWindow, Address: address, MerchantID: merchantID, Asset: tok,
				ChainBalance: big.NewInt(9), InvoicesTotal: big.NewInt(5), LedgerTotal: big.NewInt(5), Excess: big.NewInt(4)},
			{Kind: recon.KindChainVsInvoices, Address: address, MerchantID: merchantID, Asset: tok,
				ChainBalance: big.NewInt(0), InvoicesTotal: big.NewInt(5), LedgerTotal: big.NewInt(5)},
		}}
}

func TestPublish(t *testing.T) {
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	p := &codecProducer{t: t, codec: codec}
	r := report()
	if err := NewPublisher(p).Publish(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if len(p.events) != 3 {
		t.Fatalf("events %d, want 2 mismatches and completed", len(p.events))
	}
	want := []string{runID + ":" + address + ":ETH", runID + ":" + address + ":" + erc20Addr, runID + ":completed"}
	for i, ev := range p.events {
		if ev.ID != want[i] || ev.Subject != runID || !ev.Time.Equal(r.FinishedAt) {
			t.Errorf("event %d: id %s subject %s time %v", i, ev.ID, ev.Subject, ev.Time)
		}
	}
	var m mismatchData
	_ = json.Unmarshal(p.events[0].Data, &m)
	if m.Kind != recon.KindInvoicesVsLedger || m.LedgerTotal != "400" || m.ChainBalance != "1000" {
		t.Errorf("mismatch data %+v", m)
	}
	var c completedData
	_ = json.Unmarshal(p.events[2].Data, &c)
	if c.Result != "MISMATCH" || c.Mismatches != 2 || c.AddressesChecked != 3 || c.BlockNumber != 93 {
		t.Errorf("completed data %+v", c)
	}

	// A report published again yields the same IDs.
	again := &codecProducer{t: t, codec: codec}
	_ = NewPublisher(again).Publish(context.Background(), r)
	for i := range again.events {
		if again.events[i].ID != p.events[i].ID {
			t.Errorf("republished id %s, want %s", again.events[i].ID, p.events[i].ID)
		}
	}
}

func TestPublishOK(t *testing.T) {
	codec, _ := kafka.NewCodec()
	p := &codecProducer{t: t, codec: codec}
	r := report()
	r.Rows, r.Mismatches, r.AfterWindow = nil, 0, 0
	if err := NewPublisher(p).Publish(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	var c completedData
	_ = json.Unmarshal(p.events[0].Data, &c)
	if len(p.events) != 1 || c.Result != "OK" {
		t.Fatalf("events %d, result %s", len(p.events), c.Result)
	}
	if err := NewPublisher(&codecProducer{t: t, codec: codec, fail: true}).Publish(context.Background(), r); err == nil {
		t.Fatal("kafka down: want error")
	}
}
