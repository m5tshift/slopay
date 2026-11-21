package watcher

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
)

const (
	usdt    = "0x5fbdb2315678afecb367f032d93f642f64180aa3"
	other20 = "0x00000000000000000000000000000000000000dd"
)

func withUSDT(c *Config) {
	c.StartBlock = ptr(0)
	c.Tokens = []chain.Token{{Address: usdt, Decimals: 6}}
}

func TestTokenTransfersPublishedBeforeNewHead(t *testing.T) {
	e := newEnv(t, 3, withUSDT)
	e.addrs.track(payee)
	e.addrs.track(other)
	var st chain.State
	e.sync(t, &st)
	e.node.extend(1, "a")
	eth := e.node.pay(3, payee)
	t1 := e.node.payToken(3, usdt, payee, 20_000_000)
	e.node.payToken(3, usdt, "0x00000000000000000000000000000000000000cc", 5) // not tracked
	t2 := e.node.payToken(3, usdt, payee, 7)
	e.node.payToken(3, other20, payee, 9) // a contract that is not configured
	e.sync(t, &st)

	got := order(e.pub.all()[3:])
	wantOrder := []string{
		"transfer:3:" + eth,
		"transfer:3:" + t1 + ":0",
		"transfer:3:" + t2 + ":2",
		"head:3",
	}
	if !slices.Equal(got, wantOrder) {
		t.Fatalf("events\n got %v\nwant %v", got, wantOrder)
	}
	full := e.pub.full
	if len(full) != 3 || full[1].Log == nil || full[1].Token != (chain.Token{Address: usdt, Decimals: 6}) ||
		full[1].Log.Amount.Int64() != 20_000_000 || full[1].Log.LogIndex != 0 || full[2].Log.LogIndex != 2 {
		t.Fatalf("published %+v", full)
	}
	if q := e.addrs.queries[3]; !slices.Equal(q, []string{payee, "0x00000000000000000000000000000000000000cc"}) {
		t.Errorf("recipients of block 3: %v", q)
	}
}

func TestNoTokensNoLogCalls(t *testing.T) {
	e := newEnv(t, 3, func(c *Config) { c.StartBlock = ptr(0) })
	var st chain.State
	e.sync(t, &st)
	if e.node.logCalls != 0 {
		t.Fatalf("%d log calls", e.node.logCalls)
	}
}

// The same logIndex of one transaction in another block after a reorg is another event.
func TestTokenTransferLogIndexInKey(t *testing.T) {
	e := newEnv(t, 3, withUSDT)
	e.addrs.track(payee)
	e.node.payToken(2, usdt, payee, 1)
	e.node.payToken(2, usdt, payee, 2)
	var st chain.State
	e.sync(t, &st)
	full := e.pub.full
	if len(full) != 2 || full[0].Log.TxHash == full[1].Log.TxHash || full[0].Log.LogIndex == full[1].Log.LogIndex {
		t.Fatalf("published %+v", full)
	}
}

func TestTokenLogsErrors(t *testing.T) {
	e := newEnv(t, 2, withUSDT)
	var st chain.State
	boom := errors.New("node down")
	e.node.logErr = boom
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if len(e.pub.all()) != 0 {
		t.Fatal("block published without its logs")
	}
	// A block the node no longer knows is a replaced block.
	e.node.logErr = chain.ErrNotFound
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, errReplaced) {
		t.Fatalf("not found: err = %v", err)
	}
	// A log of another block than the asked one is a replaced block too.
	e.node.logErr = nil
	e.node.logs[e.node.canonical[0].Hash] = []chain.TokenTransfer{{BlockHash: "x", TxHash: "t", Contract: usdt, To: payee}}
	if _, err := e.w.Step(context.Background(), &st); !errors.Is(err, errReplaced) {
		t.Fatalf("foreign log: err = %v", err)
	}
}

func TestTokenRescan(t *testing.T) {
	e := newEnv(t, 8, withUSDT)
	early := e.node.payToken(2, usdt, payee, 1) // before the invoice
	tx5 := e.node.payToken(5, usdt, payee, 20_000_000)
	var st chain.State
	e.sync(t, &st)
	e.addrs.add(payee, at(6))
	e.clk.advance(at(7).Sub(e.clk.now()) + 5*time.Second)
	if _, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatal(err)
	}
	if got := order(e.pub.all()[8:]); !slices.Equal(got, []string{"transfer:5:" + tx5 + ":0"}) {
		t.Fatalf("events %v", got)
	}
	for _, ev := range e.pub.all() {
		if ev.hash == early+":0" {
			t.Fatal("transfer before the invoice published")
		}
	}
	if v, _ := value(e.metrics(t), "weigate_chain_transfers_published", "mode=rescan"); v != 1 {
		t.Errorf("rescan transfers %d", v)
	}
}

// A block replaced while its logs are searched postpones the search, like a missing receipt.
func TestTokenRescanPostponedByReplacedBlock(t *testing.T) {
	e := newEnv(t, 6, withUSDT)
	var st chain.State
	e.sync(t, &st)
	e.addrs.add(payee, at(3))
	e.node.logErr = chain.ErrNotFound
	if progressed, err := e.w.Step(context.Background(), &st); err != nil {
		t.Fatalf("step = %v, %v", progressed, err)
	}
	if len(e.addrs.rescanned) != 0 {
		t.Fatal("marked although the block was replaced")
	}
	e.node.logErr = errors.New("node down")
	if _, err := e.w.Step(context.Background(), &st); err == nil {
		t.Fatal("node error swallowed")
	}
}
