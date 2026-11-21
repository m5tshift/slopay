//go:build integration

package watcher_test

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"go.opentelemetry.io/otel/attribute"

	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
)

// usdtBytecode is the creation bytecode of the test USDT (testkit/erc20), as stand-setup deploys it.
func usdtBytecode(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "testkit", "stand-setup", "internal", "usdt", "TestUSDT.bin"))
	if err != nil {
		t.Fatal(err)
	}
	code, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return code
}

// sendTx sends a transaction of the unlocked payer account and waits for its block.
func sendTx(t *testing.T, to string, data []byte) (tx string, block uint64, contract string) {
	t.Helper()
	msg := map[string]any{"from": payer, "data": hexutil.Bytes(data)}
	if to != "" {
		msg["to"] = to
	}
	rpcCall(t, &tx, "eth_sendTransaction", msg)
	eventually(t, "receipt of "+tx, func() bool {
		var r *struct {
			BlockNumber     hexutil.Uint64 `json:"blockNumber"`
			ContractAddress string         `json:"contractAddress"`
			Status          hexutil.Uint64 `json:"status"`
		}
		rpcCall(t, &r, "eth_getTransactionReceipt", tx)
		if r == nil {
			return false
		}
		if r.Status != 1 {
			t.Fatalf("transaction %s reverted", tx)
		}
		block, contract = uint64(r.BlockNumber), strings.ToLower(r.ContractAddress)
		return true
	})
	return tx, block, contract
}

func call2(sig string, a common.Address, v *big.Int) []byte {
	d := crypto.Keccak256([]byte(sig))[:4]
	d = append(d, common.LeftPadBytes(a.Bytes(), 32)...)
	return append(d, common.LeftPadBytes(v.Bytes(), 32)...)
}

func deployUSDT(t *testing.T) string {
	t.Helper()
	_, _, addr := sendTx(t, "", usdtBytecode(t))
	_, _, _ = sendTx(t, addr, call2("mint(address,uint256)", common.HexToAddress(payer), big.NewInt(1_000_000_000)))
	return addr
}

// TestERC20Transfers pays tracked addresses in the test USDT: the Transfer logs of the block are published
// before its chain.new_head with the logIndex, the contract, decimals and the amount in minimal units; a token
// that is not configured and an address that is not tracked are ignored; a payment made before the invoice is
// found by the rescan.
func TestERC20Transfers(t *testing.T) {
	token := deployUSDT(t)
	h := newHarness(t)
	h.tokens = []chain.Token{{Address: token, Decimals: 6}}
	h.run(t)
	mine(t, 1)
	h.events.waitHead(t, headNumber(t))

	addr, stranger := newAddress(t), newAddress(t)
	h.createInvoice(t, addr, time.Now(), time.Now().Add(15*time.Minute))
	h.waitSearched(t, addr)

	pay := func(to string, amount int64) (string, uint64) {
		tx, n, _ := sendTx(t, token, call2("transfer(address,uint256)", common.HexToAddress(to), big.NewInt(amount)))
		return tx, n
	}
	live, n := pay(addr, 20_000_000)
	pay(stranger, 5)
	h.events.waitHead(t, n+1) // the block of the stranger transfer is processed too
	got := h.events.waitTransfer(t, live)
	evs := h.events.all(t)
	tr, head := position(t, evs, live, n)
	if tr < 0 || tr >= head {
		t.Fatalf("transfer at %d, head %d at %d", tr, n, head)
	}
	if got.BlockNumber != n || got.BlockHash != blockAt(t, n).Hash || got.To != addr || got.From != payer ||
		got.Amount != "20000000" || got.LogIndex == nil || got.Asset.Kind != "ERC20" || got.Asset.Contract != token ||
		got.Asset.Decimals != 6 || got.key != addr ||
		got.id != fmt.Sprintf("%s:%s:%d", got.BlockHash, live, *got.LogIndex) {
		t.Fatalf("transfer %+v", got)
	}
	for _, e := range evs {
		if e.typ == "chain.transfer" && e.To == stranger {
			t.Fatal("transfer to an untracked address published")
		}
	}

	// A payment before the invoice is found in the processed block.
	late := newAddress(t)
	early, m := pay(late, 7)
	h.events.waitHead(t, m)
	h.createInvoice(t, late, time.Now(), time.Now().Add(15*time.Minute))
	h.waitSearched(t, late)
	rescanned := h.events.waitTransfer(t, early)
	if rescanned.Amount != "7" || rescanned.Asset.Contract != token || rescanned.LogIndex == nil {
		t.Fatalf("rescanned %+v", rescanned)
	}
	mine(t, 1)
	h.events.waitHead(t, m+1)
	tr, _ = position(t, h.events.all(t), early, m)
	_, headNext := position(t, h.events.all(t), early, m+1)
	if tr < 0 || tr >= headNext {
		t.Fatalf("rescanned transfer at %d, head %d at %d", tr, m+1, headNext)
	}
	if got := h.metric(t, "weigate_chain_transfers_published", attribute.String("mode", "rescan")); got != 1 {
		t.Errorf("rescan transfers %d", got)
	}
	view, _ := replay(t, h.events.all(t))
	matchesNode(t, view, h.start, headNumber(t))
}

// TestERC20TransferReorged moves a token payment to another block with a reorg: the transfer of the new branch
// has another blockHash, so another ID, and the same txHash.
func TestERC20TransferReorged(t *testing.T) {
	token := deployUSDT(t)
	h := newHarness(t)
	h.tokens = []chain.Token{{Address: token, Decimals: 6}}
	h.run(t)
	addr := newAddress(t)
	h.createInvoice(t, addr, time.Now(), time.Now().Add(15*time.Minute))
	h.waitSearched(t, addr)
	mine(t, 1)
	h.events.waitHead(t, headNumber(t))

	snap := snapshot(t)
	tx, n, _ := sendTx(t, token, call2("transfer(address,uint256)", common.HexToAddress(addr), big.NewInt(42)))
	h.events.waitHead(t, n)
	first := h.events.waitTransfer(t, tx)

	// The reorged branch has the same transaction in the same block number but another hash.
	revertAndMine(t, snap, 0)
	rpcCall(t, nil, "eth_sendTransaction", map[string]any{"from": payer, "to": token,
		"data": hexutil.Bytes(call2("transfer(address,uint256)", common.HexToAddress(addr), big.NewInt(42)))})
	mine(t, 2)
	h.events.waitHead(t, headNumber(t))
	eventually(t, "the transfer in the new branch", func() bool {
		count := 0
		for _, e := range h.events.all(t) {
			if e.typ == "chain.transfer" && e.To == addr && e.Amount == "42" && e.BlockHash != first.BlockHash {
				count++
			}
		}
		return count == 1
	})
	view, reorgs := replay(t, h.events.all(t))
	if len(reorgs) == 0 {
		t.Fatal("no reorg published")
	}
	matchesNode(t, view, h.start, headNumber(t))
}
