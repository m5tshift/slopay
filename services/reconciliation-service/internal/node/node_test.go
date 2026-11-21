package node

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"
)

type rpcReq struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params []any           `json:"params"`
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcErr         `json:"error,omitempty"`
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// fakeNode answers single and batch JSON-RPC requests with handle and records the batches.
type fakeNode struct {
	mu      sync.Mutex
	batches [][]rpcReq
	handle  func(r rpcReq) (any, *rpcErr)
}

func (f *fakeNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	answer := func(q rpcReq) rpcResp {
		res, e := f.handle(q)
		return rpcResp{JSONRPC: "2.0", ID: q.ID, Result: res, Error: e}
	}
	w.Header().Set("Content-Type", "application/json")
	if strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
		var qs []rpcReq
		_ = json.Unmarshal(body, &qs)
		f.mu.Lock()
		f.batches = append(f.batches, qs)
		f.mu.Unlock()
		out := make([]rpcResp, len(qs))
		for i, q := range qs {
			out[i] = answer(q)
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	var q rpcReq
	_ = json.Unmarshal(body, &q)
	_ = json.NewEncoder(w).Encode(answer(q))
}

func dial(t *testing.T, f *fakeNode) *Client {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c, err := Dial(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

const (
	addrA     = "0x00000000000000000000000000000000000000aa"
	addrB     = "0x00000000000000000000000000000000000000bb"
	erc20Addr = "0x5fbdb2315678afecb367f032d93f642f64180aa3"
)

func TestBalancesBatch(t *testing.T) {
	f := &fakeNode{handle: func(q rpcReq) (any, *rpcErr) {
		switch q.Method {
		case "eth_getBalance":
			if q.Params[0] != addrA || q.Params[1] != "0x2a" {
				return nil, &rpcErr{-32602, "bad params"}
			}
			return "0xde0b6b3a7640000", nil // 1e18
		case "eth_call":
			call := q.Params[0].(map[string]any)
			want := "0x70a08231" + strings.Repeat("0", 24) + strings.TrimPrefix(addrB, "0x")
			if call["to"] != erc20Addr || call["data"] != want || q.Params[1] != "0x2a" {
				return nil, &rpcErr{-32602, "bad call"}
			}
			return "0x" + strings.Repeat("0", 60) + "0400", nil // 1024
		}
		return nil, &rpcErr{-32601, "method not found"}
	}}
	c := dial(t, f)
	got, err := c.Balances(context.Background(), 42, []recon.BalanceQuery{
		{Address: addrA, Asset: recon.Asset{Kind: recon.KindETH, Decimals: 18}},
		{Address: addrB, Asset: recon.Asset{Kind: recon.KindERC20, Contract: erc20Addr, Decimals: 6}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Cmp(big.NewInt(1e18)) != 0 || got[1].Cmp(big.NewInt(1024)) != 0 {
		t.Fatalf("balances = %v", got)
	}
	if len(f.batches) != 1 || len(f.batches[0]) != 2 {
		t.Fatalf("want one batch of 2, got %v", f.batches)
	}
}

func TestBalancesErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		handle func(rpcReq) (any, *rpcErr)
	}{
		{"element error", func(rpcReq) (any, *rpcErr) { return nil, &rpcErr{-32000, "header not found"} }},
		{"short balanceOf", func(q rpcReq) (any, *rpcErr) {
			if q.Method == "eth_call" {
				return "0x", nil
			}
			return "0x1", nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t, &fakeNode{handle: tc.handle})
			_, err := c.Balances(context.Background(), 1, []recon.BalanceQuery{
				{Address: addrA, Asset: recon.Asset{Kind: recon.KindETH}},
				{Address: addrB, Asset: recon.Asset{Kind: recon.KindERC20, Contract: erc20Addr}},
			})
			if err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestHeadAndBlockTime(t *testing.T) {
	c := dial(t, &fakeNode{handle: func(q rpcReq) (any, *rpcErr) {
		switch q.Method {
		case "eth_chainId":
			return "0x7a69", nil
		case "eth_blockNumber":
			return "0x64", nil
		case "eth_getBlockByNumber":
			if q.Params[0] == "0x5d" {
				return map[string]any{"number": "0x5d", "timestamp": "0x6553f100"}, nil
			}
			return nil, nil
		}
		return nil, &rpcErr{-32601, "method not found"}
	}})
	ctx := context.Background()
	if id, err := c.ChainID(ctx); err != nil || id != 31337 {
		t.Fatalf("chain id = %d, %v", id, err)
	}
	if n, err := c.HeadNumber(ctx); err != nil || n != 100 {
		t.Fatalf("head = %d, %v", n, err)
	}
	ts, err := c.BlockTime(ctx, 93)
	if err != nil || !ts.Equal(time.Unix(0x6553f100, 0)) {
		t.Fatalf("block time = %v, %v", ts, err)
	}
	if _, err := c.BlockTime(ctx, 94); err == nil {
		t.Fatal("missing block: want error")
	}
}

func TestDialValidates(t *testing.T) {
	if _, err := Dial(context.Background(), "ws://node", time.Second); err == nil {
		t.Fatal("ws url: want error")
	}
	if _, err := Dial(context.Background(), "http://node", 0); err == nil {
		t.Fatal("zero timeout: want error")
	}
}
