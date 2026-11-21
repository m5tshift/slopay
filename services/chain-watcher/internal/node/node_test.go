package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
)

const (
	hashA = "0xB2C3D4E5B2C3D4E5B2C3D4E5B2C3D4E5B2C3D4E5B2C3D4E5B2C3D4E5B2C3D4E5"
	hashB = "0xa1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4"
)

type rpcReq struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params []any           `json:"params"`
}

// server answers JSON-RPC calls with results[method]; a missing method answers a JSON-RPC error.
type recorder struct {
	mu   sync.Mutex
	reqs []rpcReq
}

func (r *recorder) first() rpcReq {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reqs[0]
}

func server(t *testing.T, results map[string]string, delay time.Duration) (*Client, *recorder) {
	t.Helper()
	seen := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req rpcReq
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("request: %v", err)
		}
		seen.mu.Lock()
		seen.reqs = append(seen.reqs, req)
		seen.mu.Unlock()
		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		res, ok := results[req.Method]
		if !ok {
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":`+string(req.ID)+`,"error":{"code":-32601,"message":"no such method"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":`+string(req.ID)+`,"result":`+res+`}`)
	}))
	t.Cleanup(srv.Close)
	c, err := Dial(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, seen
}

func TestDialValidates(t *testing.T) {
	if _, err := Dial(context.Background(), "ws://node:8546", time.Second); err == nil {
		t.Error("ws url accepted")
	}
	if _, err := Dial(context.Background(), "http://node:8545", 0); err == nil {
		t.Error("zero timeout accepted")
	}
}

func TestChainIDAndHead(t *testing.T) {
	c, _ := server(t, map[string]string{"eth_chainId": `"0x7a69"`, "eth_blockNumber": `"0x65"`}, 0)
	id, err := c.ChainID(context.Background())
	if err != nil || id != 31337 {
		t.Fatalf("chain id = %d, %v", id, err)
	}
	n, err := c.HeadNumber(context.Background())
	if err != nil || n != 101 {
		t.Fatalf("head = %d, %v", n, err)
	}
}

func TestHeaderByNumber(t *testing.T) {
	c, seen := server(t, map[string]string{"eth_getBlockByNumber": `{"number":"0x65","hash":"` + hashA +
		`","parentHash":"` + hashB + `","timestamp":"0x6afa0a24","transactions":["` + hashB + `"]}`}, 0)
	h, err := c.HeaderByNumber(context.Background(), 101)
	if err != nil {
		t.Fatal(err)
	}
	want := chain.Header{Number: 101, Hash: strings.ToLower(hashA), ParentHash: hashB, Timestamp: 0x6afa0a24}
	if h != want {
		t.Fatalf("header = %+v, want %+v", h, want)
	}
	if p := seen.first().Params; len(p) != 2 || p[0] != "0x65" || p[1] != false {
		t.Fatalf("params = %v", p)
	}
}

func TestHeaderByNumberNotFound(t *testing.T) {
	c, _ := server(t, map[string]string{"eth_getBlockByNumber": `null`}, 0)
	if _, err := c.HeaderByNumber(context.Background(), 5); !errors.Is(err, chain.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestHeaderByNumberInvalid(t *testing.T) {
	for name, res := range map[string]string{
		"missing hash": `{"number":"0x5","parentHash":"` + hashB + `","timestamp":"0x1"}`,
		"wrong number": `{"number":"0x6","hash":"` + hashA + `","parentHash":"` + hashB + `","timestamp":"0x1"}`,
		"huge timestamp": `{"number":"0x5","hash":"` + hashA + `","parentHash":"` + hashB +
			`","timestamp":"0x8000000000000000"}`,
		"bad json":      `{"number":5}`,
		"not an object": `"0x5"`,
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := server(t, map[string]string{"eth_getBlockByNumber": res}, 0)
			_, err := c.HeaderByNumber(context.Background(), 5)
			if err == nil || errors.Is(err, chain.ErrNotFound) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestRPCError(t *testing.T) {
	c, _ := server(t, map[string]string{}, 0)
	if _, err := c.HeadNumber(context.Background()); err == nil || !strings.Contains(err.Error(), "eth_blockNumber") {
		t.Fatalf("err = %v", err)
	}
	if _, err := c.ChainID(context.Background()); err == nil {
		t.Fatal("no error")
	}
	if _, err := c.HeaderByNumber(context.Background(), 1); err == nil {
		t.Fatal("no error")
	}
}

func TestTimeout(t *testing.T) {
	results := map[string]string{"eth_blockNumber": `"0x1"`}
	c, _ := server(t, results, 300*time.Millisecond)
	c.timeout = 50 * time.Millisecond
	if _, err := c.HeadNumber(context.Background()); err == nil {
		t.Fatal("slow node did not time out")
	}
}

const (
	addrTo   = "0x71C7656EC7ab88b098defB751B7401B5f6d8976F"
	addrFrom = "0x00000000000000000000000000000000000000f1"
)

func TestBlockByNumber(t *testing.T) {
	txs := `[
		{"hash":"` + hashA + `","from":"` + addrFrom + `","to":"` + addrTo + `","value":"0x1bc16d674ec80000"},
		{"hash":"` + hashB + `","from":"` + addrFrom + `","to":null,"value":"0x1"},
		{"hash":"` + hashB + `","from":"` + addrFrom + `","to":"` + addrTo + `","value":"0x0"}]`
	c, seen := server(t, map[string]string{"eth_getBlockByNumber": `{"number":"0x65","hash":"` + hashA +
		`","parentHash":"` + hashB + `","timestamp":"0x6afa0a24","transactions":` + txs + `}`}, 0)
	b, err := c.BlockByNumber(context.Background(), 101)
	if err != nil {
		t.Fatal(err)
	}
	if b.Number != 101 || b.Hash != strings.ToLower(hashA) || b.ParentHash != hashB || b.Timestamp != 0x6afa0a24 {
		t.Fatalf("header %+v", b.Header)
	}
	// Contract creations and zero-value calls are not transfers.
	if len(b.Txs) != 1 {
		t.Fatalf("txs %+v", b.Txs)
	}
	tx := b.Txs[0]
	if tx.Hash != strings.ToLower(hashA) || tx.From != addrFrom || tx.To != strings.ToLower(addrTo) ||
		tx.Value.String() != "2000000000000000000" {
		t.Fatalf("tx %+v", tx)
	}
	if p := seen.first().Params; len(p) != 2 || p[0] != "0x65" || p[1] != true {
		t.Fatalf("params = %v", p)
	}
}

func TestBlockByNumberInvalid(t *testing.T) {
	header := `"number":"0x5","hash":"` + hashA + `","parentHash":"` + hashB + `","timestamp":"0x1"`
	for name, res := range map[string]string{
		"missing header": `{"hash":"` + hashA + `","transactions":[]}`,
		"missing value":  `{` + header + `,"transactions":[{"hash":"` + hashA + `","from":"` + addrFrom + `","to":"` + addrTo + `"}]}`,
		"missing hash":   `{` + header + `,"transactions":[{"from":"` + addrFrom + `","to":"` + addrTo + `","value":"0x1"}]}`,
		"hashes only":    `{` + header + `,"transactions":["` + hashA + `"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := server(t, map[string]string{"eth_getBlockByNumber": res}, 0)
			if _, err := c.BlockByNumber(context.Background(), 5); err == nil || errors.Is(err, chain.ErrNotFound) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	c, _ := server(t, map[string]string{"eth_getBlockByNumber": `null`}, 0)
	if _, err := c.BlockByNumber(context.Background(), 5); !errors.Is(err, chain.ErrNotFound) {
		t.Fatalf("not found: err = %v", err)
	}
	c, _ = server(t, map[string]string{}, 0)
	if _, err := c.BlockByNumber(context.Background(), 5); err == nil {
		t.Fatal("rpc error accepted")
	}
}

func TestReceipt(t *testing.T) {
	for name, tc := range map[string]struct {
		status  string
		success bool
	}{"success": {"0x1", true}, "reverted": {"0x0", false}} {
		t.Run(name, func(t *testing.T) {
			c, seen := server(t, map[string]string{"eth_getTransactionReceipt": `{"blockHash":"` + hashA +
				`","status":"` + tc.status + `"}`}, 0)
			r, err := c.Receipt(context.Background(), hashB)
			if err != nil {
				t.Fatal(err)
			}
			if r.BlockHash != strings.ToLower(hashA) || r.Success != tc.success {
				t.Fatalf("receipt %+v", r)
			}
			if p := seen.first().Params; len(p) != 1 || p[0] != hashB {
				t.Fatalf("params = %v", p)
			}
		})
	}
}

func TestReceiptErrors(t *testing.T) {
	c, _ := server(t, map[string]string{"eth_getTransactionReceipt": `null`}, 0)
	if _, err := c.Receipt(context.Background(), hashB); !errors.Is(err, chain.ErrNotFound) {
		t.Fatalf("not found: err = %v", err)
	}
	for name, res := range map[string]string{
		"pending":   `{"blockHash":null,"status":"0x1"}`,
		"no status": `{"blockHash":"` + hashA + `"}`,
		"bad json":  `{"status":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := server(t, map[string]string{"eth_getTransactionReceipt": res}, 0)
			if _, err := c.Receipt(context.Background(), hashB); err == nil || errors.Is(err, chain.ErrNotFound) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	c, _ = server(t, map[string]string{}, 0)
	if _, err := c.Receipt(context.Background(), hashB); err == nil {
		t.Fatal("rpc error accepted")
	}
}
