// Package node reads the chain for the reconciliation over JSON-RPC (go-ethereum rpc client): the head, the time of
// a block and the balances of many addresses at a block in one batch (eth_getBalance for ETH, ERC-20 balanceOf
// through eth_call).
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/m5tshift/weigate/services/reconciliation-service/internal/recon"
)

// balanceOfSelector is the selector of balanceOf(address).
const balanceOfSelector = "0x70a08231"

// Client is a JSON-RPC client of one node. It is safe for concurrent use.
type Client struct {
	rpc     *rpc.Client
	timeout time.Duration
}

// Dial creates a client of the node at url (http or https). No connection is made until the first call.
// timeout bounds every call and batch.
func Dial(ctx context.Context, url string, timeout time.Duration) (*Client, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return nil, errors.New("node: url must be http:// or https://")
	}
	if timeout <= 0 {
		return nil, errors.New("node: timeout must be positive")
	}
	c, err := rpc.DialOptions(ctx, url, rpc.WithHTTPClient(&http.Client{Timeout: timeout}))
	if err != nil {
		return nil, fmt.Errorf("node: dial: %w", err)
	}
	return &Client{rpc: c, timeout: timeout}, nil
}

// Close releases the client.
func (c *Client) Close() { c.rpc.Close() }

func (c *Client) call(ctx context.Context, result any, method string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := c.rpc.CallContext(ctx, result, method, args...); err != nil {
		return fmt.Errorf("node: %s: %w", method, err)
	}
	return nil
}

// ChainID returns eth_chainId.
func (c *Client) ChainID(ctx context.Context) (uint64, error) {
	var id hexutil.Uint64
	if err := c.call(ctx, &id, "eth_chainId"); err != nil {
		return 0, err
	}
	return uint64(id), nil
}

// HeadNumber returns eth_blockNumber.
func (c *Client) HeadNumber(ctx context.Context) (uint64, error) {
	var n hexutil.Uint64
	if err := c.call(ctx, &n, "eth_blockNumber"); err != nil {
		return 0, err
	}
	return uint64(n), nil
}

// maxTimestamp keeps block timestamps within int64 seconds.
const maxTimestamp = 1 << 62

// BlockTime returns the timestamp of block n (chain time).
func (c *Client) BlockTime(ctx context.Context, n uint64) (time.Time, error) {
	var raw json.RawMessage
	if err := c.call(ctx, &raw, "eth_getBlockByNumber", hexutil.Uint64(n), false); err != nil {
		return time.Time{}, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return time.Time{}, fmt.Errorf("node: block %d not found", n)
	}
	var h struct {
		Number    *hexutil.Uint64 `json:"number"`
		Timestamp *hexutil.Uint64 `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return time.Time{}, fmt.Errorf("node: block %d: %w", n, err)
	}
	if h.Number == nil || h.Timestamp == nil || uint64(*h.Number) != n || uint64(*h.Timestamp) > maxTimestamp {
		return time.Time{}, fmt.Errorf("node: block %d: number or timestamp missing or invalid", n)
	}
	return time.Unix(int64(*h.Timestamp), 0).UTC(), nil //nolint:gosec // bounded by maxTimestamp.
}

// Balances reads the balances of qs at block in one JSON-RPC batch, in the order of qs: eth_getBalance for ETH,
// eth_call of balanceOf(address) on the token contract for ERC-20. Any failed element fails the batch.
func (c *Client) Balances(ctx context.Context, block uint64, qs []recon.BalanceQuery) ([]*big.Int, error) {
	if len(qs) == 0 {
		return nil, nil
	}
	tag := hexutil.Uint64(block)
	elems := make([]rpc.BatchElem, len(qs))
	results := make([]hexutil.Bytes, len(qs))
	balances := make([]hexutil.Big, len(qs))
	for i, q := range qs {
		addr := strings.ToLower(q.Address)
		if q.Asset.Kind == recon.KindETH {
			elems[i] = rpc.BatchElem{Method: "eth_getBalance", Args: []any{addr, tag}, Result: &balances[i]}
			continue
		}
		data := balanceOfSelector + strings.Repeat("0", 24) + strings.TrimPrefix(addr, "0x")
		elems[i] = rpc.BatchElem{Method: "eth_call",
			Args:   []any{map[string]string{"to": strings.ToLower(q.Asset.Contract), "data": data}, tag},
			Result: &results[i]}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := c.rpc.BatchCallContext(ctx, elems); err != nil {
		return nil, fmt.Errorf("node: balances batch: %w", err)
	}
	out := make([]*big.Int, len(qs))
	for i, e := range elems {
		if e.Error != nil {
			return nil, fmt.Errorf("node: %s %s: %w", e.Method, qs[i].Address, e.Error)
		}
		if qs[i].Asset.Kind == recon.KindETH {
			out[i] = new(big.Int).Set(balances[i].ToInt())
			continue
		}
		if len(results[i]) != 32 {
			return nil, fmt.Errorf("node: balanceOf %s on %s: %d bytes returned, want 32",
				qs[i].Address, qs[i].Asset.Contract, len(results[i]))
		}
		out[i] = new(big.Int).SetBytes(results[i])
	}
	return out, nil
}
