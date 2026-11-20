// Package node reads chain data from an EVM node over JSON-RPC (go-ethereum rpc client). Block hashes are
// taken from the node as is, not recomputed, so the client does not depend on the header fields of the fork.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/m5tshift/slopay/services/chain-watcher/internal/chain"
)

// Client is a JSON-RPC client of one node. It is safe for concurrent use.
type Client struct {
	rpc     *rpc.Client
	timeout time.Duration
}

// Dial creates a client of the node at url (http or https). No connection is made until the first call.
// timeout bounds every call.
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

type rpcHeader struct {
	Number     *hexutil.Uint64 `json:"number"`
	Hash       *common.Hash    `json:"hash"`
	ParentHash *common.Hash    `json:"parentHash"`
	Timestamp  *hexutil.Uint64 `json:"timestamp"`
}

// HeaderByNumber returns the header of block n (eth_getBlockByNumber without transactions) or
// chain.ErrNotFound when the node has no such block.
func (c *Client) HeaderByNumber(ctx context.Context, n uint64) (chain.Header, error) {
	var raw json.RawMessage
	if err := c.call(ctx, &raw, "eth_getBlockByNumber", hexutil.Uint64(n), false); err != nil {
		return chain.Header{}, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return chain.Header{}, chain.ErrNotFound
	}
	var h rpcHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return chain.Header{}, fmt.Errorf("node: block %d: %w", n, err)
	}
	if h.Number == nil || h.Hash == nil || h.ParentHash == nil || h.Timestamp == nil {
		return chain.Header{}, fmt.Errorf("node: block %d: number, hash, parentHash or timestamp missing", n)
	}
	if uint64(*h.Timestamp) > chain.MaxValue {
		return chain.Header{}, fmt.Errorf("node: block %d: timestamp %d out of range", n, uint64(*h.Timestamp))
	}
	if uint64(*h.Number) != n {
		return chain.Header{}, fmt.Errorf("node: asked block %d, got %d", n, uint64(*h.Number))
	}
	return chain.Header{
		Number:     n,
		Hash:       strings.ToLower(h.Hash.Hex()),
		ParentHash: strings.ToLower(h.ParentHash.Hex()),
		Timestamp:  uint64(*h.Timestamp),
	}, nil
}
