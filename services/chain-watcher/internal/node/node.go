// Package node reads chain data from an EVM node over JSON-RPC (go-ethereum rpc client). Block hashes are
// taken from the node as is, not recomputed, so the client does not depend on the header fields of the fork.
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

type rpcTx struct {
	Hash  *common.Hash    `json:"hash"`
	From  *common.Address `json:"from"`
	To    *common.Address `json:"to"`
	Value *hexutil.Big    `json:"value"`
}

type rpcBlock struct {
	rpcHeader
	Transactions []rpcTx `json:"transactions"`
}

// HeaderByNumber returns the header of block n (eth_getBlockByNumber without transactions) or
// chain.ErrNotFound when the node has no such block.
func (c *Client) HeaderByNumber(ctx context.Context, n uint64) (chain.Header, error) {
	// Without full transactions the node lists their hashes: only the header fields are read.
	var h rpcHeader
	if err := c.block(ctx, n, false, &h); err != nil {
		return chain.Header{}, err
	}
	return h.header(n)
}

// BlockByNumber returns block n with its value transfers (eth_getBlockByNumber with transactions): the
// transactions with a recipient and a positive value. A contract creation has no recipient and is left out.
func (c *Client) BlockByNumber(ctx context.Context, n uint64) (chain.Block, error) {
	var b rpcBlock
	if err := c.block(ctx, n, true, &b); err != nil {
		return chain.Block{}, err
	}
	h, err := b.header(n)
	if err != nil {
		return chain.Block{}, err
	}
	out := chain.Block{Header: h}
	for i, tx := range b.Transactions {
		if tx.Hash == nil || tx.From == nil || tx.Value == nil {
			return chain.Block{}, fmt.Errorf("node: block %d: transaction %d: hash, from or value missing", n, i)
		}
		v := (*big.Int)(tx.Value)
		if tx.To == nil || v.Sign() <= 0 {
			continue
		}
		out.Txs = append(out.Txs, chain.Tx{
			Hash: strings.ToLower(tx.Hash.Hex()), From: strings.ToLower(tx.From.Hex()),
			To: strings.ToLower(tx.To.Hex()), Value: new(big.Int).Set(v),
		})
	}
	return out, nil
}

func (c *Client) block(ctx context.Context, n uint64, full bool, b any) error {
	var raw json.RawMessage
	if err := c.call(ctx, &raw, "eth_getBlockByNumber", hexutil.Uint64(n), full); err != nil {
		return err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return chain.ErrNotFound
	}
	if err := json.Unmarshal(raw, b); err != nil {
		return fmt.Errorf("node: block %d: %w", n, err)
	}
	return nil
}

func (h rpcHeader) header(n uint64) (chain.Header, error) {
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

type rpcReceipt struct {
	BlockHash *common.Hash    `json:"blockHash"`
	Status    *hexutil.Uint64 `json:"status"`
}

// Receipt returns the receipt of a transaction (eth_getTransactionReceipt) or chain.ErrNotFound when the
// node does not know it, for example after a reorg dropped it.
func (c *Client) Receipt(ctx context.Context, txHash string) (chain.Receipt, error) {
	var raw json.RawMessage
	if err := c.call(ctx, &raw, "eth_getTransactionReceipt", txHash); err != nil {
		return chain.Receipt{}, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return chain.Receipt{}, chain.ErrNotFound
	}
	var r rpcReceipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return chain.Receipt{}, fmt.Errorf("node: receipt %s: %w", txHash, err)
	}
	if r.BlockHash == nil || r.Status == nil {
		return chain.Receipt{}, fmt.Errorf("node: receipt %s: blockHash or status missing", txHash)
	}
	return chain.Receipt{BlockHash: strings.ToLower(r.BlockHash.Hex()), Success: *r.Status == 1}, nil
}
