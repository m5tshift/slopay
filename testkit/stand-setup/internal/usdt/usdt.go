// Package usdt deploys the test USDT contract (testkit/erc20) to Anvil and
// mints tokens to the payer-sim accounts (SRS FR-10). The creation bytecode is
// embedded: it is the output of `forge build` for testkit/erc20/src/TestUSDT.sol.
package usdt

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

//go:embed TestUSDT.bin
var bytecodeHex string

// Chain is the part of the Ethereum client the deployment uses; *ethclient.Client implements it.
type Chain interface {
	ChainID(ctx context.Context) (*big.Int, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	TransactionReceipt(ctx context.Context, h common.Hash) (*types.Receipt, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error)
}

// Config is the deployment configuration.
type Config struct {
	Chain      Chain
	Key        *ecdsa.PrivateKey
	Recipients []common.Address
	// Amount is minted to each recipient, in minimal units (10^-6 USDT).
	Amount *big.Int
	// PollInterval and Timeout bound the wait for a receipt.
	PollInterval, Timeout time.Duration
}

// Result is what a run did.
type Result struct {
	Address common.Address
	Minted  int
}

// Bytecode returns the embedded creation bytecode.
func Bytecode() ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimSpace(bytecodeHex))
	if err != nil || len(b) == 0 {
		return nil, errors.New("usdt: embedded bytecode is invalid")
	}
	return b, nil
}

// Deploy creates the contract and mints Amount to every recipient. It verifies
// the balances afterwards.
func Deploy(ctx context.Context, cfg Config, log *slog.Logger) (Result, error) {
	var res Result
	if cfg.Chain == nil || cfg.Key == nil {
		return res, errors.New("usdt: chain and deployer key are required")
	}
	if len(cfg.Recipients) > 0 && (cfg.Amount == nil || cfg.Amount.Sign() <= 0) {
		return res, errors.New("usdt: mint amount must be positive")
	}
	code, err := Bytecode()
	if err != nil {
		return res, err
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	chainID, err := cfg.Chain.ChainID(ctx)
	if err != nil {
		return res, fmt.Errorf("chain id: %w", err)
	}
	s := &sender{cfg: cfg, from: crypto.PubkeyToAddress(cfg.Key.PublicKey), signer: types.LatestSignerForChainID(chainID)}
	if s.nonce, err = cfg.Chain.PendingNonceAt(ctx, s.from); err != nil {
		return res, fmt.Errorf("nonce: %w", err)
	}
	rcpt, err := s.send(ctx, nil, code)
	if err != nil {
		return res, fmt.Errorf("deploy: %w", err)
	}
	res.Address = rcpt.ContractAddress
	log.Info("usdt deployed", "address", res.Address.Hex())
	for _, to := range cfg.Recipients {
		if _, err := s.send(ctx, &res.Address, mintData(to, cfg.Amount)); err != nil {
			return res, fmt.Errorf("mint to %s: %w", to.Hex(), err)
		}
		res.Minted++
		log.Info("usdt minted", "to", to.Hex(), "amount", cfg.Amount.String())
	}
	for _, to := range cfg.Recipients {
		out, err := cfg.Chain.CallContract(ctx, ethereum.CallMsg{To: &res.Address, Data: balanceData(to)}, nil)
		if err != nil {
			return res, fmt.Errorf("balanceOf %s: %w", to.Hex(), err)
		}
		if new(big.Int).SetBytes(out).Cmp(cfg.Amount) < 0 {
			return res, fmt.Errorf("balance of %s is below the minted amount", to.Hex())
		}
	}
	return res, nil
}

type sender struct {
	cfg    Config
	from   common.Address
	signer types.Signer
	nonce  uint64
}

func (s *sender) send(ctx context.Context, to *common.Address, data []byte) (*types.Receipt, error) {
	c := s.cfg.Chain
	price, err := c.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("gas price: %w", err)
	}
	gas, err := c.EstimateGas(ctx, ethereum.CallMsg{From: s.from, To: to, Data: data})
	if err != nil {
		return nil, fmt.Errorf("estimate gas: %w", err)
	}
	tx, err := types.SignNewTx(s.cfg.Key, s.signer, &types.LegacyTx{Nonce: s.nonce, GasPrice: price, Gas: gas, To: to, Data: data})
	if err != nil {
		return nil, err
	}
	if err := c.SendTransaction(ctx, tx); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}
	s.nonce++
	deadline := time.Now().Add(s.cfg.Timeout)
	for {
		r, err := c.TransactionReceipt(ctx, tx.Hash())
		if err == nil {
			if r.Status != types.ReceiptStatusSuccessful {
				return nil, fmt.Errorf("transaction %s reverted", tx.Hash())
			}
			return r, nil
		}
		if !errors.Is(err, ethereum.NotFound) {
			return nil, fmt.Errorf("receipt: %w", err)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no receipt for %s in %s", tx.Hash(), s.cfg.Timeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.cfg.PollInterval):
		}
	}
}

// mintData is the calldata of mint(address,uint256).
func mintData(to common.Address, amount *big.Int) []byte {
	d := crypto.Keccak256([]byte("mint(address,uint256)"))[:4]
	d = append(d, common.LeftPadBytes(to.Bytes(), 32)...)
	return append(d, common.LeftPadBytes(amount.Bytes(), 32)...)
}

// balanceData is the calldata of balanceOf(address).
func balanceData(who common.Address) []byte {
	d := crypto.Keccak256([]byte("balanceOf(address)"))[:4]
	return append(d, common.LeftPadBytes(who.Bytes(), 32)...)
}

// ParseKeys reads hex private keys, one per line, optional 0x prefix; blank
// lines and lines starting with # are ignored. Keys never appear in errors.
func ParseKeys(data string) ([]*ecdsa.PrivateKey, error) {
	var keys []*ecdsa.PrivateKey
	sc := bufio.NewScanner(strings.NewReader(data))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, err := crypto.HexToECDSA(strings.TrimPrefix(line, "0x"))
		if err != nil {
			return nil, fmt.Errorf("line %d: not a private key", n)
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, errors.New("no keys in the file")
	}
	return keys, nil
}

// Addresses returns the addresses of the keys.
func Addresses(keys []*ecdsa.PrivateKey) []common.Address {
	out := make([]common.Address, len(keys))
	for i, k := range keys {
		out[i] = crypto.PubkeyToAddress(k.PublicKey)
	}
	return out
}

// ParseAddresses parses a comma-separated address list.
func ParseAddresses(s string) ([]common.Address, error) {
	var out []common.Address
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !common.IsHexAddress(p) {
			return nil, fmt.Errorf("not an address: %q", p)
		}
		out = append(out, common.HexToAddress(p))
	}
	return out, nil
}
