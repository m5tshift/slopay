// Package payer decides what the simulated buyer does with an invoice, signs the ETH transfer, keeps the signed
// transaction and sends it again on command (SRS 4.1.3, 8.1, 8.5).
package payer

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/m5tshift/slopay/testkit/payer-sim/internal/invoice"
)

// transferGas is the gas of a plain ETH transfer to an externally owned account.
const transferGas = 21000

// Outcome is what the payer does with an invoice.
type Outcome string

// Outcomes.
const (
	Pay      Outcome = "pay"
	Underpay Outcome = "underpay"
	Skip     Outcome = "skip"
)

// Shares are the percentages of invoices per outcome; they sum to 100.
type Shares struct{ Pay, Underpay, Skip int }

// Decide maps a roll in [0, 100) to an outcome.
func (s Shares) Decide(roll int) Outcome {
	switch {
	case roll < s.Pay:
		return Pay
	case roll < s.Pay+s.Underpay:
		return Underpay
	default:
		return Skip
	}
}

// Chain is the part of the Ethereum client the payer uses; *ethclient.Client implements it.
type Chain interface {
	ChainID(ctx context.Context) (*big.Int, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}

// Invoices returns the payment data of an invoice.
type Invoices interface {
	Get(ctx context.Context, id string) (invoice.Payment, error)
}

// Config configures a Payer.
type Config struct {
	Chain       Chain
	Invoices    Invoices
	Keys        []*ecdsa.PrivateKey
	Shares      Shares
	UnderpayBps int
	// Roll returns a number in [0, n); math/rand/v2.IntN in production.
	Roll func(n int) int
}

// Payment is the record of one invoice.
type Payment struct {
	InvoiceID string
	Outcome   Outcome
	TxHash    common.Hash // zero when the payer did not pay
	Amount    *big.Int    // nil when the payer did not pay
}

// ErrNotETH is returned for an invoice in another asset.
var ErrNotETH = errors.New("invoice is not an ETH invoice")

// Payer pays invoices from the pre-funded accounts in turn.
type Payer struct {
	cfg Config

	mu       sync.Mutex
	payments []Payment
	byID     map[string]int
	txs      []*types.Transaction // signed transactions, same order as the paid entries of payments
	txInv    []string
	nonces   map[common.Address]uint64
	next     int
	signer   types.Signer
}

// New returns a Payer. It needs at least one key.
func New(ctx context.Context, cfg Config) (*Payer, error) {
	if len(cfg.Keys) == 0 {
		return nil, errors.New("no keys")
	}
	id, err := cfg.Chain.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("chain id: %w", err)
	}
	return &Payer{cfg: cfg, byID: map[string]int{}, nonces: map[common.Address]uint64{}, signer: types.LatestSignerForChainID(id)}, nil
}

// ParseKeys reads hex private keys, one per line, with an optional 0x prefix. Blank lines and lines starting
// with # are ignored. Keys never appear in errors.
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

// Pay decides, signs and sends. The decision is made once per invoice: a repeated call returns the same payment.
func (p *Payer) Pay(ctx context.Context, invoiceID string) (Payment, error) {
	if pay, ok := p.find(invoiceID); ok {
		return pay, nil
	}
	inv, err := p.cfg.Invoices.Get(ctx, invoiceID)
	if err != nil {
		return Payment{}, err
	}
	if inv.Asset != "ETH" {
		return Payment{}, ErrNotETH
	}
	if !common.IsHexAddress(inv.Address) {
		return Payment{}, fmt.Errorf("invalid payment address %q", inv.Address)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if i, ok := p.byID[invoiceID]; ok {
		return p.payments[i], nil
	}
	pay := Payment{InvoiceID: invoiceID, Outcome: p.cfg.Shares.Decide(p.cfg.Roll(100))}
	if pay.Outcome != Skip {
		amount := new(big.Int).Set(inv.Amount)
		if pay.Outcome == Underpay {
			// Rounded down, at least one wei: the payer always sends less than the invoice.
			amount.Mul(amount, big.NewInt(int64(p.cfg.UnderpayBps)))
			amount.Div(amount, big.NewInt(10000))
			if amount.Sign() == 0 {
				amount.SetInt64(1)
			}
		}
		tx, err := p.send(ctx, common.HexToAddress(inv.Address), amount)
		if err != nil {
			return Payment{}, err
		}
		pay.TxHash, pay.Amount = tx.Hash(), amount
		p.txs = append(p.txs, tx)
		p.txInv = append(p.txInv, invoiceID)
	}
	p.byID[invoiceID] = len(p.payments)
	p.payments = append(p.payments, pay)
	return pay, nil
}

func (p *Payer) find(id string) (Payment, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if i, ok := p.byID[id]; ok {
		return p.payments[i], true
	}
	return Payment{}, false
}

// send signs the transfer from the next account and sends it; the caller holds p.mu.
func (p *Payer) send(ctx context.Context, to common.Address, amount *big.Int) (*types.Transaction, error) {
	key := p.cfg.Keys[p.next%len(p.cfg.Keys)]
	p.next++
	from := crypto.PubkeyToAddress(key.PublicKey)
	nonce, ok := p.nonces[from]
	if !ok {
		var err error
		if nonce, err = p.cfg.Chain.PendingNonceAt(ctx, from); err != nil {
			return nil, fmt.Errorf("nonce: %w", err)
		}
	}
	price, err := p.cfg.Chain.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("gas price: %w", err)
	}
	// Twice the suggestion keeps the stored transaction valid when the base fee rises before it is replayed.
	price = new(big.Int).Mul(price, big.NewInt(2))
	tx, err := types.SignTx(types.NewTx(&types.LegacyTx{Nonce: nonce, To: &to, Value: amount, Gas: transferGas, GasPrice: price}), p.signer, key)
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	if err := p.cfg.Chain.SendTransaction(ctx, tx); err != nil {
		delete(p.nonces, from) // the node knows the truth; read the nonce again next time
		return nil, fmt.Errorf("send transaction: %w", err)
	}
	p.nonces[from] = nonce + 1
	return tx, nil
}

// Payments returns the payments in the order they were made.
func (p *Payer) Payments() []Payment {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Payment(nil), p.payments...)
}

// ReplayResult counts the outcome of Replay.
type ReplayResult struct{ Sent, Known int }

// Replay sends the stored signed transactions again, in signing order: all of them, or only those of invoiceID
// when it is not empty. The same transaction has the same hash. A transaction the node already has (it is in
// the pool or already included again) is counted as known.
func (p *Payer) Replay(ctx context.Context, invoiceID string) (ReplayResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var res ReplayResult
	for i, tx := range p.txs {
		if invoiceID != "" && p.txInv[i] != invoiceID {
			continue
		}
		err := p.cfg.Chain.SendTransaction(ctx, tx)
		switch {
		case err == nil:
			res.Sent++
		case isKnown(err):
			res.Known++
		default:
			return res, fmt.Errorf("send transaction %s: %w", tx.Hash(), err)
		}
	}
	return res, nil
}

func isKnown(err error) bool {
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "already known") || strings.Contains(m, "nonce too low") || strings.Contains(m, "already imported")
}
