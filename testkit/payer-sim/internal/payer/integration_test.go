//go:build integration

package payer_test

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/m5tshift/weigate/testkit/payer-sim/internal/invoice"
	"github.com/m5tshift/weigate/testkit/payer-sim/internal/payer"
)

const anvilImage = "ghcr.io/foundry-rs/foundry:v1.10.0"

var anvilURL string

func TestMain(m *testing.M) {
	code := 1
	defer func() { os.Exit(code) }()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        anvilImage,
			Entrypoint:   []string{"/bin/sh", "-c"},
			Cmd:          []string{"exec anvil --host 0.0.0.0"},
			ExposedPorts: []string{"8545/tcp"},
			WaitingFor:   wait.ForLog("Listening on").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err == nil {
		defer func() { _ = testcontainers.TerminateContainer(c) }()
		anvilURL, err = c.PortEndpoint(ctx, "8545/tcp", "http")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "anvil:", err)
		return
	}
	code = m.Run()
}

// setup returns the payer with funded generated accounts and a fake invoice-service serving one invoice.
func setup(t *testing.T, amount *big.Int, to common.Address, shares payer.Shares) (*payer.Payer, *ethclient.Client, *rpc.Client) {
	t.Helper()
	ctx := context.Background()
	rc, err := rpc.DialContext(ctx, anvilURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Close)
	ec := ethclient.NewClient(rc)

	var keys []*ecdsa.PrivateKey
	for range 2 {
		k, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		if err := rc.CallContext(ctx, nil, "anvil_setBalance", crypto.PubkeyToAddress(k.PublicKey), hexutil.EncodeBig(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"address":%q,"amount":%q,"asset":{"kind":"ETH","decimals":18}}`, to.Hex(), amount.String())
	}))
	t.Cleanup(srv.Close)
	p, err := payer.New(ctx, payer.Config{
		Chain: ec, Invoices: invoice.NewClient(srv.URL, srv.Client()), Keys: keys,
		Shares: shares, UnderpayBps: 5000, Roll: func(int) int { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, ec, rc
}

// deployUSDT deploys the test USDT (bytecode of stand-setup) from a funded account and mints amount to each key.
func deployUSDT(t *testing.T, ec *ethclient.Client, rc *rpc.Client, holders []common.Address, amount *big.Int) common.Address {
	t.Helper()
	ctx := context.Background()
	raw, err := os.ReadFile("../../../stand-setup/internal/usdt/TestUSDT.bin")
	if err != nil {
		t.Fatal(err)
	}
	code := common.FromHex(strings.TrimSpace(string(raw)))
	dk, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	from := crypto.PubkeyToAddress(dk.PublicKey)
	if err := rc.CallContext(ctx, nil, "anvil_setBalance", from, hexutil.EncodeBig(big.NewInt(1e18))); err != nil {
		t.Fatal(err)
	}
	id, err := ec.ChainID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	signer := types.LatestSignerForChainID(id)
	nonce := uint64(0)
	send := func(to *common.Address, data []byte) *types.Receipt {
		price, err := ec.SuggestGasPrice(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := types.SignNewTx(dk, signer, &types.LegacyTx{Nonce: nonce, GasPrice: price, Gas: 2_000_000, To: to, Data: data})
		if err != nil {
			t.Fatal(err)
		}
		if err := ec.SendTransaction(ctx, tx); err != nil {
			t.Fatal(err)
		}
		nonce++
		r := waitReceipt(t, ec, tx.Hash())
		if r.Status != 1 {
			t.Fatalf("reverted %s", tx.Hash())
		}
		return r
	}
	token := send(nil, code).ContractAddress
	for _, h := range holders {
		d := crypto.Keccak256([]byte("mint(address,uint256)"))[:4]
		d = append(d, common.LeftPadBytes(h.Bytes(), 32)...)
		d = append(d, common.LeftPadBytes(amount.Bytes(), 32)...)
		send(&token, d)
	}
	return token
}

func tokenBalance(t *testing.T, ec *ethclient.Client, token, who common.Address) *big.Int {
	t.Helper()
	d := append(crypto.Keccak256([]byte("balanceOf(address)"))[:4], common.LeftPadBytes(who.Bytes(), 32)...)
	out, err := ec.CallContract(context.Background(), ethereum.CallMsg{To: &token, Data: d}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return new(big.Int).SetBytes(out)
}

// TestPayTokenOnAnvil pays an ERC20 invoice with the real test USDT, rolls the chain back and replays.
func TestPayTokenOnAnvil(t *testing.T) {
	ctx := context.Background()
	to := randomAddress(t)
	rc, err := rpc.DialContext(ctx, anvilURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Close)
	ec := ethclient.NewClient(rc)
	var keys []*ecdsa.PrivateKey
	var holders []common.Address
	for range 2 {
		k, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		a := crypto.PubkeyToAddress(k.PublicKey)
		holders = append(holders, a)
		if err := rc.CallContext(ctx, nil, "anvil_setBalance", a, hexutil.EncodeBig(big.NewInt(1e18))); err != nil {
			t.Fatal(err)
		}
	}
	token := deployUSDT(t, ec, rc, holders, big.NewInt(1_000_000_000))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"address":%q,"amount":"5000000","asset":{"kind":"ERC20","contract":%q,"decimals":6}}`, to.Hex(), token.Hex())
	}))
	t.Cleanup(srv.Close)
	mk := func(shares payer.Shares) *payer.Payer {
		p, err := payer.New(ctx, payer.Config{Chain: ec, Invoices: invoice.NewClient(srv.URL, srv.Client()), Keys: keys,
			Shares: shares, UnderpayBps: 5000, Roll: func(int) int { return 0 }})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Underpay: half of the amount.
	pu := mk(payer.Shares{Underpay: 100})
	under, err := pu.Pay(ctx, "0b6ce2b8-6a8e-4c3c-9b1d-5a3c1b2f7f11")
	if err != nil {
		t.Fatal(err)
	}
	if r := waitReceipt(t, ec, under.TxHash); r.Status != 1 {
		t.Fatalf("receipt %v", r)
	}
	if got := tokenBalance(t, ec, token, to); got.Int64() != 2_500_000 {
		t.Fatalf("balance after underpay = %s", got)
	}

	// Full payment, then revert and replay: same hash, included again once.
	var snap string
	if err := rc.CallContext(ctx, &snap, "evm_snapshot"); err != nil {
		t.Fatal(err)
	}
	pf := mk(payer.Shares{Pay: 100})
	pay, err := pf.Pay(ctx, "0b6ce2b8-6a8e-4c3c-9b1d-5a3c1b2f7f10")
	if err != nil {
		t.Fatal(err)
	}
	if r := waitReceipt(t, ec, pay.TxHash); r.Status != 1 || len(r.Logs) != 1 {
		t.Fatalf("receipt %v", r)
	}
	if got := tokenBalance(t, ec, token, to); got.Int64() != 7_500_000 {
		t.Fatalf("balance after pay = %s", got)
	}
	var ok bool
	if err := rc.CallContext(ctx, &ok, "evm_revert", snap); err != nil || !ok {
		t.Fatalf("revert: %v %v", ok, err)
	}
	if got := tokenBalance(t, ec, token, to); got.Int64() != 2_500_000 {
		t.Fatalf("balance after revert = %s", got)
	}
	if res, err := pf.Replay(ctx, ""); err != nil || res.Sent != 1 {
		t.Fatalf("replay %+v %v", res, err)
	}
	if r := waitReceipt(t, ec, pay.TxHash); r.Status != 1 {
		t.Fatalf("receipt %v", r)
	}
	if got := tokenBalance(t, ec, token, to); got.Int64() != 7_500_000 {
		t.Fatalf("balance after replay = %s", got)
	}
}

func randomAddress(t *testing.T) common.Address {
	t.Helper()
	k, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return crypto.PubkeyToAddress(k.PublicKey)
}

// waitReceipt waits until the transaction is included.
func waitReceipt(t *testing.T, ec *ethclient.Client, h common.Hash) *types.Receipt {
	t.Helper()
	for range 100 {
		if r, err := ec.TransactionReceipt(context.Background(), h); err == nil {
			return r
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no receipt for %s", h)
	return nil
}

func balance(t *testing.T, ec *ethclient.Client, a common.Address) *big.Int {
	t.Helper()
	b, err := ec.BalanceAt(context.Background(), a, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPayOnAnvil(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		shares payer.Shares
		want   int64
	}{
		"pay":      {payer.Shares{Pay: 100}, 1_000_000},
		"underpay": {payer.Shares{Underpay: 100}, 500_000},
		"skip":     {payer.Shares{Skip: 100}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			to := randomAddress(t)
			p, ec, _ := setup(t, big.NewInt(1_000_000), to, tc.shares)
			pay, err := p.Pay(ctx, "0b6ce2b8-6a8e-4c3c-9b1d-5a3c1b2f7f10")
			if err != nil {
				t.Fatal(err)
			}
			if tc.want > 0 {
				if r := waitReceipt(t, ec, pay.TxHash); r.Status != 1 {
					t.Errorf("receipt %v", r)
				}
			}
			if got := balance(t, ec, to); got.Int64() != tc.want {
				t.Errorf("balance = %s, want %d", got, tc.want)
			}
		})
	}
}

// TestReplayAfterRevert is SRS 8.5: after the chain is rolled back the same signed transaction, with the same
// hash, is included again.
func TestReplayAfterRevert(t *testing.T) {
	ctx := context.Background()
	to := randomAddress(t)
	p, ec, rc := setup(t, big.NewInt(1_000_000), to, payer.Shares{Pay: 100})

	var snap string
	if err := rc.CallContext(ctx, &snap, "evm_snapshot"); err != nil {
		t.Fatal(err)
	}
	pay, err := p.Pay(ctx, "0b6ce2b8-6a8e-4c3c-9b1d-5a3c1b2f7f10")
	if err != nil {
		t.Fatal(err)
	}
	var ok bool
	if err := rc.CallContext(ctx, &ok, "evm_revert", snap); err != nil || !ok {
		t.Fatalf("revert: %v %v", ok, err)
	}
	if _, err := ec.TransactionReceipt(ctx, pay.TxHash); err != ethereum.NotFound {
		t.Fatalf("receipt after revert: %v", err)
	}
	if balance(t, ec, to).Sign() != 0 {
		t.Fatal("transfer survived the revert")
	}

	res, err := p.Replay(ctx, "")
	if err != nil || res.Sent != 1 {
		t.Fatalf("replay %+v %v", res, err)
	}
	if r := waitReceipt(t, ec, pay.TxHash); r.Status != 1 || r.TxHash != pay.TxHash {
		t.Fatalf("receipt %v", r)
	}
	if balance(t, ec, to).Int64() != 1_000_000 {
		t.Error("transfer not included again")
	}
	// A second replay is not an error: the node already has the transaction.
	if res, err = p.Replay(ctx, ""); err != nil || res.Known != 1 {
		t.Errorf("second replay %+v %v", res, err)
	}
}
