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

	"github.com/m5tshift/slopay/testkit/payer-sim/internal/invoice"
	"github.com/m5tshift/slopay/testkit/payer-sim/internal/payer"
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
		fmt.Fprintf(w, `{"address":%q,"amount":%q,"asset":"ETH"}`, to.Hex(), amount.String())
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
