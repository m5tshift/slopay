//go:build integration

package usdt_test

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/m5tshift/weigate/testkit/stand-setup/internal/usdt"
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

// The real contract on a real Anvil: deploy, mint, read back balance, decimals and the Transfer log.
func TestDeployToAnvil(t *testing.T) {
	ctx := context.Background()
	rc, err := rpc.DialContext(ctx, anvilURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Close)
	ec := ethclient.NewClient(rc)
	k := key(t)
	if err := rc.CallContext(ctx, nil, "anvil_setBalance", crypto.PubkeyToAddress(k.PublicKey), hexutil.EncodeBig(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))); err != nil {
		t.Fatal(err)
	}
	a, b := addr(t), addr(t)
	amount := big.NewInt(1_000_000_000_000)
	res, err := usdt.Deploy(ctx, usdt.Config{Chain: ec, Key: k, Recipients: []common.Address{a, b}, Amount: amount}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	call := func(sig string, args ...[]byte) *big.Int {
		data := crypto.Keccak256([]byte(sig))[:4]
		for _, x := range args {
			data = append(data, common.LeftPadBytes(x, 32)...)
		}
		out, err := ec.CallContract(ctx, ethereum.CallMsg{To: &res.Address, Data: data}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return new(big.Int).SetBytes(out)
	}
	for _, who := range []common.Address{a, b} {
		if got := call("balanceOf(address)", who.Bytes()); got.Cmp(amount) != 0 {
			t.Errorf("balance %s = %s", who, got)
		}
	}
	if got := call("decimals()"); got.Int64() != 6 {
		t.Errorf("decimals %s", got)
	}
	if got := call("totalSupply()"); got.Cmp(new(big.Int).Mul(amount, big.NewInt(2))) != 0 {
		t.Errorf("supply %s", got)
	}
	logs, err := ec.FilterLogs(ctx, ethereum.FilterQuery{Addresses: []common.Address{res.Address}, FromBlock: big.NewInt(0)})
	if err != nil || len(logs) != 2 {
		t.Fatalf("logs %d err %v", len(logs), err)
	}
}
