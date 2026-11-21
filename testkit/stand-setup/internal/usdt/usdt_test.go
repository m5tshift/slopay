package usdt_test

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/m5tshift/weigate/testkit/stand-setup/internal/usdt"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeChain records transactions and serves receipts; balances are kept by decoding mint calls.
type fakeChain struct {
	txs      []*types.Transaction
	nonce    uint64
	reverted bool
	notFound int // receipts answered with NotFound before the real one
	failAt   string
	balances map[common.Address]*big.Int
}

func (f *fakeChain) ChainID(context.Context) (*big.Int, error) {
	if f.failAt == "chainid" {
		return nil, errors.New("down")
	}
	return big.NewInt(31337), nil
}

func (f *fakeChain) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	if f.failAt == "nonce" {
		return 0, errors.New("down")
	}
	return f.nonce, nil
}

func (f *fakeChain) SuggestGasPrice(context.Context) (*big.Int, error) {
	if f.failAt == "price" {
		return nil, errors.New("down")
	}
	return big.NewInt(1), nil
}

func (f *fakeChain) EstimateGas(context.Context, ethereum.CallMsg) (uint64, error) {
	if f.failAt == "gas" {
		return 0, errors.New("down")
	}
	return 100000, nil
}

func (f *fakeChain) SendTransaction(_ context.Context, tx *types.Transaction) error {
	if f.failAt == "send" {
		return errors.New("boom")
	}
	f.txs = append(f.txs, tx)
	if d := tx.Data(); tx.To() != nil && len(d) == 68 {
		to := common.BytesToAddress(d[4:36])
		if f.balances[to] == nil {
			f.balances[to] = new(big.Int)
		}
		f.balances[to].Add(f.balances[to], new(big.Int).SetBytes(d[36:]))
	}
	return nil
}

func (f *fakeChain) TransactionReceipt(_ context.Context, h common.Hash) (*types.Receipt, error) {
	switch f.failAt {
	case "receipt":
		return nil, errors.New("down")
	case "never":
		return nil, ethereum.NotFound
	}
	if f.notFound > 0 {
		f.notFound--
		return nil, ethereum.NotFound
	}
	st := types.ReceiptStatusSuccessful
	if f.reverted {
		st = types.ReceiptStatusFailed
	}
	return &types.Receipt{Status: st, TxHash: h, ContractAddress: common.HexToAddress("0xabc0000000000000000000000000000000000001")}, nil
}

func (f *fakeChain) CallContract(_ context.Context, msg ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	if f.failAt == "call" {
		return nil, errors.New("down")
	}
	who := common.BytesToAddress(msg.Data[4:])
	b := f.balances[who]
	if b == nil || f.failAt == "short" {
		b = new(big.Int)
	}
	return common.LeftPadBytes(b.Bytes(), 32), nil
}

func newFake() *fakeChain { return &fakeChain{balances: map[common.Address]*big.Int{}, nonce: 7} }

func key(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func addr(t *testing.T) common.Address { return crypto.PubkeyToAddress(key(t).PublicKey) }

func TestDeployAndMint(t *testing.T) {
	f, k := newFake(), key(t)
	f.notFound = 2
	a, b := addr(t), addr(t)
	amount := big.NewInt(5_000_000)
	res, err := usdt.Deploy(context.Background(), usdt.Config{Chain: f, Key: k, Recipients: []common.Address{a, b}, Amount: amount, PollInterval: time.Millisecond}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if res.Minted != 2 || res.Address == (common.Address{}) || len(f.txs) != 3 {
		t.Fatalf("result %+v, %d txs", res, len(f.txs))
	}
	code, _ := usdt.Bytecode()
	if f.txs[0].To() != nil || string(f.txs[0].Data()) != string(code) {
		t.Error("first transaction must create the contract")
	}
	for i, tx := range f.txs {
		if tx.Nonce() != 7+uint64(i) { //nolint:gosec // small index
			t.Errorf("tx %d nonce %d", i, tx.Nonce())
		}
	}
	for _, who := range []common.Address{a, b} {
		if f.balances[who].Cmp(amount) != 0 {
			t.Errorf("balance %s = %s", who, f.balances[who])
		}
	}
	mint := f.txs[1].Data()
	if common.Bytes2Hex(mint[:4]) != "40c10f19" {
		t.Errorf("mint selector %x", mint[:4])
	}
}

func TestDeployWithoutRecipients(t *testing.T) {
	f := newFake()
	res, err := usdt.Deploy(context.Background(), usdt.Config{Chain: f, Key: key(t)}, quiet)
	if err != nil || res.Minted != 0 || len(f.txs) != 1 {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func TestDeployErrors(t *testing.T) {
	k := key(t)
	one := []common.Address{addr(t)}
	cases := map[string]struct {
		f    *fakeChain
		cfg  usdt.Config
		want string
	}{
		"no chain":    {nil, usdt.Config{Key: k}, "required"},
		"no key":      {newFake(), usdt.Config{}, "required"},
		"zero amount": {newFake(), usdt.Config{Key: k, Recipients: one, Amount: big.NewInt(0)}, "positive"},
		"nil amount":  {newFake(), usdt.Config{Key: k, Recipients: one}, "positive"},
		"chain id":    {&fakeChain{failAt: "chainid"}, usdt.Config{Key: k}, "chain id"},
		"nonce":       {&fakeChain{failAt: "nonce"}, usdt.Config{Key: k}, "nonce"},
		"gas price":   {&fakeChain{failAt: "price"}, usdt.Config{Key: k}, "gas price"},
		"estimate":    {&fakeChain{failAt: "gas"}, usdt.Config{Key: k}, "estimate gas"},
		"send":        {&fakeChain{failAt: "send"}, usdt.Config{Key: k}, "send"},
		"receipt":     {&fakeChain{failAt: "receipt"}, usdt.Config{Key: k}, "receipt"},
		"reverted":    {&fakeChain{reverted: true}, usdt.Config{Key: k}, "reverted"},
		"timeout":     {&fakeChain{failAt: "never"}, usdt.Config{Key: k, PollInterval: time.Millisecond, Timeout: 10 * time.Millisecond}, "no receipt"},
		"balance rpc": {&fakeChain{failAt: "call", balances: map[common.Address]*big.Int{}}, usdt.Config{Key: k, Recipients: one, Amount: big.NewInt(1)}, "balanceOf"},
		"balance low": {&fakeChain{failAt: "short", balances: map[common.Address]*big.Int{}}, usdt.Config{Key: k, Recipients: one, Amount: big.NewInt(1)}, "below"},
	}
	for name, tc := range cases {
		if tc.f != nil {
			tc.cfg.Chain = tc.f
		}
		_, err := usdt.Deploy(context.Background(), tc.cfg, quiet)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v", name, err)
		}
	}
}

func TestDeployCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := usdt.Deploy(ctx, usdt.Config{Chain: &fakeChain{failAt: "never"}, Key: key(t), PollInterval: time.Millisecond}, quiet)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestParseKeys(t *testing.T) {
	k, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	hexKey := common.Bytes2Hex(crypto.FromECDSA(k))
	keys, err := usdt.ParseKeys("# c\n\n0x" + hexKey + "\n" + hexKey + "\n")
	if err != nil || len(keys) != 2 {
		t.Fatalf("%d %v", len(keys), err)
	}
	if got := usdt.Addresses(keys); got[0] != crypto.PubkeyToAddress(k.PublicKey) {
		t.Error("address mismatch")
	}
	for in, want := range map[string]string{"zz\n": "line 1: not a private key", "# only\n": "no keys"} {
		_, err := usdt.ParseKeys(in)
		if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "zz") {
			t.Errorf("%q: %v", in, err)
		}
	}
}

func TestParseAddresses(t *testing.T) {
	a := addr(t)
	got, err := usdt.ParseAddresses(" " + a.Hex() + ", ,")
	if err != nil || len(got) != 1 || got[0] != a {
		t.Fatalf("%v %v", got, err)
	}
	if got, err := usdt.ParseAddresses(""); err != nil || len(got) != 0 {
		t.Errorf("empty: %v %v", got, err)
	}
	if _, err := usdt.ParseAddresses("0x12"); err == nil {
		t.Error("bad address accepted")
	}
}

// The embedded bytecode must match the contract; the check runs when
// `forge build` output exists in testkit/erc20/out.
func TestBytecodeMatchesForgeBuild(t *testing.T) {
	code, err := usdt.Bytecode()
	if err != nil || len(code) == 0 {
		t.Fatalf("bytecode: %v", err)
	}
	raw, err := os.ReadFile("../../../erc20/out/TestUSDT.sol/TestUSDT.json")
	if err != nil {
		t.Skip("forge output not found: run forge build in testkit/erc20")
	}
	var art struct {
		Bytecode struct {
			Object string `json:"object"`
		} `json:"bytecode"`
	}
	if err := json.Unmarshal(raw, &art); err != nil {
		t.Fatal(err)
	}
	if got := common.Bytes2Hex(code); got != strings.TrimPrefix(art.Bytecode.Object, "0x") {
		t.Fatal("embedded TestUSDT.bin is stale: regenerate it from testkit/erc20/out")
	}
}
