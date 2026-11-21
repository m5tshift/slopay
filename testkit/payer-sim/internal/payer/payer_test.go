package payer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/m5tshift/weigate/testkit/payer-sim/internal/invoice"
)

type fakeChain struct {
	id       *big.Int
	idErr    error
	nonceErr error
	gasErr   error
	sendErr  error
	sent     []*types.Transaction
	nonceReq int
}

func (f *fakeChain) ChainID(context.Context) (*big.Int, error) { return f.id, f.idErr }
func (f *fakeChain) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	f.nonceReq++
	return 7, f.nonceErr
}
func (f *fakeChain) SuggestGasPrice(context.Context) (*big.Int, error) {
	return big.NewInt(1_000_000_000), f.gasErr
}
func (f *fakeChain) SendTransaction(_ context.Context, tx *types.Transaction) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, tx)
	return nil
}

type fakeInvoices struct {
	p   invoice.Payment
	err error
}

func (f fakeInvoices) Get(context.Context, string) (invoice.Payment, error) { return f.p, f.err }

const addr = "0x71c7656ec7ab88b098defb751b7401b5f6d8976f"

func keys(t *testing.T, n int) []*ecdsa.PrivateKey {
	t.Helper()
	var ks []*ecdsa.PrivateKey
	for range n {
		k, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		ks = append(ks, k)
	}
	return ks
}

func newPayer(t *testing.T, ch *fakeChain, inv fakeInvoices, roll int, nkeys int) *Payer {
	t.Helper()
	p, err := New(context.Background(), Config{
		Chain: ch, Invoices: inv, Keys: keys(t, nkeys), Shares: Shares{80, 10, 10}, UnderpayBps: 5000,
		Roll: func(int) int { return roll },
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func ethInvoice(amount int64) fakeInvoices {
	return fakeInvoices{p: invoice.Payment{Address: addr, Amount: big.NewInt(amount), Asset: "ETH"}}
}

func TestDecide(t *testing.T) {
	s := Shares{80, 10, 10}
	for roll, want := range map[int]Outcome{0: Pay, 79: Pay, 80: Underpay, 89: Underpay, 90: Skip, 99: Skip} {
		if got := s.Decide(roll); got != want {
			t.Errorf("roll %d: %s, want %s", roll, got, want)
		}
	}
}

func TestPayFull(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, ethInvoice(1000), 0, 1)
	pay, err := p.Pay(context.Background(), "inv-1")
	if err != nil || pay.Outcome != Pay || pay.Amount.Int64() != 1000 || len(ch.sent) != 1 {
		t.Fatalf("%+v %v", pay, err)
	}
	tx := ch.sent[0]
	if tx.Nonce() != 7 || tx.Value().Int64() != 1000 || tx.To().Hex() != common.HexToAddress(addr).Hex() || tx.Hash() != pay.TxHash {
		t.Errorf("tx %+v", tx)
	}
}

func TestPayUnderpay(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, ethInvoice(1001), 85, 1)
	pay, err := p.Pay(context.Background(), "inv-1")
	if err != nil || pay.Outcome != Underpay || pay.Amount.Int64() != 500 { // 1001*5000/10000 rounded down
		t.Fatalf("%+v %v", pay, err)
	}
	// Never zero.
	p = newPayer(t, &fakeChain{id: big.NewInt(1)}, ethInvoice(1), 85, 1)
	if pay, err = p.Pay(context.Background(), "x"); err != nil || pay.Amount.Int64() != 1 {
		t.Fatalf("%+v %v", pay, err)
	}
}

func TestPaySkip(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, ethInvoice(1000), 95, 1)
	pay, err := p.Pay(context.Background(), "inv-1")
	if err != nil || pay.Outcome != Skip || pay.Amount != nil || (pay.TxHash != common.Hash{}) || len(ch.sent) != 0 {
		t.Fatalf("%+v %v", pay, err)
	}
	if res, err := p.Replay(context.Background(), ""); err != nil || res != (ReplayResult{}) {
		t.Errorf("replay %+v %v", res, err)
	}
}

func TestPayIdempotent(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, ethInvoice(1000), 0, 1)
	a, _ := p.Pay(context.Background(), "inv-1")
	b, err := p.Pay(context.Background(), "inv-1")
	if err != nil || a.TxHash != b.TxHash || len(ch.sent) != 1 || len(p.Payments()) != 1 {
		t.Errorf("%+v %+v %v", a, b, err)
	}
}

func TestNonceAndAccounts(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	ks := keys(t, 2)
	p, err := New(context.Background(), Config{Chain: ch, Invoices: ethInvoice(10), Keys: ks, Shares: Shares{100, 0, 0}, UnderpayBps: 5000, Roll: func(int) int { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if _, err := p.Pay(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	// Two accounts in turn; each reads the nonce once (7) and then counts locally (8).
	nonces := []uint64{7, 7, 8, 8}
	for i, tx := range ch.sent {
		from, _ := types.Sender(p.signer, tx)
		if tx.Nonce() != nonces[i] || from != crypto.PubkeyToAddress(ks[i%2].PublicKey) {
			t.Errorf("tx %d: nonce %d from %s", i, tx.Nonce(), from)
		}
	}
	if ch.nonceReq != 2 {
		t.Errorf("nonce requests = %d", ch.nonceReq)
	}
}

func TestSendFailureResetsNonce(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, ethInvoice(10), 0, 1)
	_, _ = p.Pay(context.Background(), "a")
	ch.sendErr = errors.New("boom")
	if _, err := p.Pay(context.Background(), "b"); err == nil {
		t.Fatal("no error")
	}
	if len(p.Payments()) != 1 {
		t.Error("failed payment recorded")
	}
	ch.sendErr = nil
	if _, err := p.Pay(context.Background(), "b"); err != nil || ch.nonceReq != 2 {
		t.Errorf("%v, nonce requests %d", err, ch.nonceReq)
	}
}

func TestPayErrors(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		ch   *fakeChain
		inv  fakeInvoices
		want string
	}{
		"invoice":  {&fakeChain{id: big.NewInt(1)}, fakeInvoices{err: errors.New("down")}, "down"},
		"asset":    {&fakeChain{id: big.NewInt(1)}, fakeInvoices{p: invoice.Payment{Address: addr, Amount: big.NewInt(1), Asset: "USDT"}}, "not ETH or ERC20"},
		"contract": {&fakeChain{id: big.NewInt(1)}, fakeInvoices{p: invoice.Payment{Address: addr, Amount: big.NewInt(1), Asset: "ERC20", Contract: "nope"}}, "invalid token contract"},
		"address":  {&fakeChain{id: big.NewInt(1)}, fakeInvoices{p: invoice.Payment{Address: "nope", Amount: big.NewInt(1), Asset: "ETH"}}, "invalid payment address"},
		"nonce":    {&fakeChain{id: big.NewInt(1), nonceErr: errors.New("x")}, ethInvoice(1), "nonce"},
		"gasprice": {&fakeChain{id: big.NewInt(1), gasErr: errors.New("x")}, ethInvoice(1), "gas price"},
	} {
		t.Run(name, func(t *testing.T) {
			p := newPayer(t, tc.ch, tc.inv, 0, 1)
			if _, err := p.Pay(ctx, "i"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, Config{Chain: &fakeChain{id: big.NewInt(1)}}); err == nil {
		t.Error("no keys: no error")
	}
	if _, err := New(ctx, Config{Chain: &fakeChain{idErr: errors.New("x")}, Keys: keys(t, 1)}); err == nil {
		t.Error("chain id error: no error")
	}
}

func TestParseKeys(t *testing.T) {
	k := keys(t, 2)
	h0, h1 := common.Bytes2Hex(crypto.FromECDSA(k[0])), common.Bytes2Hex(crypto.FromECDSA(k[1]))
	got, err := ParseKeys("# funded\n\n0x" + h0 + "\n  " + h1 + "  \n")
	if err != nil || len(got) != 2 || !got[1].Equal(k[1]) {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"", "# only a comment\n", "zz\n"} {
		if _, err := ParseKeys(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
	if _, err := ParseKeys("secret-looking-text"); err != nil && strings.Contains(err.Error(), "secret-looking") {
		t.Error("error leaks the line")
	}
}

func TestReplay(t *testing.T) {
	ctx := context.Background()
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, ethInvoice(10), 0, 1)
	a, _ := p.Pay(ctx, "a")
	b, _ := p.Pay(ctx, "b")
	ch.sent = nil

	res, err := p.Replay(ctx, "")
	if err != nil || res != (ReplayResult{Sent: 2}) || ch.sent[0].Hash() != a.TxHash || ch.sent[1].Hash() != b.TxHash {
		t.Fatalf("%+v %v", res, err)
	}
	ch.sent = nil
	if res, err = p.Replay(ctx, "b"); err != nil || res.Sent != 1 || ch.sent[0].Hash() != b.TxHash {
		t.Fatalf("%+v %v", res, err)
	}
	for _, msg := range []string{"already known", "nonce too low: next nonce 9", "ALREADY IMPORTED"} {
		ch.sendErr = errors.New(msg)
		if res, err = p.Replay(ctx, ""); err != nil || res != (ReplayResult{Known: 2}) {
			t.Errorf("%s: %+v %v", msg, res, err)
		}
	}
	ch.sendErr = errors.New("insufficient funds")
	if _, err = p.Replay(ctx, ""); err == nil {
		t.Error("other error ignored")
	}
}

const usdtAddr = "0xe7f1725e7734ce288f8367e1bb143e90bb3f0512"

func tokenInvoice(amount int64) fakeInvoices {
	return fakeInvoices{p: invoice.Payment{Address: addr, Amount: big.NewInt(amount), Asset: "ERC20", Contract: usdtAddr}}
}

// An ERC20 payment is a call of transfer(to, amount) to the token contract with no ETH value.
func TestPayToken(t *testing.T) {
	for name, tc := range map[string]struct {
		roll int
		want int64
	}{"pay": {0, 1000}, "underpay": {85, 500}} {
		t.Run(name, func(t *testing.T) {
			ch := &fakeChain{id: big.NewInt(31337)}
			p := newPayer(t, ch, tokenInvoice(1000), tc.roll, 1)
			pay, err := p.Pay(context.Background(), "inv-1")
			if err != nil || pay.Amount.Int64() != tc.want || len(ch.sent) != 1 {
				t.Fatalf("%+v %v", pay, err)
			}
			tx := ch.sent[0]
			data := tx.Data()
			if tx.To().Hex() != common.HexToAddress(usdtAddr).Hex() || tx.Value().Sign() != 0 || tx.Gas() != tokenTransferGas || tx.Hash() != pay.TxHash {
				t.Errorf("tx %+v", tx)
			}
			if len(data) != 68 || common.Bytes2Hex(data[:4]) != "a9059cbb" ||
				common.BytesToAddress(data[4:36]) != common.HexToAddress(addr) || new(big.Int).SetBytes(data[36:]).Int64() != tc.want {
				t.Errorf("calldata %x", data)
			}
		})
	}
}

func TestPayTokenSkipAndReplay(t *testing.T) {
	ch := &fakeChain{id: big.NewInt(31337)}
	p := newPayer(t, ch, tokenInvoice(1000), 95, 1)
	if pay, err := p.Pay(context.Background(), "inv-1"); err != nil || pay.Outcome != Skip || len(ch.sent) != 0 {
		t.Fatalf("%+v %v", pay, err)
	}
	ch = &fakeChain{id: big.NewInt(31337)}
	p = newPayer(t, ch, tokenInvoice(1000), 0, 1)
	pay, _ := p.Pay(context.Background(), "inv-1")
	if res, err := p.Replay(context.Background(), ""); err != nil || res.Sent != 1 || ch.sent[1].Hash() != pay.TxHash {
		t.Errorf("replay %+v %v", res, err)
	}
}
