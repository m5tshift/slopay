package wallet

import (
	"errors"
	"strings"
	"testing"

	"github.com/m5tshift/slopay/pkg/hdwallet"
)

// Standard Hardhat/Anvil test mnemonic and its known account-level xpub and
// first addresses (same golden data as pkg/hdwallet).
const (
	testMnemonic = "test test test test test test test test test test test junk"
	goldenXpub   = "xpub6Ce9NcJvTk36xtLSrJLZqE7wtgA5deCeYs7rSQtreh4cj6ByPtrg9sD7V2FNFLPnf8heNP3FGkeV9qwfzvZNSd54JoNXVsXFYSYwHsnJxqP"
)

func TestAccountXpubGolden(t *testing.T) {
	w, err := New(testMnemonic)
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.AccountXpub(0)
	if err != nil {
		t.Fatal(err)
	}
	if got != goldenXpub {
		t.Fatalf("xpub = %s, want %s", got, goldenXpub)
	}
	addr, err := hdwallet.Derive(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if addr != "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266" {
		t.Fatalf("address = %s", addr)
	}
}

func TestAccountsDiffer(t *testing.T) {
	w, _ := New(testMnemonic)
	seen := map[string]bool{}
	for i := range uint32(5) {
		x, err := w.AccountXpub(i)
		if err != nil {
			t.Fatal(err)
		}
		if seen[x] {
			t.Fatalf("duplicate xpub for account %d", i)
		}
		seen[x] = true
		if err := hdwallet.Validate(x); err != nil {
			t.Fatalf("account %d: %v", i, err)
		}
	}
}

func TestWhitespaceNormalised(t *testing.T) {
	a, _ := New(testMnemonic)
	b, err := New("  " + strings.ReplaceAll(testMnemonic, " ", "\n") + "\n")
	if err != nil {
		t.Fatal(err)
	}
	xa, _ := a.AccountXpub(1)
	xb, _ := b.AccountXpub(1)
	if xa != xb {
		t.Fatal("whitespace changes the result")
	}
}

func TestBadMnemonic(t *testing.T) {
	for _, m := range []string{"", "one two three", strings.Repeat("a ", 13)} {
		if _, err := New(m); !errors.Is(err, ErrMnemonic) {
			t.Fatalf("%q: err = %v", m, err)
		}
	}
}

func TestAccountOutOfRange(t *testing.T) {
	w, _ := New(testMnemonic)
	if _, err := w.AccountXpub(hardened); err == nil {
		t.Fatal("want error")
	}
}
