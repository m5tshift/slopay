// Package wallet derives account-level extended public keys from a test
// mnemonic (BIP-39 seed, BIP-32 hardened derivation). It is a stand tool: the
// mnemonic is read from a file outside the repository and never logged. The
// service side only ever sees the resulting xpub (pkg/hdwallet).
package wallet

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"golang.org/x/crypto/ripemd160" //nolint:staticcheck,gosec // BIP-32 fingerprints are defined with RIPEMD-160
)

const hardened uint32 = 1 << 31

// ErrMnemonic is returned for a mnemonic of an unsupported shape. The text
// never contains the mnemonic.
var ErrMnemonic = errors.New("wallet: mnemonic must have 12, 15, 18, 21 or 24 words")

type node struct {
	key   [32]byte
	chain [32]byte
	depth byte
	fp    [4]byte
	num   uint32
}

// Wallet is the master node of a mnemonic.
type Wallet struct{ master node }

// New builds the wallet from a mnemonic (no passphrase). The word list and the
// BIP-39 checksum are not verified: the mnemonic is test data from the stand
// secret, a wrong one only yields different addresses.
func New(mnemonic string) (*Wallet, error) {
	words := strings.Fields(mnemonic)
	switch len(words) {
	case 12, 15, 18, 21, 24:
	default:
		return nil, ErrMnemonic
	}
	seed, err := pbkdf2.Key(sha512.New, strings.Join(words, " "), []byte("mnemonic"), 2048, 64)
	if err != nil {
		return nil, fmt.Errorf("wallet: seed: %w", err)
	}
	mac := hmac.New(sha512.New, []byte("Bitcoin seed"))
	mac.Write(seed)
	sum := mac.Sum(nil)
	var s secp256k1.ModNScalar
	if overflow := s.SetByteSlice(sum[:32]); overflow || s.IsZero() {
		return nil, errors.New("wallet: invalid master key")
	}
	w := &Wallet{}
	copy(w.master.key[:], sum[:32])
	copy(w.master.chain[:], sum[32:])
	return w, nil
}

func pubOf(key [32]byte) []byte {
	return secp256k1.PrivKeyFromBytes(key[:]).PubKey().SerializeCompressed()
}

// childHardened is CKDpriv for a hardened index.
func (n node) childHardened(i uint32) (node, error) {
	idx := i | hardened
	data := make([]byte, 0, 37)
	data = append(data, 0)
	data = append(data, n.key[:]...)
	data = binary.BigEndian.AppendUint32(data, idx)
	mac := hmac.New(sha512.New, n.chain[:])
	mac.Write(data)
	sum := mac.Sum(nil)

	var il, k secp256k1.ModNScalar
	if overflow := il.SetByteSlice(sum[:32]); overflow {
		return node{}, errors.New("wallet: invalid child key, use another index")
	}
	k.SetByteSlice(n.key[:])
	il.Add(&k)
	if il.IsZero() {
		return node{}, errors.New("wallet: invalid child key, use another index")
	}
	c := node{depth: n.depth + 1, num: idx}
	b := il.Bytes()
	c.key = b
	copy(c.chain[:], sum[32:])
	fp := hash160(pubOf(n.key))
	copy(c.fp[:], fp[:4])
	return c, nil
}

// AccountXpub returns the xpub of m/44'/60'/account'. The address with index
// n is then m/44'/60'/account'/0/n, which pkg/hdwallet derives from this key.
func (w *Wallet) AccountXpub(account uint32) (string, error) {
	if account >= hardened {
		return "", errors.New("wallet: account index out of range")
	}
	n := w.master
	for _, i := range []uint32{44, 60, account} {
		var err error
		if n, err = n.childHardened(i); err != nil {
			return "", err
		}
	}
	raw := make([]byte, 0, 78)
	raw = append(raw, 0x04, 0x88, 0xB2, 0x1E, n.depth)
	raw = append(raw, n.fp[:]...)
	raw = binary.BigEndian.AppendUint32(raw, n.num)
	raw = append(raw, n.chain[:]...)
	raw = append(raw, pubOf(n.key)...)
	return base58Check(raw), nil
}

func hash160(b []byte) [20]byte {
	s := sha256.Sum256(b)
	h := ripemd160.New() //nolint:staticcheck,gosec // see import
	h.Write(s[:])
	var out [20]byte
	copy(out[:], h.Sum(nil))
	return out
}

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func base58Check(payload []byte) string {
	a := sha256.Sum256(payload)
	c := sha256.Sum256(a[:])
	full := append(append([]byte{}, payload...), c[:4]...)
	n := new(big.Int).SetBytes(full)
	base, mod := big.NewInt(58), new(big.Int)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, alphabet[mod.Int64()])
	}
	for _, b := range full {
		if b != 0 {
			break
		}
		out = append(out, alphabet[0])
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}
