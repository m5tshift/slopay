// Package xpubtest provides extended keys for tests. There is no private key material here: the
// "private" key is a public one with the xprv version bytes, enough to exercise the rejection path.
package xpubtest

import (
	"crypto/sha256"
	"math/big"
	"strings"
)

// Xpub is the master public key of BIP-32 test vector 1 (also the example of the contract).
const Xpub = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8"

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// WithVersion re-encodes a Base58Check extended key with other version bytes and a valid checksum.
func WithVersion(key string, version [4]byte) string {
	raw := decode(key)
	payload := append(append([]byte{}, version[:]...), raw[4:len(raw)-4]...)
	return encode(payload)
}

// PrivateVersion turns Xpub into a string that parses as "xprv": the BIP-32 mainnet private version.
func PrivateVersion() string { return WithVersion(Xpub, [4]byte{0x04, 0x88, 0xAD, 0xE4}) }

// BadChecksum changes the last character of Xpub.
func BadChecksum() string {
	last := Xpub[len(Xpub)-1]
	repl := byte('1')
	if last == '1' {
		repl = '2'
	}
	return Xpub[:len(Xpub)-1] + string(repl)
}

func decode(s string) []byte {
	n := new(big.Int)
	for _, c := range s {
		n.Mul(n, big.NewInt(58))
		n.Add(n, big.NewInt(int64(strings.IndexRune(alphabet, c))))
	}
	return n.Bytes()
}

func encode(payload []byte) string {
	h1 := sha256.Sum256(payload)
	h2 := sha256.Sum256(h1[:])
	b := append(append([]byte{}, payload...), h2[:4]...)
	n := new(big.Int).SetBytes(b)
	var out []byte
	mod := new(big.Int)
	for n.Sign() > 0 {
		n.DivMod(n, big.NewInt(58), mod)
		out = append(out, alphabet[mod.Int64()])
	}
	for _, c := range b {
		if c != 0 {
			break
		}
		out = append(out, '1')
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}
