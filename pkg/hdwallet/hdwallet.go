// Package hdwallet derives EVM addresses from a merchant extended public key
// (BIP-32 public child derivation, SRS 3.3 and 4.1.3). It holds no private
// keys: only public derivation is implemented.
//
// Path (BIP-44, coin type 60). The merchant exports the account-level xpub
// of m/44'/60'/0'. An address with a given index is the one at
//
//	m/44'/60'/0'/0/<index>
//
// that is, the xpub is first derived with the external-chain child 0 and then
// with the non-hardened child <index> (0 <= index < 2^31). The hardened part
// of the path stays on the merchant's side.
package hdwallet

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"golang.org/x/crypto/ripemd160" //nolint:staticcheck,gosec // BIP-32 fingerprints are defined with RIPEMD-160
	"golang.org/x/crypto/sha3"
)

// HardenedOffset is the first hardened child index; indexes at or above it
// cannot be derived from a public key.
const HardenedOffset uint32 = 1 << 31

// ExternalChain is the BIP-44 "change" level of the external (receiving) chain.
const ExternalChain uint32 = 0

// Versions of the serialized extended public key.
var (
	versionXpub = [4]byte{0x04, 0x88, 0xB2, 0x1E}
	versionTpub = [4]byte{0x04, 0x35, 0x87, 0xCF}
	versionXprv = [4]byte{0x04, 0x88, 0xAD, 0xE4}
	versionTprv = [4]byte{0x04, 0x35, 0x83, 0x94}
)

// Errors returned by the package.
var (
	ErrInvalidEncoding   = errors.New("hdwallet: invalid extended key encoding")
	ErrInvalidChecksum   = errors.New("hdwallet: invalid extended key checksum")
	ErrPrivateKey        = errors.New("hdwallet: private extended key is not accepted")
	ErrUnknownVersion    = errors.New("hdwallet: unknown extended key version")
	ErrInvalidKey        = errors.New("hdwallet: invalid public key")
	ErrHardenedIndex     = errors.New("hdwallet: hardened index cannot be derived from xpub")
	ErrInvalidChildKey   = errors.New("hdwallet: derived key is invalid, use another index")
	ErrNonZeroParentless = errors.New("hdwallet: root key with non-zero parent fingerprint")
)

// ExtendedKey is a parsed extended public key.
type ExtendedKey struct {
	testnet   bool
	depth     byte
	parentFP  [4]byte
	childNum  uint32
	chainCode [32]byte
	key       *secp256k1.PublicKey
}

// Parse decodes and validates a Base58Check extended public key ("xpub...",
// or "tpub..." for test networks). Private keys are rejected.
func Parse(s string) (*ExtendedKey, error) {
	raw, err := base58CheckDecode(s)
	if err != nil {
		return nil, err
	}
	if len(raw) != 78 {
		return nil, ErrInvalidEncoding
	}
	var ver [4]byte
	copy(ver[:], raw[:4])
	k := &ExtendedKey{}
	switch ver {
	case versionXpub:
	case versionTpub:
		k.testnet = true
	case versionXprv, versionTprv:
		return nil, ErrPrivateKey
	default:
		return nil, ErrUnknownVersion
	}
	k.depth = raw[4]
	copy(k.parentFP[:], raw[5:9])
	k.childNum = binary.BigEndian.Uint32(raw[9:13])
	copy(k.chainCode[:], raw[13:45])
	if k.depth == 0 && (k.parentFP != [4]byte{} || k.childNum != 0) {
		return nil, ErrNonZeroParentless
	}
	if raw[45] != 0x02 && raw[45] != 0x03 {
		return nil, ErrInvalidKey
	}
	pub, err := secp256k1.ParsePubKey(raw[45:])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	k.key = pub
	return k, nil
}

// Validate reports whether s is a well-formed extended public key.
func Validate(s string) error {
	_, err := Parse(s)
	return err
}

// Testnet reports whether the key is a "tpub".
func (k *ExtendedKey) Testnet() bool { return k.testnet }

// Depth returns the depth in the derivation tree.
func (k *ExtendedKey) Depth() byte { return k.depth }

// String serializes the key back to Base58Check.
func (k *ExtendedKey) String() string {
	raw := make([]byte, 0, 78)
	ver := versionXpub
	if k.testnet {
		ver = versionTpub
	}
	raw = append(raw, ver[:]...)
	raw = append(raw, k.depth)
	raw = append(raw, k.parentFP[:]...)
	raw = binary.BigEndian.AppendUint32(raw, k.childNum)
	raw = append(raw, k.chainCode[:]...)
	raw = append(raw, k.key.SerializeCompressed()...)
	return base58CheckEncode(raw)
}

// Child derives the non-hardened child with the given index (CKDpub, BIP-32).
func (k *ExtendedKey) Child(index uint32) (*ExtendedKey, error) {
	if index >= HardenedOffset {
		return nil, ErrHardenedIndex
	}
	if k.depth == 255 {
		return nil, errors.New("hdwallet: depth overflow")
	}
	parent := k.key.SerializeCompressed()
	data := make([]byte, 0, 37)
	data = append(data, parent...)
	data = binary.BigEndian.AppendUint32(data, index)
	mac := hmac.New(sha512.New, k.chainCode[:])
	mac.Write(data)
	sum := mac.Sum(nil)

	var il secp256k1.ModNScalar
	if overflow := il.SetByteSlice(sum[:32]); overflow {
		return nil, ErrInvalidChildKey
	}
	var ilG, parentJ, res secp256k1.JacobianPoint
	secp256k1.ScalarBaseMultNonConst(&il, &ilG)
	k.key.AsJacobian(&parentJ)
	secp256k1.AddNonConst(&ilG, &parentJ, &res)
	if (res.X.IsZero() && res.Y.IsZero()) || res.Z.IsZero() {
		return nil, ErrInvalidChildKey
	}
	res.ToAffine()

	fp := hash160(parent)
	child := &ExtendedKey{
		testnet:  k.testnet,
		depth:    k.depth + 1,
		childNum: index,
		key:      secp256k1.NewPublicKey(&res.X, &res.Y),
	}
	copy(child.parentFP[:], fp[:4])
	copy(child.chainCode[:], sum[32:])
	return child, nil
}

// Address returns the EIP-55 checksummed EVM address of the key.
func (k *ExtendedKey) Address() string {
	return addressOf(k.key)
}

// Derive returns the address at m/.../0/index for an account-level xpub (see
// the package comment). The result is deterministic: the same xpub and index
// always give the same address, and different indexes give different ones.
func Derive(xpub string, index uint32) (string, error) {
	k, err := Parse(xpub)
	if err != nil {
		return "", err
	}
	return k.DeriveAddress(index)
}

// DeriveAddress is Derive for an already parsed key.
func (k *ExtendedKey) DeriveAddress(index uint32) (string, error) {
	chain, err := k.Child(ExternalChain)
	if err != nil {
		return "", err
	}
	leaf, err := chain.Child(index)
	if err != nil {
		return "", err
	}
	return leaf.Address(), nil
}

func addressOf(pub *secp256k1.PublicKey) string {
	uncompressed := pub.SerializeUncompressed() // 0x04 || X || Y
	h := sha3.NewLegacyKeccak256()
	h.Write(uncompressed[1:])
	hexAddr := fmt.Sprintf("%x", h.Sum(nil)[12:])

	h = sha3.NewLegacyKeccak256()
	h.Write([]byte(hexAddr))
	hash := h.Sum(nil)
	out := []byte(hexAddr)
	for i, c := range out {
		if c < 'a' {
			continue
		}
		nib := hash[i/2]
		if i%2 == 0 {
			nib >>= 4
		} else {
			nib &= 0x0f
		}
		if nib >= 8 {
			out[i] = c - 'a' + 'A'
		}
	}
	return "0x" + string(out)
}

func hash160(b []byte) [20]byte {
	s := sha256.Sum256(b)
	h := ripemd160.New() //nolint:staticcheck,gosec // see import
	h.Write(s[:])
	var out [20]byte
	copy(out[:], h.Sum(nil))
	return out
}

const b58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func checksum(b []byte) []byte {
	a := sha256.Sum256(b)
	c := sha256.Sum256(a[:])
	return c[:4]
}

func base58CheckEncode(payload []byte) string {
	full := append(append([]byte{}, payload...), checksum(payload)...)
	n := new(big.Int).SetBytes(full)
	base, mod := big.NewInt(58), new(big.Int)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, b58Alphabet[mod.Int64()])
	}
	for _, b := range full {
		if b != 0 {
			break
		}
		out = append(out, b58Alphabet[0])
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

func base58CheckDecode(s string) ([]byte, error) {
	if s == "" {
		return nil, ErrInvalidEncoding
	}
	n := new(big.Int)
	base := big.NewInt(58)
	for _, c := range []byte(s) {
		idx := -1
		for i := 0; i < len(b58Alphabet); i++ {
			if b58Alphabet[i] == c {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, ErrInvalidEncoding
		}
		n.Mul(n, base)
		n.Add(n, big.NewInt(int64(idx)))
	}
	body := n.Bytes()
	zeros := 0
	for zeros < len(s) && s[zeros] == b58Alphabet[0] {
		zeros++
	}
	full := append(make([]byte, zeros), body...)
	if len(full) < 5 {
		return nil, ErrInvalidEncoding
	}
	payload, sum := full[:len(full)-4], full[len(full)-4:]
	if !hmac.Equal(checksum(payload), sum) {
		return nil, ErrInvalidChecksum
	}
	return payload, nil
}
