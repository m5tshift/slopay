package hdwallet

import (
	"errors"
	"strings"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"pgregory.net/rapid"
)

// BIP-32 test vector 1 (public parts only).
const (
	vec1M0H  = "xpub68Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw"
	vec1M0H1 = "xpub6ASuArnXKPbfEwhqN6e3mwBcDTgzisQN1wXN9BJcM47sSikHjJf3UFHKkNAWbWMiGj7Wf5uMash7SyYq527Hqck2AxYysAA7xmALppuCkwQ"
	vec1Root = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8"
)

func TestChildMatchesBIP32Vector(t *testing.T) {
	parent, err := Parse(vec1M0H)
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.Child(1)
	if err != nil {
		t.Fatal(err)
	}
	if got := child.String(); got != vec1M0H1 {
		t.Fatalf("child xpub = %s, want %s", got, vec1M0H1)
	}
}

func TestRoundTrip(t *testing.T) {
	for _, s := range []string{vec1Root, vec1M0H, vec1M0H1} {
		k, err := Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		if k.String() != s {
			t.Fatalf("round trip %s -> %s", s, k.String())
		}
	}
}

func TestParseErrors(t *testing.T) {
	badChecksum := vec1Root[:len(vec1Root)-1] + "9"
	cases := []struct {
		name string
		in   string
		want error
	}{
		{"empty", "", ErrInvalidEncoding},
		{"bad alphabet", "xpub0OIl", ErrInvalidEncoding},
		{"short", "xpub661MyMwAqRbc", ErrInvalidChecksum},
		{"checksum", badChecksum, ErrInvalidChecksum},
		{"private", rawKey(versionXprv, 0, [4]byte{}, make([]byte, 33)), ErrPrivateKey},
		{"wrong length", base58CheckEncode(make([]byte, 10)), ErrInvalidEncoding},
		{"tiny", "1", ErrInvalidEncoding},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Parse(c.in); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if err := Validate(c.in); err == nil {
				t.Fatal("Validate accepted bad key")
			}
		})
	}
}

func rawKey(ver [4]byte, depth byte, fp [4]byte, pub []byte) string {
	raw := append([]byte{}, ver[:]...)
	raw = append(raw, depth)
	raw = append(raw, fp[:]...)
	raw = append(raw, 0, 0, 0, 0)
	raw = append(raw, make([]byte, 32)...)
	raw = append(raw, pub...)
	return base58CheckEncode(raw)
}

func TestParseStructuralErrors(t *testing.T) {
	g := secp256k1.NewPrivateKey(new(secp256k1.ModNScalar).SetInt(1)).PubKey().SerializeCompressed()
	badPoint := append([]byte{0x02}, make([]byte, 32)...) // x = 0 is not on the curve
	badPrefix := append([]byte{0x04}, g[1:]...)

	cases := []struct {
		name string
		in   string
		want error
	}{
		{"unknown version", rawKey([4]byte{1, 2, 3, 4}, 0, [4]byte{}, g), ErrUnknownVersion},
		{"tprv", rawKey(versionTprv, 0, [4]byte{}, g), ErrPrivateKey},
		{"root with parent", rawKey(versionXpub, 0, [4]byte{1}, g), ErrNonZeroParentless},
		{"point off curve", rawKey(versionXpub, 1, [4]byte{}, badPoint), ErrInvalidKey},
		{"prefix", rawKey(versionXpub, 1, [4]byte{}, badPrefix), ErrInvalidKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Parse(c.in); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestTestnetKey(t *testing.T) {
	g := secp256k1.NewPrivateKey(new(secp256k1.ModNScalar).SetInt(1)).PubKey().SerializeCompressed()
	s := rawKey(versionTpub, 3, [4]byte{1, 2, 3, 4}, g)
	if !strings.HasPrefix(s, "tpub") {
		t.Fatalf("not a tpub: %s", s)
	}
	k, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	if !k.Testnet() || k.Depth() != 3 {
		t.Fatalf("testnet=%v depth=%d", k.Testnet(), k.Depth())
	}
	child, err := k.Child(5)
	if err != nil {
		t.Fatal(err)
	}
	if !child.Testnet() || !strings.HasPrefix(child.String(), "tpub") || child.Depth() != 4 {
		t.Fatalf("bad child %s", child.String())
	}
	if _, err := k.DeriveAddress(0); err != nil {
		t.Fatal(err)
	}
}

func TestHardenedIndexRejected(t *testing.T) {
	k, _ := Parse(vec1M0H)
	if _, err := k.Child(HardenedOffset); !errors.Is(err, ErrHardenedIndex) {
		t.Fatalf("err = %v", err)
	}
	if _, err := Derive(vec1M0H, HardenedOffset+7); !errors.Is(err, ErrHardenedIndex) {
		t.Fatalf("err = %v", err)
	}
	if _, err := k.Child(HardenedOffset - 1); err != nil {
		t.Fatal(err)
	}
}

func TestDepthOverflow(t *testing.T) {
	k, _ := Parse(vec1Root)
	k.depth = 255
	if _, err := k.Child(0); err == nil {
		t.Fatal("expected depth overflow error")
	}
}

func TestDeriveInvalidXpub(t *testing.T) {
	if _, err := Derive("nope", 0); err == nil {
		t.Fatal("expected error")
	}
}

// The public key of the scalar 1 is the generator G; its address is a
// well-known public value.
func TestAddressKnownVector(t *testing.T) {
	pub := secp256k1.NewPrivateKey(new(secp256k1.ModNScalar).SetInt(1)).PubKey()
	const want = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
	if got := addressOf(pub); got != want {
		t.Fatalf("address = %s, want %s", got, want)
	}
}

// Golden addresses for the public BIP-32 vector xpub (m/0H of vector 1),
// path .../0/index. The key derivation itself is pinned by
// TestChildMatchesBIP32Vector; these values pin the address step and the
// path layout against regressions.
func TestDeriveGolden(t *testing.T) {
	for index, want := range goldenAddresses {
		got, err := Derive(vec1M0H, uint32(index))
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("index %d: %s, want %s", index, got, want)
		}
	}
}

func TestDeriveMatchesManualPath(t *testing.T) {
	k, _ := Parse(vec1M0H)
	c0, _ := k.Child(0)
	leaf, _ := c0.Child(42)
	got, _ := k.DeriveAddress(42)
	if got != leaf.Address() {
		t.Fatalf("%s != %s", got, leaf.Address())
	}
}

func TestPropertyDeterministicAndUnique(t *testing.T) {
	keys := []string{vec1Root, vec1M0H, vec1M0H1}
	rapid.Check(t, func(t *rapid.T) {
		xpub := rapid.SampledFrom(keys).Draw(t, "xpub")
		i := rapid.Uint32Range(0, HardenedOffset-1).Draw(t, "i")
		j := rapid.Uint32Range(0, HardenedOffset-1).Draw(t, "j")
		a1, err := Derive(xpub, i)
		if err != nil {
			t.Fatal(err)
		}
		a2, _ := Derive(xpub, i)
		if a1 != a2 {
			t.Fatalf("not deterministic: %s vs %s", a1, a2)
		}
		b, _ := Derive(xpub, j)
		if (i == j) != (a1 == b) {
			t.Fatalf("i=%d j=%d a=%s b=%s", i, j, a1, b)
		}
		if len(a1) != 42 || a1[:2] != "0x" {
			t.Fatalf("bad address %s", a1)
		}
	})
}

func TestPropertyBase58RoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := rapid.SliceOfN(rapid.Byte(), 1, 80).Draw(t, "b")
		got, err := base58CheckDecode(base58CheckEncode(b))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(b) {
			t.Fatalf("%x != %x", got, b)
		}
	})
}
