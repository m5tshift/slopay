package chain

import (
	"fmt"
	"math"
	"slices"
	"testing"
)

func hdr(n uint64) Header {
	return Header{Number: n, Hash: fmt.Sprintf("h%d", n), ParentHash: fmt.Sprintf("h%d", n-1), Timestamp: 1000 + n}
}

func TestEmptyState(t *testing.T) {
	var s State
	if _, ok := s.Tip(); ok {
		t.Fatal("empty state has a tip")
	}
	if _, ok := s.At(0); ok {
		t.Fatal("empty state has a header")
	}
	s.TruncateAfter(5)
	if len(s.Window) != 0 {
		t.Fatal("truncate of an empty window")
	}
}

func TestAppendKeepsWindowSize(t *testing.T) {
	var s State
	for n := uint64(10); n < 20; n++ {
		s.Append(hdr(n), 4)
	}
	if len(s.Window) != 4 || s.Window[0].Number != 16 {
		t.Fatalf("window = %+v", s.Window)
	}
	tip, ok := s.Tip()
	if !ok || tip.Number != 19 {
		t.Fatalf("tip = %+v", tip)
	}
}

func TestAppendWithoutLimit(t *testing.T) {
	var s State
	for n := range uint64(3) {
		s.Append(hdr(n), 0)
	}
	if len(s.Window) != 3 {
		t.Fatalf("window = %d", len(s.Window))
	}
}

func TestAppendDoesNotAlias(t *testing.T) {
	var s State
	s.Append(hdr(1), 4)
	s.Append(hdr(2), 4)
	c := s
	c.Append(hdr(3), 4)
	if len(s.Window) != 2 {
		t.Fatal("append changed the original window")
	}
}

func TestAt(t *testing.T) {
	var s State
	for n := uint64(5); n <= 8; n++ {
		s.Append(hdr(n), 64)
	}
	for _, tc := range []struct {
		n  uint64
		ok bool
	}{{4, false}, {5, true}, {7, true}, {8, true}, {9, false}} {
		h, ok := s.At(tc.n)
		if ok != tc.ok || (ok && h.Number != tc.n) {
			t.Errorf("At(%d) = %+v, %v", tc.n, h, ok)
		}
	}
}

func TestTruncateAfter(t *testing.T) {
	var s State
	for n := uint64(5); n <= 8; n++ {
		s.Append(hdr(n), 64)
	}
	orig := s.Window
	s.TruncateAfter(6)
	if tip, _ := s.Tip(); tip.Number != 6 || len(s.Window) != 2 {
		t.Fatalf("window = %+v", s.Window)
	}
	if len(orig) != 4 {
		t.Fatal("truncate changed the original slice")
	}
	s.TruncateAfter(100)
	if len(s.Window) != 2 {
		t.Fatal("truncate above the tip dropped headers")
	}
	s.TruncateAfter(1)
	if len(s.Window) != 0 {
		t.Fatal("truncate below the window kept headers")
	}
}

func TestInt64Conversions(t *testing.T) {
	if Int64(42) != 42 || Int64(math.MaxInt64) != math.MaxInt64 || Int64(math.MaxUint64) != math.MaxInt64 {
		t.Error("Int64 does not saturate at MaxValue")
	}
	if Uint64(42) != 42 || Uint64(-1) != 0 {
		t.Error("Uint64 does not clamp negative values")
	}
}

func TestRecipients(t *testing.T) {
	b := Block{Txs: []Tx{{To: "0xb"}, {To: "0xa"}, {To: "0xb"}, {To: "0xc"}}}
	if got := b.Recipients(); !slices.Equal(got, []string{"0xb", "0xa", "0xc"}) {
		t.Fatalf("recipients %v", got)
	}
	if got := (Block{}).Recipients(); len(got) != 0 {
		t.Fatalf("empty block %v", got)
	}
}
