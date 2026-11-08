// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/azure"
)

func sign(n int) int { return cmp.Compare(n, 0) }

// alphabet is every scalar in the corpus, with the UTF-16 boundaries.
func alphabet() []rune {
	set := map[rune]bool{0xd7ff: true, 0xe000: true, 0xffff: true, 0x10000: true, 0x10ffff: true}
	for _, k := range Keys() {
		for _, r := range k {
			set[r] = true
		}
	}
	out := make([]rune, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}

// TestCompareUTF16AgreesWithTheReplay draws random valid UTF-8 strings
// over the corpus alphabet, the second often an edit of the first so the
// two share a prefix, and requires CompareUTF16 and the replay's Compare
// to agree on every pair.
func TestCompareUTF16AgreesWithTheReplay(t *testing.T) {
	letters := alphabet()
	rng := rand.New(rand.NewPCG(1, 2))
	draw := func(n int) []rune {
		s := make([]rune, n)
		for i := range s {
			s[i] = letters[rng.IntN(len(letters))]
		}
		return s
	}
	for i := range 200_000 {
		a := draw(rng.IntN(7))
		b := slices.Clone(a)
		switch rng.IntN(4) {
		case 0:
			b = draw(rng.IntN(7))
		case 1:
			if len(b) > 0 {
				b[rng.IntN(len(b))] = letters[rng.IntN(len(letters))]
			}
		case 2:
			b = append(b, draw(1+rng.IntN(3))...)
		case 3:
			if len(b) > 0 {
				j := rng.IntN(len(b))
				b = append(b[:j:j], draw(1+rng.IntN(3))...)
			}
		}
		x, y := string(a), string(b)
		if i%2 == 1 {
			x, y = y, x
		}
		if got, want := sign(CompareUTF16(x, y)), sign(azure.Compare([]byte(x), []byte(y))); got != want {
			t.Fatalf("CompareUTF16(%+q, %+q) = %d, replay Compare %d", x, y, got, want)
		}
	}
}

// TestCompareUTF16Boundaries holds both comparators to the UTF-16 order
// written out by hand at the boundaries: U+D7FF (the last scalar before
// the surrogates) < U+10000 < U+10FFFF (surrogate pairs, D800..DBFF) <
// U+E000 < U+FFFF, alone, after a shared prefix, and with suffixes that
// would reverse a comparison past the first difference.
func TestCompareUTF16Boundaries(t *testing.T) {
	order := []string{"퟿", "\U00010000", "\U0010ffff", "", "￿"}
	for _, prefix := range []string{"", "Q", "a/b/", "é", "\U0001f600"} {
		for _, suffix := range []string{"", "a", "\U0010ffff", "\u0000"} {
			for i, x := range order {
				for j, y := range order {
					a, b := prefix+x+suffix, prefix+y
					want := cmp.Compare(i, j)
					if want == 0 {
						want = cmp.Compare(len(suffix), 0)
					}
					if got := sign(CompareUTF16(a, b)); got != want {
						t.Errorf("CompareUTF16(%+q, %+q) = %d, want %d", a, b, got, want)
					}
					if got := sign(azure.Compare([]byte(a), []byte(b))); got != want {
						t.Errorf("replay Compare(%+q, %+q) = %d, want %d", a, b, got, want)
					}
				}
			}
		}
	}
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"", "", 0}, {"", "\u0000", -1}, {"a", "a/", -1}, {"a/", "a/b", -1}, {"a/b", "b", -1},
		{"\u007f", "\u0080", -1}, {"߿", "ࠀ", -1}, {"퟿", "", -1},
		{"￿", "\U00010000", 1}, {"", "\U0010ffff", 1}, {"\U0010ffff", "\U0001f600", 1},
		{"pｚ", "p\U0001f600", 1}, {"p�", "p\U00010000/y", 1},
	} {
		if got := sign(CompareUTF16(c.a, c.b)); got != c.want {
			t.Errorf("CompareUTF16(%+q, %+q) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := sign(azure.Compare([]byte(c.a), []byte(c.b))); got != c.want {
			t.Errorf("replay Compare(%+q, %+q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// TestCompareUTF16SortsTheCorpusAsTheReplay sorts the corpus with each
// comparator: the same order, which is not byte order.
func TestCompareUTF16SortsTheCorpusAsTheReplay(t *testing.T) {
	keys := Keys()
	mine, replay := slices.Clone(keys), slices.Clone(keys)
	slices.SortFunc(mine, CompareUTF16)
	slices.SortFunc(replay, func(a, b string) int { return azure.Compare([]byte(a), []byte(b)) })
	if !slices.Equal(mine, replay) {
		for i := range mine {
			if mine[i] != replay[i] {
				t.Fatalf("first difference at %d: %.40q against %.40q", i, mine[i], replay[i])
			}
		}
	}
	if slices.Equal(mine, keys) {
		t.Fatal("the corpus sorts the same in byte order")
	}
	if i := slices.Index(mine, "p\U00010000"); i < 0 || !strings.HasPrefix(mine[i+1], "p\U00010000") {
		t.Fatal("the F0..F4 branch is not contiguous")
	}
}
