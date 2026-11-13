// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"math/big"
	"math/rand/v2"
	"testing"
)

// oldExtrapolateWhy is extrapolateWhy as it was at 8c8f8708, before its
// cost changes: the oracle of TestExtrapolateMatchesOld.
func oldExtrapolateWhy(page []string, c, hi string, bounded bool, jump int64) (string, extReason) {
	if len(page) < 2 {
		return "", extShort
	}
	var seen [128]bool
	for _, k := range append(page, c) {
		for i := 0; i < len(k); i++ {
			b := k[i]
			if b >= 0x80 || (!isSafe(int(b)) && b != '%') {
				return "", extAlphabet
			}
			seen[b] = true
		}
	}
	var alphabet []byte
	var rank [128]int64
	for b := range seen {
		if seen[b] {
			alphabet = append(alphabet, byte(b))
			rank[b] = int64(len(alphabet)) // 0 is end-of-string
		}
	}
	radix := big.NewInt(int64(len(alphabet)) + 1)
	n := min(max(len(page[0]), len(c), len(hi))+1, maxKeyLen)
	value := func(s string) (*big.Int, bool) {
		v := new(big.Int)
		for i := 0; i < n; i++ {
			v.Mul(v, radix)
			if i < len(s) {
				b := s[i]
				if b >= 0x80 || !seen[b] {
					return nil, false
				}
				v.Add(v, big.NewInt(rank[b]))
			}
		}
		return v, true
	}
	vc, _ := value(c)
	vf, _ := value(page[0])
	span := new(big.Int).Sub(vc, vf)
	if span.Sign() <= 0 {
		return "", extZeroSpan
	}
	target := new(big.Int).Add(vc, span.Mul(span, big.NewInt(jump)))
	if bounded {
		if vh, ok := value(hi); ok {
			mid := new(big.Int).Add(vc, vh)
			if mid.Rsh(mid, 1); target.Cmp(mid) > 0 {
				target = mid
			}
		}
	}
	digits := make([]int64, n)
	t := new(big.Int).Set(target)
	r := new(big.Int)
	for i := n - 1; i >= 0; i-- {
		t.DivMod(t, radix, r)
		digits[i] = r.Int64()
	}
	if t.Sign() != 0 {
		return "", extPastTop
	}
	var m []byte
	for i := 0; i < n; i++ {
		var cd int64
		if i < len(c) {
			cd = rank[c[i]]
		}
		if digits[i] == 0 {
			return "", extHole
		}
		if digits[i] != cd {
			b := alphabet[digits[i]-1]
			if b == '%' {
				b = '%' + 1
			}
			m = append(m, b)
			break
		}
		m = append(m, alphabet[digits[i]-1])
	}
	key := string(m)
	if key <= c || len(key) > maxKeyLen || (bounded && key >= hi) {
		return "", extOutside
	}
	return key, extOK
}

// extrapolateCase is one generated call: a page, its cursor, a bound and
// a stride.
type extrapolateCase struct {
	page    []string
	c, hi   string
	bounded bool
	jump    int64
}

// genExtrapolate draws pages over small random alphabets ('%', unsafe and
// non-ASCII bytes among them, rarely), sorted or not, with cursors and
// bounds from the page, its neighbourhood or nothing, and strides from 1
// to 2^40.
func genExtrapolate(r *rand.Rand) extrapolateCase {
	alpha := []byte("ab%0189/-_.=~ AZz{}")
	if r.IntN(10) == 0 {
		alpha = append(alpha, "\x01\x7f\x80\xc3\xa9"[r.IntN(5)])
	}
	alpha = alpha[:1+r.IntN(len(alpha))]
	key := func() string {
		n := r.IntN(12)
		if r.IntN(20) == 0 {
			n = 1000 + r.IntN(40) // near maxKeyLen
		}
		b := make([]byte, n)
		for i := range b {
			b[i] = alpha[r.IntN(len(alpha))]
		}
		return string(b)
	}
	prefix := key()
	page := make([]string, r.IntN(6))
	for i := range page {
		page[i] = prefix + key()
	}
	if r.IntN(2) == 0 {
		sortStrings(page)
	}
	c := prefix + key()
	if len(page) > 0 && r.IntN(3) > 0 {
		c = page[len(page)-1]
	}
	hi := ""
	switch r.IntN(4) {
	case 0:
		hi = c + key()
	case 1:
		hi = prefix + key()
	case 2:
		hi = key()
	}
	if r.IntN(4) == 0 {
		// Spare capacity, as a page sliced from a longer buffer has: it
		// must not be written.
		spare := make([]string, len(page), len(page)+2)
		copy(spare, page)
		spare[:cap(spare)][len(page)], spare[:cap(spare)][len(page)+1] = "spare", "spare"
		page = spare
	}
	return extrapolateCase{page, c, hi, r.IntN(2) == 0, int64(1) << r.IntN(41)}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// TestExtrapolateMatchesOld: the density step returns what it did before
// its cost changes, pivot and reason, on generated calls, on every page of
// a blockchain-shaped listing and on the reasons table's cases, and it
// leaves the page, including its spare capacity, untouched.
func TestExtrapolateMatchesOld(t *testing.T) {
	check := func(x extrapolateCase) {
		t.Helper()
		full := x.page[:cap(x.page)]
		before := append([]string(nil), full...)
		m, why := extrapolateWhy(x.page, x.c, x.hi, x.bounded, x.jump)
		for i := range full {
			if full[i] != before[i] {
				t.Fatalf("page[%d] (len %d) changed: %q, was %q", i, len(x.page), full[i], before[i])
			}
		}
		wm, wwhy := oldExtrapolateWhy(x.page, x.c, x.hi, x.bounded, x.jump)
		if m != wm || why != wwhy {
			t.Fatalf("extrapolateWhy(%q, %q, %q, %v, %d) = %q, %d; was %q, %d", x.page, x.c, x.hi, x.bounded, x.jump, m, why, wm, wwhy)
		}
	}
	r := rand.New(rand.NewPCG(3, 4))
	n := 200000
	if testing.Short() {
		n = 20000
	}
	reasons := map[extReason]int{}
	for range n {
		x := genExtrapolate(r)
		check(x)
		_, why := oldExtrapolateWhy(x.page, x.c, x.hi, x.bounded, x.jump)
		reasons[why]++
	}
	for why := extOK; why <= extOutside; why++ {
		if reasons[why] == 0 {
			t.Errorf("no generated call refused for reason %d: %v", why, reasons)
		}
	}
	keys := blockchainPage(0, 5000)
	for i := 2; i+1000 <= len(keys); i += 250 {
		page := keys[i : i+1000]
		for _, jump := range []int64{4, 64, 1 << 16} {
			check(extrapolateCase{page, page[len(page)-1], "", false, jump})
			check(extrapolateCase{page, page[len(page)-1], "v1.0/btc/transactions/date=2023-12-31/", true, jump})
		}
	}
}

// blockchainPage is n sorted keys shaped like aws-public-blockchain's,
// from the i-th.
func blockchainPage(i, n int) []string {
	keys := make([]string, n)
	for j := range keys {
		k := i + j
		keys[j] = fmt.Sprintf("v1.0/btc/transactions/date=2023-%02d-%02d/part-%05d-%08x-%04x-c000.snappy.parquet",
			1+k/2800%12, 1+k/100%28, k%100, uint32(k)*2654435761, k%65536)
	}
	sortStrings(keys)
	return keys
}

// BenchmarkExtrapolate is one density step over a 1,000-key page shaped
// like aws-public-blockchain's: the call gallop makes after every
// continuing page of a range that may split.
func BenchmarkExtrapolate(b *testing.B) {
	page := blockchainPage(0, 1000)
	c := page[len(page)-1]
	for _, x := range []struct {
		name    string
		hi      string
		bounded bool
	}{
		{"frontier", "", false},
		{"bounded", "v1.0/btc/transactions/date=2023-12-31/", true},
	} {
		b.Run(x.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, why := extrapolateWhy(page, c, x.hi, x.bounded, 64); why != extOK {
					b.Fatal(why)
				}
			}
		})
	}
}
