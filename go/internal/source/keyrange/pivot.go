// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"math/big"
	"sort"
	"unicode/utf8"
)

// Pivot synthesis, ported from Swath's ByteMidpoint (swath-model): a split
// point strictly between two keys, built only from "safe" scalars, because
// it is later sent as a start bound, and object stores reject control characters
// and non-characters there (a verbatim-echo endpoint also mishandles '%').

const (
	maxKeyLen = 1024
	minSafe   = 0x20
	safeHigh  = 0x8000
	noScalar  = -1
	below     = -1 // a bound below every scalar
)

// isSafe excludes C0, DEL, C1, the U+FDD0..FDEF non-characters, every
// xFFFE/xFFFF, '%' and surrogates.
func isSafe(cp int) bool {
	switch {
	case cp < minSafe, cp == 0x7F, cp >= 0x80 && cp <= 0x9F, cp >= 0xFDD0 && cp <= 0xFDEF,
		cp&0xFFFE == 0xFFFE, cp == 0x25:
		return false
	}
	return cp < 0xD800 || cp >= 0xE000
}

// Safe reports whether every scalar of s is in the pivot alphabet: valid
// UTF-8 without C0, DEL, C1, '%', surrogates or non-characters. Synthesized
// pivots are always Safe; it is also the default and S3's bound policy
// for real keys sent as bounds (Description.AcceptBound).
func Safe(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if !isSafe(int(r)) {
			return false
		}
	}
	return true
}

// safePrefix is the longest prefix of s made of safe scalars. It sorts at
// or before s, so it is a usable start-after for listing from s.
func safePrefix(s string) string {
	for i, r := range s {
		if r == utf8.RuneError || !isSafe(int(r)) {
			return s[:i]
		}
	}
	return s
}

// idx/cp map scalars to a dense index without surrogates and back.
func idx(cp int) int {
	if cp < 0xD800 {
		return cp
	}
	return cp - 0x800
}

func cpOf(j int) int {
	if j < 0xD800 {
		return j
	}
	return j + 0x800
}

// safeBetween returns a safe scalar strictly between lo (or below) and hi,
// starting at the midpoint and scanning outward, or noScalar.
func safeBetween(lo, hi int) int {
	jl, jh := -1, idx(hi)
	if lo != below {
		jl = idx(lo)
	}
	if jh-jl <= 1 {
		return noScalar
	}
	jm := jl + (jh-jl)/2
	jm = min(max(jm, jl+1), jh-1)
	for d := 0; ; d++ {
		down, up, any := jm-d, jm+d, false
		if down > jl && down < jh {
			any = true
			if c := cpOf(down); isSafe(c) {
				return c
			}
		}
		if up != down && up > jl && up < jh {
			any = true
			if c := cpOf(up); isSafe(c) {
				return c
			}
		}
		if !any {
			return noScalar
		}
	}
}

func safeAbove(cp int) int {
	for j := idx(cp) + 1; j <= idx(0x10FFFF); j++ {
		if c := cpOf(j); isSafe(c) {
			return c
		}
	}
	return noScalar
}

func scalars(s string) []int {
	out := make([]int, 0, len(s))
	for _, r := range s {
		out = append(out, int(r))
	}
	return out
}

func encode(cps []int) string {
	b := make([]byte, 0, len(cps)*2)
	for _, c := range cps {
		b = utf8.AppendRune(b, rune(c))
	}
	return string(b)
}

func commonPrefix(a, b []int) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func with(base []int, cp int) []int {
	return append(append(make([]int, 0, len(base)+1), base...), cp)
}

// between returns a safe key m with a < m < b (valid UTF-8, at most 1024
// bytes), or ok=false when only a control or non-character sliver lies
// between them. a must be < b and valid UTF-8; b need not be (validCeil).
func between(a, b string) (string, bool) {
	m, _, ok := betweenFrom(a, b)
	return m, ok
}

// betweenFrom is between with the synthesis that made m: srcMidpoint for a
// safe scalar between the keys' first difference, srcSliver for a + " " or
// capFallback (a pivot just above a).
func betweenFrom(a, b string) (string, pivotSource, bool) {
	if !utf8.ValidString(a) {
		return "", srcBound, false
	}
	ca := scalars(a)
	if !utf8.ValidString(b) {
		v, ok := validCeil(b)
		if !ok { // every valid key above a lies below b
			if m := encode(with(ca, safeHigh)); len(m) <= maxKeyLen {
				return m, srcMidpoint, true
			}
			m, ok := capFallbackAfter(ca)
			return m, srcSliver, ok
		}
		b = v
	}
	if a >= b {
		return "", srcBound, false
	}
	cb := scalars(b)
	i := commonPrefix(ca, cb)
	if i == len(ca) { // a is a scalar prefix of b
		if v := safeBetween(below, cb[i]); v != noScalar {
			if m := encode(with(ca, v)); len(m) <= maxKeyLen {
				return m, srcMidpoint, true
			}
			return encode(with(ca, minSafe)), srcSliver, true
		}
		if isSafe(cb[i]) && len(cb) > len(ca)+1 {
			return encode(with(ca, cb[i])), srcSliver, true // cb[i] is ' ': a + " "
		}
		return "", srcBound, false
	}
	if v := safeBetween(ca[i], cb[i]); v != noScalar {
		if m := encode(with(ca[:i], v)); len(m) <= maxKeyLen {
			return m, srcMidpoint, true
		}
	} else if m := encode(with(ca, minSafe)); len(m) <= maxKeyLen {
		return m, srcSliver, true
	}
	m, ok := capFallback(ca, b)
	return m, srcSliver, ok
}

// capFallback bumps a's right-most bumpable scalar and drops the tail, for
// pivots that would exceed 1024 bytes.
func capFallback(ca []int, b string) (string, bool) {
	for p := len(ca) - 1; p >= 0; p-- {
		next := safeAbove(ca[p])
		if next == noScalar {
			continue
		}
		cand := append(append([]int{}, ca[:p]...), next)
		if m := encode(cand); len(m) <= maxKeyLen && m < b {
			return m, true
		}
	}
	return "", false
}

func capFallbackAfter(cc []int) (string, bool) {
	for p := len(cc) - 1; p >= 0; p-- {
		next := safeAbove(cc[p])
		if next == noScalar {
			continue
		}
		if m := encode(append(append([]int{}, cc[:p]...), next)); len(m) <= maxKeyLen {
			return m, true
		}
	}
	return "", false
}

// forwardReflect picks m > c for the open frontier (no upper bound): it
// reflects the scalar where c leaves lo forward by the distance already
// covered, snapped up to a safe scalar. The frontier passes its chain's
// anchor as lo, so the covered distance doubles per split. From ⊥ (lo "")
// there is no distance at the first scalar, and the last scalar is
// reflected instead.
func forwardReflect(lo, c string) (string, bool) {
	if !utf8.ValidString(c) || !utf8.ValidString(lo) {
		return "", false
	}
	cc, cl := scalars(c), scalars(lo)
	if len(cc) == 0 {
		return "", false
	}
	i := commonPrefix(cl, cc)
	if i >= len(cc) || len(cl) == 0 {
		i = len(cc) - 1
	}
	jc, jl := idx(cc[i]), -1
	if i < len(cl) {
		jl = idx(cl[i])
	}
	jm := min(jc+(jc-jl), idx(0x10FFFF))
	if jm > jc {
		target := cpOf(jm)
		snapped := target
		if !isSafe(target) {
			snapped = safeAbove(target)
		}
		if snapped != noScalar {
			bumped := append(append([]int{}, cc[:i]...), snapped)
			if m := encode(bumped); len(m) <= maxKeyLen {
				return m, true
			}
			return capFallbackAfter(cc)
		}
	}
	if m := encode(with(cc, safeHigh)); len(m) <= maxKeyLen {
		return m, true
	}
	return capFallbackAfter(cc)
}

// prefixCeil is the least string greater than every string with prefix p:
// drop trailing 0xFF bytes and increment the last byte. ok=false means none
// exists (p empty or all 0xFF): the scope is open above.
func prefixCeil(p string) (string, bool) {
	b := []byte(p)
	for len(b) > 0 && b[len(b)-1] == 0xFF {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return "", false
	}
	b[len(b)-1]++
	return string(b), true
}

// validCeil is the least valid UTF-8 string at or above b; ok=false when
// none exists. For every valid s, s < b exactly when s < validCeil(b), so a
// bound that is not UTF-8 (prefixCeil("ÿ") is c3 c0) is replaced by it
// where scalars are synthesized ("Ā" there), and pivots below it are the
// pivots below b.
func validCeil(b string) (string, bool) {
	i := 0
	for i < len(b) {
		r, n := utf8.DecodeRuneInString(b[i:])
		if r == utf8.RuneError && n == 1 {
			break
		}
		i += n
	}
	if i == len(b) {
		return b, true
	}
	v, rest := b[:i], b[i:]
	// rest starts with no scalar's encoding, so the least valid string at
	// or above it is a single scalar: the least whose encoding sorts there.
	top := idx(0x10FFFF)
	if j := sort.Search(top+1, func(j int) bool { return string(rune(cpOf(j))) >= rest }); j <= top {
		return v + string(rune(cpOf(j))), true
	}
	// None: the least valid string above every string with prefix v.
	cs := scalars(v)
	for len(cs) > 0 {
		last := idx(cs[len(cs)-1])
		cs = cs[:len(cs)-1]
		if last < top {
			return encode(with(cs, cpOf(last+1))), true
		}
	}
	return "", false
}

// extrapolate picks a pivot from the observed key density. Keys are read as
// numbers in the alphabet seen on the last page (its distinct bytes, all
// safe ASCII, plus an end-of-string digit below them, which preserves
// order); the page spanned (first, c], and the target is c + span·jump,
// capped at the midpoint of (c, hi] for a bounded range. The target is cut
// just after its first character that differs from c. ok=false when the
// alphabet is not safe ASCII, or the result is not a safe key strictly
// inside (c, hi); the caller then uses between/forwardReflect.
func extrapolate(page []string, c, hi string, bounded bool, jump int64) (string, bool) {
	m, why := extrapolateWhy(page, c, hi, bounded, jump)
	return m, why == extOK
}

// extReason is why extrapolate refused (extOK: it did not).
type extReason uint8

const (
	extOK       extReason = iota
	extShort              // fewer than 2 keys on the page
	extAlphabet           // a byte on the page or the cursor is not safe ASCII
	extZeroSpan           // the page spans no distance in the alphabet's numbers
	extPastTop            // the target is past the top of the representable keys
	extHole               // the target has an end-of-string digit inside it: not a string
	extOutside            // the key is not a safe key strictly inside (c, hi)
)

// extrapolateWhy is extrapolate with the reason for a refusal.
func extrapolateWhy(page []string, c, hi string, bounded bool, jump int64) (string, extReason) {
	if len(page) < 2 {
		return "", extShort
	}
	var seen [128]bool
	for _, k := range append(page, c) {
		for i := 0; i < len(k); i++ {
			b := k[i]
			if b >= 0x80 || !isSafe(int(b)) {
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
		m = append(m, alphabet[digits[i]-1])
		if digits[i] != cd {
			break
		}
	}
	key := string(m)
	if key <= c || len(key) > maxKeyLen || (bounded && key >= hi) {
		return "", extOutside
	}
	return key, extOK
}

// The gallop: the density step's stride, in page spans, is
// max(gallopMin, hunger) << g, where g counts the consecutive upper
// children along the range's chain (Range.gen), so a remainder that keeps
// splitting off its upper side hands out twice as much per round trip, up
// to the midpoint of (c, hi], instead of peeling the same few pages each
// time. An unusable target is halved down to gallopMin within the
// decision.
const (
	gallopMin    = 4  // the least stride, in page spans
	gallopMaxGen = 16 // the generation's clamp
)

// gallop is the density step with the stride max(gallopMin, hunger) << g,
// halved down to gallopMin while the target is unusable (past the
// representable keys, a hole, or outside (c, hi)); halvings counts them.
func gallop(page []string, c, hi string, bounded bool, hunger int, g uint8) (m string, why extReason, halvings int) {
	jump := int64(max(gallopMin, hunger)) << min(g, gallopMaxGen)
	for {
		m, why = extrapolateWhy(page, c, hi, bounded, jump)
		if why == extOK || jump <= gallopMin || (why != extPastTop && why != extHole && why != extOutside) {
			return m, why, halvings
		}
		jump = max(gallopMin, jump/2)
		halvings++
	}
}
