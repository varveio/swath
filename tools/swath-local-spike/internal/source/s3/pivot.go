// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"math/big"
	"unicode/utf8"
)

// Pivot synthesis, ported from Swath's ByteMidpoint (swath-model): a split
// point strictly between two keys, built only from "safe" scalars, because
// it is later sent as start-after and real S3 rejects control characters
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

// safeString reports whether every scalar of s is safe (a usable start-after).
func safeString(s string) bool {
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
// between them. a must be < b and both valid UTF-8.
func between(a, b string) (string, bool) {
	if a >= b || !utf8.ValidString(a) || !utf8.ValidString(b) {
		return "", false
	}
	ca, cb := scalars(a), scalars(b)
	i := commonPrefix(ca, cb)
	if i == len(ca) { // a is a scalar prefix of b
		if v := safeBetween(below, cb[i]); v != noScalar {
			if m := encode(with(ca, v)); len(m) <= maxKeyLen {
				return m, true
			}
			return encode(with(ca, minSafe)), true
		}
		if isSafe(cb[i]) && len(cb) > len(ca)+1 {
			return encode(with(ca, cb[i])), true
		}
		return "", false
	}
	if v := safeBetween(ca[i], cb[i]); v != noScalar {
		if m := encode(with(ca[:i], v)); len(m) <= maxKeyLen {
			return m, true
		}
	} else if m := encode(with(ca, minSafe)); len(m) <= maxKeyLen {
		return m, true
	}
	return capFallback(ca, b)
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
// covered, snapped up to a safe scalar.
func forwardReflect(lo, c string) (string, bool) {
	if !utf8.ValidString(c) || !utf8.ValidString(lo) {
		return "", false
	}
	cc, cl := scalars(c), scalars(lo)
	if len(cc) == 0 {
		return "", false
	}
	i := commonPrefix(cl, cc)
	if i >= len(cc) || i == 0 {
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

// extrapolate picks a pivot from the observed key density. Keys are read as
// numbers in the alphabet seen on the last page (its distinct bytes, all
// safe ASCII, plus an end-of-string digit below them, which preserves
// order); the page spanned (first, c], and the target is c + span·jump,
// capped at the midpoint of (c, hi] for a bounded range. The target is cut
// just after its first character that differs from c. ok=false when the
// alphabet is not safe ASCII, or the result is not a safe key strictly
// inside (c, hi); the caller then uses between/forwardReflect.
func extrapolate(page []string, c, hi string, bounded bool, jump int64) (string, bool) {
	if len(page) < 2 {
		return "", false
	}
	var seen [128]bool
	for _, k := range append(page, c) {
		for i := 0; i < len(k); i++ {
			b := k[i]
			if b >= 0x80 || !isSafe(int(b)) {
				return "", false
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
		return "", false
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
		return "", false // past the top of the representable keys
	}
	var m []byte
	for i := 0; i < n; i++ {
		var cd int64
		if i < len(c) {
			cd = rank[c[i]]
		}
		if digits[i] == 0 {
			return "", false // a hole: not a string
		}
		m = append(m, alphabet[digits[i]-1])
		if digits[i] != cd {
			break
		}
	}
	key := string(m)
	if key <= c || len(key) > maxKeyLen || (bounded && key >= hi) {
		return "", false
	}
	return key, true
}
