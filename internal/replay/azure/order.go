// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"bytes"

	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/utf16order"
)

// Azure Blob lists names in UTF-16 code-unit order. For valid UTF-8 that is
// unsigned byte order with one exception at the first differing byte: lead
// bytes F0..F4 (U+10000 and above, surrogate pairs in UTF-16) sort before
// lead bytes EE..EF (U+E000 to U+FFFF). Continuation bytes never meet a
// lead byte at the first difference of two valid names, so the order is a
// fixed byte permutation applied position by position:
// 00..ED, F0..F4, EE..EF, F5..FF. Names that are not valid UTF-8 are placed
// by the same permutation (the route refuses to render them, so their order
// only has to be total and stable).
//
// A prefix window is contiguous in both orders, and the two orders disagree
// only between an EE/EF branch and an F0..F4 branch under the same parent,
// so provider order over a byte range is the byte order of the range with
// each such pair of sibling branches swapped, recursively; cursor does that
// as it reads the byte-sorted store.

// Compare is the provider-order comparison of two names
// (utf16order.Compare).
func Compare(a, b []byte) int { return utf16order.Compare(a, b) }

// upperOf is the least name after every name that starts with prefix in
// provider order (the resume point past a skipped BlobPrefix subtree), or
// nil when there is none.
func upperOf(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if r := utf16order.Rank(prefix[i]); r != 0xff {
			upper := bytes.Clone(prefix[:i+1])
			upper[i] = utf16order.Unrank(r + 1)
			return upper
		}
	}
	return nil
}

// firstSwap is the first position of name holding an EE/EF byte whose
// F0..F4 siblings fall below upper (nil: open), or -1. Names in that
// sibling branch rank before name although they follow it in byte order;
// no other name read after name below upper can.
func firstSwap(name, upper []byte) int {
	for at, v := range name {
		if (v == 0xee || v == 0xef) && (upper == nil || bytes.Compare(child(name, at, 0xf0), upper) < 0) {
			return at
		}
	}
	return -1
}

// child is name[:at] followed by v.
func child(name []byte, at int, v byte) []byte {
	c := make([]byte, at+1)
	copy(c, name[:at])
	c[at] = v
	return c
}

// span is one byte range [from, upper) of the byte-sorted store; a nil
// upper is open.
type span struct {
	from      []byte
	inclusive bool
	upper     []byte
}

// rangesFrom returns the byte ranges, in provider order, holding every name
// of the prefix window at or after lower (after it when inclusive is false)
// in provider order; lower starts with prefix. Each range is a set of whole
// subtrees whose provider order cursor still reads; consecutive ranges
// never interleave.
//
// The names after L are, from the deepest position outward, the subtree
// below L and then, at each position i, the siblings of L[i] under L[:i]
// that rank after it. Those siblings are one byte range unless L[i] is a
// lead byte EE..F4: after an F0..F4 byte come (L[i], F4], then EE..EF, then
// F5..FF; after an EE/EF byte come (L[i], EF] and F5..FF, leaving out the
// F0 branch that ranks before it. A name without such lead bytes yields one
// range and each lead byte adds at most two.
func rangesFrom(lower []byte, inclusive bool, prefix []byte) []span {
	var out []span
	from, fromInclusive := lower, inclusive
	for i := len(lower) - 1; i >= len(prefix); i-- {
		v := lower[i]
		if v < 0xee || v > 0xf4 {
			continue
		}
		supplementary := v >= 0xf0
		end := byte(0xf0)
		if supplementary {
			end = 0xf5
		}
		out = append(out, span{from, fromInclusive, child(lower, i, end)})
		if supplementary {
			out = append(out, span{child(lower, i, 0xee), true, child(lower, i, 0xf0)})
		}
		from, fromInclusive = child(lower, i, 0xf5), true
	}
	upper, _ := s3.PrefixUpper(prefix)
	return append(out, span{from, fromInclusive, upper})
}
