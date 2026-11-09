// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package utf16order is Azure Blob's listing order, UTF-16 code units,
// computed on UTF-8 bytes. For valid UTF-8 it is unsigned byte order
// except at the first differing byte, where lead bytes F0..F4 (U+10000 and
// above, surrogate pairs in UTF-16) rank before EE..EF (U+E000..U+FFFF):
// at a first difference both bytes are lead bytes or both continuation
// bytes, so ranking the byte values 00..ED, F0..F4, EE..EF, F5..FF
// position by position compares the code units. Names that are not valid
// UTF-8 are placed by the same permutation, a total and stable order.
//
// The Azure client's page check, the provider's byte-order adapter and the
// replay's Azure route share it; azblobtest.CompareUTF16, written from the
// definition, is the independent oracle the tests hold it to.
package utf16order

// Rank is a byte's position in the order.
func Rank(v byte) int {
	switch {
	case v >= 0xf0 && v <= 0xf4:
		return int(v) - 2
	case v == 0xee || v == 0xef:
		return int(v) + 5
	}
	return int(v)
}

// Unrank is the byte at position r, Rank's inverse.
func Unrank(r int) byte {
	switch {
	case r >= 0xee && r <= 0xf2:
		return byte(r + 2)
	case r == 0xf3 || r == 0xf4:
		return byte(r - 5)
	}
	return byte(r)
}

// Compare orders two names: -1, 0 or 1.
func Compare[T ~string | ~[]byte](a, b T) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			if Rank(a[i]) < Rank(b[i]) {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}
