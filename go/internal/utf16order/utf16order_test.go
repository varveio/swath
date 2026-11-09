// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package utf16order

import (
	"cmp"
	"slices"
	"testing"
	"unicode/utf16"
)

// TestCompareIsUTF16: on scalars around every lead-byte boundary, alone
// and after a common prefix, Compare agrees with comparing UTF-16 code
// units, for strings and byte slices; Unrank inverts Rank.
func TestCompareIsUTF16(t *testing.T) {
	var scalars []rune
	for _, r := range []rune{0, 'a', 0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xe000, 0xe001, 0xff5a, 0xfffd, 0xffff,
		0x10000, 0x1f600, 0x40000, 0x80000, 0x100000, 0x10ffff} {
		scalars = append(scalars, r)
	}
	for _, x := range scalars {
		for _, y := range scalars {
			for _, p := range []string{"", "p", "", "\U00010000"} {
				a, b := p+string(x), p+string(y)+"z"
				want := slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
				if got := Compare(a, b); got != want || Compare([]byte(a), []byte(b)) != want || Compare(b, a) != -want {
					t.Errorf("Compare(%+q, %+q) = %d, UTF-16 says %d", a, b, got, want)
				}
			}
		}
	}
	for v := range 256 {
		if Unrank(Rank(byte(v))) != byte(v) || cmp.Compare(Rank(byte(v)), 0) < 0 {
			t.Errorf("Rank/Unrank at %#x", v)
		}
	}
}
