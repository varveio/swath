// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"strings"
	"testing"
)

// TestCount: up to 999 as it is, else three significant digits.
func TestCount(t *testing.T) {
	for _, c := range []struct {
		n    int64
		want string
	}{{0, "0"}, {750, "750"}, {999, "999"}, {1000, "1.00k"}, {11_050, "11.1k"}, {12_000, "12.0k"}, {999_499, "999k"}, {999_500, "1.00M"}, {37_100_000, "37.1M"}} {
		if got := Count(c.n); got != c.want {
			t.Errorf("count %d: %q, want %q", c.n, got, c.want)
		}
	}
}

// TestGrouped: a count's digits in groups of three.
func TestGrouped(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 60010: "60,010", 1234567: "1,234,567", -1000: "-1,000"} {
		if got := Grouped(n); got != want {
			t.Errorf("%d: %q", n, got)
		}
	}
}

// TestClipCells: a name is cut in the middle, its end kept, to terminal
// columns, not runes: an East Asian wide character takes two, a combining
// mark none.
func TestClipCells(t *testing.T) {
	if got := Clip("abcdefghij.txt", 9); got != "abcd….txt" {
		t.Errorf("clip: %q", got)
	}
	wide := strings.Repeat("界", 30) + ".bin"
	for _, room := range []int{10, 21, 40} {
		if got := Clip(wide, room); Cells(got) > room || !strings.HasSuffix(got, ".bin") || !strings.Contains(got, "…") {
			t.Errorf("clip %d: %q, %d columns", room, got, Cells(got))
		}
	}
	if n := Cells("e\u0301界a"); n != 4 {
		t.Errorf("cells: %d", n)
	}
}
