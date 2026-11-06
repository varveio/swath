// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import "testing"

// TestPartOrdinal: names are parsed to ordinals, so part-99999 precedes
// part-100000 although its name sorts after (review R2, 2026-11-06).
func TestPartOrdinal(t *testing.T) {
	for _, i := range []int{0, 1, 99_999, 100_000, 1_234_567} {
		if got, ok := PartOrdinal(PartName(i)); !ok || got != i {
			t.Errorf("PartOrdinal(%q) = %d, %v", PartName(i), got, ok)
		}
	}
	a, _ := PartOrdinal(PartName(99_999))
	b, _ := PartOrdinal(PartName(100_000))
	if !(a < b) || PartName(99_999) < PartName(100_000) {
		t.Fatalf("ordinals %d, %d; names %q < %q: %v", a, b, PartName(99_999), PartName(100_000), PartName(99_999) < PartName(100_000))
	}
	for _, bad := range []string{"part-1.parquet", "part-000001.parquet", "part-+0001.parquet", "part--0001.parquet",
		"part-0000a.parquet", "part-.parquet", "part-00001.parquet.claimed", "part-00001.parq", "keep-me.txt",
		"part-099999.parquet", "part-99999999999999999999999.parquet"} {
		if i, ok := PartOrdinal(bad); ok {
			t.Errorf("PartOrdinal(%q) = %d, accepted", bad, i)
		}
	}
}
