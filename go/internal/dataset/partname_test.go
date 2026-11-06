// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import "testing"

// TestPartOrdinal: a five-digit name parses to its ordinal, and every
// other name is refused, six-digit ones included: a dataset has at most
// MaxParts parts (review R2 and the owner's part limit, 2026-11-06).
func TestPartOrdinal(t *testing.T) {
	for _, i := range []int{0, 1, 99_998, MaxParts - 1} {
		if got, ok := PartOrdinal(PartName(i)); !ok || got != i {
			t.Errorf("PartOrdinal(%q) = %d, %v", PartName(i), got, ok)
		}
	}
	for _, bad := range []string{"part-1.parquet", "part-000001.parquet", "part-+0001.parquet", "part--0001.parquet",
		"part-0000a.parquet", "part-.parquet", "part-00001.parquet.claimed", "part-00001.parq", "keep-me.txt",
		"part-099999.parquet", "part-100000.parquet", "part-1234567.parquet", "part-99999999999999999999999.parquet"} {
		if i, ok := PartOrdinal(bad); ok {
			t.Errorf("PartOrdinal(%q) = %d, accepted", bad, i)
		}
	}
}
