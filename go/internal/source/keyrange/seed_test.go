// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// seedShapes are keyspaces of the shapes a seed meets: flat, dense, narrow
// and deep trees, partitions, a wide top level.
func seedShapes() map[string][]string {
	gen := func(n int, f func(i int) string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	cat := func(parts ...[]string) []string { return slices.Concat(parts...) }
	return map[string][]string{
		"flat small":      gen(300, func(i int) string { return fmt.Sprintf("k%04d", i) }),
		"dense flat root": gen(2500, func(i int) string { return fmt.Sprintf("%08x", i*2654435761%4294967291) }),
		"narrow tree": gen(6000, func(i int) string {
			return fmt.Sprintf("r%d/s%02d/t%d/obj%04d", i%5, i/5%20, i/100%3, i)
		}),
		// A truncated folder of key=value partitions.
		"partitions": cat(gen(3000, func(i int) string { return fmt.Sprintf("tbl/dt=2024-%04d/part-%d", i/2, i%2) }),
			[]string{"readme"}),
		// A truncated folder of 1:1 subfolders (one object each) next to a
		// heavy one (subfolders with many objects).
		"explosion and heavy": cat(gen(1500, func(i int) string { return fmt.Sprintf("leaf/%05d/x", i) }),
			gen(12000, func(i int) string { return fmt.Sprintf("heavy/h%04d/f%02d", i/10, i%10) })),
		// Deep shared prefixes, as in the census recording.
		"deep census": gen(9000, func(i int) string {
			return fmt.Sprintf("tiff/1950census/43290879-State%02d/43290879-State%02d-%03d/%04d.tif", i%7, i%7, i/7%40, i)
		}),
		// More top-level folders than one page.
		"wide top": gen(2600, func(i int) string { return fmt.Sprintf("d%05d/x", i) }),
	}
}

// TestSeedListsEveryKey: with the seed, every shape and the
// fixture keyspaces list row-identical to the keyspace, and the covered
// intervals tile (⊥, ⊤], including under a prefix.
func TestSeedListsEveryKey(t *testing.T) {
	spaces := seedShapes()
	for k, v := range keyspaces() {
		spaces["fixture "+k] = v
	}
	for name, keys := range spaces {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		got, _, leaves := run(t, newSorted(keys, true, 3), Options{Seed: true, Floors: true}, 16)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(want))
		}
		checkTiling(t, name, leaves)
	}
	keys := seedShapes()["deep census"]
	var want []string
	for _, k := range keys {
		if strings.HasPrefix(k, "tiff/1950census/43290879-State03") {
			want = append(want, k)
		}
	}
	slices.Sort(want)
	got, _, _ := run(t, newSorted(keys, false, 1), Options{Seed: true, Prefix: "tiff/1950census/43290879-State03"}, 8)
	if !slices.Equal(got, want) {
		t.Fatalf("prefix: %d keys, want %d", len(got), len(want))
	}
}
