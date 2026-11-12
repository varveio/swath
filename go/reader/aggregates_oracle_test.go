// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

// checkAggregate fails unless every total of a is nil or exactly the full
// scan's, and reports whether its bytes were summed.
func checkAggregate(t *testing.T, label string, objects []Object, prefix []byte, a *PrefixAggregate) (summed bool) {
	t.Helper()
	if a == nil {
		t.Fatalf("%s %q: nil aggregate", label, prefix)
	}
	n, size := prefixTotals(objects, prefix)
	if a.Count != nil && *a.Count != n {
		t.Fatalf("%s %q: count %d, want %d", label, prefix, *a.Count, n)
	}
	if a.Bytes != nil && *a.Bytes != size {
		t.Fatalf("%s %q: bytes %d, want %d", label, prefix, *a.Bytes, size)
	}
	if a.Bytes != nil && a.Count == nil {
		t.Fatalf("%s %q: bytes without a count", label, prefix)
	}
	return a.Bytes != nil
}

// TestAggregatesAgainstFullScan compares Aggregates and Page, under every
// budget, with totals computed from a full scan: random keys repeated
// across three parts of small pages and row groups, large sizes, budgets
// from 1 to 2^40, and made-up row cursors.
func TestAggregatesAgainstFullScan(t *testing.T) {
	var checked, summed int
	for seed := uint64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewPCG(seed, 99))
		keys := randomKeys(rng, 300)
		var repeated []string
		for _, k := range keys {
			for range 1 + rng.IntN(3) {
				repeated = append(repeated, k)
			}
		}
		s := s3Spec()
		s.key.Unique = false
		rows := keyRows(repeated)
		for i := range rows {
			rows[i].Size = ptr(int64(rng.IntN(1 << 30)))
		}
		// Three parts, cut where the key changes.
		cuts := []int{0}
		for _, c := range []int{len(rows) / 4, len(rows) * 3 / 4} {
			for c < len(rows) && rows[c].Key == rows[c-1].Key {
				c++
			}
			cuts = append(cuts, c)
		}
		cuts = append(cuts, len(rows))
		dir := writeParts(t, s, 3, 11, cut(rows, cuts...)...)
		objects, err := stream(t, openDir(t, dir))
		if err != nil {
			t.Fatal(err)
		}
		prefixes := randomPrefixes(rng, keys)
		for _, budget := range []int64{1, 16, 100, 300, 1000, 3000, 1 << 40} {
			for _, verified := range []bool{false, true} {
				d := openDir(t, dir)
				if verified {
					if err := d.sp.Verify(context.Background(), nil); err != nil {
						t.Fatal(err)
					}
				}
				opts := AggregateOptions{Counts: true, Sums: SumsWithinBudget, SumBudgetBytes: budget}
				got, err := d.Aggregates(context.Background(), prefixes, opts)
				if err != nil {
					t.Fatal(err)
				}
				label := fmt.Sprintf("seed %d aggregates budget %d verified %t", seed, budget, verified)
				for i, p := range prefixes {
					checked++
					if checkAggregate(t, label, objects, p, &got[i]) {
						summed++
					}
					if verified && budget == 1<<40 && (got[i].Count == nil || got[i].Bytes == nil) {
						t.Fatalf("%s %q: unknown %s", label, p, describeAggregate(got[i]))
					}
				}
				for _, p := range prefixes[:20] {
					for _, limit := range []int{1, 2, 7, 1000} {
						req := PageRequest{ListRequest: ListRequest{Prefix: p, Delimiter: '/', Limit: limit}, Aggregates: opts}
						if rng.IntN(2) == 0 {
							req.Cursor = seekparquet.RowCursor([]byte(repeated[rng.IntN(len(repeated))]), int64(rng.IntN(2)))
						}
						for {
							page, err := d.Page(context.Background(), req)
							if err != nil {
								t.Fatal(err)
							}
							for _, e := range page.Entries {
								if e.CommonPrefix != nil {
									checked++
									if checkAggregate(t, fmt.Sprintf("seed %d page budget %d limit %d cursor %v", seed, budget, limit, req.Cursor), objects, e.CommonPrefix, e.Aggregate) {
										summed++
									}
								}
							}
							if page.Next == nil {
								break
							}
							req.Cursor = page.Next
						}
					}
				}
			}
		}
	}
	if summed == 0 || summed == checked {
		t.Fatalf("%d of %d totals summed: the oracle is vacuous", summed, checked)
	}
}

// TestSumOverflowNestedStaysExact: a sum that overflows an int64 is nil for
// every prefix containing it, while the siblings stay exact, in both modes.
func TestSumOverflowNestedStaysExact(t *testing.T) {
	rows := keyRows([]string{"a/0", "a/b/1", "a/b/2", "c/3"})
	rows[0].Size = ptr(int64(5))
	rows[1].Size = ptr(int64(1 << 62))
	rows[2].Size = ptr(int64(1<<62 - 1 + 1<<62))
	rows[3].Size = ptr(int64(7))
	d := openDir(t, writeParts(t, s3Spec(), 1, 1, rows[:2], rows[2:]))
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []SumMode{SumsAlways, SumsWithinBudget} {
		opts := AggregateOptions{Counts: true, Sums: mode}
		if mode == SumsWithinBudget {
			opts.SumBudgetBytes = 1 << 30
		}
		got, err := d.Aggregates(context.Background(), [][]byte{[]byte("a/"), []byte("a/b/"), []byte("c/"), []byte("a/b/2")}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Bytes != nil || got[1].Bytes != nil {
			t.Errorf("mode %d: overflowing prefixes summed: %s, %s", mode, describeAggregate(got[0]), describeAggregate(got[1]))
		}
		if got[2].Bytes == nil || *got[2].Bytes != 7 || got[3].Bytes == nil || *got[3].Bytes != 1<<62-1+1<<62 {
			t.Errorf("mode %d: siblings not exact: %s, %s", mode, describeAggregate(got[2]), describeAggregate(got[3]))
		}
	}
}

// TestBudgetedSumsWithBoundedOffsetIndexMemory: pricing must not depend on
// how many page locations the Dataset kept (Limits.OffsetIndexMemory), so a
// long-lived Dataset whose cache has filled still sums within the budget.
func TestBudgetedSumsWithBoundedOffsetIndexMemory(t *testing.T) {
	rows := keyRows([]string{"a/0", "a/1", "b/0", "b/1", "c/0"})
	dir := writeParts(t, s3Spec(), 1, 2, rows[:3], rows[3:])
	d, err := OpenDir(context.Background(), dir, seekparquet.OpenOptions{Limits: seekparquet.Limits{OffsetIndexMemory: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	prefixes := [][]byte{[]byte("a/"), []byte("b/"), []byte("c/")}
	always, err := d.Aggregates(context.Background(), prefixes, AggregateOptions{Counts: true, Sums: SumsAlways})
	if err != nil {
		t.Fatal(err)
	}
	budgeted, err := d.Aggregates(context.Background(), prefixes, AggregateOptions{Counts: true, Sums: SumsWithinBudget})
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range prefixes {
		if always[i].Bytes == nil {
			t.Fatalf("%q: SumsAlways gave no sum: %s", p, describeAggregate(always[i]))
		}
		if budgeted[i].Bytes == nil || *budgeted[i].Bytes != *always[i].Bytes {
			t.Errorf("%q: SumsWithinBudget %s, SumsAlways %s", p, describeAggregate(budgeted[i]), describeAggregate(always[i]))
		}
	}
}
