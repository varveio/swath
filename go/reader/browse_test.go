// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

// browseSource disables speculation so scheduling cannot add optional reads.
// Demanded ranges can still be batched as concurrent ReadAt calls, so trace
// comparisons use multisets. It also keeps value indexes out of the footer tail.
func browseSource(t *testing.T, dir string) (*Dataset, *countingSource) {
	t.Helper()
	src := newCountingSource(t, dir)
	d, err := Open(t.Context(), src, Options{Read: seekparquet.ReadPolicy{DisableSpeculation: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, src
}

// TestPageCountsAddNoReads restores varve's identical-trace invariant,
// including final pages, Limit=1, shortcuts, cross-part and repeated-key
// prefixes, and an unchecked middle part with a false manifest row count.
func TestPageCountsAddNoReads(t *testing.T) {
	rows := keyRows([]string{"a/0", "a/0", "a/1", "a/2", "b/0", "b/1", "c/0", "c/1"})
	s := s3Spec()
	s.key.Unique = false
	dir := writeParts(t, s, 1, 2, rows[:3], rows[3:4], rows[4:])
	editManifest(t, dir, func(m *seekparquet.Manifest) { m.Parts[1].Rows = 100 })
	for _, limit := range []int{1, 2, 100} {
		for _, sums := range []SumMode{SumsOff, SumsWithinBudget, SumsAlways} {
			t.Run(fmt.Sprintf("limit=%d/sums=%d", limit, sums), func(t *testing.T) {
				d0, src0 := browseSource(t, dir)
				d1, src1 := browseSource(t, dir)
				req := PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: limit}, Aggregates: AggregateOptions{Sums: sums}}
				for {
					src0.reset()
					src1.reset()
					req.Aggregates.Counts = false
					p0, err := d0.Page(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					req.Aggregates.Counts = true
					p1, err := d1.Page(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					o0, r0 := src0.taken()
					o1, r1 := src1.taken()
					if !sameMultiset(o0, o1) || !sameMultiset(r0, r1) {
						t.Fatalf("counts changed trace: opens %v / %v, reads %v / %v", o0, o1, r0, r1)
					}
					if !reflect.DeepEqual(p0.Next, p1.Next) || len(p0.Entries) != len(p1.Entries) {
						t.Fatalf("different pages: %+v / %+v", p0, p1)
					}
					for i, e := range p1.Entries {
						if e.Aggregate == nil {
							continue
						}
						if string(e.CommonPrefix) == "a/" && (e.Aggregate.Count != nil || e.Aggregate.Bytes != nil) {
							t.Fatalf("unchecked middle part counted: %s", describeAggregate(*e.Aggregate))
						}
						if !reflect.DeepEqual(e.Aggregate.Bytes, p0.Entries[i].Aggregate.Bytes) {
							t.Fatal("counts changed sums")
						}
					}
					if p1.Next == nil {
						break
					}
					req.Cursor = p1.Next
				}
			})
		}
	}
}

// TestPageRandomOracle covers arbitrary byte prefixes (including empty
// and absent), repeated keys within parts, and prefixes spanning parts.
func TestPageRandomOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 29))
	keys := randomKeys(rng, 160)
	// Parts cannot split equal-key groups, but row groups and pages can.
	var repeated []string
	for _, k := range keys {
		for range 1 + rng.IntN(3) {
			repeated = append(repeated, k)
		}
	}
	s := s3Spec()
	s.key.Unique = false
	rows := keyRows(repeated)
	middle := len(rows) / 2
	for middle < len(rows) && rows[middle].Key == rows[middle-1].Key {
		middle++
	}
	dir := writeParts(t, s, 3, 13, rows[:middle], rows[middle:])
	objects, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	// Verify the row evidence without claiming that this non-unique-key
	// fixture satisfies swath's unique-key schema.
	if err := d.sp.Verify(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	prefixes := randomPrefixes(rng, keys)
	got, err := d.Aggregates(t.Context(), prefixes, AggregateOptions{Counts: true, Sums: SumsAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range prefixes {
		n, size := prefixTotals(objects, p)
		if a := got[i]; a.Count == nil || *a.Count != n || a.Bytes == nil || *a.Bytes != size {
			t.Fatalf("prefix %q: %s, want %d/%d", p, describeAggregate(a), n, size)
		}
	}
	for _, p := range prefixes[:min(35, len(prefixes))] {
		for _, limit := range []int{1, 3, 100} {
			listed, entries := pageAll(t, d, PageRequest{ListRequest: ListRequest{Prefix: p, Delimiter: '/', Limit: limit},
				Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}})
			if want := listOracle(repeated, string(p), '/'); !slices.Equal(listed, want) {
				t.Fatalf("prefix %q limit %d: %v, want %v", p, limit, listed, want)
			}
			for _, e := range entries {
				if e.CommonPrefix == nil {
					continue
				}
				n, size := prefixTotals(objects, e.CommonPrefix)
				if a := e.Aggregate; a == nil || a.Count == nil || *a.Count != n || a.Bytes == nil || *a.Bytes != size {
					t.Fatalf("prefix %q limit %d: %+v, want %d/%d", e.CommonPrefix, limit, e.Aggregate, n, size)
				}
			}
		}
	}
}

func TestPageInsidePrefixCursorLeavesTotalsUnknown(t *testing.T) {
	dir := listDir(t)
	for _, cursor := range []*Cursor{seekparquet.RowCursor([]byte("a/b"), 0), seekparquet.PrefixCursor([]byte("a/b/"))} {
		d, src := browseSource(t, dir)
		req := PageRequest{ListRequest: ListRequest{Cursor: cursor, Delimiter: '/', Limit: 1}}
		src.reset()
		_, err := d.Page(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		offOpens, offReads := src.taken()
		d, src = browseSource(t, dir)
		src.reset()
		req.Aggregates = AggregateOptions{Counts: true, Sums: SumsAlways}
		page, err := d.Page(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) != 1 || string(page.Entries[0].CommonPrefix) != "a/" {
			t.Fatalf("cursor %+v: %+v", cursor, page)
		}
		a := page.Entries[0].Aggregate
		if a.Count != nil || a.Bytes != nil {
			t.Fatalf("clipped prefix got totals: %+v", a)
		}
		opens, reads := src.taken()
		if !sameMultiset(opens, offOpens) || !sameMultiset(reads, offReads) {
			t.Fatalf("clipped prefix read more: opens %v / %v, reads %v / %v", opens, offOpens, reads, offReads)
		}
	}
}

func TestAggregateUnknownRowsReadNothing(t *testing.T) {
	d, src := browseSource(t, listDir(t))
	src.reset()
	got, err := d.aggregate(t.Context(), []target{{span: prefixSpan([]byte("a/"))}}, AggregateOptions{Counts: true, Sums: SumsAlways}, nil)
	if err != nil || len(got) != 1 || got[0].Count != nil || got[0].Bytes != nil {
		t.Fatalf("unknown rows: %v, %v", got, err)
	}
	if opens, reads := src.taken(); len(opens)+len(reads) != 0 {
		t.Fatalf("unknown rows opened %v, read %v", opens, reads)
	}
}

// TestBudgetSumsKeepsLaterCandidates is the regression for narrowing the
// candidate universe after the first accepted prefix.
func TestBudgetSumsKeepsLaterCandidates(t *testing.T) {
	d := openDir(t, writeDataset(t, s3Spec(), keyRows([]string{"a/0", "b/0", "c/0"})))
	got, err := d.Aggregates(t.Context(), [][]byte{[]byte("c/"), []byte("a/"), []byte("b/"), []byte("a/")},
		AggregateOptions{Counts: true, Sums: SumsWithinBudget})
	if err != nil {
		t.Fatal(err)
	}
	for i, size := range []int64{2, 0, 1, 0} {
		if a := got[i]; a.Count == nil || *a.Count != 1 || a.Bytes == nil || *a.Bytes != size {
			t.Fatalf("candidate %d: %s, want 1/%d", i, describeAggregate(a), size)
		}
	}
}

// TestBudgetSumsStopsMetadataDiscovery checks both an unaffordable
// multi-part candidate and a later candidate after a budget is consumed.
func TestBudgetSumsStopsMetadataDiscovery(t *testing.T) {
	rows := keyRows([]string{"a/0", "a/1", "a/2", "b/0"})
	dir := writeParts(t, s3Spec(), 1, 0, rows[:1], rows[1:2], rows[2:3], rows[3:])
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%t", partial), func(t *testing.T) {
			d, src := browseSource(t, dir)
			if err := d.CheckSchema(t.Context()); err != nil {
				t.Fatal(err)
			}
			// Price the first part only, retaining its index but no size
			// data. The next part's size index must remain unread.
			p, err := d.sp.Plan(t.Context(), []seekparquet.Selection{{Span: seekparquet.KeySpan([]byte("a/0"))}}, []string{"size"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			cost, exact := p.ProjectedBytes()
			if !exact || cost < minPageBytes {
				t.Fatalf("cost %d exact %t", cost, exact)
			}
			prefixes := [][]byte{[]byte("a/0"), []byte("b/")}
			if partial {
				prefixes = [][]byte{[]byte("a/"), []byte("b/")}
				cost += 2 * minPageBytes // floor fits, but the actual pages cannot
			}
			src.reset()
			got, err := d.Aggregates(t.Context(), prefixes, AggregateOptions{Counts: true, Sums: SumsWithinBudget, SumBudgetBytes: cost})
			if err != nil {
				t.Fatal(err)
			}
			if !partial && (got[0].Bytes == nil || *got[0].Bytes != 0) {
				t.Fatalf("first candidate: %s", describeAggregate(got[0]))
			}
			if partial {
				if got[0].Bytes != nil || got[1].Bytes == nil || *got[1].Bytes != 3 {
					t.Fatalf("rejected prefix must not charge the next candidate: %v", got)
				}
			} else if got[1].Bytes != nil {
				t.Fatalf("sum after exhausted budget: %s", describeAggregate(got[1]))
			}
			_, reads := src.taken()
			// A partial rejection must stop before a/'s final part; a fully
			// consumed budget must stop before the later b/ candidate.
			unread := 3
			if partial {
				unread = 2
			}
			part := d.meta.Manifest.Parts[unread].Path
			for _, r := range reads {
				if r.path == part {
					// CheckSchema and locate already kept the key page and
					// indexes; no further read in this part is necessary.
					t.Fatalf("opened later metadata after budget exhaustion: %+v", r)
				}
			}
		})
	}
}

func TestAggregateSizeErrorsNameOrdinal(t *testing.T) {
	for _, size := range []*int64{nil, ptr(int64(-1))} {
		rows := keyRows([]string{"a/0", "a/1"})
		rows[1].Size = size
		d := openDir(t, writeDataset(t, s3Spec(), rows))
		got, err := d.Aggregates(context.Background(), [][]byte{[]byte("a/")}, AggregateOptions{Sums: SumsAlways})
		se := semanticAt(t, err, KindRow, "size")
		if got != nil || se.Row != nil || !strings.Contains(err.Error(), "dataset row 1") {
			t.Fatalf("got %v, error %v; want owned ordinal error", got, err)
		}
	}
}

func TestPageSumsUseValuesOnly(t *testing.T) {
	d, src := browseSource(t, listDir(t))
	if err := d.CheckSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	var spans []seekparquet.RowSpan
	entries, err := d.list(t.Context(), ListRequest{Delimiter: '/', Limit: 100}, &spans)
	if err != nil {
		t.Fatal(err)
	}
	var targets []target
	for i, e := range entries {
		if e.CommonPrefix != nil {
			targets = append(targets, target{span: prefixSpan(e.CommonPrefix), rows: spans[i], known: true})
		}
	}
	src.reset()
	var st ReadStats
	got, err := d.aggregate(t.Context(), targets, AggregateOptions{Counts: true, Sums: SumsAlways}, &st)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range got {
		if a.Count == nil || a.Bytes == nil {
			t.Fatalf("missing totals: %s", describeAggregate(a))
		}
	}
	if st.KeyPagesDecoded != 0 || st.KeysDecoded != 0 || st.ValuePagesDecoded == 0 {
		t.Fatalf("value-only sums: %+v", st)
	}
	if opens, _ := src.taken(); len(opens) != 0 {
		t.Fatalf("sum reopened parts: %v", opens)
	}
}
