// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

type fixture struct {
	name string
	path string
	kind Kind
}

var (
	fixturesOnce sync.Once
	fixturesDir  string
	fixturesList []fixture
	fixturesErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if fixturesDir != "" {
		_ = os.RemoveAll(fixturesDir)
	}
	os.Exit(code)
}

// fixtures builds the conformance fixtures once: a 10,000-key Java-era
// fixture of three parts, the same keys in a format-2 dataset of four parts
// (an earlier build's format), the same keys sorted into a SeekParquet
// dataset by sort-fixture, a SeekParquet dataset of keys longer than the
// 1,024-byte value index bound, and the Go lister's own S3 and local
// datasets (golister_test.go).
func fixtures(t *testing.T) []fixture {
	t.Helper()
	fixturesOnce.Do(func() {
		dir, err := os.MkdirTemp("", "replay-store-")
		if err != nil {
			fixturesErr = err
			return
		}
		fixturesDir = dir
		keys := fixtureKeys(10_000)
		long := make([]string, 0, 600)
		for i := range 600 {
			long = append(long, fmt.Sprintf("%s/%s%04d", strings.Repeat("p", 1100), strings.Repeat("q", i%3*400), i))
		}
		slices.Sort(long)
		fixturesList = []fixture{
			{"java-era-10k", writeJavaEra(t, filepath.Join(dir, "java"), keys, 3, 1500), KindJavaEra},
			{"format2-10k", writeLegacyFormat2(t, filepath.Join(dir, "legacy"), keys, 4), KindFormat2},
			{"seekparquet-10k", writeSorted(t, filepath.Join(dir, "f2"), keys, 0), KindSeekParquet},
			{"seekparquet-long-keys", writeSorted(t, filepath.Join(dir, "long"), long, 4096), KindSeekParquet},
			{"go-lister-s3", listS3(t, dir), KindSeekParquet},
			{"go-lister-local", listLocal(t, dir), KindSeekParquet},
		}
	})
	if fixturesErr != nil {
		t.Fatal(fixturesErr)
	}
	return fixturesList
}

func openFixture(t *testing.T, f fixture, readers int) (*Sorted, Metadata) {
	t.Helper()
	s, meta, err := Open(context.Background(), f.path, Options{Readers: readers})
	if err != nil {
		t.Fatalf("%s: %v", f.name, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if meta.Kind != f.kind || meta.Ordering != OrderingProfile || len(meta.Identity) != 64 {
		t.Fatalf("%s: metadata %+v", f.name, meta)
	}
	return s.(*Sorted), meta
}

// TestRowsPagesToCompletion pages every fixture through Rows to the end at
// several limits, resuming after the last key exclusively, and requires
// exactly the reference keys in order: no gap, no duplicate. Under -short
// or the race detector, limits 1 and 2 page a window of 1,500 keys around
// the middle (a part boundary in the multi-part fixtures) to its upper bound
// instead of the whole fixture.
func TestRowsPagesToCompletion(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			s, meta := openFixture(t, f, 0)
			rows, _ := reference(t, f.path)
			if meta.Rows != int64(len(rows)) {
				t.Fatalf("metadata rows %d, reference %d", meta.Rows, len(rows))
			}
			for _, limit := range []int{1, 2, 999, 1000, 1001, 5000} {
				var from, to *string
				if limit <= 2 && (raceEnabled || testing.Short()) && len(rows) > 1500 {
					lo, hi := rows[len(rows)/2-750].key, rows[len(rows)/2+750].key
					from, to = &lo, &hi
				}
				want := expectKeys(rows, from, true, to, len(rows))
				var got []string
				var cursor, upper []byte
				inclusive := false
				if from != nil {
					cursor, upper, inclusive = []byte(*from), []byte(*to), true
				}
				for {
					page, err := s.Rows(context.Background(), cursor, inclusive, upper, limit, Projection{Owner: true})
					if err != nil {
						t.Fatal(err)
					}
					if len(page) > limit {
						t.Fatalf("limit %d: %d rows", limit, len(page))
					}
					if len(page) == 0 {
						break
					}
					got = append(got, keysOf(page)...)
					cursor, inclusive = page[len(page)-1].Key, false
				}
				if !slices.Equal(got, want) {
					t.Fatalf("limit %d: %d keys, want %d; first difference at %d", limit, len(got), len(want), firstDiff(got, want))
				}
			}
		})
	}
}

// TestRowsAtBoundaries reads from and up to every row-group and page
// boundary (at, just before and just after it, inclusive and exclusive)
// and compares keys and values with the sequential reference.
func TestRowsAtBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			s, _ := openFixture(t, f, 0)
			rows, boundaries := reference(t, f.path)
			byKey := map[string]refRow{}
			for _, r := range rows {
				byKey[r.key] = r
			}
			bounds := probes(rows, boundaries)
			check := func(from *string, inclusive bool, to *string, limit int) {
				t.Helper()
				var lo, hi []byte
				if from != nil {
					lo = []byte(*from)
				}
				if to != nil {
					hi = []byte(*to)
				}
				got, err := s.Rows(ctx, lo, inclusive, hi, limit, Projection{Owner: true})
				if err != nil {
					t.Fatal(err)
				}
				want := expectKeys(rows, from, inclusive, to, limit)
				if !slices.Equal(keysOf(got), want) {
					t.Fatalf("from %q (inclusive %v) to %q limit %d: got %d keys, want %d, first difference at %d",
						short(from), inclusive, short(to), limit, len(got), len(want), firstDiff(keysOf(got), want))
				}
				for _, o := range got {
					r := byKey[string(o.Key)]
					if !eqInt(o.Size, r.size) || !eqStr(o.ETag, r.etag) || !eqStr(o.OwnerID, r.owner) || !eqStr(o.ChecksumAlgorithm, r.sumAlg) {
						t.Fatalf("%q: values differ from the reference", short(&r.key))
					}
				}
			}
			for i := range bounds {
				b := bounds[i]
				for _, inclusive := range []bool{true, false} {
					check(&b, inclusive, nil, 2)
					if i%4 == 0 {
						check(&b, inclusive, nil, 1001)
					}
				}
				check(nil, true, &b, 1)
				if i%8 == 0 {
					check(nil, true, &b, 5000)
				}
			}
			// Both bounds at boundaries: every pair of nearby probes.
			for i := range bounds {
				for j := i; j < min(i+6, len(bounds)); j++ {
					check(&bounds[i], true, &bounds[j], 1001)
					check(&bounds[i], false, &bounds[j], 2)
				}
			}
		})
	}
}

// TestDelimitedRollupMatchesRangeWalk requires the skip-scan to return
// entries identical to a walk over Rows that rolls keys up one at a time,
// for both floor rules, at prefixes, resume points and limits around the
// fixture's boundaries.
func TestDelimitedRollupMatchesRangeWalk(t *testing.T) {
	ctx := context.Background()
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			s, _ := openFixture(t, f, 0)
			rows, boundaries := reference(t, f.path)
			prefixes := []string{"", "d01/", "d01/s01/", "long/", "é/", "zzz/", "a/b/", strings.Repeat("p", 1100) + "/"}
			bounds := probes(rows, boundaries)
			var engaged, sampled int64
			for _, prefix := range prefixes {
				upper, ok := legacy.Successor([]byte(prefix))
				if !ok {
					upper = nil
				}
				froms := [][]byte{nil}
				if prefix != "" {
					froms = [][]byte{[]byte(prefix)}
				}
				for _, b := range bounds {
					if strings.HasPrefix(b, prefix) && b > prefix {
						froms = append(froms, []byte(b))
					}
				}
				if len(froms) > 60 {
					// Every resume point near a boundary would be slow on a
					// large prefix; keep an even spread.
					step := len(froms) / 60
					var kept [][]byte
					for i := 0; i < len(froms); i += step {
						kept = append(kept, froms[i])
					}
					froms = kept
				}
				for _, from := range froms {
					for _, inclusive := range []bool{true, false} {
						for _, suppress := range []bool{true, false} {
							for _, limit := range []int{1, 3, 1000} {
								got, ok, err := s.DelimitedRollup(ctx, from, inclusive, upper, []byte(prefix), []byte("/"), limit, Projection{}, suppress)
								if err != nil || !ok {
									t.Fatalf("rollup: ok %v, %v", ok, err)
								}
								engaged++
								want := rangeWalk(referenceWalk(rows, upper), from, inclusive, []byte(prefix), limit, suppress)
								if limit == 3 && sampled%7 == 0 {
									// The reference walk stands for the walk over Rows.
									if d := diffEntries(want, rangeWalk(storeWalk(t, s, upper), from, inclusive, []byte(prefix), limit, suppress)); d != "" {
										t.Fatalf("reference walk differs from the Rows walk: %s", d)
									}
								}
								sampled++
								if d := diffEntries(got, want); d != "" {
									t.Fatalf("prefix %q from %q (inclusive %v, suppress %v) limit %d: %s", short(&prefix), from, inclusive, suppress, limit, d)
								}
							}
						}
					}
				}
			}
			if _, ok, _ := s.DelimitedRollup(ctx, nil, true, nil, nil, []byte("//"), 10, Projection{}, true); ok {
				t.Fatal("a delimiter other than / must be declined")
			}
			if got := s.Counters().DelimiterEngagements.Load(); got != engaged {
				t.Fatalf("delimiter engagements %d, want %d", got, engaged)
			}
		})
	}
}

// rangeWalk is the pager's range-based rollup: read one key at a time
// from the cursor; a key with "/" after the prefix contributes its common
// prefix (left out at or before the floor when suppress) and the cursor
// jumps to the prefix's successor; any other key is an object and the
// cursor moves past it. It stops after limit+1 entries. next returns the
// first row at or after (after, when !inclusive) a cursor below to; the
// test walks the in-memory reference with it, and checks on a sample that
// a walk over Rows gives the same entries.
func rangeWalk(next func(cursor []byte, inclusive bool) (Object, bool), from []byte, inclusive bool, prefix []byte, limit int, suppress bool) []DelimitedEntry {
	var out []DelimitedEntry
	cursor, incl := from, inclusive
	for len(out) <= limit { // limit+1 entries without overflowing at math.MaxInt
		o, ok := next(cursor, incl)
		if !ok {
			break
		}
		if cp := legacy.CommonPrefix(o.Key, prefix, '/'); cp != nil {
			if !suppress || from == nil || string(cp) > string(from) {
				out = append(out, DelimitedEntry{CommonPrefix: append([]byte(nil), cp...)})
			}
			successor, ok := legacy.Successor(cp)
			if !ok {
				break
			}
			cursor, incl = successor, true
			continue
		}
		out = append(out, DelimitedEntry{Object: &o})
		cursor, incl = o.Key, false
	}
	return out
}

// storeWalk reads the walk's rows from the store, one per call.
func storeWalk(t *testing.T, s *Sorted, to []byte) func([]byte, bool) (Object, bool) {
	return func(cursor []byte, inclusive bool) (Object, bool) {
		rows, err := s.Rows(context.Background(), cursor, inclusive, to, 1, Projection{})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			return Object{}, false
		}
		return rows[0], true
	}
}

// referenceWalk reads the walk's rows from the reference, by binary search.
func referenceWalk(rows []refRow, to []byte) func([]byte, bool) (Object, bool) {
	return func(cursor []byte, inclusive bool) (Object, bool) {
		i := sort.Search(len(rows), func(i int) bool {
			return cursor == nil || rows[i].key > string(cursor) || inclusive && rows[i].key == string(cursor)
		})
		if i == len(rows) || to != nil && rows[i].key >= string(to) {
			return Object{}, false
		}
		r := rows[i]
		return Object{Key: []byte(r.key), Size: r.size, ETag: r.etag, ChecksumAlgorithm: r.sumAlg}, true
	}
}

func diffEntries(got, want []DelimitedEntry) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d entries, want %d (%s)", len(got), len(want), describe(got, want))
	}
	for i := range got {
		g, w := got[i], want[i]
		if (g.Object == nil) != (w.Object == nil) || string(g.CommonPrefix) != string(w.CommonPrefix) {
			return fmt.Sprintf("entry %d differs (%s)", i, describe(got, want))
		}
		if g.Object != nil && !sameObject(g.Object, w.Object) {
			return fmt.Sprintf("entry %d object differs", i)
		}
	}
	return ""
}

func describe(got, want []DelimitedEntry) string {
	name := func(e []DelimitedEntry) []string {
		var out []string
		for _, x := range e[:min(len(e), 6)] {
			if x.Object != nil {
				out = append(out, "o:"+trim(string(x.Object.Key)))
			} else {
				out = append(out, "p:"+trim(string(x.CommonPrefix)))
			}
		}
		return out
	}
	return fmt.Sprintf("got %v want %v", name(got), name(want))
}

// sameObject compares the fields the reference reads.
func sameObject(a, b *Object) bool {
	return string(a.Key) == string(b.Key) && eqInt(a.Size, b.Size) && eqStr(a.ETag, b.ETag) &&
		eqStr(a.OwnerID, b.OwnerID) && eqStr(a.ChecksumAlgorithm, b.ChecksumAlgorithm)
}

func eqInt(a, b *int64) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
func eqStr(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func firstDiff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func short(s *string) string {
	if s == nil {
		return "<open>"
	}
	return trim(*s)
}

func trim(s string) string {
	if len(s) > 40 {
		return fmt.Sprintf("%s…(%d bytes)", s[:40], len(s))
	}
	return s
}
