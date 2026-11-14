// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

// cutsOracle is the cut file of parts by the rule stated, computed
// directly: for R = 1, 2, 4, … the cuts are the first page end where the
// rows reach each multiple of R, the last key closes the file, and the
// first R whose file holds at most maxCuts lines (unwritten cuts counted)
// and maxBytes bytes (each cut's line counted as written) is the file's.
func cutsOracle(parts [][]PageEnd, maxCuts int, maxBytes int64) string {
	for unit := int64(1); ; unit *= 2 {
		var kept []cutEnd
		var rows int64
		var last string
		for _, ends := range parts {
			base := rows
			for _, e := range ends {
				at := base + e.Rows
				if at/unit > rows/unit {
					kept = append(kept, cutEnd{key: e.Key, rows: at})
				}
				rows, last = at, e.Key
			}
		}
		if len(kept) == 0 || kept[len(kept)-1].rows != rows {
			kept = append(kept, cutEnd{key: last, rows: rows})
		}
		var size, prev int64
		for _, k := range kept {
			size += int64(len(k.key)) + 2 + int64(len(strconv.FormatInt(k.rows-prev, 10)))
			prev = k.rows
		}
		if len(kept) > maxCuts || size > maxBytes {
			continue
		}
		var b strings.Builder
		prev = 0
		for _, k := range kept {
			if !strings.Contains(k.key, "\n") {
				fmt.Fprintf(&b, "%s\t%d\n", k.key, k.rows-prev)
				prev = k.rows
			}
		}
		return b.String()
	}
}

// cutParts makes n parts of random rows, their page ends every 1,024 rows
// (some dropped, as a version group across the page end drops them) and
// at their last row, under ascending keys of random lengths, some holding
// a line feed (never the last).
func cutParts(r *rand.Rand, n int) [][]PageEnd {
	var parts [][]PageEnd
	key := 0
	next := func(last bool) string {
		key++
		k := fmt.Sprintf("k%08d/%s", key, strings.Repeat("x", r.IntN(40)))
		if !last && r.IntN(20) == 0 {
			k += "\nline"
		}
		return k
	}
	for i := range n {
		rows := 1 + r.Int64N(6000)
		var ends []PageEnd
		for at := int64(1024); at < rows; at += 1024 {
			if r.IntN(4) != 0 {
				ends = append(ends, PageEnd{Key: next(false), Rows: at})
			}
		}
		ends = append(ends, PageEnd{Key: next(i == n-1), Rows: rows})
		parts = append(parts, ends)
	}
	return parts
}

// TestCutsCoarsenInPlace: the cut file of random parts, under bounds from
// tiny to the real ones, is the rule's computed directly (cutsOracle),
// whether the parts are added one by one (doubling R as the bounds are
// crossed, in the middle of a part too) or joined from two sets split at
// any part (a resume's retained parts, then the run's); its lines ascend,
// carry no line feed, sum to the rows and end at the last key.
func TestCutsCoarsenInPlace(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for trial := range 40 {
		parts := cutParts(r, 1+r.IntN(20))
		var total int64
		for _, p := range parts {
			total += p[len(p)-1].Rows
		}
		lastKey := parts[len(parts)-1][len(parts[len(parts)-1])-1].Key
		for _, bound := range []struct {
			cuts  int
			bytes int64
		}{{3, 1 << 20}, {10, 400}, {50, 2000}, {1 << 20, 900}, {MaxCuts, MaxCutsBytes}} {
			want := cutsOracle(parts, bound.cuts, bound.bytes)
			bounded := func(base int64) *Cuts {
				c := NewCuts(base)
				c.maxCuts, c.maxBytes = bound.cuts, bound.bytes
				return c
			}
			check := func(how string, c *Cuts) {
				t.Helper()
				b, err := c.File()
				if err != nil || string(b) != want {
					t.Fatalf("trial %d, bounds %+v, %s: %v\n%q\nwant\n%q", trial, bound, how, err, b, want)
				}
				var sum int64
				prev := ""
				lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
				for _, l := range lines {
					i := strings.LastIndexByte(l, '\t')
					n, err := strconv.ParseInt(l[i+1:], 10, 64)
					if err != nil || n <= 0 || l[:i] <= prev {
						t.Fatalf("trial %d, %s: line %q after %q", trial, how, l, prev)
					}
					sum, prev = sum+n, l[:i]
				}
				if sum != total || prev != lastKey {
					t.Fatalf("trial %d, %s: %d rows to %q, want %d to %q", trial, how, sum, prev, total, lastKey)
				}
			}
			all := bounded(0)
			for _, p := range parts {
				all.Part(p)
			}
			check("one by one", all)
			for k := range parts {
				a := bounded(0)
				var rows int64
				for _, p := range parts[:k] {
					a.Part(p)
					rows += p[len(p)-1].Rows
				}
				b := bounded(rows)
				for _, p := range parts[k:] {
					b.Part(p)
				}
				if err := a.Join(b); err != nil {
					t.Fatal(err)
				}
				check(fmt.Sprintf("joined after part %d", k), a)
			}
		}
	}
}

// TestCutsFileEdges: a dataset of no rows has no cut file; one whose last
// key holds a line feed cannot have one; cuts that do not follow are not
// joined.
func TestCutsFileEdges(t *testing.T) {
	if b, err := NewCuts(0).File(); b != nil || err != nil {
		t.Fatalf("no rows: %q, %v", b, err)
	}
	c := NewCuts(0)
	c.Part([]PageEnd{{Key: "a", Rows: 1024}, {Key: "b\nc", Rows: 1500}})
	if _, err := c.File(); err == nil {
		t.Fatal("a last key with a line feed made a file")
	}
	c = NewCuts(0)
	c.Part([]PageEnd{{Key: "a\nb", Rows: 1024}, {Key: "a\tb\r", Rows: 2048}, {Key: "c\t12", Rows: 2049}})
	if b, err := c.File(); err != nil || string(b) != "a\tb\r\t2048\nc\t12\t1\n" {
		t.Fatalf("%q, %v", b, err)
	}
	if err := c.Join(NewCuts(5)); err == nil {
		t.Fatal("joined cuts that do not follow")
	}
}
