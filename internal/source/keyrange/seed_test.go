// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/order"
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
		"wide top":            gen(2600, func(i int) string { return fmt.Sprintf("d%05d/x", i) }),
		"date":                dateLayout(10),
		"nbm-like":            nbmLike(),
		"mass under the last": massUnderLast(),
		"100 deep":            deepChain(100, 6),
	}
}

// TestSeedListsEveryKey: with the seed (paginated, descending), every
// shape, the fixture and the hostile keyspaces list row-identical to the
// keyspace, the covered intervals tile (⊥, ⊤], and the seed's own tiles
// meet the tiling contract (seedTiles) on short pages and at 4 and 16
// workers, including under a prefix.
func TestSeedListsEveryKey(t *testing.T) {
	spaces := seedShapes()
	for k, v := range keyspaces() {
		spaces["fixture "+k] = v
	}
	for k, v := range hostile() {
		spaces["hostile "+k] = v
	}
	for name, keys := range spaces {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		got, _, leaves := run(t, newSorted(keys, true, 3), Options{Seed: true}, 16)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(want))
		}
		checkTiling(t, name, leaves)
		got, _, leaves = run(t, newSorted(keys, true, 5), Options{Seed: true, SeedFirstPage: true, PageSize: 100}, 16)
		if !slices.Equal(got, want) {
			t.Fatalf("%s with the first page: %d keys, want %d", name, len(got), len(want))
		}
		checkTiling(t, name+" with the first page", leaves)
		for _, w := range []int{4, 16} {
			api := newSorted(want, false, 1)
			api.PageCap = 7
			seedTiles(t, fmt.Sprintf("%s w=%d", name, w), want, fetchFrom(api), false, Options{Seed: true, Workers: w})
		}
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

// nbmLike is a top level wider than one page: 2,300 day folders.
func nbmLike() []string {
	var keys []string
	for d := range 2300 {
		for h := range 2 {
			keys = append(keys, fmt.Sprintf("blend.%05d/%02d/core/grib", d, h))
		}
	}
	return keys
}

// massUnderLast holds 95 % of its keys under the last top-level folder
// (Java's sentinel motivation): seven light folders and a heavy "h/".
func massUnderLast() []string {
	var keys []string
	for f := range 7 {
		for i := range 20 {
			keys = append(keys, fmt.Sprintf("%c/s%d/k%02d", 'a'+f, i%2, i))
		}
	}
	for s := range 200 {
		for i := range 14 {
			keys = append(keys, fmt.Sprintf("h/s%03d/k%02d", s, i))
		}
	}
	return keys
}

// deepChain is one folder per level, depth deep, plus objects at the top.
func deepChain(depth, topObjects int) []string {
	var b strings.Builder
	for i := range depth {
		fmt.Fprintf(&b, "l%02d/", i)
	}
	keys := []string{b.String() + "x"}
	for i := range topObjects {
		keys = append(keys, fmt.Sprintf("a%02d", i))
	}
	return keys
}

// tileRec records a seed's tiles; the seed emits no rows.
type tileRec struct{ tiles []Range }

func (r *tileRec) Rows(rows []Row) error {
	return fmt.Errorf("the seed emitted %d rows", len(rows))
}
func (r *tileRec) Child(c Range) error { r.tiles = append(r.tiles, c); return nil }

// seedTiles runs only the seed over fetch and checks its tiling: the tiles
// are contiguous from the start (⊥, or K and its safe start on a resume) to
// ⊤, their cuts strictly ascending, safe, inside the scope, strictly above
// a resume key, and real folders of the keyspace.
func seedTiles(t *testing.T, what string, keys []string, fetch Fetch[Object], incl bool, opt Options) ([]Range, Stats) {
	t.Helper()
	p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, StartInclusive: incl}, fetch)
	if err != nil {
		t.Fatal(err)
	}
	rec := &tileRec{}
	if err := p.Expand(context.Background(), p.Root(), rec); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	tiles := rec.tiles
	after := ""
	if opt.Resume != nil {
		after = opt.Resume.After
	}
	if len(tiles) == 0 || tiles[0].lo != after && !(tiles[0].after == after && tiles[0].lo == p.safeStart(after)) {
		t.Fatalf("%s: first tile %+v, want one from %q", what, tiles, after)
	}
	for i, r := range tiles {
		if r.open != (i == len(tiles)-1) {
			t.Fatalf("%s: tile %d open=%v of %d", what, i, r.open, len(tiles))
		}
		if i > 0 && (r.lo != tiles[i-1].hi || r.after != "") {
			t.Fatalf("%s: tile %d (%q, ..] after (.., %q]", what, i, r.lo, tiles[i-1].hi)
		}
		if r.open {
			break
		}
		c := r.hi
		if c <= r.lo || c <= after || !Safe(c) || !strings.HasPrefix(c, opt.Prefix) {
			t.Fatalf("%s: cut %d %q after %q (resume %q, scope %q)", what, i, c, r.lo, after, opt.Prefix)
		}
		if i < len(tiles)-2 || p.c.Load(cSeedSentinel) == 0 { // the sentinel is a ceiling, not a folder
			j := sort.SearchStrings(keys, c)
			if j == len(keys) || !strings.HasPrefix(keys[j], c) || !strings.HasSuffix(c, seedDelimiter) {
				t.Fatalf("%s: cut %q is not a folder", what, c)
			}
		}
	}
	return tiles, p.Stats()
}

// tileKeys counts the keys in each tile.
func tileKeys(keys []string, tiles []Range) []int {
	n := make([]int, len(tiles))
	for _, k := range keys {
		i := sort.Search(len(tiles), func(i int) bool { return tiles[i].open || k <= tiles[i].hi })
		if k > tiles[i].lo || (i == 0 && tiles[0].lo == "") {
			n[i]++
		}
	}
	return n
}

// TestSeedDescent: the seed paginates the top level and descends narrow
// levels in parallel waves within its budgets, on the shapes it is for,
// and its tiles meet the tiling contract (seedTiles).
func TestSeedDescent(t *testing.T) {
	type want struct {
		levels, requests, trips, pages int64
		stop                           counters.ID
		perLevel                       []int64
		sentinel                       int64
	}
	for _, c := range []struct {
		name    string
		keys    []string
		workers int
		pageCap int
		want    want
	}{
		// 1 + 1 + 1 + 1 + 12 requests in five waves; 336 days >= 64.
		{"date", dateLayout(10), 64, 0, want{4, 16, 5, 1, cSeedStopWide, []int64{1, 1, 1, 12, 336}, 1}},
		// Three pages of the top level, no descent.
		{"nbm-like", nbmLike(), 64, 0, want{0, 3, 3, 3, cSeedStopWide, []int64{2300}, 1}},
		// Eight top folders < 16 workers: one level under all of them.
		{"mass under the last", massUnderLast(), 16, 0, want{1, 9, 2, 1, cSeedStopWide, []int64{8, 214}, 1}},
		// One folder per level: 4 levels, then the level budget.
		{"100 deep", deepChain(100, 0), 64, 0, want{4, 5, 5, 1, cSeedStopLevels, []int64{1, 1, 1, 1, 1}, 1}},
		// A top level whose first page holds objects only is not followed
		// (seed.flat_chains): the folder after them is not found, and the
		// tiles list it. The cost of the flat rule: a top level with more
		// than a page of objects before its folders is not tiled (before
		// it, four waves found the folder and two levels below it).
		{"100 deep, objects first", deepChain(100, 6), 64, 2, want{0, 1, 1, 1, cSeedStopFlat, []int64{0}, 0}},
		// 100 folders at 10 a page stop at the page cap (truncated); the 40
		// found are listed once more and hold no folder.
		{"wide at the cap", seedShapes()["wide top"][:100], 64, 10, want{1, 44, 5, 4, cSeedStopLeaf, []int64{40, 0}, 0}},
		// A flat level: its first page holds no folder, the chain stops
		// there (before the flat rule, three waves of objects).
		{"flat", seedShapes()["dense flat root"], 64, 0, want{0, 1, 1, 1, cSeedStopFlat, []int64{0}, 0}},
	} {
		t.Run(c.name, func(t *testing.T) {
			keys := slices.Compact(slices.Sorted(slices.Values(c.keys)))
			api := newSorted(keys, false, 1)
			api.PageCap = c.pageCap
			tiles, st := seedTiles(t, c.name, keys, fetchFrom(api), false, Options{Seed: true, Workers: c.workers})
			got := want{st.Get(cSeedLevels), st.Get(cSeedRequests), st.Get(cSeedRoundTrips), st.Get(cSeedPages), 0, nil, st.Get(cSeedSentinel)}
			for _, id := range seedStopBy {
				if st.Get(id) == 1 {
					got.stop = id
				}
			}
			for l := range c.want.perLevel {
				got.perLevel = append(got.perLevel, st.Get(seedLevelCuts[l]))
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
			if got.trips > seedRoundTrips || got.requests > seedRequests || got.levels > seedLevels {
				t.Fatalf("over budget: %+v", got)
			}
			n := tileKeys(keys, tiles)
			t.Logf("%d tiles, cuts %d, largest tile %d of %d keys, last tile %d", len(tiles), st.Get(cSeedCuts), slices.Max(n), len(keys), n[len(n)-1])
			switch c.name {
			case "date", "nbm-like", "mass under the last":
				// Balanced: no tile holds more than a few of the 4×workers shares
				// and the open frontier is empty.
				if limit := 3 * len(keys) / (4 * c.workers); slices.Max(n) > limit || n[len(n)-1] != 0 {
					t.Fatalf("unbalanced tiling: largest tile %d keys (limit %d), open frontier %d", slices.Max(n), limit, n[len(n)-1])
				}
			}
			if c.name == "wide at the cap" && st.Get(cSeedTruncated) != 1 {
				t.Fatal("a top level past the page cap is truncated")
			}
		})
	}
}

// TestSeedResume: a resumed scope (K, ⊤] is seeded: its tiles start at K
// (or K's safe start, dropping keys up to K), every cut is above K, and the
// resumed listing returns exactly the keys after K, under random splits,
// on the S3-shaped (exclusive start-after) and the inclusive-start
// (GCS-shaped: a folder at or below the bound is returned, and dropped)
// providers.
func TestSeedResume(t *testing.T) {
	spaces := seedShapes()
	for k, v := range keyspaces() {
		spaces["fixture "+k] = v
	}
	for k, v := range hostile() {
		spaces["hostile "+k] = v
	}
	names := slices.Sorted(maps.Keys(spaces))
	trials := 6
	if testing.Short() {
		trials = 2
	}
	for _, name := range names {
		keys := slices.Compact(slices.Sorted(slices.Values(spaces[name])))
		if len(keys) < 2 {
			continue
		}
		for s := range trials {
			rng := rand.New(rand.NewSource(int64(s)))
			k := keys[rng.Intn(len(keys)-1)]
			want := keys[slices.Index(keys, k)+1:]
			for _, incl := range []bool{false, true} {
				what := fmt.Sprintf("%s K=%q inclusive=%v", name, k, incl)
				fetch := func() Fetch[Object] {
					if incl {
						return (&inclusive{keys: keys, bounded: true, pageCap: 9}).fetch
					}
					api := newSorted(keys, false, int64(s))
					api.PageCap = 9
					return fetchFrom(api)
				}
				opt := Options{Seed: true, Workers: 16, Resume: &Resume{After: k}}
				tiles, st := seedTiles(t, what, keys, fetch(), incl, opt)
				if st.Get(cSeedResumed) != 1 || st.Get(cResumedRanges) != int64(len(tiles)) {
					t.Fatalf("%s: seed.resumed %d, resumed_ranges %d, %d tiles", what, st.Get(cSeedResumed), st.Get(cResumedRanges), len(tiles))
				}
				var mu sync.Mutex
				opt.SplitCoin = func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, StartInclusive: incl}, fetch())
				if err != nil {
					t.Fatal(err)
				}
				col := &collect{}
				if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: 16, Budget: 1 << 20}); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got := keysOf(col.rows); !slices.Equal(got, want) {
					t.Fatalf("%s: resumed %d keys, want %d", what, len(got), len(want))
				}
			}
		}
	}
	// A resume inside the date layout's tenth day of March tiles the rest
	// of the year: the folders above K list its later days and months, and
	// the seed descends into them.
	keys := slices.Sorted(slices.Values(dateLayout(10)))
	k := "warehouse/events/year=2025/month=03/day=10/part-00004.parquet"
	tiles, st := seedTiles(t, "date resume", keys, fetchFrom(newSorted(keys, false, 1)), false, Options{Seed: true, Workers: 64, Resume: &Resume{After: k}})
	n := tileKeys(keys, tiles)
	t.Logf("date resume: %d tiles, levels %d, requests %d, round trips %d, largest tile %d", len(tiles), st.Get(cSeedLevels), st.Get(cSeedRequests), st.Get(cSeedRoundTrips), slices.Max(n))
	if st.Get(cResumedRanges) < 64 || st.Get(cSeedLevels) != 1 || slices.Max(n) > 30 {
		t.Fatalf("date resume: %d tiles, %d levels, largest tile %d", len(tiles), st.Get(cSeedLevels), slices.Max(n))
	}
}

// TestSeedResumeAfterCrash: a seeded run killed at a random split or range
// end (faults.At), then resumed after the last key it delivered, with the
// seed, lists row-identical to a clean run: nothing lost, nothing twice.
// The keyspaces include keys whose resume start is truncated at a '%' or a
// control character; with and without the seed's first data page.
func TestSeedResumeAfterCrash(t *testing.T) {
	for _, firstPage := range []bool{false, true} {
		t.Run(fmt.Sprint("first-page=", firstPage), func(t *testing.T) { seedResumeAfterCrash(t, firstPage) })
	}
}

func seedResumeAfterCrash(t *testing.T, firstPage bool) {
	spaces := map[string][]string{"date": dateLayout(10), "mass under the last": massUnderLast(), "nbm-like": nbmLike()}
	for k, v := range hostile() {
		spaces["hostile "+k] = v
	}
	spaces["fixture markersAndOddChars"] = append(keyspaces()["markersAndOddChars"], keyspaces()["deepTree"]...)
	trials := 12
	if testing.Short() {
		trials = 3
	}
	for _, name := range slices.Sorted(maps.Keys(spaces)) {
		want := slices.Compact(slices.Sorted(slices.Values(spaces[name])))
		// A clean run counts the fault points, so the crash index falls
		// within the run on a small keyspace too (one the admission gates
		// list in a few ranges), at most the 40th.
		points := map[string]int{}
		var pmu sync.Mutex
		restore := faults.Set(func(p string) error { pmu.Lock(); points[p]++; pmu.Unlock(); return nil })
		p, err := newTest(newSorted(want, true, 0), Options{Seed: true, SeedFirstPage: firstPage, Workers: 16})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := order.Run[Range, Object](context.Background(), p, &collect{}, &Schema, order.Options{Workers: 16, Budget: 64 * 256}); err != nil {
			t.Fatal(err)
		}
		restore()
		crashed := 0
		for s := range trials {
			rng := rand.New(rand.NewSource(int64(s)))
			point := []string{"split", "range-done"}[s%2]
			n := 1 + rng.Intn(max(1, min(40, points[point])))
			var mu sync.Mutex
			hits := 0
			restore := faults.Set(func(p string) error {
				mu.Lock()
				defer mu.Unlock()
				if p == point {
					if hits++; hits == n {
						return faults.ErrCrash
					}
				}
				return nil
			})
			first := &collect{}
			p, err := newTest(newSorted(want, true, int64(s)), Options{Seed: true, SeedFirstPage: firstPage, Workers: 16})
			if err != nil {
				t.Fatal(err)
			}
			_, err = order.Run[Range, Object](context.Background(), p, first, &Schema, order.Options{Workers: 16, Budget: 64 * 256})
			restore()
			got := keysOf(first.rows)
			what := fmt.Sprintf("%s trial %d (%s #%d)", name, s, point, n)
			if err == nil {
				if !slices.Equal(got, want) {
					t.Fatalf("%s: uninterrupted run lists %d keys, want %d", what, len(got), len(want))
				}
				continue
			}
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("%s: %v", what, err)
			}
			crashed++
			opt := Options{Seed: true, SeedFirstPage: firstPage, Workers: 16}
			if len(got) > 0 {
				opt.Resume = &Resume{After: got[len(got)-1]}
			}
			second := &collect{}
			p, err = newTest(newSorted(want, true, int64(s+100)), opt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := order.Run[Range, Object](context.Background(), p, second, &Schema, order.Options{Workers: 16, Budget: 1 << 20}); err != nil {
				t.Fatalf("%s: resume: %v", what, err)
			}
			if all := append(got, keysOf(second.rows)...); !slices.Equal(all, want) {
				var missing []string
				for _, k := range want {
					if !slices.Contains(all, k) {
						missing = append(missing, k)
					}
				}
				t.Fatalf("%s: %d + %d keys after the resume, want %d; prefix ok %v, K %q, missing %q", what, len(got), len(second.rows), len(want),
					slices.Equal(got, want[:min(len(got), len(want))]), got[len(got)-1], missing)
			}
		}
		if crashed < trials/3 {
			t.Fatalf("%s: %d of %d trials crashed", name, crashed, trials)
		}
	}
}

// emptyFirst serves each listed folder's delimiter chain behind n empty
// truncated pages (tokens E1..En), then its real first page, and counts
// the delimiter requests; fail answers a folder's chain with its error.
type emptyFirst struct {
	api   *sorted
	n     map[string]int
	fail  map[string]func() error
	mu    sync.Mutex
	calls int
}

func (f *emptyFirst) fetch() Fetch[Object] {
	real := fetchFrom(f.api)
	return func(ctx context.Context, q *Query) (*Page[Object], error) {
		if q.Delimiter == "" {
			return real(ctx, q)
		}
		f.mu.Lock()
		f.calls++
		f.mu.Unlock()
		if fail := f.fail[q.Prefix]; fail != nil {
			return nil, fail()
		}
		k := 0
		if strings.HasPrefix(q.Token, "E") {
			fmt.Sscan(q.Token[1:], &k)
		} else if q.Token != "" {
			return real(ctx, q)
		}
		if k < f.n[q.Prefix] {
			return &Page[Object]{Truncated: true, Token: fmt.Sprint("E", k+1)}, nil
		}
		in := *q
		in.Token = ""
		return real(ctx, &in)
	}
}

// TestSeedChargesEmptyPages: the empty continuing pages list follows are
// the seed's requests and round trips too, and its budgets stop the
// following: a top level behind 10 empty pages spends the 6 round trips
// (one chain, 6 serial requests) and stops there; 40 folders behind an
// empty page each spend the 64 requests in their wave (40, then 24 of the
// 40 follow-ups) and stop there.
// seed.requests is the delimiter requests sent, in both.
func TestSeedChargesEmptyPages(t *testing.T) {
	var wide []string
	for i := range 40 {
		wide = append(wide, fmt.Sprintf("d%02d/x", i))
	}
	for _, c := range []struct {
		name                  string
		keys                  []string
		n                     map[string]int
		requests, trips, cuts int64
		stop                  counters.ID
	}{
		{"top behind 10", []string{"a/x", "b/x"}, map[string]int{"": 10}, 6, 6, 0, cSeedStopRoundTrips},
		{"40 folders behind 1", wide, func() map[string]int {
			m := map[string]int{}
			for _, k := range wide {
				m[k[:4]] = 1
			}
			return m
		}(), 64, 3, 40, cSeedStopRequests},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &emptyFirst{api: newSorted(c.keys, false, 1), n: c.n}
			tiles, st := seedTiles(t, c.name, c.keys, f.fetch(), false, Options{Seed: true, Workers: 64})
			if st.Get(cSeedRequests) != int64(f.calls) || st.Get(cSeedRequests) != c.requests || st.Get(cSeedRoundTrips) != c.trips ||
				st.Get(c.stop) != 1 || st.Get(cSeedCuts) != c.cuts || st.Get(cRequests) != int64(f.calls) {
				t.Fatalf("calls=%d seed.requests=%d round_trips=%d cuts=%d requests=%d, %s=%d", f.calls, st.Get(cSeedRequests),
					st.Get(cSeedRoundTrips), st.Get(cSeedCuts), st.Get(cRequests), Counters[c.stop].Key, st.Get(c.stop))
			}
			if c.cuts == 0 && (st.Get(cSeedTruncated) != 1 || len(tiles) != 1) {
				t.Fatalf("truncated=%d tiles=%d", st.Get(cSeedTruncated), len(tiles))
			}
		})
	}
}

// TestSeedFailurePublishesCounters: a seed whose second wave fails (a
// permanent error, or the run canceled) still reports what it spent: the
// top page and both folders' requests, two round trips, the top level's
// cuts and pages, and seed.stop.failed.
func TestSeedFailurePublishesCounters(t *testing.T) {
	for _, name := range []string{"error", "canceled"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fail := func() error { return errors.New("permanent") }
			if name == "canceled" {
				fail = func() error { cancel(); return ctx.Err() }
			}
			keys := []string{"a/x", "b/x"}
			f := &emptyFirst{api: newSorted(keys, false, 1), fail: map[string]func() error{"b/": fail}}
			p, err := New(Options{Seed: true, Workers: 64}, Description[Object]{Target: "test", Listing: Listing}, f.fetch())
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Expand(ctx, p.Root(), &tileRec{}); err == nil {
				t.Fatal("the seed did not fail")
			}
			st := p.Stats()
			if st.Get(cSeedRequests) != 3 || st.Get(cSeedRoundTrips) != 2 || st.Get(cSeedPages) != 1 || st.Get(cSeedPrefixes) != 2 ||
				st.Get(cSeedLevel0Cuts) != 2 || st.Get(cSeedStopFailed) != 1 || st.Get(cPages) < 1 {
				t.Fatalf("requests=%d round_trips=%d pages=%d prefixes=%d level0=%d failed=%d all pages=%d", st.Get(cSeedRequests),
					st.Get(cSeedRoundTrips), st.Get(cSeedPages), st.Get(cSeedPrefixes), st.Get(cSeedLevel0Cuts), st.Get(cSeedStopFailed), st.Get(cPages))
			}
		})
	}
}

// pageRec records a seed's rows and tiles.
type pageRec struct {
	rows  []string
	tiles []Range
}

func (r *pageRec) Rows(rows []Row) error {
	if len(r.tiles) > 0 {
		return errors.New("rows after a tile")
	}
	r.rows = append(r.rows, keysOf(rows)...)
	return nil
}
func (r *pageRec) Child(c Range) error { r.tiles = append(r.tiles, c); return nil }

// TestSeedFirstPage: with SeedFirstPage the seed's first wave carries the
// scope's first data page. A scope of one page is listed by it in one
// round trip (seed.stop.one_page, no tile, also on a resume after K); a
// larger scope's page is discarded (seed.first_page_discarded): the seed
// emits no row and its tiles are those of a seed without the page, from
// the scope's start, at one request more.
func TestSeedFirstPage(t *testing.T) {
	expand := func(keys []string, opt Options, accept func(string) bool) (*pageRec, Stats) {
		t.Helper()
		p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, AcceptBound: accept}, fetchFrom(newSorted(keys, false, 1)))
		if err != nil {
			t.Fatal(err)
		}
		rec := &pageRec{}
		if err := p.Expand(context.Background(), p.Root(), rec); err != nil {
			t.Fatal(err)
		}
		return rec, p.Stats()
	}
	small := seedShapes()["flat small"] // 300 keys under a page of 1,000
	for _, after := range []string{"", small[99]} {
		opt := Options{Seed: true, SeedFirstPage: true, Workers: 64}
		want := small
		if after != "" {
			opt.Resume, want = &Resume{After: after}, small[100:]
		}
		rec, st := expand(small, opt, nil)
		if !slices.Equal(rec.rows, want) || len(rec.tiles) != 0 || st.Get(cSeedStopOnePage) != 1 ||
			st.Get(cSeedFirstPageKeys) != int64(len(want)) || st.Get(cSeedRoundTrips) > 1 || st.Get(cRequests) > 2 {
			t.Fatalf("after %q: %d rows, %d tiles, one_page %d, round trips %d, requests %d", after, len(rec.rows), len(rec.tiles),
				st.Get(cSeedStopOnePage), st.Get(cSeedRoundTrips), st.Get(cRequests))
		}
	}
	keys := dateLayout(10) // 3,360 keys: the first page is a third of a month
	rec, st := expand(keys, Options{Seed: true, SeedFirstPage: true, Workers: 64}, nil)
	plain, _ := expand(keys, Options{Seed: true, Workers: 64}, nil)
	if len(rec.rows) != 0 || st.Get(cSeedFirstPageDiscarded) != 1 || st.Get(cSeedFirstPageKeys) != 0 || st.Get(cSeedStopWide) != 1 {
		t.Fatalf("%d rows, discarded %d, first_page_keys %d", len(rec.rows), st.Get(cSeedFirstPageDiscarded), st.Get(cSeedFirstPageKeys))
	}
	if !reflect.DeepEqual(rec.tiles, plain.tiles) {
		t.Fatalf("%d tiles from %q, want the %d of a seed without the page", len(rec.tiles), rec.tiles[0].lo, len(plain.tiles))
	}
	var requests [2]int64
	for i, first := range []bool{false, true} {
		got, st, leaves := run(t, newSorted(keys, false, 1), Options{Seed: true, SeedFirstPage: first}, 64)
		if !slices.Equal(got, keys) {
			t.Fatalf("first=%v: %d keys, want %d", first, len(got), len(keys))
		}
		checkTiling(t, fmt.Sprint("date first=", first), leaves)
		requests[i] = st.Get(cRequests)
	}
	if requests[1] > requests[0]+1 {
		t.Fatalf("the first page cost %d requests over %d", requests[1]-requests[0], requests[0])
	}
}

// TestSeedFirstPageLargeFirstTile is the megascenes regression (12.6M
// keys: 12 s, mean active 57, became 20 s, mean active 35, for the same
// requests and splits, when the seed emitted its truncated first page and
// started the first tile after it): a first folder far larger than the
// rest, whose first 1,000 keys the first page holds. With SeedFirstPage
// the seed's tiles are those of a seed without the page, from the scope's
// start (the regression's tiles started after the page), and the run is as
// wide. The tiles are the assertion; the mean of active ranges is a
// timing and is held only to a tenth of the run without the page, a
// tenfold regression (under -race on the ubuntu runner, 40 runs a width:
// 0.86 to 1.11 of it at 2 CPUs, 0.89 to 1.08 at GOMAXPROCS=1; 0.69 once
// in the busy race shard, #298).
func TestSeedFirstPageLargeFirstTile(t *testing.T) {
	var keys []string
	for i := range 30000 {
		keys = append(keys, fmt.Sprintf("a/img-%06d.jpg", i))
	}
	for d := range 300 {
		for i := range 100 {
			keys = append(keys, fmt.Sprintf("b%03d/img-%04d.jpg", d, i))
		}
	}
	var tiles [2][]Range
	for i, first := range []bool{false, true} {
		opt := Options{Seed: true, SeedFirstPage: first, PageSize: 100, Workers: 64}
		p, err := New(opt, Description[Object]{Target: "test", Listing: Listing}, fetchFrom(newSorted(keys, false, 1)))
		if err != nil {
			t.Fatal(err)
		}
		rec := &pageRec{}
		if err := p.Expand(context.Background(), p.Root(), rec); err != nil {
			t.Fatal(err)
		}
		if len(rec.rows) != 0 || len(rec.tiles) < 2 || rec.tiles[0].lo != "" {
			t.Fatalf("first=%v: the seed emitted %d rows, %d tiles", first, len(rec.rows), len(rec.tiles))
		}
		tiles[i] = rec.tiles
	}
	if !reflect.DeepEqual(tiles[1], tiles[0]) {
		t.Fatalf("%d tiles with the first page, from %q; want the %d of a seed without it", len(tiles[1]), tiles[1][0].lo, len(tiles[0]))
	}
	var mean [2]float64
	for i, first := range []bool{false, true} {
		f := newSorted(keys, false, 1)
		f.Latency = time.Millisecond
		got, st, leaves := runPolicy(t, f, Options{Seed: true, SeedFirstPage: first, PageSize: 100}, 64, 1<<20, nil)
		if !slices.Equal(got, keys) {
			t.Fatalf("first=%v: %d keys, want %d", first, len(got), len(keys))
		}
		checkTiling(t, fmt.Sprint("first=", first), leaves)
		if first && (st.Get(cSeedFirstPageDiscarded) != 1 || st.Get(cSeedFirstPageKeys) != 0) {
			t.Fatalf("the truncated first page was not discarded: %d keys", st.Get(cSeedFirstPageKeys))
		}
		mean[i] = st.MeanActive
		t.Logf("first page %v: mean active %.1f, requests %d, depth %d", first, st.MeanActive, st.Get(cRequests), st.Get(cSplitMaxDepth))
	}
	if mean[1] < mean[0]/10 {
		t.Fatalf("mean active %.1f with the first page, %.1f without", mean[1], mean[0])
	}
}
