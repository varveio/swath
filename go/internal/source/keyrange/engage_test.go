// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/order"
)

// shapes are the synthetic layouts the pivot policy is gated on (about
// 500 pages of 100 keys each): a date-partitioned warehouse, an nbm-like
// forecast archive, flat sequential keys and hashed keys.
func shapes() map[string][]string {
	var date []string
	for m := 1; m <= 12; m++ {
		for d := 1; d <= 28; d++ {
			for i := range 150 {
				date = append(date, fmt.Sprintf("warehouse/events/year=2025/month=%02d/day=%02d/part-%05d.parquet", m, d, i))
			}
		}
	}
	var nbm []string
	for day := range 30 {
		ymd := time.Date(2020, 11, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, day).Format("20060102")
		for h := range 24 {
			for _, kind := range []string{"core", "grib2", "text"} {
				for f := range 20 {
					nbm = append(nbm, fmt.Sprintf("blend.%s/%02d/%s/blend.t%02dz.%s.f%03d.co.grib2", ymd, h, kind, h, kind, f))
				}
			}
		}
	}
	flat := make([]string, 50000)
	for i := range flat {
		flat[i] = fmt.Sprintf("logs/p/%09d", i)
	}
	rng := rand.New(rand.NewSource(3))
	hash := make([]string, 50000)
	for i := range hash {
		hash[i] = fmt.Sprintf("%016x", rng.Uint64())
	}
	return map[string][]string{"date": date, "nbm": nbm, "flat": flat, "hash": hash}
}

// depthBound is the gate on a split tree of keys/page pages: a balanced
// tree needs about log2(pages) dependent splits.
func depthBound(keys, page int, k float64) int64 {
	return int64(k*math.Log2(float64(keys)/float64(page)) + 8)
}

const shapePage = 100

// TestPivotPolicyEngages: on each synthetic shape at 64 workers (pages of
// 100 keys, 1 ms each), the split tree stays within 4·log2(pages)+8
// (before the anchor reflection and the gallop: date 336, nbm 66, flat 55,
// hash 38 to 55), the output is the keyspace, the ranges tile it, and the
// report shows the paths that placed the pivots: the frontier reflected
// from its anchor, bounded chains galloped, no sliver was taken; on the
// hierarchical shapes reflections reached their range's hi and fell back
// to the midpoint.
func TestPivotPolicyEngages(t *testing.T) {
	for _, name := range []string{"date", "nbm", "flat", "hash"} {
		keys := shapes()[name]
		f := newSorted(keys, false, 1)
		f.Latency = time.Millisecond
		got, st, leaves := runBudget(t, f, Options{PageSize: shapePage}, 64, 1<<20)
		if !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(keys))
		}
		checkTiling(t, name, leaves)
		check(t, name, st, int64(len(got)))
		g := st.Get
		t.Logf("%s: depth=%d (bound %d) requests=%d frontier=%d bounded=%d reflect=%d midpoint=%d density=%d/%d ceiling=%d shrink_prevented=%d max_gen=%d halvings=%d sliver_refused=%d empty=%d/%d",
			name, g(cSplitMaxDepth), depthBound(len(keys), shapePage, 4), g(cRequests), g(cSplitAcceptedFrontier), g(cSplitAcceptedBounded),
			g(cSplitPivotReflect), g(cSplitPivotMidpoint), g(cSplitPivotDensityBounded), g(cSplitPivotDensityFrontier), g(cReflectCeiling),
			g(cDensityShrinkPrevented), g(cGallopMaxGen), g(cGallopHalvings), g(cSplitSliverRefused), g(cChildEmptyLower), g(cChildEmptyUpper))
		if d := g(cSplitMaxDepth); d > depthBound(len(keys), shapePage, 4) {
			t.Errorf("%s: split.max_depth %d over %d", name, d, depthBound(len(keys), shapePage, 4))
		}
		fired := []counters.ID{cSplitAcceptedFrontier, cFrontierAnchored, cSplitPivotReflect, cSplitPivotDensityBounded, cGallopMaxGen}
		if name == "date" || name == "nbm" {
			fired = append(fired, cReflectCeiling, cSplitPivotMidpoint)
		}
		for _, id := range fired {
			if g(id) == 0 {
				t.Errorf("%s: %s never fired", name, Counters[id].Key)
			}
		}
	}
}

// TestPrefixedFrontierSplits: a scope prefix bounds the frontier at
// prefixCeil(prefix); the reflection stays below it, so the prefixed
// frontier still splits and the tree stays within the bound. Keys outside
// the scope are not listed.
func TestPrefixedFrontierSplits(t *testing.T) {
	keys := shapes()["date"]
	const prefix = "warehouse/events/year=2025/"
	all := append(slices.Clone(keys), "warehouse/a", "warehouse/events/year=2024/x", "warehouse/events/year=20250", "zz/top")
	f := newSorted(all, false, 1)
	f.Latency = time.Millisecond
	got, st, _ := runBudget(t, f, Options{Prefix: prefix, PageSize: shapePage}, 64, 1<<20)
	if !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	if st.Get(cSplitAcceptedFrontier) == 0 || st.Get(cFrontierAnchored) == 0 {
		t.Fatalf("the prefixed frontier never split: %d frontier splits, %d anchored", st.Get(cSplitAcceptedFrontier), st.Get(cFrontierAnchored))
	}
	if d := st.Get(cSplitMaxDepth); d > depthBound(len(keys), shapePage, 4) {
		t.Fatalf("split.max_depth %d over %d", d, depthBound(len(keys), shapePage, 4))
	}
}

// TestReflectionCeiling: a reflection at or past the ceiling falls back to
// the midpoint between the cursor and the ceiling, and the upper child
// starts a new chain; when only a sliver lies there, the split is refused.
func TestReflectionCeiling(t *testing.T) {
	// From ⊥ the root reflects its cursor's last scalar: "a" → "Ã", past
	// the ceiling "c" of the scope "b".
	pr := propose(Range{open: true}, []string{"a"}, "a", "a", "c", true, 4)
	if !pr.ceiling || pr.src != srcMidpoint || pr.m != "b" || pr.sliver {
		t.Fatalf("ceiling c: %+v", pr)
	}
	pr = propose(Range{open: true}, []string{"a"}, "a", "a", "b", true, 4)
	if !pr.ceiling || !pr.sliver || pr.why != extShort {
		t.Fatalf("ceiling b: %+v", pr)
	}
	// Bounded: the chain's anchor is its first key.
	pr = propose(Range{hi: "k5"}, []string{"k1", "k3"}, "k1", "k3", "k5", true, 4)
	if pr.src != srcMidpoint || pr.m != "k4" || !pr.ceiling {
		t.Fatalf("bounded: %+v", pr)
	}
	pr = propose(Range{hi: "k9"}, []string{"k1", "k3"}, "k1", "k3", "k9", true, 4)
	if pr.src == srcSliver || pr.m <= "k3" || pr.m >= "k9" || pr.ceiling || pr.anchor != "k1" {
		t.Fatalf("bounded reflection: %+v", pr)
	}
}

// TestGallopStride: the density stride doubles per generation and is
// capped at the midpoint of (c, hi]; an unusable target is halved down to
// the least stride within the decision.
func TestGallopStride(t *testing.T) {
	page := make([]string, 100)
	for i := range page {
		page[i] = fmt.Sprintf("k%06d", i)
	}
	c := page[len(page)-1]
	var prev string
	for g := range uint8(5) {
		m, why, _ := gallop(page, c, "", false, 0, g)
		if why != extOK || m <= prev || m <= c {
			t.Fatalf("g=%d: %q %d after %q", g, m, why, prev)
		}
		prev = m
	}
	m, why, _ := gallop(page, c, "k000500", true, 0, gallopMaxGen)
	if mid, _, _ := gallop(page, c, "k000500", true, 0, gallopMaxGen-1); why != extOK || m != mid || m >= "k000500" {
		t.Fatalf("capped at the midpoint: %q %q", m, mid)
	}
	// A target past the representable keys at the widest stride halves
	// until one fits.
	short := []string{"a", "y"}
	m, why, halvings := gallop(short, "y", "", false, 64, gallopMaxGen)
	if why != extPastTop || halvings == 0 {
		t.Fatalf("past top: %q %d %d", m, why, halvings)
	}
}

// TestGenerationAndAnchor: an upper child is one generation on and keeps
// its chain's anchor; a lower child starts a chain at generation 0; the
// frontier from ⊥ anchors at its first pivot.
func TestGenerationAndAnchor(t *testing.T) {
	p, err := newTest(newSorted(nil, false, 1), Options{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	st := &running{id: 1, open: true}
	r := Range{open: true, id: 1, gen: gallopMaxGen}
	lower, upper := p.commit(st, r, "k1", pivot{m: "k5", src: srcReflect, chain: "k5"})
	if lower.gen != 0 || lower.anchor != "" || upper.gen != gallopMaxGen || upper.anchor != "k5" || !upper.open {
		t.Fatalf("lower %+v upper %+v", lower, upper)
	}
	r = Range{lo: "k5", open: true, id: 2, gen: 3, anchor: "k5"}
	pr := propose(r, []string{"k6", "k7"}, "k6", "k7", "", false, 4)
	if pr.anchor != "k5" || !pr.anchored || pr.m <= "k7" {
		t.Fatalf("frontier keeps its anchor: %+v", pr)
	}
	_, upper = p.commit(st, r, "k7", pivot{m: pr.m, src: pr.src, chain: pr.anchor})
	if upper.gen != 4 || upper.anchor != "k5" {
		t.Fatalf("upper %+v", upper)
	}
}

// TestResumeWithSplitKills: a resume whose key K lies inside a folder and
// is not a usable bound ('%', a control character: the resume lists from
// K's safe prefix and drops keys up to K) is killed at random split
// points (faults "split", as a crash) and resumed after its last listed key
// until it completes: the concatenated output is exactly the keys after K,
// once each, in order.
func TestResumeWithSplitKills(t *testing.T) {
	keys := slices.Clone(keyspaces()["dateTree"])
	for i := range 40 {
		keys = append(keys, fmt.Sprintf("warehouse/events/year=2025/month=05/day=14/part-%%%04d", i),
			fmt.Sprintf("warehouse/events/year=2025/month=05/day=14/part-\x01%04d", i))
	}
	keys = slices.Sorted(slices.Values(keys))
	seeds := 20
	if testing.Short() {
		seeds = 4
	}
	crashes := 0
	for _, k0 := range []string{"warehouse/events/year=2025/month=05/day=14/part-%0007", "warehouse/events/year=2025/month=05/day=14/part-\x010031",
		"warehouse/events/year=2025/month=02/day=03/part-00004.parquet"} {
		want := keys[slices.Index(keys, k0)+1:]
		for s := range seeds {
			rng := rand.New(rand.NewSource(int64(s)))
			var got []string
			after := k0
			for attempt := 0; ; attempt++ {
				if attempt > 200 {
					t.Fatalf("K=%q seed=%d: no completion after %d attempts", k0, s, attempt)
				}
				kill := int64(1 + rng.Intn(4+attempt))
				var splits atomic.Int64
				restore := faults.Set(func(point string) error {
					if point == "split" && splits.Add(1) == kill {
						return faults.ErrCrash
					}
					return nil
				})
				rows, err := resumeRun(t, keys, after, int64(s*100+attempt))
				restore()
				// What reached the sink continues the output exactly: a
				// crashed run hands on nothing past a gap.
				if n := len(got); !slices.Equal(rows, want[n:n+min(len(rows), len(want)-n)]) {
					t.Fatalf("K=%q seed=%d attempt %d (err %v): %d rows after %q do not continue the keyspace", k0, s, attempt, err, len(rows), after)
				}
				got = append(got, rows...)
				if len(rows) > 0 {
					after = rows[len(rows)-1]
				}
				if err == nil {
					break
				}
				if !errors.Is(err, faults.ErrCrash) {
					t.Fatal(err)
				}
				crashes++
			}
			if !slices.Equal(got, want) {
				t.Fatalf("K=%q seed=%d: %d keys after the kills, want %d", k0, s, len(got), len(want))
			}
		}
	}
	if crashes < seeds {
		t.Fatalf("only %d injected crashes", crashes)
	}
}

// resumeRun lists (after, ⊤] at 16 workers over short pages, and returns
// what reached the sink before it ended.
func resumeRun(t *testing.T, keys []string, after string, seed int64) ([]string, error) {
	t.Helper()
	api := newSorted(keys, true, seed)
	api.PageCap = 13
	opt := Options{Workers: 16, Resume: &Resume{After: after}}
	if seed%2 == 1 {
		opt.Structure = StructureAuto
	}
	p, err := newTest(api, opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	_, err = order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: 16, Budget: 500 * 256})
	return keysOf(c.rows), err
}

// FuzzPropose: whatever the page, cursor, bound, anchor, generation and
// demand, a proposed pivot that is not a refused sliver is a valid split
// point: strictly between the cursor and the ceiling (hi, or the scope's
// prefixCeil on the frontier), valid UTF-8, Safe, at most 1,024 bytes. The
// seed corpus is the fixture and hostile keyspaces' neighbouring keys.
func FuzzPropose(f *testing.F) {
	spaces := keyspaces()
	for k, v := range hostile() {
		spaces[k] = v
	}
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range spaces {
			if !yield(k) {
				return
			}
		}
	}) {
		keys := slices.Compact(slices.Sorted(slices.Values(spaces[name])))
		for i := 0; i+3 < len(keys); i += max(1, len(keys)/12) {
			f.Add(keys[i], keys[i+1], keys[i+3], "", uint8(i%17), i%70, i%2 == 0, i%5)
			f.Add(keys[i], keys[i+2], keys[i+3], keys[i], uint8(i%3), 64, i%3 == 0, 0)
		}
	}
	f.Fuzz(func(t *testing.T, first, c, hi, anchor string, gen uint8, hunger int, open bool, prefixLen int) {
		if !utf8.ValidString(first) || !utf8.ValidString(c) || !utf8.ValidString(hi) || !utf8.ValidString(anchor) ||
			!Safe(c) || first > c || anchor > c || len(c) > maxKeyLen || len(first) > maxKeyLen {
			return
		}
		hunger = min(max(hunger, -1024), 1024)
		r := Range{open: open, hi: hi, gen: min(gen, gallopMaxGen), anchor: anchor}
		ceil, bounded := hi, true
		if open {
			prefix := c[:min(max(prefixLen, 0), len(c))]
			if !strings.HasPrefix(first, prefix) {
				prefix = ""
			}
			ceil, bounded = prefixCeil(prefix)
		} else if c >= hi {
			return
		}
		pr := propose(r, []string{first, c}, first, c, ceil, bounded, hunger)
		if !pr.found || pr.sliver {
			return
		}
		m := pr.m
		if m <= c || (bounded && m >= ceil) || !utf8.ValidString(m) || !Safe(m) || len(m) > maxKeyLen {
			t.Fatalf("propose(open=%v, first=%q, c=%q, ceil=%q, anchor=%q, g=%d, hunger=%d) = %q (%v)", open, first, c, ceil, anchor, gen, hunger, m, pr.src)
		}
	})
}

// TestStructureEngages: with structure probes on their evidence (the
// command's default), the hierarchical shapes reach a tree within
// 3·log2(pages)+8 (without probes: date 31, nbm 24), pivoting on folder
// boundaries the frontier and the stalled bounded chains found; the flat
// shape's frontier stops probing after at most 8 probes, its bounded
// ranges never stall, and hashed keys have no folder to probe.
//
// A split never waits for a probe, so whether a probe's boundaries reach a
// decision is timing: on nbm-like keys only two frontier probes are sent,
// and when both lose the race to the range's next page (structure.late)
// the frontier splits on its anchored reflection instead, within the same
// bound. The timed runs require that one of the two paths engaged; the
// inline arm (probeInline: every probe lands before the next decision)
// requires the structural path on both hierarchical shapes.
func TestStructureEngages(t *testing.T) {
	for _, inline := range []bool{false, true} {
		t.Run(fmt.Sprintf("inline=%v", inline), func(t *testing.T) {
			probeInline = inline
			defer func() { probeInline = false }()
			structureEngages(t, inline)
		})
	}
}

func structureEngages(t *testing.T, inline bool) {
	for _, name := range []string{"date", "nbm", "flat", "hash"} {
		keys := shapes()[name]
		f := newSorted(keys, false, 1)
		f.Latency, f.DelimLatency = time.Millisecond, 500*time.Microsecond
		got, st, leaves := runBudget(t, f, Options{PageSize: shapePage, Structure: StructureAuto}, 64, 1<<20)
		if !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(keys))
		}
		checkTiling(t, name, leaves)
		check(t, name, st, int64(len(got)))
		g := st.Get
		probes := g(cStructureSentFrontier) + g(cStructureSentStall) + g(cStructureSentForced)
		t.Logf("%s: depth=%d (bound %d) requests=%d probes=%d/%d/%d requests=%d boundaries=%d pivots=%d frontier=%d suppressed=%d/%d late=%d",
			name, g(cSplitMaxDepth), depthBound(len(keys), shapePage, 3), g(cRequests), g(cStructureSentFrontier), g(cStructureSentStall),
			g(cStructureSentForced), g(cStructureRequests), g(cStructureBoundaries), g(cSplitPivotStructure), g(cStructureFrontierPivots),
			g(cStructureSuppressed), g(cStructureSuppressedInherited), g(cStructureLate))
		if g(cStructureSentForced) != 0 {
			t.Errorf("%s: %d probes without evidence", name, g(cStructureSentForced))
		}
		switch name {
		case "date", "nbm":
			if d := g(cSplitMaxDepth); d > depthBound(len(keys), shapePage, 3) {
				t.Errorf("%s: split.max_depth %d over %d", name, d, depthBound(len(keys), shapePage, 3))
			}
			if g(cStructureSentFrontier) == 0 {
				t.Errorf("%s: %s never fired", name, Counters[cStructureSentFrontier].Key)
			}
			structural := g(cStructureBoundaries) > 0 && g(cSplitPivotStructure) > 0
			switch {
			case inline && !structural:
				t.Errorf("%s: probes landed, but boundaries=%d structure pivots=%d", name, g(cStructureBoundaries), g(cSplitPivotStructure))
			case !structural && (g(cStructureLate) == 0 || g(cFrontierAnchored) == 0):
				t.Errorf("%s: neither path engaged: boundaries=%d structure pivots=%d late=%d anchored=%d", name,
					g(cStructureBoundaries), g(cSplitPivotStructure), g(cStructureLate), g(cFrontierAnchored))
			}
		case "flat":
			if probes > structureZeroLimit || g(cStructureSentStall) != 0 {
				t.Errorf("flat: %d probes, %d on stall evidence", probes, g(cStructureSentStall))
			}
		case "hash":
			if probes != 0 {
				t.Errorf("hash: %d probes", probes)
			}
		}
	}
}

// TestStructureSuppressionInherited: on flat keys inside one folder, with
// every range probing (StructureOn), the children inherit the count of
// probes that found no folder, so no chain from the root pays more than
// 8 and the suppression is mostly inherited.
func TestStructureSuppressionInherited(t *testing.T) {
	keys := shapes()["flat"]
	f := newSorted(keys, false, 1)
	f.Latency, f.DelimLatency = time.Millisecond, 500*time.Microsecond
	got, st, _ := runBudget(t, f, Options{PageSize: shapePage, Structure: StructureOn}, 64, 1<<20)
	if len(got) != len(keys) {
		t.Fatalf("%d keys", len(got))
	}
	if st.Get(cStructureSuppressedInherited) == 0 || st.Get(cSplitPivotStructure) != 0 {
		t.Fatalf("suppressed %d, inherited %d, structure pivots %d", st.Get(cStructureSuppressed), st.Get(cStructureSuppressedInherited), st.Get(cSplitPivotStructure))
	}
	// A range that inherited the limit sends nothing.
	p, err := newTest(newSorted(keys, false, 1), Options{Workers: 4, Structure: StructureOn})
	if err != nil {
		t.Fatal(err)
	}
	r := Range{lo: "logs/p/000000100", hi: "logs/p/000040000", id: 1, born: fromLower, zeroes: structureZeroLimit}
	st2 := p.enter(r)
	p.maybeProbe(t.Context(), st2, r, "logs/p/000000200")
	if st2.pending != nil || p.c.Load(cStructureSuppressedInherited) != 1 {
		t.Fatalf("an inherited suppression probed: pending %v, inherited %d", st2.pending != nil, p.c.Load(cStructureSuppressedInherited))
	}
}

// TestStructureLevels: the frontier probes the scope root and the two
// folders above the cursor's own; the stall evidence needs the pivot in
// the cursor's folder and hi beyond it; within treats hi "" as ⊤.
func TestStructureLevels(t *testing.T) {
	for _, c := range []struct {
		c     string
		floor int
		want  []string
	}{
		{"w/e/y=1/m=01/d=02/part-1", 0, []string{"", "w/e/y=1/", "w/e/y=1/m=01/"}},
		{"w/e/y=1/m=01/d=02/part-1", 4, []string{"w/e/", "w/e/y=1/", "w/e/y=1/m=01/"}},
		{"logs/p/0001", 0, []string{"", "logs/"}},
		{"a/b", 0, []string{""}},
		{"flat", 0, []string{""}},
	} {
		if got := frontierPrefixes(c.c, c.floor); !slices.Equal(got, c.want) {
			t.Errorf("frontierPrefixes(%q, %d) = %q, want %q", c.c, c.floor, got, c.want)
		}
	}
	for _, c := range []struct {
		c, m, hi string
		want     bool
	}{
		{"m=01/d=02/p-1", "m=01/d=02/p-5", "m=09", true},
		{"m=01/d=02/p-1", "m=01/d=05", "m=09", false},        // the pivot left the folder
		{"logs/p/0001", "logs/p/0005", "logs/p/0100", false}, // hi in the same folder: flat
		{"flat1", "flat5", "flat9", false},                   // no folder
	} {
		if got := stalled(c.c, c.m, Range{hi: c.hi}, 0); got != c.want {
			t.Errorf("stalled(%q, %q, %q) = %v", c.c, c.m, c.hi, got)
		}
	}
	if got := within([]string{"a/", "b/", "c/"}, "a/", ""); !slices.Equal(got, []string{"b/", "c/"}) {
		t.Errorf("within to ⊤: %q", got)
	}
	if m, ok := boundary(structure{bounds: []string{"b/", "c/", "d/"}}, "a", ""); !ok || m != "d/" {
		t.Errorf("frontier boundary: %q %v", m, ok)
	}
}

// TestInvalidScopeCeiling: the scope "ÿ" (c3 bf) has the byte ceiling
// c3 c0, which is not UTF-8. A first page holding only "ÿ" reflects past
// it and finds only a sliver below it: the split is refused for that page,
// not declared impossible, and the next page splits strictly inside the
// scope.
func TestInvalidScopeCeiling(t *testing.T) {
	if v, ok := validCeil("\xc3\xc0"); !ok || v != "Ā" {
		t.Fatalf("validCeil(c3 c0) = %q %v", v, ok)
	}
	// For every valid key s: s < b exactly when s < validCeil(b).
	probe := []string{"", "a", "\x7f", "ÿ", "ÿa", "Ā", "\u07ff", "\u0800", "\ud7ff", "\ue000", "\U0010ffff", "\U0010ffffa"}
	for _, b := range []string{"\xc3\xc0", "a\x80", "\xed\x9f\xc0", "\xf4\x90", "\xf4\x8f\xbf\xc0", "x\xff", "\xc3"} {
		v, ok := validCeil(b)
		for _, s := range probe {
			if (s < b) != (!ok || s < v) {
				t.Fatalf("validCeil(%q) = %q %v: %q < b is %v", b, v, ok, s, s < b)
			}
		}
	}
	p, err := newTest(newSorted(nil, false, 1), Options{Workers: 4, Prefix: "ÿ"})
	if err != nil {
		t.Fatal(err)
	}
	st, r := &running{id: 1, open: true}, Range{open: true, id: 1}
	if _, ok := p.decidePage(st, r, []string{"ÿ"}, "ÿ"); ok || st.unsplittable || p.c.Load(cUnsplittable) != 0 || p.c.Load(cSplitRejectedNoPivot) != 0 {
		t.Fatalf("first page: ok=%v unsplittable=%v (%d) no_pivot=%d", ok, st.unsplittable, p.c.Load(cUnsplittable), p.c.Load(cSplitRejectedNoPivot))
	}
	pv, ok := p.decidePage(st, r, []string{"ÿa", "ÿb"}, "ÿb")
	if !ok || pv.m <= "ÿb" || pv.m >= "\xc3\xc0" || !Safe(pv.m) {
		t.Fatalf("second page: %+v %v", pv, ok)
	}
}

// TestInvalidAnchor: a chain anchored at a first key that is not UTF-8
// cannot be reflected from; the reflection is measured from ⊥ instead,
// and the interval is not declared pivot-less.
func TestInvalidAnchor(t *testing.T) {
	pr := propose(Range{hi: "k9"}, []string{"k1\xff", "k3"}, "k1\xff", "k3", "k9", true, 4)
	if !pr.found || pr.m != "k6" || pr.anchored {
		t.Fatalf("%+v", pr)
	}
}
