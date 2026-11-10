// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/order"
)

type span struct {
	lo, hi string
	open   bool
}

func run(t *testing.T, api *sorted, opt Options, workers int) ([]string, Stats, []span) {
	return runBudget(t, api, opt, workers, 500)
}

// runBudget runs with a reorder budget in rows (the CLI default is 131,072).
func runBudget(t *testing.T, api lister, opt Options, workers, budget int) ([]string, Stats, []span) {
	t.Helper()
	return runPolicy(t, api, opt, workers, budget, nil)
}

// runPolicy is runBudget with the provider's bound policy accept (nil: the
// default, Safe).
func runPolicy(t *testing.T, api lister, opt Options, workers, budget int, accept func(string) bool) ([]string, Stats, []span) {
	t.Helper()
	var mu sync.Mutex
	var leaves []span
	opt.Workers = workers
	opt.Trace = func(lo, hi string, open bool) {
		mu.Lock()
		leaves = append(leaves, span{lo, hi, open})
		mu.Unlock()
	}
	p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, AcceptBound: accept, Size: func(o *Object) int64 { return o.Size }}, fetchFrom(api))
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: int64(budget) * 256}); err != nil {
		t.Fatal(err)
	}
	return keysOf(c.rows), p.Stats(), leaves
}

// T1: random splits (a seeded coin instead of the demand gate, guard on)
// cover every key exactly once, in order, and the covered intervals tile
// (⊥, ⊤].
func TestT1RandomSplitsCoverEveryKeyOnce(t *testing.T) {
	seeds := 50
	if testing.Short() {
		seeds = 5
	}
	for name, keys := range keyspaces() {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for s := range seeds {
			for _, w := range []int{1, 4, 64} {
				rng := rand.New(rand.NewSource(int64(s*1000 + w)))
				var mu sync.Mutex
				coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				opt := Options{SplitCoin: coin, Seed: s%2 == 1}
				if s%3 == 2 {
					opt.Structure = StructureAuto // folder boundaries on the frontier and stalled chains
				}
				got, st, leaves := run(t, newSorted(keys, true, int64(s)), opt, w)
				if !slices.Equal(got, want) {
					t.Fatalf("%s seed=%d w=%d: %d keys, want %d (splits %d)", name, s, w, len(got), len(want), st.Get(cSplitAcceptedBounded)+st.Get(cSplitAcceptedFrontier))
				}
				checkTiling(t, fmt.Sprintf("%s seed=%d w=%d", name, s, w), leaves)
			}
		}
	}
}

func checkTiling(t *testing.T, what string, leaves []span) {
	t.Helper()
	slices.SortFunc(leaves, func(a, b span) int { return strings.Compare(a.lo, b.lo) })
	prev := ""
	for i, l := range leaves {
		if l.lo != prev {
			t.Fatalf("%s: gap or overlap at leaf %d: lo %q, previous hi %q", what, i, l.lo, prev)
		}
		if l.open {
			if i != len(leaves)-1 {
				t.Fatalf("%s: open leaf %d is not last", what, i)
			}
			return
		}
		if l.hi <= l.lo && l.lo != "" {
			t.Fatalf("%s: empty leaf (%q, %q]", what, l.lo, l.hi)
		}
		prev = l.hi
	}
	t.Fatalf("%s: no open leaf: the frontier (.., ⊤] was not covered", what)
}

// T2: pivot properties.
func TestT2Pivots(t *testing.T) {
	check := func(a, b string) {
		t.Helper()
		m, _, ok := betweenFrom(a, b)
		if !ok {
			return
		}
		if !(a < m && m < b) || !utf8.ValidString(m) || len(m) > maxKeyLen {
			t.Fatalf("betweenFrom(%q, %q) = %q", a, b, m)
		}
	}
	rng := rand.New(rand.NewSource(1))
	alphabet := []rune{0, 1, 0x1F, ' ', '!', '%', 'a', 'z', 0x7F, 0x80, 0x9F, 0xA0, 0xD7FF, 0xE000, 0xFDD0, 0xFFFD, 0xFFFE, 0x10000, 0x10FFFF}
	for range 20000 {
		mk := func() string {
			var b []rune
			for range rng.Intn(6) {
				b = append(b, alphabet[rng.Intn(len(alphabet))])
			}
			return string(b)
		}
		a, b := mk(), mk()
		if a > b {
			a, b = b, a
		}
		if a < b {
			check(a, b)
		}
	}
	for _, c := range [][2]string{
		{"", "b"}, {"a", "ab"}, {"a", "a b"}, {"a", "b"}, {"a\x1f", "a!"}, {"�", "\U00010000"}, {"퟿", ""},
		{"\U0001F600", "\U0001F610"}, {strings.Repeat("x", 1023) + "a", strings.Repeat("x", 1023) + "c"},
		{"A" + strings.Repeat("\x00", 1023), "B"},
	} {
		check(c[0], c[1])
	}
	for _, nilCase := range [][2]string{{"a", "a\x00"}, {"a", "a\x01"}} {
		if m, _, ok := betweenFrom(nilCase[0], nilCase[1]); ok {
			t.Fatalf("betweenFrom(%q, %q) = %q, want none", nilCase[0], nilCase[1], m)
		}
	}
	if m, _, ok := betweenFrom("a", "a "); ok {
		t.Fatalf("a < m < 'a ' has no safe pivot, got %q", m)
	}
	for range 5000 {
		lo := string(alphabet[rng.Intn(len(alphabet))]) + "x"
		c := lo + string(rune('a'+rng.Intn(26)))
		if m, ok := forwardReflect(lo, c); ok && (m <= c || !utf8.ValidString(m) || len(m) > maxKeyLen) {
			t.Fatalf("forwardReflect(%q, %q) = %q", lo, c, m)
		}
	}
	if u, ok := prefixCeil("ab\xff\xff"); !ok || u != "ac" {
		t.Fatalf("prefixCeil = %q %v", u, ok)
	}
	if _, ok := prefixCeil("\xff"); ok {
		t.Fatal("prefixCeil of all-0xFF must be open")
	}
}

// T3: one test per guard clause, the progress gate and the demand gate.
func TestT3Guard(t *testing.T) {
	for _, tc := range []struct {
		c, m    string
		found   bool
		ceil    string
		bounded bool
		want    clause
	}{
		{"b", "c", true, "d", true, clauseOK},
		{"b", "b", true, "d", true, clauseCursor},
		{"b", "a", true, "d", true, clauseCursor},
		{"b", "d", true, "d", true, clauseBound},
		{"b", "e", true, "d", true, clauseBound},
		{"b", "zz", true, "", false, clauseOK}, // open frontier, no ceiling
		{"b\x01", "c", true, "d", true, clauseUnsafeCursor},
		{"b", "", false, "d", true, clauseNoPivot},
		{"b", "c\x01", true, "d", true, clauseUnsafePivot},
		{"b", "cv", true, "d", true, clauseUnsafePivot}, // vetoed by the provider below
	} {
		veto := func(s string) bool { return Safe(s) && s != "cv" }
		if got := guard(tc.c, tc.m, tc.found, tc.ceil, tc.bounded, veto); got != tc.want {
			t.Errorf("guard(%+v) = %d, want %d", tc, got, tc.want)
		}
	}
	keys := keyspaces()["singlePrefixFlat"]
	// No demand with one worker: never split.
	_, st, _ := run(t, newSorted(keys, false, 1), Options{}, 1)
	if st.Get(cSplitAcceptedBounded)+st.Get(cSplitAcceptedFrontier) != 0 || st.Get(cSplitSkippedNoDemand) == 0 {
		t.Fatalf("w1 split: %+v", st.Counters)
	}
	// Unsafe cursor: keys ending in a control character never become a lower bound.
	odd := make([]string, 3000)
	for i := range odd {
		odd[i] = fmt.Sprintf("k%05d\x01", i)
	}
	got, st, _ := run(t, newSorted(odd, false, 1), Options{}, 8)
	if len(got) != 3000 || st.Get(cSplitRejectedUnsafeCursor) == 0 {
		t.Fatalf("unsafe cursor: %d keys, %+v", len(got), st.Counters)
	}
	// A page that ends a range is never followed by a split (the loop ends
	// before the decision), and splits need a full page (progress gate):
	// one page of keys never splits.
	_, st, _ = run(t, newSorted(keys[:900], false, 1), Options{}, 8)
	if st.Get(cSplitConsidered) != 0 {
		t.Fatalf("split considered on a single final page: %+v", st.Counters)
	}
}

// R7: one unsafe page-ending cursor declines that page's split only. With
// 100,000 safe keys but the 1,000th (the first page's last) unsafe, by its
// bytes or by the provider's veto, the remaining 99,000 keys still split
// and every row arrives once, in order.
func TestSingleUnsafeCursorDoesNotDisableLaterSplits(t *testing.T) {
	keys := make([]string, 100000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%06d", i)
	}
	splits := func(st Stats) int64 { return st.Get(cSplitAcceptedBounded) + st.Get(cSplitAcceptedFrontier) }
	check := func(name string, want []string, got []string, st Stats) {
		t.Helper()
		t.Logf("%s: rows=%d splits=%d unsafe_rejections=%d max_active=%d", name, len(got), splits(st), st.Get(cSplitRejectedUnsafeCursor), st.Get(cMaxActive))
		if !slices.Equal(got, want) {
			t.Fatalf("%s: rows differ (%d, want %d)", name, len(got), len(want))
		}
		if st.Get(cSplitRejectedUnsafeCursor) == 0 {
			t.Fatalf("%s: the unsafe cursor was never met", name)
		}
		// Splits, not peak concurrency: with GOMAXPROCS=1 and a fake
		// without latency, one worker may drain each child before the next
		// starts.
		if splits(st) < 10 {
			t.Errorf("%s: one unsafe cursor suppressed later splits: %d splits", name, splits(st))
		}
	}
	odd := slices.Clone(keys)
	odd[999] += "%"
	got, st, _ := run(t, newSorted(odd, false, 1), Options{}, 16)
	check("unsafe byte", odd, got, st)

	veto := func(s string) bool { return Safe(s) && s != keys[999] }
	p, err := New(Options{Workers: 16}, Description[Object]{Target: "test", Listing: Listing, AcceptBound: veto}, fetchFrom(newSorted(keys, false, 1)))
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: 16, Budget: 500 * 256}); err != nil {
		t.Fatal(err)
	}
	check("provider veto", keys, keysOf(c.rows), p.Stats())
}

// T4: paging edges.
func TestT4Paging(t *testing.T) {
	keys := keyspaces()["exactly3000"]
	f := newSorted(keys, false, 1)
	p, _ := newTest(f, Options{Workers: 1})
	c := &collect{}
	r := Range{lo: "", hi: keys[999], id: 1, born: fromLower}
	if err := p.expandRange(context.Background(), r, &direct{c}); err != nil || len(c.rows) != 1000 || f.Calls != 1 {
		t.Fatalf("page ending at hi: err=%v rows=%d calls=%d", err, len(c.rows), f.Calls)
	}
	// A truncated empty page is followed by its token (paging_test.go).
	// Non-ascending keys and keys not after lo.
	bad := &script{pages: []Page[Object]{last("b", "a")}}
	p, _ = New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing}, bad.fetch)
	if err := p.expandRange(context.Background(), Range{open: true}, &direct{&collect{}}); err == nil {
		t.Fatal("non-ascending page accepted")
	}
	behind := &script{pages: []Page[Object]{last("a")}}
	p, _ = New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing}, behind.fetch)
	if err := p.expandRange(context.Background(), Range{lo: "b", open: true}, &direct{&collect{}}); err == nil {
		t.Fatal("key not after lo accepted")
	}
	// Cancellation mid-request.
	slow := newSorted(keys, false, 1)
	slow.Latency = time.Second
	p, _ = newTest(slow, Options{Workers: 4})
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if _, err := order.Run[Range, Object](ctx, p, &collect{}, &Schema, order.Options{Workers: 4, Budget: 100 * 256}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines left: %d -> %d", before, after)
	}
	// Seed: a top level of two pages is followed whole (sentinel), one past
	// the page cap is truncated (no sentinel); both subsampled to 4W = 8.
	for _, c := range []struct{ n, pages, truncated, sentinel int64 }{{1500, 2, 0, 1}, {4500, seedPages, 1, 0}} {
		var many []string
		for i := range c.n {
			many = append(many, fmt.Sprintf("d%04d/x", i))
		}
		_, st, leaves := run(t, newSorted(many, false, 1), Options{Seed: true}, 2)
		if st.Get(cSeedPages) != c.pages || st.Get(cSeedPrefixes) != min(c.n, seedPages*PageKeys) || st.Get(cSeedTruncated) != c.truncated ||
			st.Get(cSeedSubsampled) == 0 || st.Get(cSeedCuts) != 8 || st.Get(cSeedSentinel) != c.sentinel || st.Get(cSeedStopWide) != 1 {
			t.Fatalf("seed (%d folders, cap 4W=8): %+v", c.n, st.Counters)
		}
		checkTiling(t, fmt.Sprintf("seed %d folders", c.n), leaves)
	}
	_, st, leaves := run(t, newSorted([]string{"a/1", "b/1", "c/1"}, false, 1), Options{Seed: true}, 4)
	if st.Get(cSeedSentinel) == 0 || st.Get(cSeedCuts) != 3 {
		t.Fatalf("seed sentinel: %+v", st.Counters)
	}
	checkTiling(t, "seed sentinel", leaves)
}

// direct is an emitter that collects rows and ignores children.
type direct struct{ c *collect }

func (d *direct) Rows(rows []Row) error {
	return d.c.Write(context.Background(), [][]Row{rows})
}
func (d *direct) Child(Range) error { return nil }

// T6: balance smoke (synthetic: singlePrefixFlat 200k keys, 20 ms per page,
// 16 workers, the default reorder budget). An engagement check, not a
// performance claim.
func TestT6BalanceSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	keys := make([]string, 200_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("p/%07d", i)
	}
	f := newSorted(keys, false, 1)
	f.Latency = 20 * time.Millisecond
	// The kernel must engage at least 12 of 16 workers.
	got, st, leaves := runBudget(t, f, Options{}, 16, 131_072)
	if len(got) != len(keys) {
		t.Fatalf("%d keys", len(got))
	}
	checkTiling(t, "T6", leaves)
	accepted := st.Get(cSplitAcceptedBounded) + st.Get(cSplitAcceptedFrontier)
	t.Logf("max_active=%d accepted=%d (bounded %d, frontier %d) child_empty=%+v requests=%d",
		st.Get(cMaxActive), accepted, st.Get(cSplitAcceptedBounded), st.Get(cSplitAcceptedFrontier), st.Counters, st.Get(cRequests))
	if st.Get(cMaxActive) < 12 || accepted == 0 {
		t.Fatalf("parallelism did not engage: max_active=%d accepted=%d", st.Get(cMaxActive), accepted)
	}
}

// clearing is an emitter that owns its rows as a filter does: it drops
// them all and clears them in place. It records the children.
type clearing struct {
	drop     bool
	children []Range
}

func (c *clearing) Rows(rows []Row) error {
	if c.drop {
		clear(rows)
	}
	return nil
}

func (c *clearing) Child(r Range) error {
	c.children = append(c.children, r)
	return nil
}

// TestFilterLeavesSplitInputs: the page's density and the split's page
// keys are read before the rows are handed over, so an emitter that drops
// and clears them (a filter) cannot change the range's observations: the
// same density and the same pivot as an emitter that keeps them.
func TestFilterLeavesSplitInputs(t *testing.T) {
	keys := make([]string, 5000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%06d", i)
	}
	type result struct {
		dens     float64
		children []span
	}
	expand := func(drop bool) result {
		var p *Engine[Object]
		var res result
		// Floors: the density is maintained only for the tail floor.
		p, err := newTest(newSorted(keys, false, 1), Options{Workers: 64, PageSize: 100, Floors: true, SplitCoin: func() bool {
			res.dens = p.running[1].dens.ewma // decide holds the engine's lock
			return true
		}})
		if err != nil {
			t.Fatal(err)
		}
		e := &clearing{drop: drop}
		if err := p.expandRange(context.Background(), Range{lo: "", hi: "l", id: 1, born: fromRoot}, e); err != nil {
			t.Fatal(err)
		}
		for _, c := range e.children {
			res.children = append(res.children, span{c.lo, c.hi, c.open})
		}
		return res
	}
	kept, dropped := expand(false), expand(true)
	if kept.dens <= 0 || len(kept.children) != 2 {
		t.Fatalf("no split observed: %+v", kept)
	}
	if dropped.dens != kept.dens || !slices.Equal(dropped.children, kept.children) {
		t.Fatalf("a filter changed the split inputs: density %v, children %v; unfiltered %v, %v", dropped.dens, dropped.children, kept.dens, kept.children)
	}
}
