// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange/keyrangetest"
)

// stream is a counted prior read lazily, as a dataset's page index is: its
// cuts in batches of random sizes from a goroutine one batch ahead, each
// cut split into pieces repeating its key (a version group across key
// pages), a piece possibly in the next batch. failAfter > 0 fails the
// stream after that many batches.
type stream struct {
	cuts      []source.Cut
	seed      int64
	failAfter int
	failErr   error
	ch        chan *source.CutBatch
	end       error
}

func (s *stream) Start(ctx context.Context, after string, wake func()) func() {
	s.ch = make(chan *source.CutBatch, 1)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer wake()
		defer close(s.ch)
		rng := rand.New(rand.NewSource(s.seed))
		var pieces []source.Cut
		for _, c := range s.cuts {
			for n := 1 + rng.Intn(3); n > 1 && c.Rows > 1; n-- {
				part := rng.Int63n(c.Rows)
				pieces = append(pieces, source.Cut{Key: c.Key, Rows: part})
				c.Rows -= part
			}
			pieces = append(pieces, c)
		}
		for sent := 0; len(pieces) > 0; sent++ {
			if s.failAfter > 0 && sent == s.failAfter {
				s.end = s.failErr
				return
			}
			n := min(len(pieces), 1+rng.Intn(7))
			select {
			case s.ch <- &source.CutBatch{Cuts: pieces[:n:n], Part: true, Indexed: true}:
				wake()
			case <-ctx.Done():
				s.end = ctx.Err()
				return
			}
			pieces = pieces[n:]
		}
		s.end = io.EOF
	}()
	return func() { cancel(); <-done }
}

func (s *stream) Next() (*source.CutBatch, error) {
	select {
	case b, ok := <-s.ch:
		if !ok {
			return nil, s.end
		}
		return b, nil
	default:
		return nil, nil
	}
}

// planOf drains p's plan, admitting every tile (as a window that holds
// everything would).
func planOf(t *testing.T, p *Engine[Object]) []Range {
	t.Helper()
	pl := p.Planner()
	if pl == nil {
		t.Fatal("no plan")
	}
	wake := make(chan struct{}, 1)
	stop := pl.Start(context.Background(), func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	defer stop()
	var out []Range
	for {
		r, ok, done, err := pl.Next(func(int64) bool { return true })
		switch {
		case err != nil:
			t.Fatal(err)
		case done:
			return out
		case ok:
			out = append(out, r)
		default:
			<-wake
		}
	}
}

// iv is an interval (lo, hi], or (lo, ⊤] when open.
type iv struct {
	lo, hi string
	open   bool
}

// minus is the parts of t outside spans (ascending, disjoint), in order:
// plain interval arithmetic, the oracle of the gaps a tile lists around
// reused certificates.
func minus(t iv, spans []iv) []iv {
	var out []iv
	cur := t.lo
	for _, s := range spans {
		if !t.open && s.lo >= t.hi {
			break // this span and the later ones start past the tile
		}
		if !s.open && s.hi <= cur {
			continue // ends before what is left of the tile
		}
		if s.lo > cur {
			out = append(out, iv{cur, s.lo, false})
		}
		if s.open || (!t.open && s.hi >= t.hi) {
			return out // the span covers the rest of the tile
		}
		cur = s.hi
	}
	return append(out, iv{cur, t.hi, t.open})
}

// inside counts the tile bounds strictly inside s: a certificate across
// n+1 tiles holds n of them.
func inside(s iv, bounds []string) int {
	n := 0
	for _, b := range bounds {
		if b > s.lo && (s.open || b < s.hi) {
			n++
		}
	}
	return n
}

// TestPlanTilesAsTilePrior: for random counted priors (ASCII, multi-byte
// scalars, cuts the provider vetoes, keys repeated across batches), resume
// keys (unsafe ones included) and reused spans, the plan made lazily is the
// tiling expandPrior makes at once from the same cuts at the same fill
// (tiles of whole pages ascending from the resume key's safe start, the
// open frontier last); with reused spans, each certificate is replayed
// once, and the ranges listed are exactly the gaps an interval oracle
// leaves of each tile, spans across several tiles included.
func TestPlanTilesAsTilePrior(t *testing.T) {
	across := 0 // spans across three tiles or more
	alpha := []string{"a", "b", "z", "/", "0", "9", "é", "日", "\U0001F600", "5"}
	veto := func(s string) bool { return Safe(s) && !strings.Contains(s, "5") }
	for seed := range 300 {
		rng := rand.New(rand.NewSource(int64(seed)))
		scope := []string{"", "p/", "日/"}[seed%3]
		var keys []string
		for range 1 + rng.Intn(200) {
			var b strings.Builder
			b.WriteString(scope)
			for range 1 + rng.Intn(6) {
				b.WriteString(alpha[rng.Intn(len(alpha))])
			}
			keys = append(keys, b.String())
		}
		keys = slices.Compact(slices.Sorted(slices.Values(keys)))
		pr := &Prior{Counted: true, Scope: scope}
		for _, k := range keys {
			pr.Cuts = append(pr.Cuts, Cut{Key: k, Rows: int64(rng.Intn(5000))})
		}
		opt := Options{Prefix: scope, Workers: 4, PageSize: 1 + rng.Intn(1000), TilePages: 1 + rng.Intn(40)}
		if seed%4 == 1 {
			opt.Resume = &Resume{After: keys[rng.Intn(len(keys))]}
		}
		var spans []source.Span
		// Certificates' bounds are accepted bounds.
		bounds := slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return !veto(k) })
		if seed%3 == 2 && len(bounds) > 8 {
			// Two spans, each across several tiles; the second may be open.
			i := slices.Sorted(slices.Values(rng.Perm(len(bounds))[:4]))
			spans = []source.Span{{Lo: bounds[i[0]], Hi: bounds[i[1]]}, {Lo: bounds[i[2]], Hi: bounds[i[3]], HiOpen: rng.Intn(2) == 0}}
			if opt.Resume != nil {
				spans = slices.DeleteFunc(spans, func(s source.Span) bool { return !s.HiOpen && s.Hi <= opt.Resume.After })
			}
		}
		what := fmt.Sprintf("seed %d page %d tile pages %d resume %v spans %v", seed, opt.PageSize, opt.TilePages, opt.Resume, spans)
		build := func(lazy bool) *Engine[Object] {
			p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, AcceptBound: veto}, fetchFrom(newSorted(nil, false, 1)))
			if err != nil {
				t.Fatal(err)
			}
			q := *pr
			if lazy {
				q.Stream = &stream{cuts: pr.Cuts, seed: int64(seed)}
			}
			if err := p.SeedFrom(&q); err != nil {
				t.Fatal(err)
			}
			p.Reuse(spans)
			return p
		}
		got := planOf(t, build(true))
		// The whole tiling at once, as expandPrior cuts it, but at the
		// plan's fill and without its cap on the tile count.
		p := build(false)
		after := p.resumeKey()
		tiles, _ := tilePrior(pr, 1<<30, float64(opt.TilePages*opt.PageSize-1), after, scope, veto, p.safeStart)
		var want []Range
		lo := after
		for _, tl := range tiles {
			want = append(want, Range{lo: lo, hi: tl.hi, est: tl.est, estOK: true, prior: true})
			lo = tl.hi
		}
		want = append(want, Range{lo: lo, open: true})
		if s := p.safeStart(after); s != after {
			want[0].lo, want[0].after = s, after
		}
		same := func(a, b Range) bool {
			return a.kind == b.kind && a.lo == b.lo && a.hi == b.hi && a.open == b.open && a.after == b.after && a.est == b.est &&
				a.prior == b.prior && a.cert == b.cert && a.gapEnd == b.gapEnd
		}
		if len(spans) == 0 && !slices.EqualFunc(got, want, same) {
			t.Fatalf("%s: the plan made %d ranges, expandPrior's tiling %d:\n%+v\n%+v", what, len(got), len(want), got, want)
		}
		if len(spans) > 0 {
			// The oracle: the certificates above K, and each tile less them.
			var certs []iv
			for _, sp := range spans {
				certs = append(certs, iv{max(sp.Lo, after), sp.Hi, sp.HiOpen})
			}
			var bounds []string
			var wantGaps []iv
			for _, w := range want {
				wantGaps = append(wantGaps, minus(iv{max(w.lo, w.after), w.hi, w.open}, certs)...)
				if !w.open {
					bounds = append(bounds, w.hi)
				}
			}
			var gotCerts, gotGaps []iv
			for _, r := range got {
				if r.kind == reused {
					gotCerts = append(gotCerts, iv{r.lo, r.hi, r.open})
				} else {
					gotGaps = append(gotGaps, iv{max(r.lo, r.after), r.hi, r.open})
				}
			}
			if !slices.Equal(gotCerts, certs) || !slices.Equal(gotGaps, wantGaps) {
				t.Fatalf("%s: replayed %v, want %v; listed %v, want %v", what, gotCerts, certs, gotGaps, wantGaps)
			}
			for _, c := range certs {
				if inside(c, bounds) >= 2 {
					across++
				}
			}
		}
		for _, r := range got {
			if !utf8.ValidString(r.lo) || (!r.open && r.kind != reused && !veto(r.hi)) {
				t.Fatalf("%s: unusable bound in %+v", what, r)
			}
		}
	}
	if across < 10 {
		t.Fatalf("%d certificates across three tiles or more: the oracle was not exercised", across)
	}
}

// staleKeys is keys after a source changed since its prior: the keys in
// (grownLo, grownHi] ten times over, none in (goneLo, goneHi), and keys
// before the first and after the last.
func staleKeys(keys []string, grownLo, grownHi, goneLo, goneHi string) []string {
	var out []string
	for _, k := range keys {
		switch {
		case k > grownLo && k <= grownHi:
			for d := range 10 {
				out = append(out, fmt.Sprintf("%s%d", k, d))
			}
		case k > goneLo && k < goneHi:
		default:
			out = append(out, k)
		}
	}
	out = append(out, "!before", keys[len(keys)-1]+"~after", "~~past")
	return slices.Compact(slices.Sorted(slices.Values(out)))
}

// TestPlanListsEveryKey: a planned run lists every key once, in order, and
// its ranges tile the scope, on every fixture keyspace, with a prior read
// lazily that is fresh or stale (a region grown tenfold, one deleted, keys
// before its first and past its last), cuts the provider vetoes and a
// resume after a vetoed key, at 1, 4 and 64 workers, under a window that
// holds a few tiles (a small reorder budget) or all of them.
func TestPlanListsEveryKey(t *testing.T) {
	veto := func(s string) bool { return Safe(s) && !strings.Contains(s, "5") }
	for name, raw := range keyspaces() {
		keys := slices.Compact(slices.Sorted(slices.Values(raw)))
		if len(keys) < 20 {
			continue
		}
		n := len(keys)
		priors := map[string][]string{"fresh": keys, "stale": staleKeys(keys, keys[n/5], keys[n/4], keys[n/2], keys[2*n/3])}
		for pname, listed := range priors {
			for s, w := range []int{1, 4, 64} {
				for _, budget := range []int{40, 1 << 16} {
					what := fmt.Sprintf("%s/%s/w%d/budget%d", name, pname, w, budget)
					pr := priorOf(keys, 1+n/50)
					pr.Stream = &stream{cuts: pr.Cuts, seed: int64(s)}
					opt := Options{Prior: pr, PageSize: 7, TilePages: 2}
					got, _, leaves := runPolicy(t, newSorted(listed, true, int64(s+1)), opt, w, budget, veto)
					if !slices.Equal(got, listed) {
						t.Fatalf("%s: %d keys, want %d", what, len(got), len(listed))
					}
					checkTiling(t, what, leaves)
					var vetoed []string
					for _, k := range listed[:len(listed)-1] {
						if !veto(k) {
							vetoed = append(vetoed, k)
						}
					}
					if len(vetoed) > 0 {
						k := vetoed[s%len(vetoed)]
						opt.Resume = &Resume{After: k}
						pr.Stream = &stream{cuts: pr.Cuts, seed: int64(s)}
						got, _, _ := runPolicy(t, newSorted(listed, true, int64(s+1)), opt, w, budget, veto)
						if want := listed[slices.Index(listed, k)+1:]; !slices.Equal(got, want) {
							t.Fatalf("%s: resumed after %q (vetoed): %d keys, want %d", what, k, len(got), len(want))
						}
					}
				}
			}
		}
	}
}

// TestPlanFailureFailsTheRun: a prior whose reader fails mid-run, or whose
// late cut is invalid (outside the scope, descending, a negative count),
// fails the run with that error; the listing never ends early as if the
// plan were complete.
func TestPlanFailureFailsTheRun(t *testing.T) {
	var keys []string
	for i := range 3000 {
		keys = append(keys, fmt.Sprintf("k%05d", i))
	}
	readErr := errors.New("the prior's part could not be read")
	for _, tc := range []struct {
		name string
		edit func(pr *Prior, s *stream)
		want error
	}{
		{"reader", func(_ *Prior, s *stream) { s.failAfter, s.failErr = 4, readErr }, readErr},
		{"outside", func(pr *Prior, _ *stream) { pr.Cuts[20].Key = "z/outside" }, source.ErrPrior},
		{"descending", func(pr *Prior, _ *stream) { pr.Cuts[20].Key = "k00000" }, source.ErrPrior},
		{"negative", func(pr *Prior, _ *stream) { pr.Cuts[20].Rows = -1 }, source.ErrPrior},
	} {
		pr := priorOf(keys, 50)
		pr.Scope = "k"
		s := &stream{seed: 1}
		tc.edit(pr, s)
		s.cuts, pr.Cuts = pr.Cuts, nil // read only as the run goes, as a dataset's
		pr.Stream = s
		p, err := newTest(newSorted(keys, true, 1), Options{Prefix: "k", Workers: 4, PageSize: 10, TilePages: 2})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.SeedFrom(pr); err != nil {
			t.Fatal(err)
		}
		c := &collect{}
		st, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: 4, Budget: 1 << 20})
		if !errors.Is(err, tc.want) || !st.ListingDone.IsZero() {
			t.Fatalf("%s: %v (listing done %v), want %v", tc.name, err, st.ListingDone, tc.want)
		}
		if got := keysOf(c.rows); !slices.Equal(got, keys[:len(got)]) {
			t.Fatalf("%s: %d keys handed on, not a prefix", tc.name, len(got))
		}
		if p.Stats().Counters.Value(Counters[cSeedFromPlanErrors].Key) != 1 {
			t.Fatalf("%s: plan errors not counted", tc.name)
		}
	}
}

// TestPlanSplitsBehindTheWindow: a coarse counted cut file (each cut 200
// pages) planned under a window that holds less than one tile: the workers
// the window holds back are idle demand, and the admitted tile splits for
// them (the tiles not yet admitted are not queued work); every key listed
// once.
func TestPlanSplitsBehindTheWindow(t *testing.T) {
	var keys []string
	for i := range 40000 {
		keys = append(keys, fmt.Sprintf("logs/%06d", i))
	}
	pr := priorOf(keys, 2000) // 20 tiles of 200 pages of 10
	got, st, leaves := runBudget(t, newSorted(keys, true, 1), Options{Prior: pr, PageSize: 10, PriorPageSize: 10, TilePages: 16}, 8, 4)
	if !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	checkTiling(t, "coarse", leaves)
	if st.Get(cSeedFromTiles) != 21 || st.Get(cSplitAcceptedBounded) == 0 {
		t.Fatalf("tiles %d, bounded splits %d", st.Get(cSeedFromTiles), st.Get(cSplitAcceptedBounded))
	}
}

// sampled is a lister that samples, at every request, the prior tile
// accounts its engine holds.
type sampled struct {
	*sorted
	p    *Engine[Object]
	most int
}

func (s *sampled) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	s.p.lock()
	s.most = max(s.most, len(s.p.tiles))
	s.p.unlock()
	return s.sorted.List(ctx, in)
}

// TestPlanAccountsStayBounded: a planned run of thousands of tiles holds an
// account only for the tiles whose ranges still list (each retired into the
// prior's error counters when its last range ends), so what the engine
// retains, and what Stats scans, stays near the workers however many tiles
// the prior makes; every tile is still counted once.
func TestPlanAccountsStayBounded(t *testing.T) {
	var keys []string
	for i := range 30000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	pr := priorOf(keys, 10)
	pr.Stream = &stream{cuts: pr.Cuts, seed: 1}
	const workers = 8
	api := &sampled{sorted: newSorted(keys, true, 1)}
	p, err := newTest(api, Options{Prior: pr, Workers: workers, PageSize: 10, TilePages: 1})
	if err != nil {
		t.Fatal(err)
	}
	api.p = p
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(c.rows); !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	st := p.Stats()
	tiles := st.Get(cSeedFromTiles) - 1 // the open frontier has no account
	counted := st.Get(cSeedFromErrWithin) + st.Get(cSeedFromErrGrown) + st.Get(cSeedFromErrShrunk)
	if tiles != 3000 || counted != tiles || len(p.tiles) != 0 || api.most > 4*workers {
		t.Fatalf("%d tiles, %d classified, %d accounts left, at most %d held at once", tiles, counted, len(p.tiles), api.most)
	}
}

// counting is a lister that counts its requests.
type counting struct {
	*sorted
	n atomic.Int64
}

func (c *counting) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	c.n.Add(1)
	return c.sorted.List(ctx, in)
}

// TestPlanManyWorkersFewTiles: far more workers than the frontier window's
// tiles can use (256 workers, a window of a few tiles of three pages): the
// workers held at the window are not demand for slicing the tiles admitted
// (split.skipped.window), so the run sends about the requests one worker
// does, with few empty children, and lists every key once, in order. Before,
// each admitted tile was split into slivers below a page for them.
func TestPlanManyWorkersFewTiles(t *testing.T) {
	var keys []string
	for i := range 40000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	// Spill on, as in the command, so no worker idles on a full budget
	// (that idle demand is a cold run's too): only the window holds them.
	run := func(workers int) (int64, Stats) {
		api := &counting{sorted: newSorted(keys, true, 1)}
		var mu sync.Mutex
		var leaves []span
		opt := Options{Prior: priorOf(keys, 100), PageSize: 100, PriorPageSize: 100, TilePages: 4, Workers: workers,
			Trace: func(lo, hi string, open bool) { mu.Lock(); leaves = append(leaves, span{lo, hi, open}); mu.Unlock() }}
		p, err := newTest(api, opt)
		if err != nil {
			t.Fatal(err)
		}
		c := &collect{}
		if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 16 << 10,
			SpillDir: t.TempDir(), SpillMax: 16 << 10}); err != nil {
			t.Fatal(err)
		}
		if got := keysOf(c.rows); !slices.Equal(got, keys) {
			t.Fatalf("w%d: %d keys, want %d", workers, len(got), len(keys))
		}
		checkTiling(t, fmt.Sprintf("w%d", workers), leaves)
		return api.n.Load(), p.Stats()
	}
	one, _ := run(1)
	many, st := run(256)
	empty := st.Get(cChildEmptyLower) + st.Get(cChildEmptyUpper)
	t.Logf("requests: 1 worker %d, 256 workers %d; splits %d, empty children %d, skipped for the window %d", one, many,
		st.Get(cSplitAcceptedBounded), empty, st.Get(cSplitSkippedWindow))
	if float64(many) > 1.03*float64(one) || empty > 10 || st.Get(cSplitSkippedWindow) == 0 {
		t.Fatalf("256 workers: %d requests against %d, %d empty children, %d splits skipped for the window", many, one, empty, st.Get(cSplitSkippedWindow))
	}
}
