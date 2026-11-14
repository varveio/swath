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
	"testing"
	"unicode/utf8"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
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

// TestPlanTilesAsTilePrior: for random counted priors (ASCII, multi-byte
// scalars, cuts the provider vetoes, keys repeated across batches), resume
// keys (unsafe ones included) and reused spans, the plan made lazily is the
// tiling expandPrior makes at once from the same cuts at the same fill:
// tiles of whole pages ascending from the resume key's safe start, the open
// frontier last, each certificate's replay once and every gap around it.
func TestPlanTilesAsTilePrior(t *testing.T) {
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
		want = p.aroundReuse(want)
		same := func(a, b Range) bool {
			return a.kind == b.kind && a.lo == b.lo && a.hi == b.hi && a.open == b.open && a.after == b.after && a.est == b.est &&
				a.prior == b.prior && a.cert == b.cert && a.gapEnd == b.gapEnd
		}
		if !slices.EqualFunc(got, want, same) {
			t.Fatalf("%s: the plan made %d ranges, expandPrior's tiling %d:\n%+v\n%+v", what, len(got), len(want), got, want)
		}
		for _, r := range got {
			if !utf8.ValidString(r.lo) || (!r.open && r.kind != reused && !veto(r.hi)) {
				t.Fatalf("%s: unusable bound in %+v", what, r)
			}
		}
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
