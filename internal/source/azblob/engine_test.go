// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/keyrange/keyrangetest"
)

// The range engine's own tests (internal/source/keyrange), run with Azure's
// order behind the byte-order adapter: the engine is built from the
// provider's description over the provider's fetch, and the store is an
// azblobtest.Store in UTF-16 order. keyrange's tests cannot import the
// adapter (it imports keyrange), so they are restated here on the
// engine's public surface: the test hooks of keyrange.Options (SplitCoin,
// Resume) and the fencing emitter.

// azureEngine is a provider over st whose engine has the options given,
// as New builds it otherwise; its Expand carries an adapter per range.
func azureEngine(t *testing.T, st *azblobtest.Store, opt keyrange.Options) *Provider {
	t.Helper()
	p, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: st, Workers: max(1, opt.Workers)})
	if err != nil {
		t.Fatal(err)
	}
	e, err := keyrange.New(opt, p.description(), p.fetch)
	if err != nil {
		t.Fatal(err)
	}
	p.Engine = e
	return p
}

// utf16Keys holds sibling pairs of scalars in U+E000..U+FFFF and above
// U+FFFF at the root, under folders, and under folders named with them.
func utf16Keys() []string {
	scalars := []string{"a", "é", "", "ｚ", "�", "\U00010000", "\U0001F600"}
	var keys []string
	for _, p := range []string{"", "d/", "d/e/", "q\U00010000/"} {
		for _, x := range scalars {
			for _, y := range scalars {
				keys = append(keys, p+x+y, p+x+"/"+y)
			}
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// engineKeyspaces are keyrange's keyspaces, its hostile ones and
// utf16Keys; with -short, the ones with names at U+E000 and above or odd
// bytes.
func engineKeyspaces() map[string][]string {
	ks := keyrangetest.Keyspaces()
	for k, v := range keyrangetest.Hostile() {
		ks[k] = v
	}
	ks["utf16Pairs"] = utf16Keys()
	if testing.Short() {
		for k := range ks {
			if !slices.Contains([]string{"utf16Pairs", "supplementaryPlane", "maxScalarRun", "markersAndOddChars", "oneKey", "empty"}, k) {
				delete(ks, k)
			}
		}
	}
	for k, v := range ks {
		ks[k] = slices.Compact(slices.Sorted(slices.Values(v)))
	}
	return ks
}

func runRows(t *testing.T, ctx context.Context, p *Provider, workers int, sink order.Sink[Object]) error {
	t.Helper()
	_, err := order.Run[Range, Object](ctx, p, sink, &Schema, order.Options{Workers: workers, Budget: 1 << 22})
	return err
}

// TestAzureEmptyContinuingPages: empty continuing pages, at every other
// response and at the first, are followed inside the adapter's reads
// (order_empty_pages), sequentially and in a range; the listing is exact
// and the engine sees none; an empty container ends at its first page.
func TestAzureEmptyContinuingPages(t *testing.T) {
	keys := utf16Keys()
	for _, empty := range []func(int) bool{func(n int) bool { return n%2 == 0 }, func(n int) bool { return n == 0 }} {
		for _, seq := range []bool{true, false} {
			st := azblobtest.NewStore(keys)
			st.Short, st.Empty = func(int) int { return 3 }, empty
			p := azureEngine(t, st, keyrange.Options{Sequential: seq, Workers: 1, PageSize: 5})
			c := &collect{}
			if err := runRows(t, context.Background(), p, 1, c); err != nil {
				t.Fatal(err)
			}
			v := p.Stats().Counters.Value
			if got := keysOf(c.rows); !slices.Equal(got, keys) || v("order_empty_pages") == 0 || v("empty_pages") != 0 {
				t.Fatalf("sequential=%v: %d keys of %d, order_empty_pages %d, empty_pages %d", seq, len(got), len(keys),
					v("order_empty_pages"), v("empty_pages"))
			}
		}
	}
	st := azblobtest.NewStore(nil)
	st.Empty = func(int) bool { return true }
	c := &collect{}
	if err := runRows(t, context.Background(), azureEngine(t, st, keyrange.Options{Sequential: true, Workers: 1}), 1, c); err != nil || len(c.rows) != 0 || st.Attempts() != 1 {
		t.Fatalf("empty container: %v, %d rows, %d requests", err, len(c.rows), st.Attempts())
	}
}

// TestAzurePageSize: at page sizes 1, 7, 1,000 and 5,000, with full and
// short responses, sequential, 8 workers, seed with structure,
// and seed: the listing is exact and no request asks for more than the
// page size.
func TestAzurePageSize(t *testing.T) {
	keys := utf16Keys()
	keys = append(keys, "d1/", "d2/e3/")
	slices.Sort(keys)
	for _, page := range []int{1, 7, 1000, 5000} {
		for _, short := range []bool{false, true} {
			for i, opt := range []keyrange.Options{
				{Sequential: true},
				{Workers: 8},
				{Workers: 8, Seed: true, Structure: keyrange.StructureOn},
				{Workers: 8, Seed: true},
			} {
				what := fmt.Sprintf("page=%d short=%v opt %d", page, short, i)
				st := azblobtest.NewStore(keys)
				if short {
					st.Short = func(n int) int { return 1 + n%3 }
				}
				opt.PageSize = page
				p := azureEngine(t, st, opt)
				c := &collect{}
				if err := runRows(t, context.Background(), p, max(1, opt.Workers), c); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got := keysOf(c.rows); !slices.Equal(got, keys) {
					t.Fatalf("%s: %d keys, want %d", what, len(got), len(keys))
				}
				if v := p.Stats().Counters.Value("bound_drops"); v != 0 {
					t.Fatalf("%s: bound_drops %d", what, v)
				}
				for _, r := range st.Log() {
					if r.In.MaxResults > page {
						t.Fatalf("%s: a request for %d", what, r.In.MaxResults)
					}
				}
			}
		}
	}
}

// TestFetchDropsCoveredPrefixes: a delimiter page that fetch returns from
// a bound holds neither the prefix equal to it nor the one it lies in, at
// an ASCII and a high bound: the adapter's exclusive start leaves the
// engine nothing to drop. (Not a restatement of keyrange's
// TestInclusiveBoundDropsCoveredPrefixes, which tests the engine's drop of
// an inclusive provider's prefixes; with StartInclusive false that path is
// off, and TestAzurePageSize checks bound_drops stays zero through the
// engine.)
func TestFetchDropsCoveredPrefixes(t *testing.T) {
	keys := []string{"f05/a", "f06/a", "f06/x/y", "f07/a", "f08/a", "f/a", "f/b", "f\U00010000/a"}
	st := azblobtest.NewStore(keys)
	p := azureEngine(t, st, keyrange.Options{Workers: 1})
	a := p.adapter()
	defer a.Release()
	ctx := context.WithValue(context.Background(), adapterKey{}, a)
	for _, c := range []struct {
		after string
		want  []string
	}{
		{"f06/", []string{"f07/", "f08/", "f/", "f\U00010000/"}},
		{"f06/x", []string{"f07/", "f08/", "f/", "f\U00010000/"}},
		{"f/", []string{"f\U00010000/"}},
		{"f/a", []string{"f\U00010000/"}},
	} {
		q := keyrange.Query{After: c.after, Delimiter: "/", Max: 10, Probe: true}
		var got []string
		for {
			pg, err := p.fetch(ctx, &q)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, pg.Prefixes...)
			if !pg.Truncated {
				break
			}
			q.Token = pg.Token
		}
		if !slices.Equal(got, c.want) {
			t.Fatalf("after %q: prefixes %q, want %q", c.after, got, c.want)
		}
	}
}

// fenceEmitter records one range's emissions in order: rows, fences
// (source.Fencing) and children.
type fenceEmitter struct {
	events []fenceEvent
	spans  int
	spanLo string
}

type fenceEvent struct {
	kind  byte // 'r' row, 'f' fence, 'c' child
	key   string
	top   bool
	child Range
}

func (e *fenceEmitter) Rows(rows []Row) error {
	for _, r := range rows {
		e.events = append(e.events, fenceEvent{kind: 'r', key: r.Key})
	}
	return nil
}
func (e *fenceEmitter) Child(r Range) error {
	e.events = append(e.events, fenceEvent{kind: 'c', child: r})
	return nil
}
func (e *fenceEmitter) Span(lo, hi string, open bool) {
	e.spans++
	e.spanLo = lo
	e.events = append(e.events, fenceEvent{kind: 'f', key: lo})
}
func (e *fenceEmitter) Fence(x string, top bool) {
	e.events = append(e.events, fenceEvent{kind: 'f', key: x, top: top})
}

// drive expands r and its descendants depth first, holding every range
// that declares a span to keyrange's fence contract as far as its public
// surface shows it (the span first; every fence at or above the span's
// start and K, a bound the provider accepts, K or the span's start;
// fences ascending; no row at or below a fence before it, none above a
// fence after it; no fence after a child), and returns the rows in order
// and the replay ranges met.
func drive(t *testing.T, p *Provider, r Range, k string, rows *[]string, replays *[]Range) {
	t.Helper()
	e := &fenceEmitter{}
	if err := p.Expand(context.Background(), r, e); err != nil {
		t.Fatal(err)
	}
	if e.spans > 1 || e.spans == 1 && e.events[0].kind != 'f' {
		t.Fatalf("%d spans, first event %+v", e.spans, e.events[0])
	}
	lastFence, lastRow, seenChild, fenced := "", "", false, false
	for _, ev := range e.events {
		switch ev.kind {
		case 'f':
			if seenChild {
				t.Fatalf("a fence after a child")
			}
			if ev.top {
				continue
			}
			if ev.key < e.spanLo || ev.key < k || !(AcceptBound(ev.key) || ev.key == k || ev.key == e.spanLo) {
				t.Fatalf("fence %q (span %q, K %q)", ev.key, e.spanLo, k)
			}
			if fenced && ev.key < lastFence || lastRow != "" && lastRow > ev.key {
				t.Fatalf("fence %q after fence %q and row %q", ev.key, lastFence, lastRow)
			}
			lastFence, fenced = ev.key, true
		case 'r':
			if fenced && ev.key <= lastFence || lastRow != "" && ev.key <= lastRow {
				t.Fatalf("row %q after fence %q and row %q", ev.key, lastFence, lastRow)
			}
			lastRow = ev.key
			*rows = append(*rows, ev.key)
		case 'c':
			seenChild = true
		}
	}
	for _, ev := range e.events {
		if ev.kind != 'c' {
			continue
		}
		if _, ok := p.Replay(ev.child); ok {
			*replays = append(*replays, ev.child)
			continue
		}
		drive(t, p, ev.child, k, rows, replays)
	}
}

// coin is a seeded split coin (keyrange.Options.SplitCoin).
func coin(seed int64) func() bool {
	rng := rand.New(rand.NewSource(seed))
	var mu sync.Mutex
	return func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
}

// TestAzureFencesHoldTheContract: for every keyspace, random split coins,
// seeded and not, and resume keys, every range declares its span and
// fences as the durable spill needs them, and the listing is complete.
func TestAzureFencesHoldTheContract(t *testing.T) {
	spaces := engineKeyspaces()
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range spaces {
			if !yield(k) {
				return
			}
		}
	}) {
		keys := spaces[name]
		if len(keys) < 2 {
			continue
		}
		for s := range 6 {
			rng := rand.New(rand.NewSource(int64(s)))
			opt := keyrange.Options{SplitCoin: coin(int64(s)), Seed: s%2 == 1, Workers: 4}
			k := ""
			if s >= 2 {
				k = keys[rng.Intn(len(keys)-1)]
				opt.Resume = &keyrange.Resume{After: k}
			}
			st := azblobtest.NewStore(keys)
			st.Short = func(n int) int { return 1 + n%5 }
			p := azureEngine(t, st, opt)
			var rows []string
			var replays []Range
			drive(t, p, p.Root(), k, &rows, &replays)
			want := keys
			if k != "" {
				want = keys[slices.Index(keys, k)+1:]
			}
			if !slices.Equal(rows, want) {
				t.Fatalf("%s seed=%d K=%q: %d rows, want %d", name, s, k, len(rows), len(want))
			}
		}
	}
}

// TestAzureReuseTilesAroundSpans: a resume handed reused certificates
// emits one replay range per span, in order, and lists only the gaps: the
// listed rows are exactly those of (K, ⊤] outside the spans, and no data
// seek starts inside a span, with folders named at U+E000 and above.
func TestAzureReuseTilesAroundSpans(t *testing.T) {
	sub := []string{"sub", "s", "s\U00010000"}
	var keys []string
	for i := range 6000 {
		keys = append(keys, fmt.Sprintf("d%02d/%s/obj%06d", i%7, sub[i%3], i))
	}
	slices.Sort(keys)
	for _, seed := range []bool{false, true} {
		for _, kIdx := range []int{-1, 100, 2500} {
			k := ""
			if kIdx >= 0 {
				k = keys[kIdx]
			}
			spans := []source.Span{{Lo: keys[50], Hi: keys[900]}, {Lo: keys[3000], Hi: keys[3400]}, {Lo: keys[5000], HiOpen: true}}
			if kIdx >= 900 {
				spans = spans[1:]
			}
			st := azblobtest.NewStore(keys)
			opt := keyrange.Options{Seed: seed, SeedFirstPage: seed, Workers: 4}
			if k != "" {
				opt.Resume = &keyrange.Resume{After: k}
			}
			p := azureEngine(t, st, opt)
			p.Reuse(spans)
			var rows []string
			var replay []Range
			drive(t, p, p.Root(), k, &rows, &replay)
			in := func(x string) bool {
				for _, s := range spans {
					if x > s.Lo && (s.HiOpen || x <= s.Hi) {
						return true
					}
				}
				return false
			}
			var want []string
			for _, x := range keys {
				if x > k && !in(x) {
					want = append(want, x)
				}
			}
			what := fmt.Sprintf("seed=%v K=%q", seed, k)
			if !slices.Equal(rows, want) {
				t.Fatalf("%s: listed %d rows, want %d", what, len(rows), len(want))
			}
			if len(replay) != len(spans) {
				t.Fatalf("%s: %d replay ranges for %d spans", what, len(replay), len(spans))
			}
			for i, r := range replay {
				if cert, _ := p.Replay(r); cert != i {
					t.Fatalf("%s: replay range %d holds certificate %d", what, i, cert)
				}
			}
			for _, r := range st.Log() {
				start := r.In.StartFrom
				if r.In.Delimiter != "" || r.In.Marker != "" || start == "" {
					continue
				}
				for _, s := range spans {
					// As keyrange's test reads its starts: a start at
					// Hi lists only what follows the span.
					if start >= max(s.Lo, k) && (s.HiOpen || start < s.Hi) {
						t.Fatalf("%s: a data seek starts at %q inside (%q, %q]", what, start, s.Lo, s.Hi)
					}
				}
			}
		}
	}
}

// TestAzureResumeAfterKey: a resume after K, a key of the listing, with
// random split coins, seeded and not, lists exactly the keys after K,
// once each.
func TestAzureResumeAfterKey(t *testing.T) {
	seeds := 10
	if testing.Short() {
		seeds = 3
	}
	for name, keys := range engineKeyspaces() {
		if len(keys) < 2 {
			continue
		}
		for s := range seeds {
			k := keys[rand.New(rand.NewSource(int64(s))).Intn(len(keys)-1)]
			st := azblobtest.NewStore(keys)
			st.Short = func(n int) int { return 1 + n%7 }
			p := azureEngine(t, st, keyrange.Options{Resume: &keyrange.Resume{After: k}, SplitCoin: coin(int64(s)), Seed: s%2 == 0, Workers: 8})
			c := &collect{}
			if err := runRows(t, context.Background(), p, 8, c); err != nil {
				t.Fatalf("%s seed=%d: %v", name, s, err)
			}
			if got, want := keysOf(c.rows), keys[slices.Index(keys, k)+1:]; !slices.Equal(got, want) {
				t.Fatalf("%s seed=%d K=%q: resumed %d keys, want %d", name, s, k, len(got), len(want))
			}
		}
	}
}

// stopAfter is a sink that keeps the rows it is given and cancels the run
// once it holds n: a crash after a published prefix.
type stopAfter struct {
	collect
	n      int
	cancel context.CancelCauseFunc
}

var errCrash = errors.New("crashed")

func (s *stopAfter) Write(ctx context.Context, segs [][]Row) error {
	if err := s.collect.Write(ctx, segs); err != nil {
		return err
	}
	if s.mu.Lock(); len(s.rows) >= s.n {
		s.cancel(errCrash)
	}
	s.mu.Unlock()
	return nil
}

// TestAzureResumeAfterCrash: a seeded run with 8 workers is cancelled
// once it has published n rows (the cancel path); the journal of a crash
// there would hold the n-th row as its last key, so exactly the first n
// rows are taken as published, whatever more one sink batch carried. The
// resume after the n-th lists the rest: together every key once, in order.
// Every arm resumes (n is below the corpus's size).
func TestAzureResumeAfterCrash(t *testing.T) {
	keys := utf16Keys()
	for _, n := range []int{1, 50, 200, len(keys) - 1} {
		if n >= len(keys) {
			t.Fatalf("n=%d of %d keys: nothing to resume", n, len(keys))
		}
		st := azblobtest.NewStore(keys)
		st.Short = func(n int) int { return 1 + n%4 }
		ctx, cancel := context.WithCancelCause(context.Background())
		first := &stopAfter{n: n, cancel: cancel}
		err := runRows(t, ctx, azureEngine(t, st, keyrange.Options{Seed: true, Workers: 8, SplitCoin: coin(int64(n))}), 8, first)
		cancel(nil)
		if err != nil && !errors.Is(err, errCrash) && !errors.Is(err, context.Canceled) {
			t.Fatalf("n=%d: %v", n, err)
		}
		got := keysOf(first.rows)
		if len(got) < n || !slices.Equal(got, keys[:len(got)]) {
			t.Fatalf("n=%d: the first run published %d rows, not a prefix of at least n", n, len(got))
		}
		got = got[:n]
		second := &collect{}
		resumed := azblobtest.NewStore(keys)
		if err := runRows(t, context.Background(), azureEngine(t, resumed,
			keyrange.Options{Seed: true, Workers: 8, Resume: &keyrange.Resume{After: got[n-1]}}), 8, second); err != nil {
			t.Fatalf("n=%d resume: %v", n, err)
		}
		if resumed.Responses() == 0 || len(second.rows) == 0 {
			t.Fatalf("n=%d: the resume listed nothing (%d responses)", n, resumed.Responses())
		}
		if all := append(got, keysOf(second.rows)...); !slices.Equal(all, keys) {
			t.Fatalf("n=%d: %d + %d keys, want %d", n, len(got), len(second.rows), len(keys))
		}
	}
}

// TestAzureSplitsReadNothingTwice: with owner splits at every other page
// on a coin, at 8 workers and pages of 100, every item the service
// returned is an object listed once, a split cursor's own name (the seek
// after a listed name returns it first, and rule 1 drops it) or one read
// past a range's end (order_overlap_rows): one read per page leaves a
// split no buffered surplus for its children to read again. The split
// children's first pages end one item short (order_short_pages).
func TestAzureSplitsReadNothingTwice(t *testing.T) {
	var keys []string
	for i := range 20000 {
		keys = append(keys, fmt.Sprintf("d%02d/obj%06d", i%13, i))
	}
	slices.Sort(keys)
	st := azblobtest.NewStore(keys)
	p := azureEngine(t, st, keyrange.Options{Workers: 8, PageSize: 100, SplitCoin: coin(7)})
	c := &collect{}
	if err := runRows(t, context.Background(), p, 8, c); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(c.rows); !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	returned, cursors := 0, 0
	for _, r := range st.Log() {
		returned += len(r.Names)
		if r.In.StartFrom != "" && r.In.Marker == "" && len(r.Names) > 0 && r.Names[0] == r.In.StartFrom {
			cursors++
		}
	}
	v := p.Stats().Counters.Value
	if v("split.accepted_frontier")+v("split.accepted_bounded") == 0 || v("order_short_pages") == 0 {
		t.Fatalf("no split (%d, %d) or no short page (%d)", v("split.accepted_frontier"), v("split.accepted_bounded"), v("order_short_pages"))
	}
	if want := len(keys) + cursors + int(v("order_overlap_rows")); returned != want {
		t.Fatalf("the service returned %d items: %d keys, %d cursors and %d past ranges' ends make %d", returned, len(keys), cursors,
			v("order_overlap_rows"), want)
	}
}
