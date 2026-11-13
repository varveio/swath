// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
)

// script is a provider that answers a fixed chain of pages: the first
// request gets pages[0], a token "tN" gets pages[N]. A page's Token is its
// own field (a test sets a stuck one); a truncated page without one gets
// the next index.
type script struct {
	pages []Page[Object]
	calls int
}

func (s *script) fetch(_ context.Context, q *Query) (*Page[Object], error) {
	s.calls++
	i := 0
	if q.Token != "" {
		if _, err := fmt.Sscanf(q.Token, "t%d", &i); err != nil {
			return nil, err
		}
	}
	out := s.pages[i]
	out.Rows, out.Prefixes = slices.Clone(out.Rows), slices.Clone(out.Prefixes)
	switch {
	case out.Token == noToken:
		out.Token = ""
	case out.Truncated && out.Token == "":
		out.Token = fmt.Sprintf("t%d", i+1)
	}
	return &out, nil
}

// noToken in a scripted page's Token sends a truncated page with no token.
const noToken = "<none>"

func rows(keys ...string) []Row {
	out := make([]Row, len(keys))
	for i, k := range keys {
		out[i] = Row{Key: k}
	}
	return out
}

func more(keys ...string) Page[Object] { return Page[Object]{Rows: rows(keys...), Truncated: true} }
func last(keys ...string) Page[Object] { return Page[Object]{Rows: rows(keys...)} }

// TestEmptyContinuingPages: a continuing page with no entry is followed by
// its token in the range, sequential and probe paths; a terminal empty page
// ends the chain; a token that makes no progress is an invalid page.
func TestEmptyContinuingPages(t *testing.T) {
	cases := []struct {
		name      string
		inclusive bool
		after     string
		pages     []Page[Object]
		want      []string
		requests  int64
		empty     int64
		drops     int64 // bound_drops
		bad       bool
	}{
		{name: "empty, empty, data", pages: []Page[Object]{more(), more(), last("a", "b")}, want: []string{"a", "b"}, requests: 3, empty: 2},
		{name: "boundary only, empty, data", inclusive: true, after: "a", pages: []Page[Object]{more("a"), more(), last("b", "c")}, want: []string{"b", "c"}, requests: 3, empty: 2, drops: 1},
		{name: "empty, boundary and data", inclusive: true, after: "a", pages: []Page[Object]{more(), last("a", "b")}, want: []string{"b"}, requests: 2, empty: 1, drops: 1},
		{name: "data, empty, terminal empty", pages: []Page[Object]{more("a"), more(), last()}, want: []string{"a"}, requests: 3, empty: 1},
		{name: "terminal empty", pages: []Page[Object]{last()}, requests: 1},
		{name: "stuck token", pages: []Page[Object]{more("a"), {Truncated: true, Token: "t1"}}, bad: true},
		{name: "token cycle", pages: []Page[Object]{more(), {Truncated: true, Token: "t0"}}, bad: true},
		{name: "empty without token", pages: []Page[Object]{more("a"), {Truncated: true, Token: noToken}}, bad: true},
		{name: "first page empty without token", pages: []Page[Object]{{Truncated: true, Token: noToken}}, bad: true},
	}
	for _, c := range cases {
		for _, mode := range []string{"range", "sequential"} {
			t.Run(c.name+"/"+mode, func(t *testing.T) {
				s := &script{pages: c.pages}
				opt := Options{Workers: 1, Sequential: mode == "sequential"}
				if c.after != "" {
					opt.Resume = &Resume{After: c.after}
				}
				p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, StartInclusive: c.inclusive}, s.fetch)
				if err != nil {
					t.Fatal(err)
				}
				col := &collect{}
				r := Range{lo: c.after, open: true, id: 1} // one worker, no demand: no split
				if mode == "sequential" {
					r = p.Root()
				}
				err = p.Expand(context.Background(), r, &direct{col})
				if c.bad {
					if !errors.Is(err, ErrInvalidPage) {
						t.Fatalf("want an invalid page, got %v", err)
					}
					return
				}
				st := p.Stats()
				if err != nil || !slices.Equal(keysOf(col.rows), c.want) || st.Get(cRequests) != c.requests ||
					st.Get(cEmptyPages) != c.empty || int64(s.calls) != c.requests || st.Get(cBoundDrops) != c.drops ||
					st.Get(cOverfetchKeys) != 0 {
					t.Fatalf("err=%v keys=%q requests=%d calls=%d empty=%d drops=%d overfetch=%d", err, keysOf(col.rows),
						st.Get(cRequests), s.calls, st.Get(cEmptyPages), st.Get(cBoundDrops), st.Get(cOverfetchKeys))
				}
			})
		}
	}
}

// TestByteOrderIsAPrerequisite: the engine applies only to byte-ordered
// listings. The UTF-16 counterexample: a listing in UTF-16 code-unit
// order returns U+10000 (a surrogate pair, D800 DC00) before U+E000, the
// reverse of byte order; on two pages, no pivot veto or per-page sort can
// repair that. A provider declaring that order is refused at New, and the
// byte-order check still rejects the pages if one claims byte order.
func TestByteOrderIsAPrerequisite(t *testing.T) {
	pages := []Page[Object]{more("\U00010000"), last("")}
	utf16 := Listing
	utf16.Order = source.OrderOther
	if _, err := New(Options{Workers: 1}, Description[Object]{Target: "utf16", Listing: utf16}, (&script{pages: pages}).fetch); err == nil {
		t.Fatal("a UTF-16-ordered listing was accepted by the range engine")
	}
	for _, seq := range []bool{false, true} {
		p, err := New(Options{Workers: 1, Sequential: seq}, Description[Object]{Target: "test", Listing: Listing}, (&script{pages: pages}).fetch)
		if err != nil {
			t.Fatal(err)
		}
		r := Range{open: true, id: 1}
		if seq {
			r = p.Root()
		}
		if err := p.Expand(context.Background(), r, &direct{&collect{}}); !errors.Is(err, ErrInvalidPage) {
			t.Fatalf("sequential=%v: UTF-16 order across pages accepted: %v", seq, err)
		}
	}
}

// TestPageSize: one configured page size feeds the data and seed requests
// alike. At page sizes 1, 7, 1,000 and 5,000, with full and
// short continuing pages, every configuration lists the keyspace exactly;
// the sequential chain asks ceil(n / page) requests on full pages, and no
// request asks for more than the page size.
func TestPageSize(t *testing.T) {
	keys := slices.Compact(slices.Sorted(slices.Values(keyspaces()["deepTree"])))
	keys = append(keys, "d1/", "d2/e3/") // folder markers on seed cuts
	slices.Sort(keys)
	for _, page := range []int{1, 7, 1000, 5000} {
		for _, short := range []bool{false, true} {
			for i, opt := range []Options{
				{Sequential: true},
				{Workers: 8},
				{Workers: 8, Seed: true, Structure: StructureOn},
				{Workers: 8, Seed: true},
			} {
				what := fmt.Sprintf("page=%d short=%v opt %d", page, short, i)
				f := newSorted(keys, short, int64(page))
				var mu sync.Mutex
				over := 0
				fetch := fetchFrom(f)
				opt.PageSize = page
				p, err := New(opt, Description[Object]{Target: "test", Listing: Listing}, func(ctx context.Context, q *Query) (*Page[Object], error) {
					if q.Max > page {
						mu.Lock()
						over++
						mu.Unlock()
					}
					return fetch(ctx, q)
				})
				if err != nil {
					t.Fatal(err)
				}
				col := &collect{}
				if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: max(1, opt.Workers), Budget: 1 << 22}); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got := keysOf(col.rows); !slices.Equal(got, keys) {
					t.Fatalf("%s: %d keys, want %d", what, len(got), len(keys))
				}
				st := p.Stats()
				if mu.Lock(); over > 0 {
					t.Fatalf("%s: %d requests above the page size", what, over)
				}
				mu.Unlock()
				if want := int64((len(keys) + page - 1) / page); opt.Sequential && !short && st.Get(cRequests) != want {
					t.Fatalf("%s: %d requests, want %d", what, st.Get(cRequests), want)
				}
			}
		}
	}
}

// TestPrefixesNormalized: common prefixes that come unsorted or repeated
// are sorted and deduplicated once: the seed's tiles and the output equal
// those from a provider that returns them in order.
func TestPrefixesNormalized(t *testing.T) {
	keys := slices.Compact(slices.Sorted(slices.Values(keyspaces()["deepTree"])))
	keys = append(keys, "d1/", "d3/e0/")
	slices.Sort(keys)
	for _, opt := range []Options{{Seed: true}} {
		var tiles [2][]string
		for j, scramble := range []bool{false, true} {
			fetch := fetchFrom(newSorted(keys, false, 1))
			opt := opt
			opt.Workers = 1 // one range at a time: the tiling is the seed's
			opt.SplitCoin = func() bool { return false }
			var mu sync.Mutex // the drainer and a worker may both expand ranges
			opt.Trace = func(lo, hi string, open bool) {
				mu.Lock()
				tiles[j] = append(tiles[j], fmt.Sprintf("%q %q %v", lo, hi, open))
				mu.Unlock()
			}
			p, _ := New(opt, Description[Object]{Target: "test", Listing: Listing}, func(ctx context.Context, q *Query) (*Page[Object], error) {
				out, err := fetch(ctx, q)
				if err == nil && scramble && len(out.Prefixes) > 1 {
					s := slices.Clone(out.Prefixes)
					slices.Reverse(s)
					out.Prefixes = append(s, s[0], s[len(s)/2])
				}
				return out, err
			})
			col := &collect{}
			if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: 1, Budget: 1 << 22}); err != nil {
				t.Fatal(err)
			}
			if got := keysOf(col.rows); !slices.Equal(got, keys) {
				t.Fatalf("scramble=%v: %d keys, want %d", scramble, len(got), len(keys))
			}
			if st := p.Stats(); scramble != (st.Get(cPrefixesNormalized) > 0) {
				t.Fatalf("scramble=%v: normalized %d pages", scramble, st.Get(cPrefixesNormalized))
			}
		}
		slices.Sort(tiles[0])
		slices.Sort(tiles[1])
		if !slices.Equal(tiles[0], tiles[1]) {
			t.Fatalf("%+v: tiles differ:\n%q\n%q", opt, tiles[0], tiles[1])
		}
	}
}

// TestInclusiveBoundDropsCoveredPrefixes: a delimiter page from an
// inclusive start bound holds the common prefix equal to the bound, and the
// one the bound lies in (GCS answers "LC08/" for startOffset "LC08/" and
// "LC08/01", measured on gcp-public-data-landsat); S3's StartAfter returns
// neither (measured on noaa-hrrr-bdp-pds). The engine's bound is S3's, so
// both are dropped at an inclusive bound, as an equal key is, and counted;
// with an exclusive bound the page is left as the provider gave it.
func TestInclusiveBoundDropsCoveredPrefixes(t *testing.T) {
	page := Page[Object]{Prefixes: []string{"f05/", "f06/", "f07/", "f08/"}, Truncated: true}
	for _, c := range []struct {
		inclusive bool
		after     string
		want      []string
		drops     int64
	}{
		{true, "f06/", []string{"f07/", "f08/"}, 2},  // f05/ and the equal f06/
		{true, "f06/x", []string{"f07/", "f08/"}, 2}, // f05/ and f06/, which the bound lies in
		{false, "f06/", []string{"f05/", "f06/", "f07/", "f08/"}, 0},
	} {
		s := &script{pages: []Page[Object]{page}}
		p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, StartInclusive: c.inclusive}, s.fetch)
		if err != nil {
			t.Fatal(err)
		}
		in := p.query()
		in.After, in.Delimiter, in.Probe = c.after, "/", true
		out, err := p.list(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		if st := p.Stats(); !slices.Equal(out.Prefixes, c.want) || st.Get(cBoundDrops) != c.drops {
			t.Fatalf("inclusive=%v after=%q: prefixes %q, bound_drops %d; want %q, %d", c.inclusive, c.after, out.Prefixes, st.Get(cBoundDrops), c.want, c.drops)
		}
	}
}

// TestFreshEmptyTokensUseBoundedMemory: a service that answers every
// request with an empty continuing page and a token never sent before is
// followed until the context ends, holding only the latest tokens (before,
// the run kept every token of the chain: 40,000 pages of 2 KiB tokens held
// some 80 MB); a token repeated within the window is still an invalid page
// (durability review).
func TestFreshEmptyTokensUseBoundedMemory(t *testing.T) {
	const pages, tokenBytes = 40000, 2048
	pad := strings.Repeat("x", tokenBytes)
	var calls int
	var base, end runtime.MemStats
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	stop := errors.New("enough pages")
	fresh := func(_ context.Context, q *Query) (*Page[Object], error) {
		calls++
		switch calls {
		case 10:
			runtime.GC()
			runtime.ReadMemStats(&base)
		case pages:
			runtime.GC()
			runtime.ReadMemStats(&end)
			cancel(stop)
		}
		return &Page[Object]{Truncated: true, Token: fmt.Sprintf("%08d", calls) + pad}, nil
	}
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing}, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Expand(ctx, Range{open: true, id: 1}, &direct{&collect{}}); !errors.Is(err, stop) {
		t.Fatalf("want the context's cause, got %v", err)
	}
	if grew := int64(end.HeapAlloc) - int64(base.HeapAlloc); grew > 16<<20 {
		t.Fatalf("the heap grew by %d MiB over %d fresh empty pages", grew>>20, pages)
	}
	// A token sent again after emptyTokenWindow-1 others is a cycle.
	calls = 0
	cycle := func(_ context.Context, q *Query) (*Page[Object], error) {
		calls++
		return &Page[Object]{Truncated: true, Token: fmt.Sprintf("t%d", calls%emptyTokenWindow)}, nil
	}
	p, _ = New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing}, cycle)
	if err := p.Expand(context.Background(), Range{open: true, id: 1}, &direct{&collect{}}); !errors.Is(err, ErrInvalidPage) || calls != emptyTokenWindow+1 {
		t.Fatalf("a cycle within the window: %v after %d pages", err, calls)
	}
}

// TestEmptyChainFrontier: the empty continuing pages of a run count as
// key-space progress (Frontier) only up to EmptyChainLive; the pages past
// it are followed and counted in empty_chain_pages, so the watchdog can
// stop a service that answers forever with fresh tokens (review R3,
// 2026-11-06). A page that holds an entry ends the run: the next run
// counts again.
func TestEmptyChainFrontier(t *testing.T) {
	const extra = 100
	var calls int
	pages := func(_ context.Context, q *Query) (*Page[Object], error) {
		calls++
		switch {
		case calls <= EmptyChainLive+extra: // one run past the bound
			return &Page[Object]{Truncated: true, Token: fmt.Sprintf("a%d", calls)}, nil
		case calls == EmptyChainLive+extra+1:
			return &Page[Object]{Rows: []Row{{Key: "k1"}}, Truncated: true, Token: "after-k1"}, nil
		case calls <= EmptyChainLive+extra+11: // a short run
			return &Page[Object]{Truncated: true, Token: fmt.Sprintf("b%d", calls)}, nil
		}
		return &Page[Object]{Rows: []Row{{Key: "k2"}}}, nil
	}
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Expand(context.Background(), Range{open: true, id: 1}, &direct{&collect{}}); err != nil {
		t.Fatal(err)
	}
	// Frontier: the first run's bounded pages, its page with k1, the short
	// run and the last page.
	advanced, chained := p.Frontier()
	st := p.Stats()
	if want := int64(EmptyChainLive + 1 + 10 + 1); advanced != want || chained != extra || st.Get(cEmptyChainPages) != extra {
		t.Fatalf("frontier %d, empty_chain_pages %d; want %d, %d", advanced, chained, want, extra)
	}
}

// TestCancelDuringRequeueBackoff: a stop (a signal, --max-duration, the
// stall timeout) that lands while a worker waits out the backoff before a
// requeued page is asked again ends the wait at once with the stop's
// cause, in a range and in the sequential chain. The backoff is an hour,
// so a wait that ignored the context (a plain sleep) would turn a graceful
// stop into the command's forced exit; the test's own deadline catches it.
func TestCancelDuringRequeueBackoff(t *testing.T) {
	defer SetRequeueBackoff(time.Hour, time.Hour)()
	exhausted := errors.New("503 SlowDown: the client's retries exhausted")
	for _, mode := range []string{"range", "sequential"} {
		t.Run(mode, func(t *testing.T) {
			var fetches atomic.Int64
			fetch := func(context.Context, *Query) (*Page[Object], error) {
				fetches.Add(1)
				return nil, exhausted
			}
			p, err := New(Options{Workers: 1, Sequential: mode == "sequential"},
				Description[Object]{Target: "test", Listing: Listing, Requeue: func(err error) bool { return errors.Is(err, exhausted) }}, fetch)
			if err != nil {
				t.Fatal(err)
			}
			r := Range{open: true, id: 1}
			if mode == "sequential" {
				r = p.Root()
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			done := make(chan error, 1)
			go func() { done <- p.Expand(ctx, r, &direct{&collect{}}) }()
			// requeues is counted after the decision to wait and before the
			// wait: once it is, the stop below lands in the backoff.
			for p.Set().Load(cRequeues) == 0 {
				select {
				case err := <-done:
					t.Fatalf("Expand returned before requeueing: %v", err)
				default:
					runtime.Gosched()
				}
			}
			stop := errors.New("terminated")
			cancel(stop)
			select {
			case err := <-done:
				if !errors.Is(err, stop) {
					t.Fatalf("Expand: %v, want the stop's cause", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a stop during the requeue backoff did not end the wait")
			}
			if n := fetches.Load(); n != 1 {
				t.Fatalf("%d requests, want 1: the page was asked again after the stop", n)
			}
		})
	}
}
