// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source"
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
	out.Rows = slices.Clone(out.Rows)
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
		{name: "empty with an assigned token", pages: []Page[Object]{more("a"), {Truncated: true, Token: ""}, last()}, want: []string{"a"}, requests: 3, empty: 1},
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
	// The truncated page with no entry no longer fails a range.
	s := &script{pages: []Page[Object]{more(), last()}}
	p, _ := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing}, s.fetch)
	if err := p.Expand(context.Background(), Range{open: true, id: 1}, &direct{&collect{}}); err != nil {
		t.Fatal(err)
	}
}

// TestInclusiveOneKeyProbe: a 1-key probe of an inclusive provider whose
// first page holds only the pivot itself follows the token: an empty page,
// then the key past the pivot; the split is taken.
func TestInclusiveOneKeyProbe(t *testing.T) {
	s := &script{pages: []Page[Object]{more("m"), more(), more("n")}}
	p, _ := New(Options{Workers: 1, Probe: true}, Description[Object]{Target: "test", Listing: Listing, StartInclusive: true}, s.fetch)
	m, ok, err := p.probe(context.Background(), Range{lo: "a", hi: "z"}, "c", "m")
	st := p.Stats()
	if err != nil || !ok || m != "m" || s.calls != 3 || st.Get(cRequests) != 3 || st.Get(cProbeRequests) != 1 || st.Get(cEmptyPages) != 2 {
		t.Fatalf("m=%q ok=%v err=%v calls=%d %+v", m, ok, err, s.calls, st.Counters)
	}
	// The key past the pivot lies beyond hi: the upper child is empty.
	s = &script{pages: []Page[Object]{more("m"), more(), last("zz")}}
	p, _ = New(Options{Workers: 1, Probe: true}, Description[Object]{Target: "test", Listing: Listing, StartInclusive: true}, s.fetch)
	_, _, err = p.probe(context.Background(), Range{lo: "a", hi: "z"}, "l", "m")
	if st = p.Stats(); err != nil || st.Get(cProbeUpperEmpty) == 0 || s.calls < 3 {
		t.Fatalf("empty upper child: err=%v calls=%d %+v", err, s.calls, st.Counters)
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
// and the floors. At page sizes 1, 7, 1,000 and 5,000, with full and
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
				{Workers: 8, Seed: true, SeedDescent: true, Floors: true, Structure: true},
				{Workers: 8, Seed: true, Probe: true},
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
// are sorted and deduplicated once, for the seed page and the seed
// descent alike: the tiles and the output equal those from a provider
// that returns them in order.
func TestPrefixesNormalized(t *testing.T) {
	keys := slices.Compact(slices.Sorted(slices.Values(keyspaces()["deepTree"])))
	keys = append(keys, "d1/", "d3/e0/")
	slices.Sort(keys)
	for _, opt := range []Options{{Seed: true}, {Seed: true, SeedDescent: true}} {
		var tiles [2][]string
		for j, scramble := range []bool{false, true} {
			fetch := fetchFrom(newSorted(keys, false, 1))
			opt := opt
			opt.Workers = 1 // one range at a time: the tiling is the seed's
			opt.SplitCoin = func() bool { return false }
			opt.Trace = func(lo, hi string, open bool) { tiles[j] = append(tiles[j], fmt.Sprintf("%q %q %v", lo, hi, open)) }
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
