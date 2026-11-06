// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
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
	if out.Truncated && out.Token == "" {
		out.Token = fmt.Sprintf("t%d", i+1)
	}
	return &out, nil
}

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
		bad       bool
	}{
		{name: "empty, empty, data", pages: []Page[Object]{more(), more(), last("a", "b")}, want: []string{"a", "b"}, requests: 3, empty: 2},
		{name: "boundary only, empty, data", inclusive: true, after: "a", pages: []Page[Object]{more("a"), more(), last("b", "c")}, want: []string{"b", "c"}, requests: 3, empty: 2},
		{name: "data, empty, terminal empty", pages: []Page[Object]{more("a"), more(), last()}, want: []string{"a"}, requests: 3, empty: 1},
		{name: "terminal empty", pages: []Page[Object]{last()}, requests: 1},
		{name: "stuck token", pages: []Page[Object]{more("a"), {Truncated: true, Token: "t1"}}, bad: true},
		{name: "token cycle", pages: []Page[Object]{more(), {Truncated: true, Token: "t0"}}, bad: true},
		{name: "empty without token", pages: []Page[Object]{more("a"), {Truncated: true, Token: ""}, last()}, want: []string{"a"}, requests: 3, empty: 1},
	}
	cases[len(cases)-1].pages[1] = Page[Object]{Truncated: true} // the script assigns t2: progress
	for _, c := range cases {
		for _, mode := range []string{"range", "sequential"} {
			t.Run(c.name+"/"+mode, func(t *testing.T) {
				s := &script{pages: c.pages}
				opt := Options{Workers: 1, Sequential: mode == "sequential"}
				if c.after != "" {
					opt.Resume = &Resume{After: c.after}
				}
				p, err := New(opt, Description[Object]{Target: "test", StartInclusive: c.inclusive}, s.fetch)
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
					st.Get(cEmptyPages) != c.empty || int64(s.calls) != c.requests {
					t.Fatalf("err=%v keys=%q requests=%d calls=%d empty=%d", err, keysOf(col.rows), st.Get(cRequests), s.calls, st.Get(cEmptyPages))
				}
			})
		}
	}
	// The truncated page with no entry no longer fails a range.
	s := &script{pages: []Page[Object]{more(), last()}}
	p, _ := New(Options{Workers: 1}, Description[Object]{Target: "test"}, s.fetch)
	if err := p.Expand(context.Background(), Range{open: true, id: 1}, &direct{&collect{}}); err != nil {
		t.Fatal(err)
	}
}

// TestInclusiveOneKeyProbe: a 1-key probe of an inclusive provider whose
// first page holds only the pivot itself follows the token: an empty page,
// then the key past the pivot; the split is taken.
func TestInclusiveOneKeyProbe(t *testing.T) {
	s := &script{pages: []Page[Object]{more("m"), more(), more("n")}}
	p, _ := New(Options{Workers: 1, Probe: true}, Description[Object]{Target: "test", StartInclusive: true}, s.fetch)
	m, ok, err := p.probe(context.Background(), Range{lo: "a", hi: "z"}, "c", "m")
	st := p.Stats()
	if err != nil || !ok || m != "m" || s.calls != 3 || st.Get(cRequests) != 3 || st.Get(cProbeRequests) != 1 || st.Get(cEmptyPages) != 2 {
		t.Fatalf("m=%q ok=%v err=%v calls=%d %+v", m, ok, err, s.calls, st.Counters)
	}
	// The key past the pivot lies beyond hi: the upper child is empty.
	s = &script{pages: []Page[Object]{more("m"), more(), last("zz")}}
	p, _ = New(Options{Workers: 1, Probe: true}, Description[Object]{Target: "test", StartInclusive: true}, s.fetch)
	_, _, err = p.probe(context.Background(), Range{lo: "a", hi: "z"}, "l", "m")
	if st = p.Stats(); err != nil || st.Get(cProbeUpperEmpty) == 0 || s.calls < 3 {
		t.Fatalf("empty upper child: err=%v calls=%d %+v", err, s.calls, st.Counters)
	}
}
