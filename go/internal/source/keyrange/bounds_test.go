// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
)

// inclusive is a fake listing whose start bound is inclusive (keys >= After)
// and which honours an exclusive end bound (keys < end): the other bound
// convention. It records every bound it was sent.
type inclusive struct {
	keys     []string
	bounded  bool // send ExclusiveEnd(Through) as the end bound
	pageCap  int
	mu       sync.Mutex
	starts   []string
	ends     []string
	requests int
}

func (f *inclusive) fetch(_ context.Context, q *Query) (*Page[Object], error) {
	f.mu.Lock()
	f.requests++
	end := ""
	if f.bounded && q.Bounded {
		end = ExclusiveEnd(q.Through)
	}
	if q.Token == "" {
		f.starts = append(f.starts, q.After)
		if end != "" {
			f.ends = append(f.ends, q.Through)
		}
	}
	f.mu.Unlock()
	i := sort.SearchStrings(f.keys, q.Prefix)
	if q.Token != "" {
		i, _ = strconv.Atoi(q.Token)
	} else if q.After != "" {
		i = max(i, sort.SearchStrings(f.keys, q.After)) // inclusive
	}
	size := q.Max
	if f.pageCap > 0 {
		size = min(size, f.pageCap)
	}
	out := &Page[Object]{}
	n := 0
	for ; i < len(f.keys) && strings.HasPrefix(f.keys[i], q.Prefix) && (end == "" || f.keys[i] < end); i++ {
		if n == size {
			out.Truncated, out.Token = true, strconv.Itoa(i)
			break
		}
		k := f.keys[i]
		if q.Delimiter != "" {
			if j := strings.Index(k[len(q.Prefix):], q.Delimiter); j >= 0 {
				cp := k[:len(q.Prefix)+j+1]
				if m := len(out.Prefixes); m == 0 || out.Prefixes[m-1] != cp {
					out.Prefixes = append(out.Prefixes, cp)
					n++
				}
				continue
			}
		}
		out.Rows = append(out.Rows, Row{Key: k, Data: Object{Size: 1}})
		n++
	}
	return out, nil
}

// TestBoundsAtExactKeys: a range (lo, hi] whose bounds are real keys lists
// the keys after lo through hi, under both start conventions and with and
// without a provider end bound, page by page.
func TestBoundsAtExactKeys(t *testing.T) {
	keys := []string{"a", "a/", "a/x", "b", "b\x00", "b\x00\x00", "ba", "c"}
	for _, c := range []struct {
		lo, hi string
		open   bool
		want   []string
	}{
		{"a", "b", false, []string{"a/", "a/x", "b"}},
		{"a/", "b\x00", false, []string{"a/x", "b", "b\x00"}},
		{"b", "ba", false, []string{"b\x00", "b\x00\x00", "ba"}},
		{"", "a", false, []string{"a"}},
		{"b\x00", "", true, []string{"b\x00\x00", "ba", "c"}},
		{"a/x", "a/x\x01", false, nil},
	} {
		for _, incl := range []bool{false, true} {
			for _, bounded := range []bool{false, true} {
				if !incl && !Safe(c.lo) {
					continue // an exclusive start-after with a control character is refused, and never sent (unsafe_cursor)
				}
				for _, pageCap := range []int{1, 2, 1000} {
					what := fmt.Sprintf("(%q, %q] open=%v inclusive=%v bounded=%v page=%d", c.lo, c.hi, c.open, incl, bounded, pageCap)
					var fetch Fetch[Object]
					if incl {
						f := &inclusive{keys: keys, bounded: bounded, pageCap: pageCap}
						fetch = f.fetch
					} else {
						s := newSorted(keys, false, 1)
						s.PageCap = pageCap
						fetch = fetchFrom(s)
					}
					p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", StartInclusive: incl}, fetch)
					if err != nil {
						t.Fatal(err)
					}
					col := &collect{}
					if err := p.expandRange(context.Background(), Range{lo: c.lo, hi: c.hi, open: c.open}, &direct{col}); err != nil {
						t.Fatalf("%s: %v", what, err)
					}
					if got := keysOf(col.rows); !slices.Equal(got, c.want) {
						t.Errorf("%s: %q, want %q", what, got, c.want)
					}
				}
			}
		}
	}
	if ExclusiveEnd("b") != "b\x00" || !("b" < ExclusiveEnd("b") && ExclusiveEnd("b") <= "b\x00\x00") {
		t.Fatal("ExclusiveEnd is the immediate successor")
	}
}

// TestInclusiveProviderMatchesKeyspace: the whole engine (seed descent,
// structure probes, random and demand splits) over an inclusive-start,
// end-bounded fake lists every keyspace exactly, and a provider veto on
// bounds is honoured: no vetoed string is ever sent as a bound.
func TestInclusiveProviderMatchesKeyspace(t *testing.T) {
	veto := func(s string) bool { return !strings.Contains(s, "5") }
	spaces := keyspaces()
	for k, v := range hostile() {
		spaces["hostile/"+k] = v
	}
	for name, keys := range spaces {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for i, opt := range []Options{
			{Workers: 8},
			{Workers: 16, Seed: true, SeedDescent: true, Structure: true},
			{Workers: 4, Seed: true, Floors: true},
		} {
			for _, vetoed := range []bool{false, true} {
				f := &inclusive{keys: want, bounded: i != 2, pageCap: 7 + 6*i}
				d := Description[Object]{Target: "test", StartInclusive: true}
				if vetoed {
					d.SafeBound = veto
				}
				if i == 0 {
					rng := rand.New(rand.NewSource(int64(len(keys))))
					var mu sync.Mutex
					opt.SplitCoin = func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				}
				p, err := New(opt, d, f.fetch)
				if err != nil {
					t.Fatal(err)
				}
				col := &collect{}
				if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: opt.Workers, Budget: 1 << 22}); err != nil {
					t.Fatalf("%s %d: %v", name, i, err)
				}
				if got := keysOf(col.rows); !slices.Equal(got, want) {
					t.Fatalf("%s opt %d veto=%v: %d keys, want %d", name, i, vetoed, len(got), len(want))
				}
				if vetoed {
					f.mu.Lock()
					sent := append(slices.Clone(f.starts), f.ends...)
					f.mu.Unlock()
					for _, b := range sent {
						if b != "" && !veto(b) {
							t.Fatalf("%s opt %d: vetoed bound %q sent", name, i, b)
						}
					}
				}
			}
		}
	}
}
