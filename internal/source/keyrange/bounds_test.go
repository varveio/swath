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

// inclusive is a fake listing with either start convention: inclusive
// (keys >= After) unless exclusive is set (keys > After); with bounded, it
// honours an exclusive end bound (keys < end) when ExclusiveEnd accepts
// one under acceptEnd. refuse names bounds it answers with a 400. It
// records every bound it was sent.
type inclusive struct {
	keys      []string
	exclusive bool
	bounded   bool // send ExclusiveEnd(Through) as the end bound
	acceptEnd func(string) bool
	refuse    func(string) bool
	pageCap   int
	mu        sync.Mutex
	starts    []string
	ends      []string
	requests  int
}

func (f *inclusive) fetch(_ context.Context, q *Query) (*Page[Object], error) {
	f.mu.Lock()
	f.requests++
	end := ""
	if f.bounded && q.Bounded {
		end, _ = ExclusiveEnd(q.Through, f.acceptEnd)
	}
	if q.Token == "" {
		f.starts = append(f.starts, q.After)
		if end != "" {
			f.ends = append(f.ends, end)
		}
	}
	f.mu.Unlock()
	if f.refuse != nil && (q.After != "" && f.refuse(q.After) || end != "" && f.refuse(end)) {
		return nil, fmt.Errorf("400: bound refused: %q, %q", q.After, end)
	}
	i := sort.SearchStrings(f.keys, q.Prefix)
	if q.Token != "" {
		i, _ = strconv.Atoi(q.Token)
	} else if q.After != "" {
		if f.exclusive {
			i = max(i, sort.Search(len(f.keys), func(j int) bool { return f.keys[j] > q.After }))
		} else {
			i = max(i, sort.SearchStrings(f.keys, q.After)) // inclusive
		}
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
					if incl || bounded {
						f := &inclusive{keys: keys, exclusive: !incl, bounded: bounded, pageCap: pageCap}
						fetch = f.fetch
					} else {
						s := newSorted(keys, false, 1) // S3-shaped: exclusive, no end bound
						s.PageCap = pageCap
						fetch = fetchFrom(s)
					}
					p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, StartInclusive: incl}, fetch)
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
	if e, ok := ExclusiveEnd("b", nil); !ok || e != "b\x00" || !("b" < e && e <= "b\x00\x00") {
		t.Fatal("ExclusiveEnd is the immediate successor")
	}
	if _, ok := ExclusiveEnd("b", func(s string) bool { return !strings.Contains(s, "\x00") }); ok {
		t.Fatal("ExclusiveEnd accepted a refused end bound")
	}
}

// TestInclusiveProviderMatchesKeyspace: the whole engine (seed,
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
			{Workers: 16, Seed: true, Structure: StructureOn},
			{Workers: 4, Seed: true},
		} {
			for _, vetoed := range []bool{false, true} {
				f := &inclusive{keys: want, bounded: i != 2, pageCap: 7 + 6*i}
				d := Description[Object]{Target: "test", Listing: Listing, StartInclusive: true}
				if vetoed {
					d.AcceptBound = veto
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

// chained binds a provider's continuation tokens to the request that
// started their chain, as a real service may: a token request whose
// prefix, delimiter, bounds, page size or probe flag differ from the
// chain's first request is refused.
type chained struct {
	next   Fetch[Object]
	mu     sync.Mutex
	chains map[string]chainOf
	n      int
}

type chainOf struct {
	token string // the inner provider's token
	q     Query  // the chain's first request, Token cleared
}

func (c *chained) fetch(ctx context.Context, q *Query) (*Page[Object], error) {
	key := *q
	key.Token = ""
	inner := *q
	if q.Token != "" {
		c.mu.Lock()
		ch, ok := c.chains[q.Token]
		c.mu.Unlock()
		if !ok || ch.q != key {
			return nil, fmt.Errorf("400: token %q used with changed parameters %+v, chain %+v", q.Token, key, ch.q)
		}
		inner.Token = ch.token
	}
	out, err := c.next(ctx, &inner)
	if err != nil || out.Token == "" {
		return out, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.chains == nil {
		c.chains = map[string]chainOf{}
	}
	c.n++
	tok := fmt.Sprintf("chain%d", c.n)
	c.chains[tok] = chainOf{token: out.Token, q: key}
	cp := *out
	cp.Token = tok
	return &cp, nil
}

// TestBoundsStableAcrossTokenChain: the engine keeps a chain's logical
// bounds (After, Through, prefix, delimiter) on every continuation, on
// every path (ranges with splits, the seed, structure
// probes, resume, the sequential chain), under both start conventions.
func TestBoundsStableAcrossTokenChain(t *testing.T) {
	keys := slices.Compact(slices.Sorted(slices.Values(keyspaces()["deepTree"])))
	if len(keys) < 100 {
		t.Fatalf("fixture: %d keys", len(keys))
	}
	for i, opt := range []Options{
		{Workers: 8},
		{Workers: 16, Seed: true, Structure: StructureOn},
		{Workers: 4, Seed: true, Resume: &Resume{After: keys[len(keys)/3]}},
		{Workers: 1, Sequential: true},
	} {
		for _, incl := range []bool{false, true} {
			var inner Fetch[Object]
			if incl {
				inner = (&inclusive{keys: keys, bounded: true, pageCap: 7}).fetch
			} else {
				s := newSorted(keys, false, 1)
				s.PageCap = 7
				inner = fetchFrom(s)
			}
			c := &chained{next: inner}
			p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, StartInclusive: incl}, c.fetch)
			if err != nil {
				t.Fatal(err)
			}
			col := &collect{}
			if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: opt.Workers, Budget: 1 << 22}); err != nil {
				t.Fatalf("opt %d inclusive=%v: %v", i, incl, err)
			}
			want := keys
			if opt.Resume != nil {
				want = keys[slices.Index(keys, opt.Resume.After)+1:]
			}
			if got := keysOf(col.rows); !slices.Equal(got, want) {
				t.Fatalf("opt %d inclusive=%v: %d keys, want %d", i, incl, len(got), len(want))
			}
			if c.n == 0 {
				t.Fatalf("opt %d: no token chain exercised", i)
			}
		}
	}
}

// TestProviderBoundPolicy: bound admissibility is the provider's, not the
// pivot alphabet's. A provider accepting '%' and non-ASCII receives real
// keys holding them as start bounds; one refusing NUL end bounds, or end
// bounds past 1,024 bytes, gets none of those and the engine trims locally.
// The fake answers a refused bound with a 400, so a refused bound sent
// fails the run.
func TestProviderBoundPolicy(t *testing.T) {
	var keys []string
	for i := range 600 {
		keys = append(keys, fmt.Sprintf("p%%%03d", i), fmt.Sprintf("qé%03d/x", i), fmt.Sprintf("r世%03d", i))
	}
	for i := range 40 {
		keys = append(keys, strings.Repeat("L", 1021)+fmt.Sprintf("%03d", i)) // 1,024 bytes
	}
	slices.Sort(keys)
	accept := func(s string) bool { return len(s) <= 1024 && !strings.ContainsAny(s, "\x00\r\n") }
	for _, c := range []struct {
		name      string
		acceptEnd func(string) bool
	}{
		{"NUL end refused: local trim", func(s string) bool { return !strings.Contains(s, "\x00") }},
		{"end past 1024 bytes refused", func(s string) bool { return len(s) <= 1024 }},
	} {
		for i, opt := range []Options{
			{Workers: 8},
			{Workers: 8, Seed: true, Structure: StructureOn},
			{Workers: 8, Resume: &Resume{After: keys[len(keys)/4]}}, // a key holding '%'
		} {
			f := &inclusive{keys: keys, bounded: true, acceptEnd: c.acceptEnd, pageCap: 9,
				refuse: func(s string) bool {
					return !c.acceptEnd(s) && strings.HasSuffix(s, "\x00") || !accept(strings.TrimSuffix(s, "\x00"))
				}}
			rng := rand.New(rand.NewSource(int64(i)))
			var mu sync.Mutex
			opt.SplitCoin = func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(2) == 0 }
			p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, StartInclusive: true, AcceptBound: accept}, f.fetch)
			if err != nil {
				t.Fatal(err)
			}
			col := &collect{}
			if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: 8, Budget: 1 << 22}); err != nil {
				t.Fatalf("%s opt %d: %v", c.name, i, err)
			}
			want := keys
			if opt.Resume != nil {
				want = keys[slices.Index(keys, opt.Resume.After)+1:]
			}
			if got := keysOf(col.rows); !slices.Equal(got, want) {
				t.Fatalf("%s opt %d: %d keys, want %d", c.name, i, len(got), len(want))
			}
			var pct, wide bool
			f.mu.Lock() // a cancelled structure probe may still be returning
			starts, ends := slices.Clone(f.starts), len(f.ends)
			f.mu.Unlock()
			for _, s := range starts {
				pct = pct || strings.Contains(s, "%")
				wide = wide || strings.ContainsFunc(s, func(r rune) bool { return r >= 0x80 })
			}
			if !pct || !wide {
				t.Fatalf("%s opt %d: no real key with '%%' (%v) or non-ASCII (%v) sent as a start bound", c.name, i, pct, wide)
			}
			if i == 0 && c.name == "end past 1024 bytes refused" && ends == 0 {
				t.Fatalf("%s: no end bound sent", c.name)
			}
		}
	}
}
