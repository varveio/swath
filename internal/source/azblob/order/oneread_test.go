// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/keyrange"
)

// buffered is the items a's registered chains hold read and not emitted.
func (a *Adapter) buffered() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, c := range a.chains {
		for i := range c.stack {
			n += len(c.stack[i].buf)
		}
	}
	return n
}

// TestOneReadAfterAListedName: a range after a listed name (a split's
// cursor) seeks at that name, which rule 1 drops, so its first read holds
// one member fewer than the page. The page ends there rather than read
// again for its last item and keep that read's surplus buffered, which a
// split would abandon: one read per page, nothing buffered between pages,
// and the chain lists every key.
func TestOneReadAfterAListedName(t *testing.T) {
	var keys []string
	for i := range 3000 {
		keys = append(keys, fmt.Sprintf("k%05d", i))
	}
	st := azblobtest.NewStore(keys)
	a := New(st, "c", nil, 0)
	q := keyrange.Query{After: "k00000", Through: "k02999", Bounded: true, Max: 1000}
	var got []string
	for i := 0; ; i++ {
		p, err := a.Fetch(context.Background(), &q)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && (len(p.Items) != 999 || p.Items[0].Name != "k00001") {
			t.Fatalf("first page: %d items from %q, want 999 from k00001", len(p.Items), p.Items[0].Name)
		}
		if reads := len(st.Log()); reads != i+1 || a.buffered() != 0 {
			t.Fatalf("page %d: %d reads in all, %d items left buffered", i, reads, a.buffered())
		}
		got = append(got, names(p.Items)...)
		if !p.Truncated {
			break
		}
		q.Token = p.Token
	}
	if !slices.Equal(got, keys[1:]) {
		t.Fatalf("%d keys, want %d", len(got), len(keys)-1)
	}
	if a.Counter(CShortPages) != 1 || a.Counter(CShortRows) != 1 {
		t.Errorf("order_short_pages %d, order_short_rows %d; want 1 and 1", a.Counter(CShortPages), a.Counter(CShortRows))
	}
}

// splitWalk lists q as the range engine does over the adapter, with a
// fresh adapter per range (the provider's Expand) and owner splits drawn
// by r: after a truncated page of a range without a delimiter, it may
// abandon the range's chain at its cursor c and list (c, m] and then
// (m, Through] for a random pivot m between them, depth first, so the
// names come out in byte order. It stops once stop names are listed
// (stop < 0: never). Every page must hold at most Max items, and while
// set counts no span cut or split (names and bounds below U+E000: one span
// per chain) no chain may keep an item buffered between pages: one read
// per page, so a split abandons nothing read. (Past U+E000 a span may hold
// items its byte order puts after a sibling still to read.)
func splitWalk(st *azblobtest.Store, set *counters.Set, q keyrange.Query, keys []string, r *rand.Rand, stop int) ([]string, error) {
	var out []string
	work := []keyrange.Query{q}
	for len(work) > 0 {
		rq := work[len(work)-1]
		work = work[:len(work)-1]
		a := New(st, "c", set, 0)
		c := rq.After
		for {
			p, err := a.Fetch(context.Background(), &rq)
			if injected(err) {
				continue
			}
			if err != nil {
				return out, err
			}
			if len(p.Items) > rq.Max {
				return out, fmt.Errorf("a page of %d items at Max %d", len(p.Items), rq.Max)
			}
			if n := a.buffered(); n > 0 && set.Load(CSpans) == 0 && set.Load(CSplits) == 0 {
				return out, fmt.Errorf("%d items buffered after a page of (%q, %q]", n, rq.After, rq.Through)
			}
			for _, it := range p.Items {
				out = append(out, it.Name)
				c = it.Name
				if len(out) == stop {
					return out, nil
				}
			}
			if !p.Truncated {
				break
			}
			if rq.Delimiter == "" && len(p.Items) > 0 && r.IntN(3) == 0 {
				m := bound(r, keys)
				if m > c && (!rq.Bounded || m < rq.Through) {
					lower, upper := rq, rq
					lower.After, lower.Through, lower.Bounded, lower.Token = c, m, true, ""
					upper.After, upper.Token = m, ""
					work = append(work, upper, lower)
					break // the chain is abandoned, as at a split
				}
			}
			rq.Token = p.Token
		}
		a.Release()
	}
	return out, nil
}

// TestPropertySplitsAndResume is the adversarial check of one read per
// page against the engine's protocol: TestProperty's random cases (names
// at U+E000 and above, made-up bounds, short, empty and failed responses)
// listed with random owner splits, and in every other case cut short at
// a random published name K and resumed after it (a crash). The ranges
// list the oracle exactly, in order, with no gap or overlap, the resumed
// run lists exactly the names after K, no page exceeds its Max, and no
// one-span chain keeps an item buffered between pages. 2,000 cases with -short,
// 20,000 otherwise.
func TestPropertySplitsAndResume(t *testing.T) {
	n := 20000
	if testing.Short() {
		n = 2000
	}
	engaged := map[int]bool{}
	failed, resumed, plain := 0, 0, 0
	for seed := range uint64(n) {
		set := counters.New(CounterDefs)
		c := newCase(seed)
		r := rand.New(rand.NewPCG(seed, 0x462))
		want := oracle(c.keys, c.q)
		stop := -1
		if seed%2 == 1 && len(want) > 0 {
			stop = 1 + r.IntN(len(want))
		}
		st := c.store()
		got, err := splitWalk(st, set, c.q, c.keys, r, stop)
		if err == nil && stop > 0 && len(got) == stop && stop < len(want) {
			resumed++
			rq := c.q
			rq.After = got[stop-1]
			var rest []string
			if rest, err = splitWalk(st, set, rq, c.keys, r, -1); err == nil {
				got = append(got, rest...)
			}
		}
		for _, i := range []int{CSplits, COverlapRows, CEmptyPages, CShortPages} {
			engaged[i] = engaged[i] || set.Load(counters.ID(i)) > 0
		}
		if set.Load(CSpans) == 0 && set.Load(CSplits) == 0 && set.Load(CShortPages) > 0 {
			plain++
		}
		msg := ""
		switch {
		case err != nil:
			msg = err.Error()
		case !slices.Equal(got, want):
			msg = fmt.Sprintf("listed %q, want %q", got, want)
		}
		if msg == "" {
			continue
		}
		t.Errorf("case %d (stop %d): %s\nkeys %q\nquery prefix=%q delimiter=%q after=%q through=%q bounded=%v max=%d\n%s",
			seed, stop, msg, c.keys, c.q.Prefix, c.q.Delimiter, c.q.After, c.q.Through, c.q.Bounded, c.q.Max, st)
		if failed++; failed == 5 {
			t.FailNow()
		}
	}
	for _, i := range []int{CSplits, COverlapRows, CEmptyPages, CShortPages} {
		if !engaged[i] {
			t.Errorf("no case engaged %s", CounterDefs[i].Key)
		}
	}
	if resumed == 0 || plain == 0 {
		t.Errorf("%d cases resumed, %d one-span cases ended a page short", resumed, plain)
	}
}
