// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source"
)

// fenceEmitter records one range's emissions in order: rows, fences and
// children (a sequential driver stands in for the order stage).
type fenceEmitter struct {
	events []fenceEvent
	spanLo string
	spans  int
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

// drive expands r and its descendants depth first, checking every range's
// fences, and returns the rows in order, the replay ranges met and the
// data requests' start bounds.
func drive(t *testing.T, p *Engine[Object], r Range, k string, rows *[]string, replays *[]Range) {
	t.Helper()
	e := &fenceEmitter{}
	if err := p.Expand(context.Background(), r, e); err != nil {
		t.Fatal(err)
	}
	if r.kind == interval {
		checkFences(t, p, r, k, e)
	}
	for _, ev := range e.events {
		switch ev.kind {
		case 'r':
			*rows = append(*rows, ev.key)
		case 'c':
			if ev.child.kind == reused {
				*replays = append(*replays, ev.child)
				continue
			}
			drive(t, p, ev.child, k, rows, replays)
		}
	}
}

// checkFences holds a range's fences to the contract: the span first, at
// llo = max(lo, after); every fence at or above llo and K, a bound the
// provider accepts or K itself; no row at or below a fence after it, none
// above it before it; the last fence before the children (the split
// cursor) or at the end (hi, or ⊤ for the frontier).
func checkFences(t *testing.T, p *Engine[Object], r Range, k string, e *fenceEmitter) {
	t.Helper()
	what := fmt.Sprintf("range (%q, %q] open=%v after=%q", r.lo, r.hi, r.open, r.after)
	llo := max(r.lo, r.after)
	if e.spans != 1 || e.spanLo != llo {
		t.Fatalf("%s: %d spans at %q, want one at %q", what, e.spans, e.spanLo, llo)
	}
	last, lastRow, seenChild := fenceEvent{}, "", false
	for _, ev := range e.events {
		switch ev.kind {
		case 'f':
			if seenChild {
				t.Fatalf("%s: a fence after a child", what)
			}
			if !ev.top && (ev.key < llo || ev.key < k || !(p.safe(ev.key) || ev.key == k || ev.key == llo)) {
				t.Fatalf("%s: fence %q (llo %q, K %q, safe %v)", what, ev.key, llo, k, p.safe(ev.key))
			}
			if !ev.top && lastRow != "" && lastRow > ev.key {
				t.Fatalf("%s: fence %q below the row %q before it", what, ev.key, lastRow)
			}
			if last.kind == 'f' && !last.top && !ev.top && ev.key < last.key {
				t.Fatalf("%s: fence %q after fence %q", what, ev.key, last.key)
			}
			last = ev
		case 'r':
			if last.kind == 'f' && (last.top || ev.key <= last.key) {
				t.Fatalf("%s: row %q at or below the fence %q before it", what, ev.key, last.key)
			}
			lastRow = ev.key
			last = ev
		case 'c':
			if !seenChild && (last.kind != 'f' || last.key != ev.child.lo) {
				t.Fatalf("%s: the first child (%q, ..] does not follow the split cursor's fence (last %+v)", what, ev.child.lo, last)
			}
			seenChild = true
		}
	}
	if !seenChild && (last.kind != 'f' || last.top != r.open || (!r.open && last.key != r.hi)) {
		t.Fatalf("%s: does not end at its end fence (last %+v)", what, last)
	}
}

// TestFencesHoldTheContract: for random keyspaces, split coins and resume
// keys (unsafe ones included), every range declares its span and fences as
// the durable spill needs them, and the listing is complete.
func TestFencesHoldTheContract(t *testing.T) {
	spaces := keyspaces()
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range spaces {
			if !yield(k) {
				return
			}
		}
	}) {
		keys := slices.Compact(slices.Sorted(slices.Values(spaces[name])))
		if len(keys) < 2 {
			continue
		}
		for s := range 6 {
			rng := rand.New(rand.NewSource(int64(s)))
			var mu sync.Mutex
			coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
			opt := Options{SplitCoin: coin, Seed: s%2 == 1, Workers: 4}
			k := ""
			if s >= 2 {
				k = keys[rng.Intn(len(keys)-1)]
				opt.Resume = &Resume{After: k}
			}
			p, err := newTest(newSorted(keys, true, int64(s+1)), opt)
			if err != nil {
				t.Fatal(err)
			}
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

// TestReuseTilesAroundSpans: a resume handed reused certificates emits one
// replay range per span, at its position, and lists only the gaps: the
// listed rows are exactly those of (K, ⊤] outside the spans, no data
// request starts inside a span's [cs, ce), and the gaps and replay ranges
// together tile (K, ⊤] (spans below K start at K).
func TestReuseTilesAroundSpans(t *testing.T) {
	var keys []string
	for i := range 6000 {
		keys = append(keys, fmt.Sprintf("d%02d/sub%d/obj%06d", i%7, i%3, i))
	}
	slices.Sort(keys)
	for _, seed := range []bool{false, true} {
		for _, kIdx := range []int{-1, 100, 2500} {
			k := ""
			if kIdx >= 0 {
				k = keys[kIdx]
			}
			// Spans: one straddling K, one inside, one open at the end.
			spans := []source.Span{{Lo: keys[50], Hi: keys[900]}, {Lo: keys[3000], Hi: keys[3400]}, {Lo: keys[5000], HiOpen: true}}
			if kIdx >= 900 {
				spans = spans[1:]
			}
			api := newSorted(keys, true, 1)
			opt := Options{Seed: seed, Workers: 4}
			if k != "" {
				opt.Resume = &Resume{After: k}
			}
			p, err := newTest(api, opt)
			if err != nil {
				t.Fatal(err)
			}
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
				if r.cert != i {
					t.Fatalf("%s: replay range %d holds certificate %d", what, i, r.cert)
				}
			}
			api.Lock()
			for _, start := range api.Starts {
				for _, s := range spans {
					if start >= max(s.Lo, k) && (s.HiOpen || start < s.Hi) {
						t.Fatalf("%s: a data request starts at %q inside (%q, %q]", what, start, s.Lo, s.Hi)
					}
				}
			}
			api.Unlock()
		}
	}
}

