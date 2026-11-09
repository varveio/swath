// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// alphabet is §7.2's: ASCII, the delimiter, two same-lead scalars, three
// scalars in U+E000..U+FFFF and two above U+FFFF.
var alphabet = []string{"a", "/", "é", "ê", "", "ｚ", "�", "\U00010000", "\U0001F600"}

func word(r *rand.Rand, lo, hi int) string {
	var b strings.Builder
	for range lo + r.IntN(hi-lo+1) {
		b.WriteString(alphabet[r.IntN(len(alphabet))])
	}
	return b.String()
}

// bound is a random bound: a key, a key's prefix or extension, or made up.
func bound(r *rand.Rand, keys []string) string {
	if len(keys) == 0 || r.IntN(3) == 0 {
		return word(r, 1, 6)
	}
	k := []rune(keys[r.IntN(len(keys))])
	switch r.IntN(3) {
	case 0:
		return string(k)
	case 1:
		return string(k[:r.IntN(len(k)+1)])
	}
	return string(k) + word(r, 1, 2)
}

type propertyCase struct {
	seed   uint64
	keys   []string
	q      keyrange.Query
	traced []span
	// The rates of short (in quarters), empty (in tenths) and failed (in
	// twentieths) responses.
	shortRate, emptyRate, failRate int
}

func newCase(seed uint64) *propertyCase {
	r := rand.New(rand.NewPCG(seed, 0x0bde))
	c := &propertyCase{seed: seed}
	for range r.IntN(13) {
		c.keys = append(c.keys, word(r, 1, 6))
	}
	slices.Sort(c.keys)
	c.keys = slices.Compact(c.keys)
	q := keyrange.Query{Max: []int{1, 2, 7, 1000}[r.IntN(4)]}
	if r.IntN(2) == 0 {
		p := []rune(bound(r, c.keys))
		q.Prefix = string(p[:r.IntN(min(len(p), 3)+1)])
	}
	if r.IntN(3) == 0 {
		q.Delimiter = "/"
	}
	if r.IntN(3) > 0 {
		q.After = bound(r, c.keys)
	}
	if r.IntN(2) == 0 {
		q.Through, q.Bounded = bound(r, c.keys), true
	}
	c.q = q
	c.shortRate, c.emptyRate, c.failRate = r.IntN(3), r.IntN(2), r.IntN(2)
	return c
}

// store is the case's fake: request n's page cap, empty page and failure
// are drawn from (seed, n), the same on every run of the case.
func (c *propertyCase) store() *azblobtest.Store {
	st := azblobtest.NewStore(c.keys)
	draw := func(n, hook int) *rand.Rand { return rand.New(rand.NewPCG(c.seed, uint64(3*n+hook))) }
	st.Short = func(n int) int {
		if r := draw(n, 0); r.IntN(4) < c.shortRate {
			return 1 + r.IntN(3)
		}
		return 0
	}
	st.Empty = func(n int) bool { return draw(n, 1).IntN(10) < c.emptyRate }
	st.Fail = func(n int) error {
		if draw(n, 2).IntN(20) < c.failRate {
			return azblobtest.ErrInjected
		}
		return nil
	}
	return st
}

// check runs the case and returns what broke, "" when nothing did; the
// adapter's counters are added to set.
func (c *propertyCase) check(set *counters.Set) (string, *azblobtest.Store) {
	st := c.store()
	a := New(st, "c", set, 0)
	before := a.Counter(CResponses)
	a.trace = func(s span) { c.traced = append(c.traced, s) }
	items, err := flat(walk(a, c.q, 1<<20, injected))
	if err != nil {
		return err.Error(), st
	}
	if got, want := names(items), oracle(c.keys, c.q); !slices.Equal(got, want) {
		return fmt.Sprintf("listed %q, want %q", got, want), st
	}
	// Responses from the adapter's own counter, attempts from the fake's
	// request log: two independent sources.
	if responses := a.Counter(CResponses) - before; responses > int64(len(st.Log())) || responses != int64(st.Responses()) {
		return fmt.Sprintf("%d responses counted, %d answered, %d attempts", responses, st.Responses(), len(st.Log())), st
	}
	if a.Len() != 0 {
		return fmt.Sprintf("%d chains registered after the last page", a.Len()), st
	}
	// The names a span is judged on: every key, and with a delimiter the
	// roll-ups the listing can return.
	universe := slices.Clone(c.keys)
	if c.q.Delimiter != "" {
		universe = append(universe, oracle(c.keys, keyrange.Query{Prefix: c.q.Prefix, Delimiter: c.q.Delimiter})...)
	}
	for _, s := range c.traced {
		if msg := spanInvariants(s, c.q.Prefix, universe); msg != "" {
			return msg, st
		}
	}
	return "", st
}

// spanInvariants checks §7.2's span invariant (no name outside the span
// lies between two of its members in Azure's order) and seek-floor
// invariant (no member lies below the span's seek in Azure's order), with
// azblobtest.CompareUTF16, not the adapter's ranks.
func spanInvariants(s span, prefix string, universe []string) string {
	var lo, hi string
	any := false
	floor := seekFrom(s.lo)
	for _, n := range universe {
		if !s.contains(n, prefix) {
			continue
		}
		if azblobtest.CompareUTF16(n, floor) < 0 {
			return fmt.Sprintf("span %s: member %q is below its seek %q", spanString(s), n, floor)
		}
		if !any || azblobtest.CompareUTF16(n, lo) < 0 {
			lo = n
		}
		if !any || azblobtest.CompareUTF16(n, hi) > 0 {
			hi = n
		}
		any = true
	}
	for _, n := range universe {
		if any && !s.contains(n, prefix) && azblobtest.CompareUTF16(lo, n) < 0 && azblobtest.CompareUTF16(n, hi) < 0 {
			return fmt.Sprintf("span %s: %q lies between its members %q and %q", spanString(s), n, lo, hi)
		}
	}
	return ""
}

// TestProperty is §7.2: random key sets over the alphabet, random prefix,
// delimiter, bounds (made-up ones included), Max and short, empty and
// failed responses (a failed page asked again): the pages concatenate to
// the oracle, responses never exceed attempts, and every span created
// holds the span and seek-floor invariants. 2,000 cases with -short,
// 50,000 otherwise.
func TestProperty(t *testing.T) {
	n := 50000
	if testing.Short() {
		n = 2000
	}
	failed, failures := 0, 0
	set := counters.New(CounterDefs)
	for seed := range uint64(n) {
		c := newCase(seed)
		msg, st := c.check(set)
		failures += st.Attempts() - st.Responses()
		if msg == "" {
			continue
		}
		t.Errorf("case %d: %s\nkeys %q\nquery prefix=%q delimiter=%q after=%q through=%q bounded=%v max=%d\n%s",
			seed, msg, c.keys, c.q.Prefix, c.q.Delimiter, c.q.After, c.q.Through, c.q.Bounded, c.q.Max, st)
		if failed++; failed == 5 {
			t.FailNow()
		}
	}
	// The generator reaches every path: splits, overlap past a span's end,
	// empty continuing pages, empty spans and failed reads.
	for _, i := range []int{CSpans, CSplits, CSeeks, CProbes, COverlapRows, CEmptyPages, CResponses} {
		if set.Load(counters.ID(i)) == 0 {
			t.Errorf("no case engaged %s", CounterDefs[i].Key)
		}
	}
	if failures == 0 {
		t.Error("no case failed a read")
	}
	t.Logf("%d cases: %d failed reads, counters %v", n, failures, set.Snapshot())
}
