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
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
)

// walkSizes is walk with each request's Max drawn by size(i): a chain's
// continuation with a page size other than its first request's (#449).
func walkSizes(a *Adapter, q keyrange.Query, size func(i int) int) ([][]client.Item, error) {
	var pages [][]client.Item
	for i, retries := 0, 1<<20; ; i++ {
		q.Max = size(i)
		p, err := a.Fetch(context.Background(), &q)
		if err != nil {
			if retries > 0 && injected(err) {
				retries--
				continue
			}
			return pages, err
		}
		if len(p.Items) > q.Max {
			return pages, fmt.Errorf("a page of %d items at Max %d", len(p.Items), q.Max)
		}
		pages = append(pages, p.Items)
		if !p.Truncated {
			return pages, nil
		}
		if p.Token == "" || p.Token == q.Token {
			return pages, fmt.Errorf("truncated page with token %q after %q", p.Token, q.Token)
		}
		q.Token = p.Token
	}
}

// TestPropertyChangingPageSizes is TestProperty's cases (splits, overflow
// transfers, short, empty and failed responses) with a page size drawn per
// request, one read per page on in every other case: the pages
// concatenate to the oracle, no page exceeds its Max, and no chain is left
// registered.
func TestPropertyChangingPageSizes(t *testing.T) {
	n := 20000
	if testing.Short() {
		n = 2000
	}
	set := counters.New(CounterDefs)
	sizes := []int{1, 2, 3, 7, 1000, client.MaxPageSize}
	failed := 0
	for seed := range uint64(n) {
		c := newCase(seed)
		st := c.store()
		a := New(st, "c", set, 0)
		oneRead := seed%2 == 1
		a.OneReadPerPage(oneRead)
		r := rand.New(rand.NewPCG(seed, 0x449))
		items, err := flat(walkSizes(a, c.q, func(int) int { return sizes[r.IntN(len(sizes))] }))
		msg := ""
		switch {
		case err != nil:
			msg = err.Error()
		case !slices.Equal(names(items), oracle(c.keys, c.q)):
			msg = fmt.Sprintf("listed %q, want %q", names(items), oracle(c.keys, c.q))
		case a.Len() != 0:
			msg = fmt.Sprintf("%d chains registered after the last page", a.Len())
		}
		if msg == "" {
			continue
		}
		t.Errorf("case %d (one-read %v): %s\nkeys %q\nquery prefix=%q delimiter=%q after=%q through=%q bounded=%v\n%s",
			seed, oneRead, msg, c.keys, c.q.Prefix, c.q.Delimiter, c.q.After, c.q.Through, c.q.Bounded, st)
		if failed++; failed == 5 {
			t.FailNow()
		}
	}
	for _, i := range []int{CSplits, COverlapRows, CEmptyPages, CShortPages} {
		if set.Load(counters.ID(i)) == 0 {
			t.Errorf("no case engaged %s", CounterDefs[i].Key)
		}
	}
}

// TestOneReadAfterAListedNameToggle: a range after a listed name (a split's
// cursor) seeks at that name, which rule 1 drops. At a fixed page the
// adapter reads again for the page's last row and keeps the rest of that
// read buffered, a surplus a split abandons; with one read per page the
// page ends one short and nothing stays buffered, and the chain needs no
// more reads than pages.
func TestOneReadAfterAListedNameToggle(t *testing.T) {
	var keys []string
	for i := range 3000 {
		keys = append(keys, fmt.Sprintf("k%05d", i))
	}
	for _, oneRead := range []bool{false, true} {
		st := azblobtest.NewStore(keys)
		a := New(st, "c", nil, 0)
		a.OneReadPerPage(oneRead)
		q := keyrange.Query{After: "k00000", Through: "k02999", Bounded: true, Max: 1000}
		p, err := a.Fetch(context.Background(), &q)
		if err != nil {
			t.Fatal(err)
		}
		reads, buffered := len(st.Log()), 0
		for _, ch := range a.chains {
			for _, s := range ch.stack {
				buffered += len(s.buf)
			}
		}
		want, wantReads, wantBuffered := 1000, 2, 999
		if oneRead {
			want, wantReads, wantBuffered = 999, 1, 0
		}
		if len(p.Items) != want || reads != wantReads || buffered != wantBuffered || p.Items[0].Name != "k00001" {
			t.Errorf("one-read %v: first page %d items from %q, %d reads, %d rows left buffered; want %d, %d, %d",
				oneRead, len(p.Items), p.Items[0].Name, reads, buffered, want, wantReads, wantBuffered)
		}
		items, err := flat(walkSizes(a, keyrange.Query{After: q.After, Through: q.Through, Bounded: true, Token: p.Token},
			func(i int) int { return []int{1000, 2000, 5000}[min(i, 2)] }))
		if err != nil {
			t.Fatal(err)
		}
		if got := append(names(p.Items), names(items)...); !slices.Equal(got, keys[1:]) {
			t.Errorf("one-read %v: %d keys, want %d", oneRead, len(got), len(keys)-1)
		}
	}
}
