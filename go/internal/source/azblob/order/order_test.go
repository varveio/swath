// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// The scalars of the specification's examples: aE is "a" + U+E000.
const (
	E    = "\uE000"
	E1   = "\uE001"
	S    = "\U00010000"
	D7FF = "\uD7FF"
)

// walk reads q's chain to its end as pages, asking a failed page again
// while retry admits the error, up to retries times (the engine's
// requeue), and checks the page contract: at most q.Max items, Truncated
// false only on the last page, a token on every truncated page that
// differs from the one before.
func walk(a *Adapter, q keyrange.Query, retries int, retry func(error) bool) ([][]client.Item, error) {
	var pages [][]client.Item
	for {
		p, err := a.Fetch(context.Background(), &q)
		if err != nil {
			if retries > 0 && retry(err) {
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

// injected admits the fake's injected failures only.
func injected(err error) bool { return errors.Is(err, azblobtest.ErrInjected) }

// flat is walk's pages concatenated.
func flat(pages [][]client.Item, err error) ([]client.Item, error) {
	return slices.Concat(pages...), err
}

func names(items []client.Item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Name
	}
	return out
}

// oracle is §2's scope: the prefix listing (rolled up with a delimiter),
// filtered by the bounds, in byte order.
func oracle(keys []string, q keyrange.Query) []string {
	var out []string
	for _, k := range keys {
		if !strings.HasPrefix(k, q.Prefix) {
			continue
		}
		if q.Delimiter != "" {
			if at := strings.Index(k[len(q.Prefix):], q.Delimiter); at >= 0 {
				k = k[:len(q.Prefix)+at+len(q.Delimiter)]
			}
		}
		if q.After < k && (!q.Bounded || k <= q.Through) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// spanString renders a span's bounds as §3.2 writes them.
func spanString(s span) string {
	open := "("
	if s.loInc {
		open = "["
	}
	hi := "nil"
	if s.hi != "" {
		hi = fmt.Sprintf("%q", s.hi)
	}
	return fmt.Sprintf("%s%q, %s)", open, s.lo, hi)
}

func spanStrings(spans []span) []string {
	var out []string
	for _, s := range spans {
		out = append(out, spanString(s))
	}
	return out
}

func sameBounds(a, b span) bool { return a.lo == b.lo && a.loInc == b.loInc && a.hi == b.hi }

// x is the span (lo, hi), in the span [lo, hi); hi "" is open.
func x(lo, hi string) span  { return span{lo: lo, hi: hi} }
func in(lo, hi string) span { return span{lo: lo, loInc: true, hi: hi} }

func bounded(prefix, after, through string) keyrange.Query {
	return keyrange.Query{Prefix: prefix, After: after, Through: through, Bounded: true, Max: 1000}
}

// TestWorkedExamples: the nine queries of §3.2 on §7.1's fixtures at
// Max 1,000: the spans of §3.2, the items in byte order and the responses
// derived by hand in §7.1 (2, 3, 1, 2, 1, 1, 1, 2, 4).
func TestWorkedExamples(t *testing.T) {
	unbounded := func(prefix, after string) keyrange.Query {
		return keyrange.Query{Prefix: prefix, After: after, Max: 1000}
	}
	for i, row := range []struct {
		q         keyrange.Query
		keys      []string
		spans     []span
		want      []string
		responses int
	}{
		{bounded("a", "a"+E, "a"+S), []string{"a" + E + "z", "a" + S},
			[]span{x("a"+E, "a\xF0"), in("a\xF0", "a"+S+"\x00")}, []string{"a" + E + "z", "a" + S}, 2},
		{bounded("a", "a"+E, "a"+E+S), []string{"a" + E, "a" + E + "a", "a" + E + E, "a" + E + S},
			[]span{x("a"+E, "a"+E+"\xEE"), in("a"+E+"\xEE", "a"+E+"\xF0"), in("a"+E+"\xF0", "a"+E+S+"\x00")},
			[]string{"a" + E + "a", "a" + E + E, "a" + E + S}, 3},
		{unbounded("a"+E, ""), []string{"a" + E, "a" + E + E, "a" + E + S},
			[]span{in("a"+E, "")}, []string{"a" + E, "a" + E + E, "a" + E + S}, 1},
		{bounded("a", "a"+E+"x", "a"+S+"x"), []string{"a" + E + "y", "a" + E + E, "a" + E + S, "a" + S, "a" + S + "w"},
			[]span{x("a"+E+"x", "a\xF0"), in("a\xF0", "a"+S+"x\x00")},
			[]string{"a" + E + "y", "a" + E + E, "a" + E + S, "a" + S, "a" + S + "w"}, 2},
		{bounded("a", "a"+E, "a"+E1), []string{"a" + E, "a" + E1},
			[]span{x("a"+E, "a"+E1+"\x00")}, []string{"a" + E1}, 1},
		{bounded("a", "ab", "ad"), []string{"aba", "ac", "ad"},
			[]span{x("ab", "ad\x00")}, []string{"aba", "ac", "ad"}, 1},
		{bounded("a", "", "a"), []string{"a"},
			[]span{in("a", "a\x00")}, []string{"a"}, 1},
		{bounded("a", "ab", "a"+E), []string{"ac", "a" + E, "a" + S},
			[]span{x("ab", "a\xEE"), in("a\xEE", "a"+E+"\x00")}, []string{"ac", "a" + E}, 2},
		// §3.2 writes the third span [a\xEE, a\xF0): its lower bound is
		// the successor aE, the same names and the same seek.
		{unbounded("a", "a"+D7FF+E), []string{"a" + E, "a" + S},
			[]span{x("a"+D7FF+E, "a"+D7FF+"\xF0"), in("a"+D7FF+"\xF0", "a"+D7FF+"\xF5"), in("a"+E, "a\xF0"), in("a\xF0", "a\xF5")},
			[]string{"a" + E, "a" + S}, 4},
	} {
		t.Run(fmt.Sprint("row", i+1), func(t *testing.T) {
			if got := decompose(normalise(row.q.Prefix, row.q.After, row.q.Through, row.q.Bounded), row.q.Through); !slices.EqualFunc(got, row.spans, sameBounds) {
				t.Errorf("spans %s, want %s", spanStrings(got), spanStrings(row.spans))
			}
			st := azblobtest.NewStore(row.keys)
			a := New(st, "c", nil, 0)
			items, err := flat(walk(a, row.q, 0, injected))
			if err != nil {
				t.Fatal(err)
			}
			if got := names(items); !slices.Equal(got, row.want) || !slices.Equal(row.want, oracle(row.keys, row.q)) {
				t.Errorf("items %q, want %q (oracle %q)", got, row.want, oracle(row.keys, row.q))
			}
			if a.Counter(CResponses) != int64(row.responses) || a.Counter(CResponses) > int64(len(st.Log())) {
				t.Errorf("%d responses counted, %d attempts, want %d", a.Counter(CResponses), len(st.Log()), row.responses)
			}
			if st.Responses() != row.responses {
				t.Errorf("%d responses, want %d\n%s", st.Responses(), row.responses, st)
			}
			if a.Len() != 0 {
				t.Errorf("%d chains registered after the last page", a.Len())
			}
		})
	}
}

// run drains q over keys with the store configured by setup, checks the
// items against the oracle and returns the store.
func run(t *testing.T, keys []string, q keyrange.Query, setup func(*azblobtest.Store)) (*azblobtest.Store, *Adapter) {
	t.Helper()
	st := azblobtest.NewStore(keys)
	if setup != nil {
		setup(st)
	}
	a := New(st, "c", nil, 0)
	items, err := flat(walk(a, q, 8, injected))
	if err != nil {
		t.Fatalf("%v\n%s", err, st)
	}
	if got, want := names(items), oracle(keys, q); !slices.Equal(got, want) {
		t.Fatalf("items %q, want %q\n%s", got, want, st)
	}
	return st, a
}

func every(n int) func(int) int { return func(int) int { return n } }

// TestRawPageAcrossGroups: a page [aS, aE, b] with a marker and a later key
// c: the split hands the marker to the rest (the child holding the
// frontier b); S and E complete from their buffers, P-before the owner.
// Two responses: the page and the marker's.
func TestRawPageAcrossGroups(t *testing.T) {
	st, a := run(t, []string{"a" + E, "a" + S, "b", "c"}, keyrange.Query{Max: 1000}, func(s *azblobtest.Store) {
		s.Short = func(n int) int { return map[int]int{0: 3}[n] }
	})
	if got := st.Log()[0].Names; !slices.Equal(got, []string{"a" + S, "a" + E, "b"}) {
		t.Fatalf("first page %q", got)
	}
	if st.Responses() != 2 || a.Counter(CSeeks) != 1 || a.Counter(CSplits) != 1 {
		t.Errorf("%d responses, %d seeks, %d splits\n%s", st.Responses(), a.Counter(CSeeks), a.Counter(CSplits), st)
	}
}

// TestGroupsAcrossPages: E, S and the rest beyond them on one page, and
// every boundary between them across pages (pages of 1 to 7 items).
func TestGroupsAcrossPages(t *testing.T) {
	keys := []string{"a", "a" + E, "a" + E + E, "a" + E + S, "a" + S, "a" + S + "b", "b"}
	st, _ := run(t, keys, keyrange.Query{Max: 1000}, nil)
	if st.Responses() != 1 {
		t.Errorf("one page: %d responses\n%s", st.Responses(), st)
	}
	for size := 1; size <= len(keys); size++ {
		for _, max := range []int{1, 2, 1000} {
			t.Run(fmt.Sprintf("page%d/max%d", size, max), func(t *testing.T) {
				run(t, keys, keyrange.Query{Max: max}, func(s *azblobtest.Store) { s.Short = every(size) })
			})
		}
	}
}

// TestSameLeadScalars: names whose first difference is two scalars with
// the same lead byte (U+00E9 and U+00EA) list in byte order in one read.
func TestSameLeadScalars(t *testing.T) {
	keys := []string{"aé", "aéb", "aê", "aêa", "ê"}
	for _, q := range []keyrange.Query{{Max: 1000}, bounded("a", "aé", "aê"), bounded("", "aéb", "ê")} {
		if st, _ := run(t, keys, q, nil); st.Responses() != 1 {
			t.Errorf("%+v: %d responses\n%s", q, st.Responses(), st)
		}
	}
}

// TestEveryLeadByte: one scalar for each lead byte EE..F4, under "a" and at
// the root, each as a key, an exclusive start and an inclusive end, at
// Max 1 and 1,000.
func TestEveryLeadByte(t *testing.T) {
	scalars := []string{"\uE000", "\uFFFF", "\U00010000", "\U00040000", "\U00080000", "\U00100000", "\U0010FFFF"}
	var keys []string
	for _, x := range scalars {
		keys = append(keys, x, "a"+x, "a"+x+"b", "a"+x+E, "a"+x+S)
	}
	keys = append(keys, "a", "b")
	for _, max := range []int{1, 1000} {
		run(t, keys, keyrange.Query{Max: max}, nil)
		for _, x := range scalars {
			for _, y := range scalars {
				run(t, keys, keyrange.Query{After: "a" + x, Through: "a" + y + "b", Bounded: true, Max: max}, nil)
				run(t, keys, keyrange.Query{Prefix: "a", After: "a" + x + "b", Through: y, Bounded: true, Max: max}, nil)
			}
			run(t, keys, keyrange.Query{Prefix: "a" + x, Max: max}, nil)
			run(t, keys, keyrange.Query{After: x, Max: max}, nil)
		}
	}
}

// TestAfterEqualsThrough: an empty scope is never read.
func TestAfterEqualsThrough(t *testing.T) {
	for _, b := range []string{"a" + E, "a", "a" + S} {
		if st, _ := run(t, []string{"a", "a" + E, "a" + S}, bounded("", b, b), nil); st.Attempts() != 0 {
			t.Errorf("After = Through = %q: %d requests", b, st.Attempts())
		}
	}
}

// TestPrefixKeys: "a/" beside "a/b", with Through at the prefix, flat and
// rolled up (the roll-up "a/" is in scope at Through "a/").
func TestPrefixKeys(t *testing.T) {
	keys := []string{"a/", "a/b", "a/" + E, "a/" + S, "b"}
	for _, q := range []keyrange.Query{
		bounded("a/", "", "a/"),
		bounded("", "", "a/"),
		{Delimiter: "/", Through: "a/", Bounded: true, Max: 1000},
		{Prefix: "a/", Delimiter: "/", Max: 1000},
		{Prefix: "a/", Delimiter: "/", After: "a/", Max: 1},
	} {
		if st, _ := run(t, keys, q, nil); q.Max == 1000 && st.Responses() != 1 {
			t.Errorf("%+v: %d responses\n%s", q, st.Responses(), st)
		}
	}
}

// TestRollupsAtSeeks: a roll-up equal to the exclusive start is dropped
// (rule 1), and so is the roll-up of the subtree a seek lands inside
// (rule 2), including at a seek into a span past an E/S cut.
func TestRollupsAtSeeks(t *testing.T) {
	d := func(after string) keyrange.Query { return keyrange.Query{Delimiter: "/", After: after, Max: 1000} }
	for _, tc := range []struct {
		keys      []string
		q         keyrange.Query
		responses int
	}{
		{[]string{"a/b", "b"}, d("a/"), 1},
		{[]string{"a/b", "a/c", "b"}, d("a/b"), 1},
		// Seek aE/ drops the roll-up aE/ and ends at b; seek aS returns
		// aS/ and ends at aE/ (rule 3: a member was accepted); seek b.
		{[]string{"a" + E + "/x", "a" + S + "/y", "b"}, d("a" + E + "/"), 3},
		{[]string{"a" + E + "/x", "a" + E + "/z" + E, "a" + E + "/z" + S, "b"}, d("a" + E + "/z" + E), 0},
	} {
		st, _ := run(t, tc.keys, tc.q, nil)
		if tc.responses > 0 && st.Responses() != tc.responses {
			t.Errorf("%q after %q: %d responses, want %d\n%s", tc.keys, tc.q.After, st.Responses(), tc.responses, st)
		}
	}
}

// TestEmptySplitBranch: a split whose E branch holds nothing. Pages of one
// item: [aS]+M splits with the frontier in S, so S owns the marker and E
// seeks; E's read [b] ends E at b, a member of the rest, which takes it
// with that response's marker (the overflow transfer); S's marker answers
// [b] past S (one overlap row); the rest continues with [c]. Four
// responses.
func TestEmptySplitBranch(t *testing.T) {
	st, a := run(t, []string{"a" + S, "b", "c"}, keyrange.Query{Max: 1000}, func(s *azblobtest.Store) { s.Short = every(1) })
	if st.Responses() != 4 || a.Counter(COverlapRows) != 1 || a.Counter(CSeeks) != 2 {
		t.Errorf("%d responses, %d overlap rows, %d seeks\n%s", st.Responses(), a.Counter(COverlapRows), a.Counter(CSeeks), st)
	}
}

// TestMarkerBoundToQuery: the fake refuses a marker resent with another
// startFrom, prefix or delimiter, so every other test also proves that the
// adapter resends a marker only with the query that issued it.
func TestMarkerBoundToQuery(t *testing.T) {
	st := azblobtest.NewStore([]string{"a", "b", "c"})
	in := client.ListInput{StartFrom: "a", MaxResults: 1}
	out, err := st.List(context.Background(), &in)
	if err != nil || out.NextMarker == "" {
		t.Fatal(out, err)
	}
	for _, bad := range []client.ListInput{
		{StartFrom: "b", Marker: out.NextMarker, MaxResults: 1},
		{Marker: out.NextMarker, MaxResults: 1},
		{StartFrom: "a", Prefix: "a", Marker: out.NextMarker, MaxResults: 1},
		{StartFrom: "a", Delimiter: "/", Marker: out.NextMarker, MaxResults: 1},
		{StartFrom: "a", Marker: "m99", MaxResults: 1},
	} {
		if _, err := st.List(context.Background(), &bad); !errors.Is(err, azblobtest.ErrMarker) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
	in.Marker = out.NextMarker
	if out, err := st.List(context.Background(), &in); err != nil || len(out.Items) != 1 || out.Items[0].Name != "b" {
		t.Errorf("the marker with its own query: %+v %v", out, err)
	}
}

// TestFailureRepeatsThePage: a read that fails after successful reads in
// the same Fetch discards them; the page asked again equals a fault-free
// run's, and every response consumed is charged (§6).
func TestFailureRepeatsThePage(t *testing.T) {
	keys := []string{"a" + E, "a" + S}
	q := keyrange.Query{Prefix: "a", After: "a" + D7FF + E, Max: 1000}
	// Row 9's four seeks in one Fetch; the third fails. Two responses
	// before it, four in the retry: six, in seven attempts.
	st := azblobtest.NewStore(keys)
	st.Fail = azblobtest.FailAt(2)
	a := New(st, "c", nil, 0)
	if _, err := a.Fetch(context.Background(), &q); !errors.Is(err, azblobtest.ErrInjected) {
		t.Fatalf("the failing page: %v", err)
	}
	if a.Len() != 0 {
		t.Fatal("a chain's failed first page registered it")
	}
	p, err := a.Fetch(context.Background(), &q)
	if err != nil || p.Truncated || !slices.Equal(names(p.Items), oracle(keys, q)) {
		t.Fatalf("retry: %+v %v", p, err)
	}
	if st.Responses() != 6 || st.Attempts() != 7 {
		t.Errorf("%d responses, %d attempts\n%s", st.Responses(), st.Attempts(), st)
	}

	// Inside a chain: every request of the run fails once in turn; each
	// run's pages, asked again on failure, equal the fault-free pages.
	keys = []string{"a", "a" + E, "a" + E + "b", "a" + E + S, "a" + S, "a" + S + E, "b", "c"}
	q = keyrange.Query{Max: 2}
	clean := azblobtest.NewStore(keys)
	clean.Short = every(1)
	want := pages(t, New(clean, "c", nil, 0), q, nil)
	for n := range clean.Attempts() {
		st := azblobtest.NewStore(keys)
		st.Short = func(i int) int { return 1 }
		st.Fail = azblobtest.FailAt(n)
		if got := pages(t, New(st, "c", nil, 0), q, st); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("failure at request %d: pages %q, want %q\n%s", n, got, want, st)
		}
		if st.Responses() > st.Attempts() || st.Attempts() <= clean.Attempts() {
			t.Errorf("failure at request %d: %d responses, %d attempts (%d fault-free)", n, st.Responses(), st.Attempts(), clean.Attempts())
		}
	}
}

// pages is q's chain as pages of names, a failed page asked again with the
// same token.
func pages(t *testing.T, a *Adapter, q keyrange.Query, st *azblobtest.Store) [][]string {
	t.Helper()
	got, err := walk(a, q, 1000, injected)
	if err != nil {
		t.Fatalf("%v\n%s", err, st)
	}
	out := make([][]string, len(got))
	for i, p := range got {
		out[i] = names(p)
	}
	return out
}

// TestCharging: §6's worked cases. Max 1, keys {a, b}, responses [a]+M,
// []+M, [b]: three. Prefix a, After a, Through ab, Max 1, keys {a, ab}:
// [a]+M (the endpoint, dropped), then [ab]: two.
func TestCharging(t *testing.T) {
	st, a := run(t, []string{"a", "b"}, keyrange.Query{Max: 1}, func(s *azblobtest.Store) {
		s.Empty = func(n int) bool { return n == 1 }
	})
	if st.Responses() != 3 || a.Counter(CEmptyPages) != 1 {
		t.Errorf("%d responses, %d empty pages\n%s", st.Responses(), a.Counter(CEmptyPages), st)
	}
	q := bounded("a", "a", "ab")
	q.Max = 1
	if st, _ := run(t, []string{"a", "ab"}, q, nil); st.Responses() != 2 {
		t.Errorf("%d responses\n%s", st.Responses(), st)
	}
	// The endpoint arriving after empty continuing pages is still dropped.
	run(t, []string{"a", "ab"}, q, func(s *azblobtest.Store) { s.Empty = func(n int) bool { return n < 3 } })
}

// TestMaximumLengthBounds: bounds of 1,024 UTF-16 code units alternating
// U+E000 and U+10000 cut the scope into spans by the thousand, almost all
// empty. Each span is entered by one seek answered by one response, and
// nothing accumulates: the responses are the spans of §3.1 and at most the
// scope's three keys are ever buffered.
func TestMaximumLengthBounds(t *testing.T) {
	after := "a" + strings.Repeat(E+S, 341)           // 1 + 341 × 3 units
	through := "a" + S + strings.Repeat(E+S, 340) + E // 1 + 2 + 340 × 3 + 1
	keys := []string{"a" + E, "a" + E1, "a" + S, "a" + S + E, "b"}
	q := bounded("", after, through)
	spans := len(decompose(normalise(q.Prefix, q.After, q.Through, q.Bounded), q.Through))
	if spans < 1000 || spans > stackBound(after, through) {
		t.Fatalf("%d spans", spans)
	}
	st, a := run(t, keys, q, nil)
	if st.Responses() != spans || a.Counter(CSeeks) != int64(spans) {
		t.Errorf("%d responses, %d seeks, %d spans", st.Responses(), a.Counter(CSeeks), spans)
	}
	if a.Counter(CBufferRowsMax) > 3 || a.Counter(CStackMax) != int64(spans) {
		t.Errorf("buffer max %d, stack max %d", a.Counter(CBufferRowsMax), a.Counter(CStackMax))
	}
	t.Logf("%d spans, %d probes", spans, a.Counter(CProbes))
}

// TestTokens: a token is accepted only on its own chain, with the sequence
// it was returned with and the chain's query.
func TestTokens(t *testing.T) {
	st := azblobtest.NewStore([]string{"a", "b", "c"})
	a := New(st, "c", nil, 0)
	q := keyrange.Query{Max: 1}
	p, err := a.Fetch(context.Background(), &q)
	if err != nil || !p.Truncated {
		t.Fatal(p, err)
	}
	q.Token = p.Token
	other := q
	other.Prefix = "a"
	for _, bad := range []keyrange.Query{{Max: 1, Token: "1:9"}, {Max: 1, Token: "x"}, {Max: 1, Token: "7:1"}, other} {
		if _, err := a.Fetch(context.Background(), &bad); !errors.Is(err, ErrToken) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
	if _, err := a.Fetch(context.Background(), &q); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Fetch(context.Background(), &q); !errors.Is(err, ErrToken) {
		t.Errorf("a token used twice: %v", err)
	}
}

// TestCancelledRead: a cancelled read fails the page and registers
// nothing.
func TestCancelledRead(t *testing.T) {
	a := New(azblobtest.NewStore([]string{"a"}), "c", nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Fetch(ctx, &keyrange.Query{Max: 1}); !errors.Is(err, context.Canceled) || a.Len() != 0 {
		t.Errorf("%v, %d chains", err, a.Len())
	}
}

// TestSuccessorSeeksSentAsIs: a successor ending in '.' (after "a-") and
// one that is DEL (after "a~") are sent as startFrom unchanged, and the
// walk stays exact.
func TestSuccessorSeeksSentAsIs(t *testing.T) {
	for _, x := range []struct{ q, succ string }{{"a-", "a."}, {"a~", "a\x7f"}} {
		keys := []string{x.q + S, x.q + E + "z", x.succ, x.succ + "b", x.succ + S, "b"}
		st, _ := run(t, keys, keyrange.Query{After: x.q + E, Max: 1000}, nil)
		sent := false
		for _, r := range st.Log() {
			sent = sent || r.In.StartFrom == x.succ
		}
		if !sent {
			t.Errorf("after %q: startFrom %q never sent\n%s", x.q+E, x.succ, st)
		}
	}
}

// TestStackBoundAtMaximumBounds: the 1,024-unit bounds with the most
// first spans found (3,070) stay within the chain's bound and list
// exactly.
func TestStackBoundAtMaximumBounds(t *testing.T) {
	after, through := strings.Repeat(E, 1024), S+strings.Repeat(E, 1022)
	q := bounded("", after, through)
	spans := len(decompose(normalise(q.Prefix, q.After, q.Through, q.Bounded), q.Through))
	if spans != 3070 || spans > 3*(units(after)+units(through))+1 {
		t.Fatalf("%d spans", spans)
	}
	_, a := run(t, []string{E, E + S, S, S + E, "b"}, q, nil)
	if a.Counter(CStackMax) < int64(spans) {
		t.Errorf("stack max %d", a.Counter(CStackMax))
	}
}

// TestChainsEvicted: first pages never continued keep the registry at
// MaxChains, the least recently used evicted (its token refused) and the
// retained buffers bounded; a chain continued between them is always the
// most recently used and is never evicted.
func TestChainsEvicted(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("k%03d", i), fmt.Sprintf("k%03d", i)+S, fmt.Sprintf("k%03d", i)+E)
	}
	st := azblobtest.NewStore(keys)
	st.Short = every(5)
	a := New(st, "c", nil, 0)
	live := keyrange.Query{Max: 1}
	var first string
	for i := range 4 * MaxChains {
		q := keyrange.Query{After: keys[i], Max: 1}
		p, err := a.Fetch(context.Background(), &q)
		if err != nil || !p.Truncated {
			t.Fatal(p, err)
		}
		if i == 0 {
			first = p.Token
		}
		if p, err = a.Fetch(context.Background(), &live); err != nil || !p.Truncated {
			t.Fatalf("the continued chain at %d: %v", i, err)
		}
		live.Token = p.Token
		if a.Len() > MaxChains {
			t.Fatalf("%d chains", a.Len())
		}
	}
	if a.Len() != MaxChains || a.Counter(CChainsMax) != MaxChains || a.Counter(CChainsEvicted) != int64(3*MaxChains+1) {
		t.Errorf("%d chains, max %d, %d evicted", a.Len(), a.Counter(CChainsMax), a.Counter(CChainsEvicted))
	}
	a.mu.Lock()
	rows := 0
	for _, c := range a.chains {
		for i := range c.stack {
			rows += len(c.stack[i].buf)
		}
	}
	a.mu.Unlock()
	if rows > MaxChains*5 {
		t.Errorf("%d rows retained", rows)
	}
	if _, err := a.Fetch(context.Background(), &keyrange.Query{After: keys[0], Max: 1, Token: first}); !errors.Is(err, ErrToken) {
		t.Errorf("an evicted chain's token: %v", err)
	}
}

// TestThroughEndsItsSpan: an item equal to Through ends its span without
// reading past it. Keys {a, b, c, d}, (a, b], Max 2: one response [a, b]
// with a marker, b emitted, the chain done. At a Through holding a
// scalar above U+FFFF, the S group's own span stops at it too.
func TestThroughEndsItsSpan(t *testing.T) {
	q := bounded("", "a", "b")
	q.Max = 2
	st, a := run(t, []string{"a", "b", "c", "d"}, q, nil)
	if st.Responses() != 1 || a.Counter(COverlapRows) != 0 {
		t.Errorf("%d responses, %d overlap rows\n%s", st.Responses(), a.Counter(COverlapRows), st)
	}
	// (ab, aS]: spans (ab, a\xEE), [a\xEE, a\xF0), [a\xF0, aS\x00); the
	// last seeks aS and stops there, before aSb and aE.
	keys := []string{"ac", "a" + E, "a" + S, "a" + S + "b", "b"}
	if st, _ := run(t, keys, bounded("", "ab", "a"+S), nil); st.Responses() != 3 || st.Log()[2].Names[0] != "a"+S {
		t.Errorf("%d responses\n%s", st.Responses(), st)
	}
}

// blocking is a Lister that holds its first continuation (a request with
// a marker) until release is closed, saying so on entered.
type blocking struct {
	l       Lister
	held    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *blocking) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	if in.Marker != "" && b.held.CompareAndSwap(false, true) {
		b.entered <- struct{}{}
		<-b.release
	}
	return b.l.List(ctx, in)
}

// TestInFlightChainNotEvicted: a chain whose continuation is in flight is
// never evicted, however many chains register meanwhile (the others are,
// least recently used first); it commits when its read returns, and its
// new token continues it.
func TestInFlightChainNotEvicted(t *testing.T) {
	var keys []string
	for i := range 3 * MaxChains {
		keys = append(keys, fmt.Sprintf("k%03d", i))
	}
	b := &blocking{l: azblobtest.NewStore(keys), entered: make(chan struct{}, 1), release: make(chan struct{})}
	a := New(b, "c", nil, 0)
	q := keyrange.Query{Max: 1}
	p, err := a.Fetch(context.Background(), &q)
	if err != nil || !p.Truncated {
		t.Fatal(p, err)
	}
	q.Token = p.Token
	next := make(chan string, 1)
	go func() {
		p, err := a.Fetch(context.Background(), &q)
		if err != nil || len(p.Items) != 1 || p.Items[0].Name != keys[1] {
			t.Errorf("the in-flight continuation: %+v %v", p, err)
			next <- ""
			return
		}
		next <- p.Token
	}()
	<-b.entered
	for i := range 2 * MaxChains {
		if _, err := a.Fetch(context.Background(), &keyrange.Query{After: keys[i], Max: 1}); err != nil {
			t.Fatal(err)
		}
	}
	// 1 + 2 × MaxChains registered, MaxChains kept, the busy one among
	// them.
	if a.Len() != MaxChains || a.Counter(CChainsEvicted) != MaxChains+1 {
		t.Fatalf("%d chains, %d evicted", a.Len(), a.Counter(CChainsEvicted))
	}
	close(b.release)
	tok := <-next
	if tok == "" {
		t.FailNow()
	}
	q.Token = tok
	if p, err := a.Fetch(context.Background(), &q); err != nil || p.Items[0].Name != keys[2] {
		t.Fatalf("after the commit: %+v %v", p, err)
	}
}

// Counter is the value of counter i of CounterDefs.
func (a *Adapter) Counter(i int) int64 { return a.set.Load(a.base + counters.ID(i)) }

// Len is the number of chains registered.
func (a *Adapter) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.chains)
}
