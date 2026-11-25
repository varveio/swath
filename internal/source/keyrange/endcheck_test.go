// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange/keyrangetest"
)

// runEnd runs a parallel listing with the end check on (unbounded) or off,
// and returns the error instead of failing.
func runEnd(t *testing.T, api lister, opt Options, workers int, unbounded bool) ([]string, Stats, error) {
	t.Helper()
	opt.Workers = workers
	p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, Size: func(o *Object) int64 { return o.Size },
		Unbounded: unbounded}, fetchFrom(api))
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	_, err = order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 500 * 256})
	return keysOf(c.rows), p.Stats(), err
}

// naraShape is the shape that lost keys on MinIO: two folders whose names
// extend the third's with '-' sort before it ('-' < '/'), each holding more
// keys than a page.
func naraShape(n int) []string {
	var keys []string
	for i := range n {
		keys = append(keys, fmt.Sprintf("census-descriptions/%05d", i), fmt.Sprintf("census-maps/%05d.jpg", i))
		keys = append(keys, fmt.Sprintf("census/%03d-State/%05d.jpg", i%7, i))
	}
	return keys
}

// The omitting fake loses the folder's keys without an error when the check
// is off (it models the fault), and the check turns that into ErrOmitted.
func TestEndCheckCatchesOmission(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	keys := naraShape(1500)
	for _, w := range []int{4, 16, 64} {
		for _, pageSize := range []int{100, 1000} {
			what := fmt.Sprintf("workers %d page %d", w, pageSize)
			omits := func() *sorted { f := newSorted(keys, false, 1); f.Omits = true; return f }
			opt := Options{Seed: true, SeedFirstPage: true, PageSize: pageSize}
			got, _, err := runEnd(t, omits(), opt, w, false)
			if err != nil || len(got) >= len(keys) {
				t.Fatalf("%s, check off: %d of %d keys, err %v: the fake does not omit keys", what, len(got), len(keys), err)
			}
			_, st, err := runEnd(t, omits(), opt, w, true)
			if !errors.Is(err, source.ErrOmitted) {
				t.Fatalf("%s, check on: err %v, want ErrOmitted (claims %d, contradicted %d)", what, err,
					st.Get(cEndClaims), st.Get(cEndContradicted))
			}
			if errors.Is(err, source.ErrProtocol) {
				t.Fatalf("%s: an omission is not a protocol violation (the journal must stay resumable)", what)
			}
			if st.Get(cEndOmitted) == 0 || st.Get(cEndAskedAgain) < endTries || st.Get(cEndWitnessChecks) <= endTries {
				t.Fatalf("%s: omitted %d, asked again %d, witness checks %d", what, st.Get(cEndOmitted), st.Get(cEndAskedAgain), st.Get(cEndWitnessChecks))
			}
		}
	}
}

// On a store that answers correctly the check finds nothing to contradict:
// the same keys, no contradiction and no request asked again, across the
// keyspaces, worker counts, short pages, the seed and structure probes. Its
// only requests are ladders, each at most one per folder level of its
// claim and two per level above, and few: a ladder's floor covers every
// later claim above it.
func TestEndCheckHealthyStoresCostLittle(t *testing.T) {
	spaces := keyspaces()
	for name, keys := range hostile() {
		spaces[name] = keys
	}
	spaces["nara"] = naraShape(300)
	for name, keys := range spaces {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for i, w := range []int{1, 4, 64} {
			for _, short := range []bool{false, true} {
				opt := Options{Seed: i != 1, SeedFirstPage: i != 1, PageSize: 7}
				if short {
					opt.Structure = StructureAuto
				}
				got, st, err := runEnd(t, newSorted(keys, short, int64(i)), opt, w, true)
				what := fmt.Sprintf("%s workers %d short %v", name, w, short)
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("%s: %d keys, want %d", what, len(got), len(want))
				}
				if st.Get(cEndContradicted)+st.Get(cEndWitnessChecks)+st.Get(cEndAskedAgain)+st.Get(cEndLadderFound) != 0 {
					t.Fatalf("%s: contradicted %d, witness checks %d, asked again %d, ladder found %d on a correct store", what,
						st.Get(cEndContradicted), st.Get(cEndWitnessChecks), st.Get(cEndAskedAgain), st.Get(cEndLadderFound))
				}
				depth := int64(0)
				for _, k := range want {
					depth = max(depth, int64(strings.Count(k, "/")))
				}
				if l, r := st.Get(cEndLadders), st.Get(cEndLadderRequests); r > l*(2*depth+1) || l > int64(w)+2 {
					t.Fatalf("%s: %d ladders, %d ladder requests (depth %d) for %d claims", what, l, r, depth, st.Get(cEndClaims))
				}
			}
		}
	}
}

// A provider that bounds its requests (GCS, Azure) checks nothing: its
// final page says only that its range is done.
func TestEndCheckOffForBoundedProviders(t *testing.T) {
	_, st, err := runEnd(t, newSorted(naraShape(300), false, 1), Options{Seed: true, PageSize: 50}, 8, false)
	if err != nil {
		t.Fatal(err)
	}
	if n := st.Get(cEndClaims); n != 0 {
		t.Fatalf("end_check.claims %d without Description.Unbounded", n)
	}
	if st.Counters.On("end_check") {
		t.Fatal("the end_check counters are reported for a provider that does not check")
	}
}

// omitScript is a fake whose answers to start-after requests a test scripts:
// answer(n, in) returns an omitted page for the n-th one (from 0) when
// omit says so, else the store's own answer.
type omitScript struct {
	*sorted
	mu   sync.Mutex
	n    int
	omit func(n int, in *keyrangetest.ListInput) bool
}

func (s *omitScript) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	if in.StartAfter != "" && in.ContinuationToken == "" && in.Delimiter == "" {
		s.mu.Lock()
		n := s.n
		s.n++
		s.mu.Unlock()
		if s.omit(n, in) {
			return &keyrangetest.Page{}, nil
		}
	}
	return s.sorted.List(ctx, in)
}

// checkOne runs checkEnd on one claim: the page out (final) that a chain
// started after after returned, with c the cursor before it, the run
// having seen witness (a key, or a folder when it ends in '/'). more: keys
// follow l, the claim's last key, and the chain goes on after it.
func checkOne(t *testing.T, api lister, witness, after string, out *Page[Object], c string) (more bool, l string, _ Stats, _ error) {
	t.Helper()
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Unbounded: true}, fetchFrom(api))
	if err != nil {
		t.Fatal(err)
	}
	if witness != "" {
		seen := &Page[Object]{}
		if witness[len(witness)-1] == '/' {
			seen.Prefixes = []string{witness}
		} else {
			seen.Rows = []Row{{Key: witness}}
		}
		p.witness(seen)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // a looping check fails, not hangs
	defer cancel()
	more, l, err = p.checkEnd(ctx, &Query{After: after, Max: 5}, out, c)
	return more, l, p.Stats(), err
}

func TestEndCheckDecisions(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	keys := []string{"a/1", "a/2", "a/3", "b/1", "b/2"}
	empty := func() *Page[Object] { return &Page[Object]{} }
	t.Run("no witness and nothing after: a ladder confirms, and is the floor", func(t *testing.T) {
		f := newSorted(keys, false, 1)
		p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Unbounded: true}, fetchFrom(f))
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range []string{"b/2", "b/3", "c"} {
			if more, _, err := p.checkEnd(context.Background(), &Query{After: "a/1", Max: 5}, &Page[Object]{Rows: []Row{{Key: l}}}, "a/1"); err != nil || more {
				t.Fatalf("claim after %q: more %v err %v", l, more, err)
			}
		}
		st := p.Stats()
		// b/2: the folder b/ after b/2, the scope after b0, and b0 itself.
		if st.Get(cEndLadders) != 1 || st.Get(cEndLadderRequests) != 3 || f.Calls != 3 || st.Get(cEndFloor) != 2 || st.Get(cEndContradicted) != 0 {
			t.Fatalf("ladders %d requests %d calls %d floor %d contradicted %d", st.Get(cEndLadders), st.Get(cEndLadderRequests), f.Calls,
				st.Get(cEndFloor), st.Get(cEndContradicted))
		}
	})
	t.Run("no witness, keys after: the ladder finds them", func(t *testing.T) {
		f := newSorted(keys, false, 1)
		more, l, st, err := checkOne(t, f, "", "a/1", empty(), "a/1")
		if err != nil || !more || l != "a/1" || st.Get(cEndLadderFound) != 1 || st.Get(cEndRacedNew) != 1 {
			t.Fatalf("err %v more %v after %q found %d raced %d", err, more, l, st.Get(cEndLadderFound), st.Get(cEndRacedNew))
		}
	})
	t.Run("a folder marker equal to the page's end is no witness", func(t *testing.T) {
		f := newSorted([]string{"a/"}, false, 1)
		_, _, st, err := checkOne(t, f, "a/", "", &Page[Object]{Rows: []Row{{Key: "a/"}}}, "")
		if err != nil || st.Get(cEndContradicted) != 0 || st.Get(cEndLadders) != 1 {
			t.Fatalf("err %v contradicted %d ladders %d", err, st.Get(cEndContradicted), st.Get(cEndLadders))
		}
	})
	t.Run("a store that keeps omitting: ErrOmitted", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(int, *keyrangetest.ListInput) bool { return true }}
		_, _, st, err := checkOne(t, s, "b/", "a/1", empty(), "a/1")
		if !errors.Is(err, source.ErrOmitted) || st.Get(cEndOmitted) != 1 || st.Get(cEndAskedAgain) != endTries {
			t.Fatalf("err %v omitted %d asked again %d", err, st.Get(cEndOmitted), st.Get(cEndAskedAgain))
		}
	})
	t.Run("keys written after the page: the chain goes on after it", func(t *testing.T) {
		// The page was the store's true answer when it was asked; the keys
		// after a/1 came later, and the run saw b/2 meanwhile.
		f := newSorted(keys, false, 1)
		more, l, st, err := checkOne(t, f, "b/2", "a/1", empty(), "a/1")
		if err != nil || !more || l != "a/1" || st.Get(cEndRacedNew) != 1 || st.Get(cEndOmitted) != 0 {
			t.Fatalf("err %v more %v after %q raced %d omitted %d", err, more, l, st.Get(cEndRacedNew), st.Get(cEndOmitted))
		}
	})
	t.Run("a store whose listing lags: the chain goes on", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(n int, _ *keyrangetest.ListInput) bool { return n < endTries-1 }}
		more, _, st, err := checkOne(t, s, "b/1", "a/3", empty(), "a/3")
		if err != nil || !more || st.Get(cEndRacedNew) != 1 {
			t.Fatalf("err %v more %v raced %d", err, more, st.Get(cEndRacedNew))
		}
	})
	t.Run("a witness deleted since: the page stands", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(int, *keyrangetest.ListInput) bool { return true }}
		more, _, st, err := checkOne(t, s, "c/1", "b/2", empty(), "b/2")
		if err != nil || more || st.Get(cEndRacedGone) != 1 || st.Get(cEndAskedAgain) != 0 {
			t.Fatalf("err %v more %v gone %d asked again %d", err, more, st.Get(cEndRacedGone), st.Get(cEndAskedAgain))
		}
	})
	t.Run("a last key the provider refuses as a bound: asked again after its accepted prefix", func(t *testing.T) {
		// The fake refuses control characters in start-after, as S3 does.
		f := newSorted([]string{"a/\x01", "a/\x01x", "b/1"}, false, 1)
		more, l, st, err := checkOne(t, f, "b/1", "a/", &Page[Object]{Rows: []Row{{Key: "a/\x01"}}}, "a/")
		if err != nil || !more || l != "a/\x01" || st.Get(cEndRacedNew) != 1 {
			t.Fatalf("err %v more %v after %q raced %d", err, more, l, st.Get(cEndRacedNew))
		}
	})
	t.Run("pages below a refused last key that repeat their token: a broken listing, not a loop", func(t *testing.T) {
		cyc := &belowCycle{sorted: newSorted([]string{"a/\x01", "a/\x02", "b/1"}, false, 1), bound: "a/"}
		_, _, _, err := checkOne(t, cyc, "b/1", "a/", &Page[Object]{Rows: []Row{{Key: "a/\x05"}}}, "a/")
		if !errors.Is(err, source.ErrProtocol) {
			t.Fatalf("err %v after %d cycling pages, want a protocol violation", err, cyc.n)
		}
	})
	t.Run("a store that answers the start key again: a broken listing, not a loop", func(t *testing.T) {
		echo := &echoAfter{sorted: newSorted(keys, false, 1)}
		_, _, _, err := checkOne(t, echo, "b/2", "a/1", &Page[Object]{Rows: []Row{{Key: "a/2"}}}, "a/1")
		if !errors.Is(err, source.ErrProtocol) || echo.n > 2 {
			t.Fatalf("err %v after %d echoed pages, want a protocol violation at the first", err, echo.n)
		}
	})
	t.Run("a page with keys: the claim is after its last key", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(int, *keyrangetest.ListInput) bool { return false }}
		more, l, st, err := checkOne(t, s, "b/1", "a/1", &Page[Object]{Rows: []Row{{Key: "a/2"}}}, "a/1")
		if err != nil || !more || l != "a/2" || st.Get(cEndRacedNew) != 1 {
			t.Fatalf("err %v more %v after %q raced %d", err, more, l, st.Get(cEndRacedNew))
		}
	})
}

// belowCycle answers a chain that starts after bound with a truncated page
// of its first key and the same token forever: pages below the last key
// asked after that make no progress.
type belowCycle struct {
	*sorted
	bound string
	n     int
}

func (c *belowCycle) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	if in.Delimiter == "" && (in.ContinuationToken == "T" || in.ContinuationToken == "" && in.StartAfter == c.bound) {
		c.n++
		return &keyrangetest.Page{Contents: []keyrangetest.Object{{Key: c.Keys[0]}}, IsTruncated: true, NextContinuationToken: "T"}, nil
	}
	return c.sorted.List(ctx, in)
}

// shortFinal answers every request that starts after a key with the next
// key alone, as a final page, though keys follow: a service whose listing
// ends early on every page and progresses only when asked again.
type shortFinal struct{ *sorted }

func (s *shortFinal) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	out, err := s.sorted.List(ctx, in)
	if err == nil && in.StartAfter != "" && in.ContinuationToken == "" && in.Delimiter == "" && len(out.Contents) > 1 {
		out = &keyrangetest.Page{Contents: out.Contents[:1]}
	}
	return out, err
}

// batches records the size of each batch of rows a range emits, and its
// fences.
type batches struct {
	sizes  []int
	keys   []string
	fences int
}

func (b *batches) Rows(rows []Row) error {
	b.sizes = append(b.sizes, len(rows))
	b.keys = append(b.keys, keysOf(rows)...)
	return nil
}
func (b *batches) Child(Range) error         { return errors.New("no split expected") }
func (b *batches) Span(string, string, bool) {}
func (b *batches) Fence(string, bool)        { b.fences++ }

// A service that ends every page early while keys follow, below a witness
// far ahead: the range emits each recovered page as it comes, never a
// buffer of all of them, and lists every key (review of #684: checkEnd
// once concatenated every recovered page before returning).
func TestEndCheckStreamsRecoveredPages(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("a/%04d", i))
	}
	keys = append(keys, "z/1")
	api := &shortFinal{newSorted(keys, false, 1)}
	p, err := New(Options{Workers: 1, PageSize: 50, SplitCoin: func() bool { return false }},
		Description[Object]{Target: "test", Listing: Listing, Unbounded: true}, fetchFrom(api))
	if err != nil {
		t.Fatal(err)
	}
	p.witness(&Page[Object]{Prefixes: []string{"z/"}})
	b := &batches{}
	if err := p.expandRange(t.Context(), Range{lo: "a/", open: true, id: 1, born: fromSeed}, b); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b.keys, keys) {
		t.Fatalf("%d keys listed, want %d", len(b.keys), len(keys))
	}
	if m := slices.Max(b.sizes); m > 1 {
		t.Fatalf("a batch of %d rows: recovered pages were buffered, not emitted one at a time", m)
	}
	if st := p.Stats(); st.Get(cEndRacedNew) < 199 || b.fences < 199 {
		t.Fatalf("raced %d, fences %d", st.Get(cEndRacedNew), b.fences)
	}
}

// A service that answers the range's chain after a key with an empty final
// page every time, while the same request asked again lists keys after it:
// no progress, an omission, not a loop.
func TestEndCheckRestartWithoutProgress(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	keys := []string{"a/1", "a/2", "b/1"}
	n := 0
	// Every other request after a/1 is empty: the range's, then the one
	// asked again lists a/2.
	s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(_ int, in *keyrangetest.ListInput) bool {
		if in.StartAfter != "a/1" {
			return false
		}
		n++
		return n%2 == 1
	}}
	p, err := New(Options{Workers: 1, PageSize: 5}, Description[Object]{Target: "test", Listing: Listing, Unbounded: true}, fetchFrom(s))
	if err != nil {
		t.Fatal(err)
	}
	p.witness(&Page[Object]{Rows: []Row{{Key: "b/1"}}})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = p.expandRange(ctx, Range{lo: "a/1", open: true, id: 1, born: fromSeed}, &batches{})
	if !errors.Is(err, source.ErrOmitted) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v after %d requests after a/1, want ErrOmitted", err, n)
	}
}

// echoAfter answers every start-after request with that key alone, as a
// final page: a service that makes no progress.
type echoAfter struct {
	*sorted
	n int
}

func (e *echoAfter) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	if in.StartAfter != "" && in.ContinuationToken == "" && in.Delimiter == "" {
		e.n++
		return &keyrangetest.Page{Contents: []keyrangetest.Object{{Key: in.StartAfter}}}, nil
	}
	return e.sorted.List(ctx, in)
}

func TestLadder(t *testing.T) {
	q := func(prefix, after string) Query { return Query{Prefix: prefix, After: after, Max: 1} }
	for _, c := range []struct {
		l, scope string
		want     []Query
	}{
		{"k", "", []Query{q("", "k")}},
		{"a/b/k", "", []Query{q("a/b/", "a/b/k"), q("a/", "a/b0"), q("a/b0", ""), q("", "a0"), q("a0", "")}},
		{"a/b/k", "a/", []Query{q("a/b/", "a/b/k"), q("a/", "a/b0"), q("a/b0", "")}},
		{"a/b/k", "a/b/", []Query{q("a/b/", "a/b/k")}},
		// A scope that is not a folder: "a" covers a-x/ and a/.
		{"a/b/k", "a", []Query{q("a/b/", "a/b/k"), q("a/", "a/b0"), q("a/b0", ""), q("a", "a0"), q("a0", "")}},
		{"a/", "", []Query{q("a/", "a/"), q("", "a0"), q("a0", "")}},
	} {
		if got := ladder(c.l, c.scope); !slices.Equal(got, c.want) {
			t.Errorf("ladder(%q, %q) = %+v, want %+v", c.l, c.scope, got, c.want)
		}
	}
}

// Whatever the tiling, an omitting store never ends a run with keys
// missing and no error: random splits with the seed off (neighbours that
// all lie), one worker (the lying tile is the last), several scopes.
func TestEndCheckNeverShortOnOmittingStore(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	keys := naraShape(400)
	for _, scope := range []string{"", "census", "census/"} {
		var want []string
		for _, k := range slices.Sorted(slices.Values(keys)) {
			if strings.HasPrefix(k, scope) {
				want = append(want, k)
			}
		}
		for s := range 12 {
			for _, w := range []int{1, 4, 16} {
				rng := rand.New(rand.NewSource(int64(s*100 + w)))
				var mu sync.Mutex
				coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				opt := Options{Prefix: scope, Seed: s%2 == 0, SeedFirstPage: s%2 == 0, PageSize: 50}
				if s%3 == 0 {
					opt.SplitCoin = coin
				}
				f := newSorted(keys, s%4 == 3, int64(s))
				f.Omits = true
				got, st, err := runEnd(t, f, opt, w, true)
				what := fmt.Sprintf("scope %q seed %d workers %d", scope, s, w)
				if err == nil && !slices.Equal(got, want) {
					t.Fatalf("%s: %d of %d keys and no error (claims %d, ladders %d, contradicted %d)", what, len(got), len(want),
						st.Get(cEndClaims), st.Get(cEndLadders), st.Get(cEndContradicted))
				}
				if err != nil && !errors.Is(err, source.ErrOmitted) {
					t.Fatalf("%s: %v", what, err)
				}
			}
		}
	}
}

// A resume from a key inside the omitting folder: the sequential chain and
// the seed's single first page (the rest of the scope fits in one page)
// check their end too.
func TestEndCheckResumedChains(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	// The siblings hold 3,000 keys, more than a page; the rest of the scope
	// after the resume key (the last folder's) fits in one.
	keys := naraShape(1500)
	after := "census/006-State/00006.jpg"
	var rest int
	for _, k := range keys {
		if k > after {
			rest++
		}
	}
	for _, c := range []struct {
		name string
		opt  Options
	}{
		{"sequential", Options{Sequential: true, PageSize: 50}},
		{"seed first page", Options{Seed: true, SeedFirstPage: true, PageSize: 1000}},
	} {
		f := newSorted(keys, false, 1)
		f.Omits = true
		c.opt.Resume = &Resume{After: after}
		got, _, err := runEnd(t, f, c.opt, 4, false)
		if err != nil || len(got) >= rest {
			t.Fatalf("%s, check off: %d of %d keys, err %v: the fake does not omit keys", c.name, len(got), rest, err)
		}
		f = newSorted(keys, false, 1)
		f.Omits = true
		_, st, err := runEnd(t, f, c.opt, 4, true)
		if !errors.Is(err, source.ErrOmitted) {
			t.Fatalf("%s: err %v, want ErrOmitted (claims %d, ladders %d, found %d)", c.name, err,
				st.Get(cEndClaims), st.Get(cEndLadders), st.Get(cEndLadderFound))
		}
	}
}
