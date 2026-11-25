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

// On a store that answers correctly the check never asks anything: the same
// keys, no contradiction, no request of its own, across the keyspaces,
// worker counts, short pages, the seed and structure probes.
func TestEndCheckHealthyStoresCostNothing(t *testing.T) {
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
				if st.Get(cEndContradicted)+st.Get(cEndWitnessChecks)+st.Get(cEndAskedAgain) != 0 {
					t.Fatalf("%s: contradicted %d, witness checks %d, asked again %d on a correct store", what,
						st.Get(cEndContradicted), st.Get(cEndWitnessChecks), st.Get(cEndAskedAgain))
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
// having seen witness (a key, or a folder when it ends in '/').
func checkOne(t *testing.T, api lister, witness, after string, out *Page[Object], c string) (*Page[Object], Stats, error) {
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
	got, err := p.checkEnd(context.Background(), &Query{After: after, Max: 5}, out, c)
	return got, p.Stats(), err
}

func TestEndCheckDecisions(t *testing.T) {
	defer SetRequeueBackoff(time.Microsecond, time.Microsecond)()
	keys := []string{"a/1", "a/2", "a/3", "b/1", "b/2"}
	empty := func() *Page[Object] { return &Page[Object]{} }
	t.Run("no witness past the page: no request", func(t *testing.T) {
		f := newSorted(keys, false, 1)
		got, st, err := checkOne(t, f, "a/1", "a/1", empty(), "a/1")
		if err != nil || len(got.Rows) != 0 || f.Calls != 0 || st.Get(cEndClaims) != 1 || st.Get(cEndContradicted) != 0 {
			t.Fatalf("rows %d err %v calls %d claims %d contradicted %d", len(got.Rows), err, f.Calls, st.Get(cEndClaims), st.Get(cEndContradicted))
		}
	})
	t.Run("a folder marker equal to the page's end is no witness", func(t *testing.T) {
		f := newSorted([]string{"a/"}, false, 1)
		_, st, err := checkOne(t, f, "a/", "", &Page[Object]{Rows: []Row{{Key: "a/"}}}, "")
		if err != nil || st.Get(cEndContradicted) != 0 || f.Calls != 0 {
			t.Fatalf("err %v contradicted %d calls %d", err, st.Get(cEndContradicted), f.Calls)
		}
	})
	t.Run("a store that keeps omitting: ErrOmitted", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(int, *keyrangetest.ListInput) bool { return true }}
		_, st, err := checkOne(t, s, "b/", "a/1", empty(), "a/1")
		if !errors.Is(err, source.ErrOmitted) || st.Get(cEndOmitted) != 1 || st.Get(cEndAskedAgain) != endTries {
			t.Fatalf("err %v omitted %d asked again %d", err, st.Get(cEndOmitted), st.Get(cEndAskedAgain))
		}
	})
	t.Run("keys written after the page: the range goes on", func(t *testing.T) {
		// The page was the store's true answer when it was asked; the keys
		// after a/1 came later, and the run saw b/2 meanwhile.
		f := newSorted(keys, false, 1)
		got, st, err := checkOne(t, f, "b/2", "a/1", empty(), "a/1")
		if err != nil || st.Get(cEndRacedNew) != 1 || st.Get(cEndOmitted) != 0 {
			t.Fatalf("err %v raced %d omitted %d", err, st.Get(cEndRacedNew), st.Get(cEndOmitted))
		}
		if k := keysOf(got.Rows); !slices.Equal(k, []string{"a/2", "a/3", "b/1", "b/2"}) || got.Truncated {
			t.Fatalf("the range goes on with %v (truncated %v), want a/2..b/2", k, got.Truncated)
		}
	})
	t.Run("a store whose listing lags: the range goes on", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(n int, _ *keyrangetest.ListInput) bool { return n < endTries-1 }}
		got, st, err := checkOne(t, s, "b/1", "a/3", empty(), "a/3")
		if err != nil || st.Get(cEndRacedNew) != 1 || len(got.Rows) != 2 {
			t.Fatalf("err %v raced %d rows %d", err, st.Get(cEndRacedNew), len(got.Rows))
		}
	})
	t.Run("a witness deleted since: the page stands", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(int, *keyrangetest.ListInput) bool { return true }}
		got, st, err := checkOne(t, s, "c/1", "b/2", empty(), "b/2")
		if err != nil || len(got.Rows) != 0 || st.Get(cEndRacedGone) != 1 || st.Get(cEndAskedAgain) != 0 {
			t.Fatalf("err %v rows %d gone %d asked again %d", err, len(got.Rows), st.Get(cEndRacedGone), st.Get(cEndAskedAgain))
		}
	})
	t.Run("a last key the provider refuses as a bound: asked again after its accepted prefix", func(t *testing.T) {
		// The fake refuses control characters in start-after, as S3 does.
		f := newSorted([]string{"a/\x01", "a/\x01x", "b/1"}, false, 1)
		got, st, err := checkOne(t, f, "b/1", "a/", &Page[Object]{Rows: []Row{{Key: "a/\x01"}}}, "a/")
		if err != nil || st.Get(cEndRacedNew) != 1 || !slices.Equal(keysOf(got.Rows), []string{"a/\x01", "a/\x01x", "b/1"}) {
			t.Fatalf("err %v raced %d keys %q", err, st.Get(cEndRacedNew), keysOf(got.Rows))
		}
	})
	t.Run("a store that answers the start key again: a broken listing, not a loop", func(t *testing.T) {
		echo := &echoAfter{sorted: newSorted(keys, false, 1)}
		_, _, err := checkOne(t, echo, "b/2", "a/1", &Page[Object]{Rows: []Row{{Key: "a/2"}}}, "a/1")
		if !errors.Is(err, source.ErrProtocol) || echo.n > 2 {
			t.Fatalf("err %v after %d echoed pages, want a protocol violation at the first", err, echo.n)
		}
	})
	t.Run("a page with keys: the claim is after its last key", func(t *testing.T) {
		s := &omitScript{sorted: newSorted(keys, false, 1), omit: func(int, *keyrangetest.ListInput) bool { return false }}
		out := &Page[Object]{Rows: []Row{{Key: "a/2"}}}
		got, st, err := checkOne(t, s, "b/1", "a/1", out, "a/1")
		if err != nil || st.Get(cEndRacedNew) != 1 || !slices.Equal(keysOf(got.Rows), []string{"a/2", "a/3", "b/1", "b/2"}) {
			t.Fatalf("err %v raced %d keys %v", err, st.Get(cEndRacedNew), keysOf(got.Rows))
		}
	})
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
