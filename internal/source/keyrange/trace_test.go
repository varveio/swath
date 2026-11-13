// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/runtrace"
)

// event is one decoded trace line.
type event map[string]any

func (e event) name() string { return e["event"].(string) }

func (e event) int(t *testing.T, f string) int64 {
	t.Helper()
	v, ok := e[f].(float64)
	if !ok {
		t.Fatalf("%s: field %s is %T %v, want a number", e.name(), f, e[f], e[f])
	}
	return int64(v)
}

func (e event) id(t *testing.T) uint64 { return uint64(e.int(t, "id")) }

// key decodes a key field: a string, or {"hex": ...} for bytes that are
// not UTF-8; ok is false when the field is absent.
func (e event) key(t *testing.T, f string) (string, bool) {
	t.Helper()
	switch v := e[f].(type) {
	case nil:
		return "", false
	case string:
		return v, true
	case map[string]any:
		b, err := hex.DecodeString(v["hex"].(string))
		if err != nil {
			t.Fatalf("%s: %s: %v", e.name(), f, err)
		}
		return string(b), true
	}
	t.Fatalf("%s: field %s is %T, want a key", e.name(), f, e[f])
	return "", false
}

func decode(t *testing.T, b []byte) []event {
	t.Helper()
	var out []event
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var e event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		out = append(out, e)
	}
	return out
}

// runTraced lists api through the order stage with a trace written to w
// (nil: none) and returns the rows' keys and the engine's stats.
func runTraced(t *testing.T, api lister, opt Options, workers int, w *runtrace.Writer) ([]string, Stats) {
	t.Helper()
	opt.Workers = workers
	p, err := newTest(api, opt)
	if err != nil {
		t.Fatal(err)
	}
	if w != nil {
		p.TraceTo(w)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 500 * 256}); err != nil {
		t.Fatal(err)
	}
	return keysOf(c.rows), p.Stats()
}

// traced is runTraced to a buffer, closed, and decoded.
func traced(t *testing.T, api lister, opt Options, workers int) ([]string, Stats, []event) {
	t.Helper()
	var buf bytes.Buffer
	w := runtrace.New(&buf)
	keys, st := runTraced(t, api, opt, workers, w)
	if err := w.Close(time.Second, "ok", ""); err != nil {
		t.Fatal(err)
	}
	return keys, st, decode(t, buf.Bytes())
}

// required are the fields every event of a kind carries (docs/internals/trace.md).
var required = map[string][]string{
	"run_start": {"trace_version", "tool", "version", "time", "prefix", "workers", "page_size", "sequential", "seed", "prior", "structure", "slow_split", "versions"},
	"seed":      {"dur_ns", "requests", "round_trips", "levels", "cuts", "stop", "keys"},
	"seeded":    {"id", "lo"},
	"claimed":   {"id", "worker", "lo", "depth", "born"},
	"page":      {"id", "worker", "after", "last", "rows", "dur_ns", "done"},
	"split":     {"id", "worker", "cursor", "pivot", "lower", "upper", "mechanism", "reason", "hunger", "rows", "depth", "gen"},
	"completed": {"id", "worker", "rows"},
	"failed":    {"id", "worker", "rows"},
	"run_end":   {"status", "events", "dropped"},
}

// checkTrace checks a complete trace of a successful run of keys against
// the schema and the engine's invariants, from the trace alone: every
// range made is claimed once and ends once (completed, or split, which
// ends it); a range's pages chain from its start, each after the last;
// a split is at its range's last cursor; the rows the pages report are
// the rows listed; and the intervals the ranges listed, completed ranges
// through their bound and split ranges through their cursor, tile the
// scope with no gap and no overlap.
func checkTrace(t *testing.T, what string, evs []event, keys []string) (splits int) {
	t.Helper()
	if len(evs) < 2 || evs[0].name() != "run_start" || evs[len(evs)-1].name() != "run_end" {
		t.Fatalf("%s: the trace does not run from run_start to run_end (%d events)", what, len(evs))
	}
	if evs[0].int(t, "trace_version") != runtrace.Version {
		t.Fatalf("%s: trace_version %v", what, evs[0]["trace_version"])
	}
	type rng struct {
		lo, hi, cur string
		open        bool
		claimed     int
		ends        int
		pages       int
		rows        int64
		made        bool
	}
	ranges := map[uint64]*rng{}
	get := func(id uint64) *rng {
		if ranges[id] == nil {
			ranges[id] = &rng{}
		}
		return ranges[id]
	}
	var leaves []span
	var rows int64
	seeded := 0
	for i, e := range evs {
		f, ok := required[e.name()]
		if !ok {
			t.Fatalf("%s: event %d: unknown kind %q", what, i, e.name())
		}
		for _, k := range append([]string{"ts_ns"}, f...) {
			if _, ok := e[k]; !ok {
				t.Fatalf("%s: %s lacks %s: %v", what, e.name(), k, e)
			}
		}
		// Concurrent ranges' events may be written slightly out of time
		// order; a range's own are on one goroutine, in order (below).
		if ts := e.int(t, "ts_ns"); ts < 0 {
			t.Fatalf("%s: negative ts_ns %d", what, ts)
		}
		switch e.name() {
		case "seed":
			if e["stop"] == "one_page" {
				leaves = append(leaves, span{"", "", true})
				rows += e.int(t, "keys")
			}
		case "seeded":
			if e["reused"] == true {
				continue
			}
			seeded++
			r := get(e.id(t))
			if r.made {
				t.Fatalf("%s: range %d made twice", what, e.id(t))
			}
			r.made = true
			r.lo, _ = e.key(t, "lo")
			r.hi, _ = e.key(t, "hi")
			r.open = e["open"] == true
		case "claimed":
			r := get(e.id(t))
			if !r.made {
				t.Fatalf("%s: range %d claimed before it was made", what, e.id(t))
			}
			if r.claimed++; r.claimed > 1 {
				t.Fatalf("%s: range %d claimed twice", what, e.id(t))
			}
			lo, _ := e.key(t, "lo")
			hi, _ := e.key(t, "hi")
			if lo != r.lo || hi != r.hi || (e["open"] == true) != r.open {
				t.Fatalf("%s: range %d claimed as (%q, %q] open=%v, made as (%q, %q] open=%v", what, e.id(t), lo, hi, e["open"], r.lo, r.hi, r.open)
			}
			r.cur = lo
		case "page":
			r := get(e.id(t))
			if r.claimed != 1 || r.ends != 0 {
				t.Fatalf("%s: a page of range %d outside its claim", what, e.id(t))
			}
			after, _ := e.key(t, "after")
			l, _ := e.key(t, "last")
			if after != r.cur {
				t.Fatalf("%s: range %d page %d starts after %q, its cursor is %q", what, e.id(t), r.pages, after, r.cur)
			}
			if n := e.int(t, "rows"); n > 0 && l <= after {
				t.Fatalf("%s: range %d page %d: %d rows, last %q not after %q", what, e.id(t), r.pages, n, l, after)
			}
			r.cur = l
			r.pages++
			r.rows += e.int(t, "rows")
			rows += e.int(t, "rows")
			if e["done"] == true {
				hi := r.hi
				leaves = append(leaves, span{r.lo, hi, r.open})
			}
		case "split":
			splits++
			r := get(e.id(t))
			if r.claimed != 1 || r.ends != 0 {
				t.Fatalf("%s: a split of range %d outside its claim", what, e.id(t))
			}
			r.ends++
			c, _ := e.key(t, "cursor")
			m, _ := e.key(t, "pivot")
			if c != r.cur || r.pages == 0 {
				t.Fatalf("%s: range %d split at %q after %d pages, its cursor is %q", what, e.id(t), c, r.pages, r.cur)
			}
			if m <= c || (!r.open && m >= r.hi) {
				t.Fatalf("%s: range %d (%q, %q] split at cursor %q pivot %q", what, e.id(t), r.lo, r.hi, c, m)
			}
			if !slices.Contains([]string{"reflect", "midpoint", "density", "structure"}, e["mechanism"].(string)) {
				t.Fatalf("%s: split mechanism %v", what, e["mechanism"])
			}
			if e.int(t, "rows") != r.rows {
				t.Fatalf("%s: range %d split with %d rows, its pages hold %d", what, e.id(t), e.int(t, "rows"), r.rows)
			}
			leaves = append(leaves, span{r.lo, c, false})
			for _, child := range []struct {
				f      string
				lo, hi string
				open   bool
			}{{"lower", c, m, false}, {"upper", m, r.hi, r.open}} {
				k := get(uint64(e.int(t, child.f)))
				if k.made {
					t.Fatalf("%s: split child %d made twice", what, e.int(t, child.f))
				}
				k.made, k.lo, k.hi, k.open = true, child.lo, child.hi, child.open
			}
		case "completed":
			r := get(e.id(t))
			if r.claimed != 1 || r.ends != 0 || r.pages == 0 {
				t.Fatalf("%s: range %d completed (claimed %d, ends %d, pages %d)", what, e.id(t), r.claimed, r.ends, r.pages)
			}
			r.ends++
			if e.int(t, "rows") != r.rows {
				t.Fatalf("%s: range %d completed with %d rows, its pages hold %d", what, e.id(t), e.int(t, "rows"), r.rows)
			}
		case "failed":
			t.Fatalf("%s: range %d failed in a successful run: %v", what, e.id(t), e["error"])
		case "run_end":
			if e.int(t, "dropped") != 0 || e.int(t, "events") != int64(len(evs)-1) {
				t.Fatalf("%s: run_end %v, %d events before it", what, e, len(evs)-1)
			}
		}
	}
	claimed, ended := 0, 0
	for id, r := range ranges {
		if !r.made || r.claimed != 1 || r.ends != 1 {
			t.Fatalf("%s: range %d made %v, claimed %d times, ended %d times", what, id, r.made, r.claimed, r.ends)
		}
		claimed++
		ended++
	}
	if seeded+2*splits != claimed {
		t.Fatalf("%s: %d seeded + 2×%d splits != %d claimed", what, seeded, splits, claimed)
	}
	if rows != int64(len(keys)) {
		t.Fatalf("%s: the trace's pages hold %d rows, the run listed %d", what, rows, len(keys))
	}
	checkTiling(t, what, leaves)
	return splits
}

// TestTraceInvariants runs the engine with a trace over every fixture
// keyspace, seeded and not, under the demand gate and under random
// splits, and checks each trace from itself alone (checkTrace).
func TestTraceInvariants(t *testing.T) {
	spaces := keyspaces()
	spaces["notUTF8"] = notUTF8()
	splits := 0
	for name, keys := range spaces {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for s, opt := range []Options{{}, {Seed: true, SeedFirstPage: true, Structure: StructureAuto}, {SplitCoin: coin(int64(len(keys)))}, {Seed: true, SplitCoin: coin(7)}} {
			for _, w := range []int{1, 8, 64} {
				what := fmt.Sprintf("%s opt=%d w=%d", name, s, w)
				got, _, evs := traced(t, newSorted(keys, true, int64(s)), opt, w)
				if !slices.Equal(got, want) {
					t.Fatalf("%s: %d keys, want %d", what, len(got), len(want))
				}
				splits += checkTrace(t, what, evs, got)
			}
		}
	}
	if splits < 100 {
		t.Fatalf("%d splits in all the traces: the split paths were barely traced", splits)
	}
	t.Logf("%d splits traced", splits)
}

// coin is a seeded random split gate (Options.SplitCoin).
func coin(seed int64) func() bool {
	rng := rand.New(rand.NewSource(seed))
	var mu sync.Mutex
	return func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
}

// notUTF8 are keys that are not UTF-8: the trace keeps their bytes (hex).
func notUTF8() []string {
	var out []string
	for i := range 3000 {
		out = append(out, fmt.Sprintf("b/%04d\xff", i), fmt.Sprintf("\xfe%04d", i))
	}
	return out
}

// TestTraceRowsUnchanged: a run with a trace lists the same rows as the
// run without one.
func TestTraceRowsUnchanged(t *testing.T) {
	for _, name := range []string{"badlySkewed", "dateTree", "percentURLs"} {
		keys := keyspaces()[name]
		for _, opt := range []Options{{}, {Seed: true, SeedFirstPage: true, Structure: StructureAuto}} {
			off, _ := runTraced(t, newSorted(keys, true, 1), opt, 16, nil)
			on, _, _ := traced(t, newSorted(keys, true, 1), opt, 16)
			if !slices.Equal(off, on) {
				t.Fatalf("%s: %d rows traced, %d without the trace", name, len(on), len(off))
			}
		}
	}
}

// stalledFile is a trace file that takes nothing until released.
type stalledFile struct {
	release chan struct{}
	w       io.Writer
}

func (s *stalledFile) Write(b []byte) (int, error) {
	<-s.release
	return s.w.Write(b)
}

// TestTraceDrops: a trace file that never keeps up does not slow or stop
// the listing; the events it could not take are dropped, and the engine's
// trace counters and run_end account for every one.
func TestTraceDrops(t *testing.T) {
	keys := keyspaces()["singlePrefixFlat"]
	var buf bytes.Buffer
	file := &stalledFile{release: make(chan struct{}), w: &buf}
	w := runtrace.New(file)
	// One-key pages and splits on every page: thousands of events.
	got, st := runTraced(t, newSorted(keys, true, 3), Options{SplitCoin: func() bool { return true }, PageSize: 1}, 8, w)
	if len(got) != len(keys) {
		t.Fatalf("%d keys listed, want %d", len(got), len(keys))
	}
	close(file.release)
	if err := w.Close(time.Second, "ok", ""); err != nil {
		t.Fatal(err)
	}
	dropped, events := st.Get(cTraceDropped), st.Get(cTraceEvents)
	if dropped == 0 || dropped != w.Dropped() || events != w.Sent()+w.Dropped() {
		t.Fatalf("trace.events %d trace.dropped %d; the writer sent %d and dropped %d", events, dropped, w.Sent(), w.Dropped())
	}
	evs := decode(t, buf.Bytes())
	end := evs[len(evs)-1]
	if end.name() != "run_end" || end.int(t, "dropped") != dropped || end.int(t, "events") != int64(len(evs)-1) {
		t.Fatalf("run_end %v with %d events written, %d dropped", end, len(evs)-1, dropped)
	}
	if !st.Counters.On("trace") {
		t.Fatal("the trace feature is off in a traced run's counters")
	}
	_, plain := runTraced(t, newSorted(keys, true, 3), Options{}, 8, nil)
	if plain.Counters.On("trace") {
		t.Fatal("the trace feature is on in a run without a trace")
	}
}
