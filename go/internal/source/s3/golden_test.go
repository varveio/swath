// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/decisions.golden")

// recorder logs every request a fake answers.
type recorder struct {
	*sorted
	mu  sync.Mutex
	log []string
}

func (r *recorder) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	r.mu.Lock()
	r.log = append(r.log, fmt.Sprintf("p=%q a=%q d=%q m=%d t=%v", in.Prefix, in.StartAfter, in.Delimiter, in.MaxKeys, in.ContinuationToken != ""))
	r.mu.Unlock()
	return r.sorted.ListObjectsV2(ctx, in)
}

// TestDecisionsGolden pins the range engine's decisions: for every fixture
// and hostile keyspace and each deterministic configuration (one
// range expanded at a time by serialRun, a seeded coin or the demand gate,
// no asynchronous probes), the
// covered intervals, the requests sent, the output and the counters hash to
// the value in testdata/decisions.golden. Moving the engine between
// packages must leave every line unchanged.
func TestDecisionsGolden(t *testing.T) {
	type cfg struct {
		name    string
		opt     Options
		short   bool
		pageCap int
		coin    bool
		workers int // the provider's worker count: split demand and the seed descent's width
	}
	cfgs := []cfg{
		{name: "coin", coin: true, short: true, workers: 1},
		{name: "coin/seedpage/floors/probe", opt: Options{Seed: true, Floors: true, Probe: true}, coin: true, short: true, workers: 1},
		{name: "demand8/seedpage/page=7", opt: Options{Seed: true}, workers: 8, pageCap: 7},
		{name: "demand8/descent/floors/page=13", opt: Options{Seed: true, SeedDescent: true, Floors: true}, workers: 8, pageCap: 13},
		{name: "coin/descent", opt: Options{Seed: true, SeedDescent: true}, coin: true, workers: 1},
		{name: "sequential", opt: Options{Sequential: true}, short: true, workers: 1},
	}
	spaces := keyspaces()
	for k, v := range hostile() {
		spaces["hostile/"+k] = v
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for k := range spaces {
			if !yield(k) {
				return
			}
		}
	})
	var lines []string
	for _, name := range names {
		keys := spaces[name]
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		resumes := []string{""}
		if len(want) > 2 {
			resumes = append(resumes, want[len(want)/3])
		}
		for _, c := range cfgs {
			for _, after := range resumes {
				f := newSorted(keys, c.short, 7)
				f.pageCap = c.pageCap
				rec := &recorder{sorted: f}
				opt := c.opt
				opt.Bucket, opt.Client, opt.Workers = "b", rec, c.workers
				if after != "" {
					opt.Resume = &Resume{After: after}
				}
				if c.coin {
					rng := rand.New(rand.NewSource(11))
					opt.splitCoin = func() bool { return rng.Intn(3) == 0 }
				}
				var spans []string
				opt.trace = func(lo, hi string, open bool) { spans = append(spans, fmt.Sprintf("%q %q %v", lo, hi, open)) }
				p, err := New(context.Background(), opt)
				if err != nil {
					t.Fatal(err)
				}
				col := &collect{}
				if err := serialRun(p, p.Root(), col); err != nil {
					t.Fatalf("%s %s: %v", name, c.name, err)
				}
				st := p.Stats()
				raw, _ := json.Marshal(st.Counters)
				var snap struct {
					Values map[string]any `json:"values"`
				}
				if err := json.Unmarshal(raw, &snap); err != nil {
					t.Fatal(err)
				}
				var vals []string
				for k, v := range snap.Values {
					if !strings.HasSuffix(k, "_ns") && !strings.HasPrefix(k, "max_") && v != float64(0) && v != false { // timings and scheduling peaks are not decisions; zero is absence
						vals = append(vals, fmt.Sprintf("%s=%v", k, v))
					}
				}
				slices.Sort(vals)
				slices.Sort(rec.log)
				h := sha256.New()
				if d := os.Getenv("GOLDEN_DUMP"); d != "" && strings.Contains(name+" "+c.name, os.Getenv("GOLDEN_CASE")) {
					var b strings.Builder
					for _, part := range [][]string{spans, rec.log, vals} {
						b.WriteString(strings.Join(part, "\n") + "\n--\n")
					}
					os.WriteFile(fmt.Sprintf("%s-%v", d, after != ""), []byte(b.String()), 0o644)
				}
				for _, part := range [][]string{spans, rec.log, keysOf(col.rows), vals} {
					for _, s := range part {
						fmt.Fprintln(h, s)
					}
					fmt.Fprintln(h, "--")
				}
				lines = append(lines, fmt.Sprintf("%s\t%s\tresume=%v\tspans=%d\trequests=%d\trows=%d\t%x",
					name, c.name, after != "", len(spans), len(rec.log), len(col.rows), h.Sum(nil)[:12]))
			}
		}
	}
	got := strings.Join(lines, "\n") + "\n"
	const path = "testdata/decisions.golden"
	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	wantB, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(wantB) {
		gl, wl := strings.Split(got, "\n"), strings.Split(string(wantB), "\n")
		for i := range min(len(gl), len(wl)) {
			if gl[i] != wl[i] {
				t.Errorf("first difference at line %d:\n got  %s\n want %s", i+1, gl[i], wl[i])
				break
			}
		}
		t.Fatalf("decisions changed (%d lines, golden %d)", len(gl), len(wl))
	}
}

// serialEvents records one Expand's output in order.
type serialEvents struct {
	ev []any // []Row or Range
}

func (s *serialEvents) Rows(rows []schema.Row[Object]) error { s.ev = append(s.ev, rows); return nil }
func (s *serialEvents) Child(r Range) error                  { s.ev = append(s.ev, r); return nil }

// serialRun expands r, then its children depth first in key order, one
// Expand at a time: the order stage's semantics (children run after their
// parent returns) without its scheduling, so the decisions are reproducible.
func serialRun(p *Provider, r Range, col *collect) error {
	s := &serialEvents{}
	if err := p.Expand(context.Background(), r, s); err != nil {
		return err
	}
	for _, e := range s.ev {
		switch v := e.(type) {
		case []schema.Row[Object]:
			col.rows = append(col.rows, v...)
		case Range:
			if err := serialRun(p, v, col); err != nil {
				return err
			}
		}
	}
	return nil
}
