// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
)

// serialSeed runs the descent one probe at a time against api: the
// reference for the provider's concurrent walk.
func serialSeed(t *testing.T, api lister, prefix string, workers int) *seedResult {
	t.Helper()
	d := newDescent(prefix, workers, PageKeys, Safe)
	q, res := d.start()
	for res == nil {
		out, err := api.List(context.Background(), &keyrangetest.ListInput{Prefix: q.prefix, StartAfter: q.after, Delimiter: seedDelimiter, MaxKeys: PageKeys})
		if err != nil {
			t.Fatal(err)
		}
		o := seedOutcome{prefixes: out.CommonPrefixes, more: out.IsTruncated, objects: len(out.Contents)}
		for _, k := range append(keysOfPage(out), out.CommonPrefixes...) {
			if !o.hasLast || k > o.last {
				o.last, o.hasLast = k, true
			}
		}
		q, res = d.next(o)
	}
	return res
}

func keysOfPage(p *keyrangetest.Page) []string {
	var k []string
	for _, o := range p.Contents {
		k = append(k, o.Key)
	}
	return k
}

// seedShapes are keyspaces for the descent's branches.
func seedShapes() map[string][]string {
	gen := func(n int, f func(i int) string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	cat := func(parts ...[]string) []string { return slices.Concat(parts...) }
	return map[string][]string{
		"flat small":      gen(300, func(i int) string { return fmt.Sprintf("k%04d", i) }),
		"dense flat root": gen(2500, func(i int) string { return fmt.Sprintf("%08x", i*2654435761%4294967291) }),
		"narrow tree": gen(6000, func(i int) string {
			return fmt.Sprintf("r%d/s%02d/t%d/obj%04d", i%5, i/5%20, i/100%3, i)
		}),
		// A truncated folder of key=value partitions.
		"partitions": cat(gen(3000, func(i int) string { return fmt.Sprintf("tbl/dt=2024-%04d/part-%d", i/2, i%2) }),
			[]string{"readme"}),
		// A truncated folder of 1:1 subfolders (one object each) next to a
		// heavy one (subfolders with many objects).
		"explosion and heavy": cat(gen(1500, func(i int) string { return fmt.Sprintf("leaf/%05d/x", i) }),
			gen(12000, func(i int) string { return fmt.Sprintf("heavy/h%04d/f%02d", i/10, i%10) })),
		// Deep shared prefixes, as in the census recording.
		"deep census": gen(9000, func(i int) string {
			return fmt.Sprintf("tiff/1950census/43290879-State%02d/43290879-State%02d-%03d/%04d.tif", i%7, i%7, i/7%40, i)
		}),
		// More top-level folders than one page.
		"wide top": gen(2600, func(i int) string { return fmt.Sprintf("d%05d/x", i) }),
	}
}

// TestSeedDescentBranches: each shape takes its branch, and the tiles the
// plan gives are strictly increasing safe keys.
func TestSeedDescentBranches(t *testing.T) {
	want := map[string]func(r *seedResult) bool{
		"flat small": func(r *seedResult) bool { return len(r.cuts) == 0 && r.marks["flat_trivial"] == 1 },
		"dense flat root": func(r *seedResult) bool {
			return r.synthesized == min(bandSpan, 4*16) && r.levels["dense_root_radix_banded"] == 1
		},
		"narrow tree": func(r *seedResult) bool {
			return r.levels["delimiter_seeded"] == 1 && r.levels["narrow"] > 20 && len(r.cuts) > 20
		},
		"partitions": func(r *seedResult) bool { return r.levels["fanout_tiled"] == 1 },
		"explosion and heavy": func(r *seedResult) bool {
			return r.levels["tiny_leaf_explosion"] == 1 && r.levels["heavy_cut_banded"] == 1
		},
		"deep census": func(r *seedResult) bool { return r.levels["narrow"] >= 4 && len(r.cuts) >= 16 },
		"wide top":    func(r *seedResult) bool { return r.marks["top_paginated"] == 1 && r.levels["tiny_leaf_explosion"] == 1 },
	}
	for name, keys := range seedShapes() {
		r := serialSeed(t, newSorted(keys, false, 1), "", 16)
		if !want[name](r) {
			t.Errorf("%s: plan %d cuts, %d bands, %d probes, marks %v, levels %v", name, len(r.cuts), r.synthesized, r.probes, r.marks, r.levels)
		}
		if r.probes > 64 {
			t.Errorf("%s: %d probes past the budget of 4×16", name, r.probes)
		}
		for i := 1; i < len(r.cuts); i++ {
			if r.cuts[i] <= r.cuts[i-1] {
				t.Fatalf("%s: cuts not increasing at %d: %q, %q", name, i, r.cuts[i-1], r.cuts[i])
			}
		}
	}
}

// tileRecorder collects the seed's children.
type tileRecorder struct {
	mu    sync.Mutex
	tiles []Range
}

func (r *tileRecorder) Rows([]Row) error { return nil }
func (r *tileRecorder) Child(c Range) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tiles = append(r.tiles, c)
	return nil
}

// inflight is a fake that records its peak concurrency.
type inflight struct {
	*sorted
	now, peak atomic.Int64
}

func (f *inflight) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	n := f.now.Add(1)
	for {
		p := f.peak.Load()
		if n <= p || f.peak.CompareAndSwap(p, n) {
			break
		}
	}
	defer f.now.Add(-1)
	return f.sorted.List(ctx, in)
}

// TestSeedDescentConcurrentEqualsSerial: the provider's walk, with probes
// fetched ahead, tiles the scope at exactly the serial walk's cuts, and
// fetches several probes at once.
func TestSeedDescentConcurrentEqualsSerial(t *testing.T) {
	// The peak is a property of the run, not of every shape: a shape whose
	// levels hold three folders peaks at three however the probes are
	// scheduled, so the fetch-ahead is asserted over all shapes.
	peak, peakShape := int64(0), ""
	for name, keys := range seedShapes() {
		for _, w := range []int{1, 4, 64} {
			ref := serialSeed(t, newSorted(keys, false, 1), "", w)
			f := &inflight{sorted: newSorted(keys, false, 1)}
			f.Latency = time.Millisecond
			p, err := newTest(f, Options{Workers: w, Seed: true, SeedDescent: true})
			if err != nil {
				t.Fatal(err)
			}
			rec := &tileRecorder{}
			if err := p.expandSeed(context.Background(), rec); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, r := range rec.tiles[:len(rec.tiles)-1] {
				got = append(got, r.hi)
			}
			var want []string
			for _, c := range ref.cuts {
				if Safe(c) {
					want = append(want, c)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s w=%d: concurrent walk tiles at %d cuts, serial at %d", name, w, len(got), len(want))
			}
			if st := p.Stats(); st.Get(cSeedProbes) != int64(ref.probes) {
				t.Fatalf("%s w=%d: %d probes used, serial %d", name, w, st.Get(cSeedProbes), ref.probes)
			}
			if w == 64 && f.peak.Load() > peak {
				peak, peakShape = f.peak.Load(), name
			}
		}
	}
	if peak < 4 {
		t.Errorf("peak %d probes in flight (%s): the walk did not fetch ahead", peak, peakShape)
	}
	t.Logf("peak %d probes in flight (%s)", peak, peakShape)
}

// TestSeedDescentListsEveryKey: with the descent seed, every shape and the
// fixture keyspaces list row-identical to the keyspace, and the covered
// intervals tile (⊥, ⊤], including under a prefix.
func TestSeedDescentListsEveryKey(t *testing.T) {
	spaces := seedShapes()
	for k, v := range keyspaces() {
		spaces["fixture "+k] = v
	}
	for name, keys := range spaces {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		got, _, leaves := run(t, newSorted(keys, true, 3), Options{Seed: true, SeedDescent: true, Floors: true}, 16)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(want))
		}
		checkTiling(t, name, leaves)
	}
	keys := seedShapes()["deep census"]
	var want []string
	for _, k := range keys {
		if strings.HasPrefix(k, "tiff/1950census/43290879-State03") {
			want = append(want, k)
		}
	}
	slices.Sort(want)
	got, _, _ := run(t, newSorted(keys, false, 1), Options{Seed: true, SeedDescent: true, Prefix: "tiff/1950census/43290879-State03"}, 8)
	if !slices.Equal(got, want) {
		t.Fatalf("prefix: %d keys, want %d", len(got), len(want))
	}
}
