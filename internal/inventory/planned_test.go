// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/runtrace"
	"github.com/varveio/swath/internal/source/keyrange"
)

// plannedConfig is crashConfig with spill on, pages of 100 keys and tiles
// of two pages, seeded from prior ("" none) at w workers.
func plannedConfig(url, out, prior string, w int) Config {
	cfg := crashConfig(url, out, false)
	cfg.Source.SeedFrom = prior
	cfg.Source.Options.(*S3Config).Policy = keyrange.Policy{PageSize: 100, TilePages: 2}
	cfg.Limits.Workers, cfg.Limits.Spill.Off = w, false
	return cfg
}

// TestPlannedRunIsTheUnseededRun: a run planned from a counted prior (its
// predecessor's dataset, from its cut file or, without it, its page index
// read as it goes; the dataset with a coarsened cut file of two cuts, each
// more keys than a tile of keyrange.TilePages pages; or a counted cut file
// whose cuts are each several tiles' worth, so the engine splits inside
// them) writes the unseeded run's parts byte for byte at 1, 4, 64 and 256
// workers with the part interval off, with a filter as well; under a spill
// cap at a fifth of the unseeded run's spill peak the frontier window binds
// and the parts are the same, and with the dataset's tiles (a key page
// each, far under half the cap) no frame is refused. A coarse cut's tile
// can spill past the cap by itself: the window admits tiles, it does not
// bound one. A dry run seeded from the same priors counts every row
// (order.Count takes no plan).
func TestPlannedRunIsTheUnseededRun(t *testing.T) {
	url := crashBucket(t)
	dir := t.TempDir()
	ref := filepath.Join(dir, "ref")
	refSum, err := Run(context.Background(), plannedConfig(url, ref, "", 8))
	if err != nil {
		t.Fatal(err)
	}
	keys := checkContract(t, ref)
	cuts := filepath.Join(dir, "coarse.cuts")
	var body []byte
	for i := 1999; i < len(keys); i += 2000 { // 20 pages of 100 a cut, ten tiles' worth
		body = append(body, fmt.Sprintf("%s\t2000\n", keys[i])...)
	}
	if err := os.WriteFile(cuts, body, 0o644); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(dir, "index")
	if err := os.CopyFS(index, os.DirFS(ref)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(index, dataset.CutsName)); err != nil {
		t.Fatal(err)
	}
	// The dataset with the cut file its writer makes under a bound of two
	// lines: each cut more keys than a tile of TilePages pages of 100.
	sparse := filepath.Join(dir, "sparse")
	if err := os.CopyFS(sparse, os.DirFS(index)); err != nil {
		t.Fatal(err)
	}
	full, err := os.ReadFile(filepath.Join(ref, dataset.CutsName))
	if err != nil {
		t.Fatal(err)
	}
	two := coarsened(t, full, 2)
	for l := range strings.SplitSeq(strings.TrimSuffix(string(two), "\n"), "\n") {
		if n, _ := strconv.Atoi(l[strings.LastIndexByte(l, '\t')+1:]); n <= keyrange.TilePages*100 {
			t.Fatalf("a coarsened cut of %d keys: not coarser than a tile of %d pages", n, keyrange.TilePages)
		}
	}
	if err := os.WriteFile(filepath.Join(sparse, dataset.CutsName), two, 0o644); err != nil {
		t.Fatal(err)
	}
	took := map[string]string{ref: "read", index: "absent", sparse: "read", cuts: ""}
	for _, prior := range []string{ref, index, sparse, cuts} {
		for _, w := range []int{1, 4, 64, 256} {
			name := fmt.Sprintf("%s/w%d", filepath.Base(prior), w)
			out := filepath.Join(dir, strings.ReplaceAll(name, "/", "-"))
			sum, err := Run(context.Background(), plannedConfig(url, out, prior, w))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			sameDataset(t, name, ref, out)
			tiles := int64(4)
			if prior == sparse {
				tiles = 2
			}
			if sum.Resources.Window == nil || sum.Resources.Window.Tiles < tiles {
				t.Fatalf("%s: window %+v", name, sum.Resources.Window)
			}
			if p := sum.Invocation.Prior; p == nil || p.CutsFile != took[prior] {
				t.Fatalf("%s: prior %+v, want its cut file %q", name, p, took[prior])
			}
		}
	}
	peak := refSum.Resources.Spill.PeakBytes
	if peak == 0 {
		t.Fatal("the unseeded run did not spill: the cap arm proves nothing")
	}
	for _, prior := range []string{ref, index, sparse, cuts} {
		out := filepath.Join(dir, "capped-"+filepath.Base(prior))
		cfg := plannedConfig(url, out, prior, 8)
		cfg.Limits.Spill.Max = peak / 5
		sum, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		sameDataset(t, out, ref, out)
		s, w := sum.Resources.Spill, sum.Resources.Window
		t.Logf("%s: unseeded peak %d, cap %d: peak %d, refused %d, window %+v", filepath.Base(prior), peak, cfg.Limits.Spill.Max, s.PeakBytes, s.Refused, w)
		if ((prior == ref || prior == index) && s.Refused != 0) || w.BoundBy != "spill_cap" || w.Waits["rows"]+w.Waits["spill"] == 0 {
			t.Fatalf("capped: refused %d frames, window %+v", s.Refused, w)
		}
	}
	// A filter: the planned run's parts are the unseeded filtered run's.
	only := &filter.Spec{IncludeRegex: []string{"^d0[1-4]/"}}
	fref := filepath.Join(dir, "filtered-ref")
	cfg := plannedConfig(url, fref, "", 8)
	cfg.Filter = only
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	fout := filepath.Join(dir, "filtered")
	cfg = plannedConfig(url, fout, ref, 8)
	cfg.Filter = only
	if sum, err := Run(context.Background(), cfg); err != nil || sum.Resources.Window == nil {
		t.Fatalf("filtered: %v", err)
	}
	sameDataset(t, "filtered", fref, fout)
	// A dry run counts with the prior tiled at once.
	for _, prior := range []string{ref, index, sparse, cuts} {
		cfg := plannedConfig(url, "", prior, 8)
		sum, err := Run(context.Background(), cfg)
		if err != nil || sum.Output.RowsEmitted != int64(len(keys)) || sum.Resources.Window != nil {
			t.Fatalf("dry run from %s: %v, %d rows (want %d), window %+v", prior, err, sum.Output.RowsEmitted, len(keys), sum.Resources.Window)
		}
	}
}

// span is an interval (lo, hi], or (lo, ⊤] when open.
type span struct {
	lo, hi string
	open   bool
}

// outside is the parts of t outside certs (ascending, disjoint), in order:
// plain interval arithmetic.
func outside(t span, certs []span) []span {
	var out []span
	cur := t.lo
	for _, c := range certs {
		if !t.open && c.lo >= t.hi {
			break
		}
		if !c.open && c.hi <= cur {
			continue
		}
		if c.lo > cur {
			out = append(out, span{cur, c.lo, false})
		}
		if c.open || (!t.open && c.hi >= t.hi) {
			return out
		}
		cur = c.hi
	}
	return append(out, span{cur, t.hi, t.open})
}

// TestDurableResumePlannedAcrossTiles: a crashed run's certificates are
// reused by a resume planned from a fine counted prior (a cut every 40
// keys, each a tile of its own). From the resume's run trace: the
// certificates replayed cross several tiles' bounds, each is replayed
// once, and the ranges listed are exactly what an interval oracle leaves
// of each tile (K, c1], (c1, c2], ..., (cn, ⊤] after the certificates; the
// dataset is the uninterrupted run's byte for byte.
func TestDurableResumePlannedAcrossTiles(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	keys := checkContract(t, ref)
	cuts := filepath.Join(t.TempDir(), "fine.cuts")
	var body []byte
	var bounds []string
	for i := 39; i < len(keys); i += 40 {
		body = append(append(body, keys[i]...), "\t40\n"...)
		bounds = append(bounds, keys[i])
	}
	if err := os.WriteFile(cuts, body, 0o644); err != nil {
		t.Fatal(err)
	}
	var trace bytes.Buffer
	tw := runtrace.New(&trace)
	cfg := durableConfig(url, out, true)
	cfg.Source.SeedFrom, cfg.Trace = cuts, tw
	cfg.Source.Options.(*S3Config).Policy.PageSize, cfg.Source.Options.(*S3Config).Policy.TilePages = 10, 1
	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(10*time.Second, "", ""); err != nil || tw.Dropped() != 0 {
		t.Fatalf("trace: %v, %d events dropped", err, tw.Dropped())
	}
	sameDataset(t, "planned resume", ref, out)
	if d := sum.Resources.Spill.Durable; d == nil || d.Resume == nil || d.Resume.Accepted == 0 || sum.Resources.Window == nil {
		t.Fatalf("nothing reused, or not planned: %+v, window %+v", d, sum.Resources.Window)
	}
	// The replay ranges are never listed: listing ends once every gap is,
	// so the tail can take the certificates left (#288).
	if sum.Timings.ProviderFinishedAt.IsZero() || sum.Resources.Drain.Segments == nil || sum.Resources.Drain.Segments.Reason == "listing_active" {
		t.Fatalf("listing never done: provider_finished_at %v, segments %+v", sum.Timings.ProviderFinishedAt, sum.Resources.Drain.Segments)
	}
	m := manifestOf(t, out)
	k := string(m.Parts[sum.Output.PartsRetained-1].MaxKey)
	var certs, listed []span
	sc := bufio.NewScanner(&trace)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var e struct {
			Event, Lo, Hi, After string
			Open, Reused         bool
		}
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		if e.Event != "seeded" {
			continue
		}
		if e.Reused {
			certs = append(certs, span{e.Lo, e.Hi, e.Open})
		} else {
			listed = append(listed, span{max(e.Lo, e.After), e.Hi, e.Open})
		}
	}
	// The oracle: the tiles of (K, ⊤] at the cuts above K, less the
	// certificates.
	var want []span
	lo := k
	for _, b := range bounds {
		if b > k {
			want = append(want, outside(span{lo, b, false}, certs)...)
			lo = b
		}
	}
	want = append(want, outside(span{lo, "", true}, certs)...)
	if !slices.Equal(listed, want) {
		t.Fatalf("listed %d ranges, the oracle %d:\n%v\n%v", len(listed), len(want), listed, want)
	}
	across := 0
	for i, c := range certs {
		if c.lo < k || (i > 0 && c.lo < certs[i-1].hi) {
			t.Fatalf("certificate %v below K %q or overlapping the one before", c, k)
		}
		n := 0
		for _, b := range bounds {
			if b > c.lo && (c.open || b < c.hi) {
				n++
			}
		}
		if n >= 2 {
			across++
		}
	}
	if across == 0 {
		t.Fatalf("no certificate of %d crosses two tile bounds: %v", len(certs), certs)
	}
	t.Logf("K %q, %d certificates (%d across three tiles or more), %d ranges listed", k, len(certs), across, len(listed))
}
