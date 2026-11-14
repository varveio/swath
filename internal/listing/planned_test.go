// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/filter"
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
// predecessor's dataset, read as it goes, or a counted cut file whose cuts
// are each several tiles' worth, so the engine splits inside them) writes
// the unseeded run's parts byte for byte at 1, 4 and 64 workers with the
// part interval off, with a filter as well; under a spill cap at a fifth of
// the unseeded run's spill peak the frontier window keeps every frame
// admitted (none refused), and the parts are the same. A dry run seeded
// from the same priors counts every row (order.Count takes no plan).
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
	for _, prior := range []string{ref, cuts} {
		for _, w := range []int{1, 4, 64} {
			name := fmt.Sprintf("%s/w%d", filepath.Base(prior), w)
			out := filepath.Join(dir, strings.ReplaceAll(name, "/", "-"))
			sum, err := Run(context.Background(), plannedConfig(url, out, prior, w))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			sameDataset(t, name, ref, out)
			if sum.Resources.Window == nil || sum.Resources.Window.Tiles < 4 {
				t.Fatalf("%s: window %+v", name, sum.Resources.Window)
			}
		}
	}
	peak := refSum.Resources.Spill.PeakBytes
	if peak == 0 {
		t.Fatal("the unseeded run did not spill: the cap arm proves nothing")
	}
	for _, prior := range []string{ref, cuts} {
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
		if s.Refused != 0 || w.BoundBy != "spill_cap" || w.Waits["rows"]+w.Waits["spill"] == 0 {
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
	for _, prior := range []string{ref, cuts} {
		cfg := plannedConfig(url, "", prior, 8)
		sum, err := Run(context.Background(), cfg)
		if err != nil || sum.Output.RowsEmitted != int64(len(keys)) || sum.Resources.Window != nil {
			t.Fatalf("dry run from %s: %v, %d rows (want %d), window %+v", prior, err, sum.Output.RowsEmitted, len(keys), sum.Resources.Window)
		}
	}
}
