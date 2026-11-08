// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
)

// TestResumeChangesPriorWorkersEncodersAndSpillFormat: a run seeded from a
// cut file, spilling delta-coded zstd frames with 8 workers and 2
// encoders, is killed after its third record (and, in the calibration
// arm, with its first size-cut part claimed but unrecorded); the resume
// runs with the prior removed or replaced by a stale one, 3 workers, 1
// encoder and plain uncompressed spill frames. The dataset is the
// unseeded clean run's, byte for byte: a prior, the workers, the encoders
// and the spill format change how the work is done, never the parts.
func TestResumeChangesPriorWorkersEncodersAndSpillFormat(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	keys := checkContract(t, ref)
	for _, tc := range []struct {
		name  string
		point string
		nth   int64
		prior string // the prior's fate before the resume: "removed" or "stale"
	}{
		{"removed/recorded#3", "recorded", 3, "removed"},
		{"stale/recorded#3", "recorded", 3, "stale"},
		{"stale/calibrating-claim", "journal-append", 2, "stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cuts := filepath.Join(dir, "prior.cuts")
			var body []byte
			for i := 999; i < len(keys); i += 1000 {
				body = append(append(body, keys[i]...), "\t1000\n"...)
			}
			if err := os.WriteFile(cuts, body, 0o644); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "ds")
			var seen int64
			restore := faults.Set(func(p string) error {
				if p == tc.point {
					if seen++; seen == tc.nth {
						return faults.ErrCrash
					}
				}
				return nil
			})
			cfg := crashConfig(url, out, false)
			cfg.Source.SeedFrom = cuts
			cfg.Limits.Spill.Off, cfg.Limits.Encoders = false, 2
			_, err := Run(context.Background(), cfg)
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("crash not injected: %v", err)
			}
			switch tc.prior {
			case "removed":
				if err := os.Remove(cuts); err != nil {
					t.Fatal(err)
				}
			case "stale": // a third of the cuts, counts ten times too high, and a cut past every key
				var stale []byte
				for i := 2999; i < len(keys); i += 3000 {
					stale = append(append(stale, keys[i]...), "\t30000\n"...)
				}
				stale = append(stale, keys[len(keys)-1]+"~\t5\n"...)
				if err := os.WriteFile(cuts, stale, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			rcfg := crashConfig(url, out, true)
			rcfg.Source.SeedFrom = cuts
			rcfg.Limits.Workers, rcfg.Limits.Encoders = 3, 1
			rcfg.Limits.Spill.Off, rcfg.Limits.Spill.Plain, rcfg.Limits.Spill.Uncompressed = false, true, true
			if _, err := Run(context.Background(), rcfg); err != nil {
				t.Fatalf("resume: %v", err)
			}
			sameDataset(t, tc.name, ref, out)
		})
	}
}
