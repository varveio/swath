// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
)

// rollingCrashMatrix is TestCrashMatrix's rolling arm: a seeded run held so
// that it writes the listed tiles ahead of its drainer as segments while
// listing goes on (rollHolds) is stopped inside such a stretch, then
// resumed (unheld) to parts byte-identical to the one-encoder unseeded
// run's. The stops: a crash at each of faults.RollingPoints (the stretch
// planned with none of its parts opened; a part finished before the part
// it follows is published (when one does: a run whose parts all finish in
// order skips that stop); a part published with the stretch's
// spill not yet retired; the stretch written, its last part kept open);
// a crash at the stretch's end after listing ended in its middle (listing
// released at segments-begun and awaited); the run cancelled, and an
// encoder failing, in the middle of a stretch. Each first occurrence is
// during listing: the last range is held until a stretch begins. Spill
// off and on (the stretch's frames read from spill files). No part is left
// unpublished after the resume.
func rollingCrashMatrix(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4)) // spare processors for the segment workers
	backend := rollBackend(t)
	dir := t.TempDir()
	ref, prior := filepath.Join(dir, "ref"), filepath.Join(dir, "prior")
	for _, out := range []string{ref, prior} {
		cfg := rollConfig(newRollServer(t, backend).url, out, "", 8, false)
		cfg.Limits.Encoders = 1
		if _, err := Run(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	errEncoder := errors.New("injected encoder failure")
	type stop struct {
		name string
		// at is the faults hook: the stop's error, nil to go on. cancel
		// cancels the run; listed waits for the listing to end.
		at   func(p string, cancel func(), listed <-chan struct{}) error
		want error
	}
	crashAt := func(point string, nth int64) stop {
		var seen atomic.Int64
		return stop{fmt.Sprintf("%s#%d", point, nth), func(p string, _ func(), _ <-chan struct{}) error {
			if p == point && seen.Add(1) == nth {
				return faults.ErrCrash
			}
			return nil
		}, faults.ErrCrash}
	}
	midListing := func() stop {
		var once sync.Once
		var kept atomic.Int64
		return stop{"listing ends mid-stretch, stretch-kept#1", func(p string, _ func(), listed <-chan struct{}) error {
			switch p {
			case "segments-begun":
				once.Do(func() { <-listed })
			case "stretch-kept":
				if kept.Add(1) == 1 {
					return faults.ErrCrash
				}
			}
			return nil
		}, faults.ErrCrash}
	}
	midStretch := func(name string, f func(cancel func()) error, want error) stop {
		var began, done atomic.Bool
		return stop{name, func(p string, cancel func(), _ <-chan struct{}) error {
			switch {
			case p == "segments-begun":
				began.Store(true)
			case p == "encoding" && began.Load() && done.CompareAndSwap(false, true):
				return f(cancel)
			}
			return nil
		}, want}
	}
	for _, spill := range []bool{false, true} {
		stops := []func() stop{
			func() stop { return crashAt("segments-begun", 1) },
			func() stop { return crashAt("segment-ahead", 1) },
			func() stop { return crashAt("segment-published", 1) },
			func() stop { return crashAt("segment-published", 2) },
			func() stop { return crashAt("stretch-kept", 1) },
			midListing,
			func() stop {
				return midStretch("cancelled mid-stretch", func(cancel func()) error { cancel(); return nil }, context.Canceled)
			},
			func() stop {
				return midStretch("encoder failure mid-stretch", func(func()) error { return errEncoder }, errEncoder)
			},
		}
		for _, mk := range stops {
			st := mk()
			t.Run(fmt.Sprintf("spill=%v/%s", spill, st.name), func(t *testing.T) {
				out := filepath.Join(t.TempDir(), "ds")
				srv := newRollServer(t, backend)
				h := newPlainHolds(t)
				srv.holds.Store(h)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				listed := make(chan struct{})
				var listedOnce sync.Once
				restore := faults.Set(func(p string) error {
					if p == "segments-begun" {
						h.release() // the stretch's first segment opens with listing free to end
					}
					return st.at(p, cancel, listed)
				})
				cfg := rollConfig(srv.url, out, prior, 8, spill)
				cfg.ListingDone = func() { listedOnce.Do(func() { close(listed) }) }
				_, err := Run(ctx, cfg)
				restore()
				if err == nil && st.name == "segment-ahead#1" {
					t.Skip("every part finished after the part it follows was published")
				}
				if !errors.Is(err, st.want) {
					t.Fatalf("stopped with %v, want %v", err, st.want)
				}
				srv.holds.Store(nil)
				rcfg := rollConfig(srv.url, out, prior, 8, spill)
				rcfg.Output.Resume = true
				if _, err := Run(context.Background(), rcfg); err != nil {
					t.Fatalf("resume: %v", err)
				}
				sameDataset(t, st.name, ref, out)
				if orphans, _ := filepath.Glob(filepath.Join(out, dataset.Workspace, dataset.PartsDir, "*")); len(orphans) > 0 {
					t.Fatalf("parts left unpublished after the resume: %v", orphans)
				}
			})
		}
	}
}
