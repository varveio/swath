// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// rollHolds hold a seeded run so that it rolls while listing goes on: the
// first fresh request (a range's first page) starting in [a, a2) waits
// until a request starting at or after ahead has been answered, so the
// drainer, held there, falls behind the tiles listed past it; the first
// fresh request starting at or after b waits until a stretch is kept
// (order's "stretch-kept" event), so listing is still going on when the
// drainer writes the listed tiles as segments. A hold not released within
// a minute is a failure of the fixture (late), not a release by time.
type rollHolds struct {
	a, a2, ahead, b  string
	seenA, seenB     atomic.Int64
	answered, kept   chan struct{}
	answeredO, keptO sync.Once
	late             atomic.Bool
	restoreEvents    func()
}

func newRollHolds(t *testing.T, a, a2, ahead, b string) *rollHolds {
	h := &rollHolds{a: a, a2: a2, ahead: ahead, b: b, answered: make(chan struct{}), kept: make(chan struct{})}
	h.restoreEvents = order.SetDurableEvents(func(e string) {
		if e == "stretch-kept" {
			h.keptO.Do(func() { close(h.kept) })
		}
	})
	t.Cleanup(func() {
		h.restoreEvents()
		h.keptO.Do(func() { close(h.kept) })
		h.answeredO.Do(func() { close(h.answered) })
		if h.late.Load() {
			t.Error("a rolling hold was not released within a minute: the run never rolled")
		}
	})
	return h
}

// before is called with a fresh request's start key before it is answered.
func (h *rollHolds) before(ctx context.Context, start string) error {
	var wait chan struct{}
	switch {
	case start >= h.b && h.seenB.Add(1) == 1:
		wait = h.kept
	case start >= h.a && start < h.a2 && h.seenA.Add(1) == 1:
		wait = h.answered
	default:
		return nil
	}
	select {
	case <-wait:
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-time.After(time.Minute):
		h.late.Store(true)
	}
	return nil
}

// release lets the last range go without a stretch kept.
func (h *rollHolds) release() { h.keptO.Do(func() { close(h.kept) }) }

// after is called with a request's start key once it is answered.
func (h *rollHolds) after(start string) {
	if start >= h.ahead {
		h.answeredO.Do(func() { close(h.answered) })
	}
}

// heldHandler is h's holds over an S3 handler, by a list request's
// start-after (a continuation is never held).
func (h *rollHolds) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start, fresh := q.Get("start-after"), q.Get("continuation-token") == "" && q.Get("delimiter") == ""
		if fresh && q.Has("list-type") {
			if err := h.before(r.Context(), start); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		next.ServeHTTP(w, r)
		h.after(start)
	})
}

// heldVersions is hotVersions with h's holds, by a request's key marker
// (a page continuation is never held).
type heldVersions struct {
	*hotVersions
	h *rollHolds
}

func (v heldVersions) ListObjectVersions(ctx context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
	if in.Delimiter == "" && in.VersionIDMarker == "" {
		if err := v.h.before(ctx, in.KeyMarker); err != nil {
			return nil, err
		}
	}
	p, err := v.hotVersions.ListObjectVersions(ctx, in)
	v.h.after(in.KeyMarker)
	return p, err
}

// rollBudget is the row budget of a rolling identity run: with spill off,
// every tile ahead held in memory (the window admits them all); with spill
// on, room for several segment workers, the rest of the rows spilled.
func rollBudget(spill bool) int64 {
	if spill {
		return 10 << 20
	}
	return 64 << 20
}

// rollBackend is 100,000 keys in d00/ to d08/: enough rows that a row
// budget leaving room for several segment workers (order.budgetWorkers)
// still spills.
func rollBackend(t *testing.T) *s3mem.Backend {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	return s3test.Backend(t, gcstest.Keys(100000), s3mem.WithTimeSource(&step{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
}

// newPlainHolds holds a run over rollBackend: the frontier at d02/ until a
// range at d06/ has been answered, and d08/ until a stretch is kept.
func newPlainHolds(t *testing.T) *rollHolds { return newRollHolds(t, "d02/", "d03/", "d06/", "d08/") }

// rollServer serves backend, held by its holds when set (nil: not held):
// one endpoint for a run and its resumes.
type rollServer struct {
	url   string
	holds atomic.Pointer[rollHolds]
}

func newRollServer(t *testing.T, backend *s3mem.Backend) *rollServer {
	s := &rollServer{}
	next := s3test.Delayed(s3test.Handler(backend), 200*time.Microsecond)
	s.url = s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h := s.holds.Load(); h != nil {
			h.handler(next).ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	}))
	return s
}

// rollConfig is a run over url with four encoders, seeded from prior (""
// none), at w workers.
func rollConfig(url, out, prior string, w int, spill bool) Config {
	cfg := plannedConfig(url, out, prior, w)
	cfg.Limits.Encoders = 4
	cfg.Limits.Spill.Off = !spill
	cfg.Limits.RowBudget = rollBudget(spill)
	return cfg
}

// TestRollingIsTheSerialRun: a seeded run (its predecessor's dataset, or a
// coarse counted cut file) with four encoders writes the listed tiles ahead
// of its drainer as segments while listing goes on, and its parts are the
// one-encoder unseeded run's byte for byte at 1, 4 and 64 workers, with
// spill on and off, for a plain listing and, at 4 and 64, for an
// --all-versions one (groups of up to 40 versions, cut only at key changes,
// across stretch and part boundaries). At 4 and 64 workers the run is held so that it must
// roll (rollHolds): the report counts a stretch, rows written by it and
// parts published before listing ended; with spill on, it spills, and its
// segments read durable spill files (journaled).
func TestRollingIsTheSerialRun(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4)) // spare processors for the segment workers
	backend := rollBackend(t)
	type arm struct {
		name   string
		config func(t *testing.T, out, prior string, w int, spill, hold bool) Config
	}
	plain := func(t *testing.T, out, prior string, w int, spill, hold bool) Config {
		srv := newRollServer(t, backend)
		if hold {
			srv.holds.Store(newPlainHolds(t))
		}
		return rollConfig(srv.url, out, prior, w, spill)
	}
	api := newHotVersionsN(t, 60000)
	versions := func(t *testing.T, out, prior string, w int, spill, hold bool) Config {
		cfg := hotVersionsConfig(api, out, false)
		api.gate.Store(nil)
		if hold {
			cfg.Source.Options.(*S3Config).Client = heldVersions{api, newRollHolds(t, "k0500", "k0600", "k3", "k9")}
		}
		cfg.Source.SeedFrom = prior
		cfg.Source.Options.(*S3Config).Policy.TilePages = 4
		cfg.Limits.Workers, cfg.Limits.Encoders = w, 4
		cfg.Limits.Spill = SpillLimits{Off: !spill}
		cfg.Limits.RowBudget = rollBudget(spill)
		return cfg
	}
	for _, a := range []arm{{"plain", plain}, {"versions", versions}} {
		dir := t.TempDir()
		ref := filepath.Join(dir, "ref")
		cfg := a.config(t, ref, "", 8, false, false)
		cfg.Limits.Encoders = 1 // the serial drain alone
		if _, err := Run(context.Background(), cfg); err != nil {
			t.Fatalf("%s reference: %v", a.name, err)
		}
		priors := []string{ref}
		if a.name == "plain" {
			keys := checkContract(t, ref)
			cuts := filepath.Join(dir, "coarse.cuts")
			var body []byte
			for i := 1999; i < len(keys); i += 2000 { // several tiles' worth a cut: split inside
				body = append(body, fmt.Sprintf("%s\t2000\n", keys[i])...)
			}
			if err := os.WriteFile(cuts, body, 0o644); err != nil {
				t.Fatal(err)
			}
			priors = append(priors, cuts)
		}
		for _, prior := range priors {
			workers := []int{1, 4, 64}
			if a.name == "versions" {
				workers = workers[1:] // seven-row pages: one worker is slow, and the plain arm covers it
			}
			for _, w := range workers {
				for _, spill := range []bool{false, true} {
					name := fmt.Sprintf("%s/%s/w%d/spill=%v", a.name, filepath.Base(prior), w, spill)
					out := filepath.Join(dir, strings.ReplaceAll(name, "/", "-"))
					hold := w > 1
					sum, err := Run(context.Background(), a.config(t, out, prior, w, spill, hold))
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
					sameDataset(t, name, ref, out)
					if spill && hold && (sum.Resources.Spill == nil || sum.Resources.Spill.Rows == 0) {
						t.Fatalf("%s: nothing spilled", name)
					}
					if d := sum.Resources.Drain; spill && hold && !datasettest.NoJournal() && (d == nil || d.Segments == nil || d.Segments.DurableFilesRead == 0) {
						t.Fatalf("%s: the segments read no durable spill file", name)
					}
					d := sum.Resources.Drain
					if d == nil || d.Segments == nil || d.Segments.Rolling == nil || sum.Resources.Window == nil {
						t.Fatalf("%s: no rolling account in the report", name)
					}
					rl := d.Segments.Rolling
					if hold && (rl.Stretches == 0 || rl.Rows == 0 || d.WriterBeforeListingDone == nil || d.WriterBeforeListingDone.Parts == 0) {
						t.Fatalf("%s: held to roll, rolling %+v, writer before listing done %+v", name, *rl, d.WriterBeforeListingDone)
					}
					t.Logf("%s: rolling %d stretches, %d segments, %d rows; not engaged %v; %d durable files read by segments", name, rl.Stretches, rl.Segments, rl.Rows, rl.NotEngaged, d.Segments.DurableFilesRead)
				}
			}
		}
	}
}
