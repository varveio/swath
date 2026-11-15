// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/jsontime"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
)

// pressured is a progress snapshot with listing waiting on a full spill
// cap since since (none when zero), after ordered rows.
func pressured(since time.Time, ordered int64) listing.ProgressSnapshot {
	g := &order.GaugeSnapshot{SpillLiveBytes: 91 << 30, SpillMaxBytes: 91 << 30, SpillReserveBytes: 1 << 30}
	if !since.IsZero() {
		at := jsontime.Of(since)
		g.SpillPressureSince = &at
	}
	return listing.ProgressSnapshot{Ordered: ordered, Order: g}
}

// TestSpillWatch: listing waiting on a full spill cap is an error event
// once it has lasted spillPressureFor, not before, and again every
// spillPressureEvery while it lasts; a run without spill, or one whose wait
// ended, says nothing, and a new wait starts the count again. With
// --spill-wait-timeout it stops the run at that length, as a resumable
// disk_full (exit 74, cause spill_budget) naming the resume.
func TestSpillWatch(t *testing.T) {
	stderr := captureSyncStderr(t)
	setLog(termOptions{logFormat: logText}, "")
	t.Chdir(t.TempDir())
	j, err := journal.Create("inventory", journal.Identity{Tool: "swath"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	t0 := time.Unix(1_000_000, 0)
	w := &spillWatch{dir: "inventory"}
	check := func(s listing.ProgressSnapshot, d time.Duration) {
		t.Helper()
		if err := w.check(s, t0.Add(d)); err != nil {
			t.Fatalf("at %s: %v", d, err)
		}
	}
	events := func() int { return strings.Count(stderr.String(), "\n") }
	check(listing.ProgressSnapshot{}, 0)
	check(pressured(time.Time{}, 0), 0)
	check(pressured(t0, 1000), 0)
	check(pressured(t0, 1000), spillPressureFor-time.Second)
	if events() != 0 {
		t.Fatalf("an event before %s: %q", spillPressureFor, stderr)
	}
	check(pressured(t0, 1000+9800*120), spillPressureFor)
	want := "swath: spill cap too small: listing has waited on the output for 2m0s: the spill cap cannot hold the rows listed ahead of it " +
		"(spill 91.0 GiB, cap 91.0 GiB, free-space reserve 1.00 GiB); the output writes 9,800 rows/s; " +
		"stop the run and resume it with more disk or a larger --spill-budget (swath resume inventory --spill-budget SIZE), or let it run: it finishes, slowly\n"
	if stderr.String() != want {
		t.Fatalf("stderr\n%q, want\n%q", stderr, want)
	}
	for d := spillPressureFor; d < spillPressureFor+spillPressureEvery; d += time.Minute {
		check(pressured(t0, 0), d)
	}
	if events() != 1 {
		t.Fatalf("%d events within %s: %q", events(), spillPressureEvery, stderr)
	}
	check(pressured(t0, 0), spillPressureFor+spillPressureEvery)
	if events() != 2 {
		t.Fatalf("no repeat after %s: %q", spillPressureEvery, stderr)
	}
	// The wait ends, and a new one is counted from its start.
	check(pressured(time.Time{}, 0), time.Hour)
	t1 := t0.Add(time.Hour)
	check(pressured(t1, 0), time.Hour)
	check(pressured(t1, 0), time.Hour+spillPressureFor-time.Second)
	if events() != 2 {
		t.Fatalf("a new wait told before %s: %q", spillPressureFor, stderr)
	}
	check(pressured(t1, 0), time.Hour+spillPressureFor)
	if events() != 3 {
		t.Fatalf("a new wait not told: %q", stderr)
	}

	w = &spillWatch{dir: "inventory", wait: 5 * time.Minute}
	if err := w.check(pressured(t0, 0), t0.Add(5*time.Minute-time.Second)); err != nil {
		t.Fatalf("stopped before --spill-wait-timeout: %v", err)
	}
	stop := w.check(pressured(t0, 0), t0.Add(5*time.Minute))
	if !errors.Is(stop, errSpillWait) || !strings.HasSuffix(stop.Error(), "; stopped by --spill-wait-timeout 5m0s: resume it with more disk or a larger --spill-budget (swath resume inventory --spill-budget SIZE)") {
		t.Fatalf("stop %v", stop)
	}
	o := classify(stop, stop, "inventory")
	if o.Class != listing.ClassDiskFull || o.ExitCode != exitDiskFull || o.Cause != causeSpillBudget || !o.Resumable || o.StopSource != "spill_wait" {
		t.Fatalf("outcome %+v", o)
	}
	w = &spillWatch{wait: time.Minute} // text output: no journal
	if stop := w.check(pressured(t0, 0), t0.Add(time.Minute)); stop == nil || !strings.HasSuffix(stop.Error(), ": run it again with more disk or a larger --spill-budget") {
		t.Fatalf("stop without a journal: %v", stop)
	}
}

// TestSpillWatchRecovered: the event and the stop come only while listing
// waits at that moment (the progress snapshot's spill_pressure_since,
// absent once the wait ends). A second of refusal and then a long healthy
// tail neither stops a run with --spill-wait-timeout 20s nor tells; nor
// does a wait that ends just short of spillPressureFor. A wait that comes
// back with its start kept (within the order stage's gap) is told, once,
// and stopped, once it holds again past the length.
func TestSpillWatchRecovered(t *testing.T) {
	stderr := captureSyncStderr(t)
	setLog(termOptions{logFormat: logText}, "")
	t0 := time.Unix(1_000_000, 0)
	feed := func(w *spillWatch, from, to time.Duration, since time.Time) error {
		t.Helper()
		for d := from; d <= to; d += 2 * time.Second {
			if err := w.check(pressured(since, 0), t0.Add(d)); err != nil {
				return err
			}
		}
		return nil
	}
	w := &spillWatch{wait: 20 * time.Second}
	if err := feed(w, 0, time.Second, t0); err != nil {
		t.Fatal(err)
	}
	if err := feed(w, 2*time.Second, time.Hour, time.Time{}); err != nil || stderr.String() != "" {
		t.Fatalf("a recovered run: %v, stderr %q", err, stderr)
	}
	w = &spillWatch{}
	if err := feed(w, 0, spillPressureFor-10*time.Second, t0); err != nil {
		t.Fatal(err)
	}
	if err := feed(w, spillPressureFor-8*time.Second, time.Hour, time.Time{}); err != nil || stderr.String() != "" {
		t.Fatalf("a wait that ended before %s: %v, stderr %q", spillPressureFor, err, stderr)
	}
	// The spell comes back with its start kept.
	w = &spillWatch{wait: spillPressureFor + time.Minute}
	if err := feed(w, 0, spillPressureFor-10*time.Second, t0); err != nil {
		t.Fatal(err)
	}
	if err := feed(w, spillPressureFor-8*time.Second, spillPressureFor+10*time.Second, time.Time{}); err != nil || stderr.String() != "" {
		t.Fatalf("told while not waiting: %v, stderr %q", err, stderr)
	}
	if err := feed(w, spillPressureFor+12*time.Second, spillPressureFor+12*time.Second, t0); err != nil || strings.Count(stderr.String(), "\n") != 1 {
		t.Fatalf("the spell back: %v, stderr %q", err, stderr)
	}
	if err := feed(w, spillPressureFor+14*time.Second, spillPressureFor+2*time.Minute, time.Time{}); err != nil {
		t.Fatalf("stopped while not waiting: %v", err)
	}
	if err := feed(w, spillPressureFor+2*time.Minute+2*time.Second, spillPressureFor+2*time.Minute+2*time.Second, t0); !errors.Is(err, errSpillWait) ||
		strings.Count(stderr.String(), "\n") != 1 {
		t.Fatalf("the spell back past --spill-wait-timeout: %v, stderr %q", err, stderr)
	}
}

// TestSpillWaitTimeoutResumes: a sorted run whose output waits on the
// range at the front of the key space (held) while the rest is listed
// fills a small spill cap, and --spill-wait-timeout stops it: exit 74,
// cause spill_budget, resumable, the error naming the resume, error.json
// and the report agreeing. Resumed with a larger --spill-budget (and the
// timeout changed) once the front answers, it commits the dataset of an
// uninterrupted run byte for byte.
func TestSpillWaitTimeoutResumes(t *testing.T) {
	datasettest.RequireJournal(t)
	captureSyncStderr(t)
	var held atomic.Bool
	release := make(chan struct{})
	h := gcstest.Handler(gcstest.Keys(60000), 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if held.Load() && q.Get("pageToken") != "" && q.Get("startOffset") < "d01/" {
			select { // the front of the key space past its first page
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	args := func(dir string, extra ...string) []string {
		return append([]string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "8", "--part-size", "8KiB",
			"--part-interval=0", "--sort-buffer", "128KiB", "--spill-budget", "64KiB", "-o", dir}, extra...)
	}
	ctx := context.Background()
	ref := filepath.Join(t.TempDir(), "ref")
	if err := run(ctx, args(ref), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	held.Store(true)
	err := run(ctx, args(dir, "--spill-wait-timeout", "1s", "--report", report), io.Discard, false)
	if c := exitCode(t, err); c != exitDiskFull || !strings.Contains(err.Error(), "spill cap too small: listing has waited on the output for ") ||
		!strings.HasSuffix(err.Error(), "stopped by --spill-wait-timeout 1s: resume it with more disk or a larger --spill-budget (swath resume "+shellOperand(dir)+" --spill-budget SIZE)") {
		t.Fatalf("exit %d (%v), want %d naming the resume", c, err, exitDiskFull)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	var o outcome
	readJSON(t, filepath.Join(dir, errorName), &o)
	if rep.Outcome.Class != listing.ClassDiskFull || rep.Outcome.Cause != causeSpillBudget || !rep.Outcome.Resumable || rep.Outcome.StopSource != "spill_wait" ||
		o.ExitCode != exitDiskFull || o.Cause != causeSpillBudget || !o.Resumable || o.Message != rep.Outcome.Error {
		t.Fatalf("report %+v and error.json %+v", rep.Outcome, o)
	}
	if s := rep.Resources.Spill; s == nil || s.Refused == 0 || s.MaxBytes != 64<<10 {
		t.Fatalf("report spill %+v", s)
	}
	held.Store(false)
	close(release)
	if err := resume(ctx, []string{dir, "--spill-budget", "64MiB", "--spill-wait-timeout", "0"}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sameParts(t, "resume", ref, dir)
	if _, err := os.Stat(filepath.Join(dir, dataset.ManifestName)); err != nil {
		t.Fatal(err)
	}
}
