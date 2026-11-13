// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// rowlessService is a GCS bucket of 20,000 keys that, in mode "empty",
// answers its first 12 requests (more rows than the row budget holds, so
// parts are published) and then every one with an empty page
// continuing to a token never sent before; in mode "hung" it never
// answers; in mode "healthy" it is the bucket.
type rowlessService struct {
	mode              atomic.Value
	listed            atomic.Int64
	fresh             atomic.Int64
	firstNs, latestNs atomic.Int64
	url               string
}

func newRowlessService(t *testing.T) *rowlessService {
	s := &rowlessService{}
	s.set("healthy")
	h := gcstest.Handler(gcstest.Keys(20000), 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch s.mode.Load().(string) {
		case "hung":
			<-r.Context().Done()
			return
		case "empty":
			if s.listed.Add(1) > 12 {
				time.Sleep(5 * time.Millisecond)
				now := time.Now().UnixNano()
				s.firstNs.CompareAndSwap(0, now)
				s.latestNs.Store(now)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"kind":"storage#objects","nextPageToken":"fresh-%d"}`, s.fresh.Add(1))
				return
			}
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL + gcstest.Path
	return s
}

func (s *rowlessService) set(m string) {
	s.mode.Store(m)
	s.listed.Store(0)
	s.firstNs.Store(0)
}

func (s *rowlessService) args(dir string, extra ...string) []string {
	return append(append([]string{"gs://bkt", "--endpoint-url", s.url, "--anonymous", "--workers", "1", "--part-size", "4KiB",
		"--sort-buffer", "512KiB", "-o", dir}, unjournaled...), extra...)
}

// unjournaled is --no-journal where a journaled run is refused (Windows),
// for a test that needs only a dataset; empty elsewhere. The flag is swath
// diag list's, so such a run passes diagList to run.
var unjournaled, diagList = func() ([]string, bool) {
	if datasettest.NoJournal() {
		return []string{"--no-journal"}, true
	}
	return nil, false
}()

func readJSON(t *testing.T, path string, v any) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, v) != nil {
		t.Fatalf("%s: %v %q", path, err, b)
	}
	return b
}

// TestInfiniteEmptyPagesHonorsDeadline: a service that answers every
// request with an empty page and a fresh continuation token keeps the stall
// clock alive for the first keyrange.EmptyChainLive pages of the run (at
// 5 ms a page, over 5 s, past the deadline here; TestEndlessEmptyPagesAreStuck
// covers what follows), so --max-duration stops the run: exit 124,
// with the report and error.json naming the same attempt, no manifest and
// the published parts kept; once the service lists again, swath resume
// completes the dataset of an uninterrupted run byte for byte, the kept
// parts unchanged (durability review).
func TestInfiniteEmptyPagesHonorsDeadline(t *testing.T) {
	datasettest.RequireJournal(t)
	const stall = time.Second
	captureStderr(t)
	s := newRowlessService(t)
	ctx := context.Background()
	ref := filepath.Join(t.TempDir(), "ref")
	if err := run(ctx, s.args(ref), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	s.set("empty")
	// The deadline leaves room for slow data pages (the race detector on
	// a shared CI runner took 1.2 s for them) before over twice the stall
	// timeout of empty pages.
	err := run(ctx, s.args(dir, "--stall-timeout", stall.String(), "--max-duration", "6s", "--report", report), io.Discard, false)
	if c := exitCode(t, err); c != exitDeadline {
		t.Fatalf("exit %d (%v), want %d", c, err, exitDeadline)
	}
	if d := time.Duration(s.latestNs.Load() - s.firstNs.Load()); d < 2*stall || s.fresh.Load() < 100 {
		t.Fatalf("%d fresh empty pages over %s, not over twice the stall timeout", s.fresh.Load(), d)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	var o outcome
	readJSON(t, filepath.Join(dir, errorName), &o)
	if rep.Attempt == nil || o.Attempt == nil || rep.Attempt.ID != o.Attempt.ID || rep.Outcome.Class != "deadline" ||
		rep.Outcome.ExitCode != exitDeadline || !rep.Outcome.Resumable || rep.Outcome.Unrecorded != "" || o.ExitCode != exitDeadline {
		t.Fatalf("report %+v %+v and error.json %+v disagree", rep.Attempt, rep.Outcome, o)
	}
	if _, err := os.Stat(filepath.Join(dir, dataset.ManifestName)); err == nil {
		t.Fatal("a manifest after the deadline")
	}
	kept, _ := filepath.Glob(filepath.Join(dir, dataset.DataDir, "*.parquet"))
	if len(kept) == 0 {
		t.Fatal("no part was published before the empty pages")
	}
	before := map[string][]byte{}
	for _, p := range kept {
		before[p], _ = os.ReadFile(p)
	}
	s.set("healthy")
	if err := resume(ctx, []string{dir}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sameParts(t, "resume", ref, dir)
	for p, b := range before {
		if got, _ := os.ReadFile(p); !bytes.Equal(got, b) {
			t.Fatalf("kept part %s changed", filepath.Base(p))
		}
	}
	if _, err := os.Stat(filepath.Join(dir, errorName)); !os.IsNotExist(err) {
		t.Fatal("error.json left after the resume completed")
	}
}

// TestEndlessEmptyPagesAreStuck: with no --max-duration, a service that
// answers every request with an empty page and a fresh continuation token
// is stopped as stuck (exit 75, cause stuck_empty_chain, resumable) once
// the run of empty pages passes keyrange.EmptyChainLive and the stall
// timeout passes after it; before, every answered page kept the watchdog
// alive and the run went on forever (review R3, 2026-11-06).
func TestEndlessEmptyPagesAreStuck(t *testing.T) {
	datasettest.RequireJournal(t)
	const stall = time.Second
	captureStderr(t)
	s := newRowlessService(t)
	s.set("empty")
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	// A guard only: the run used to go on until this.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := run(ctx, s.args(dir, "--stall-timeout", stall.String(), "--report", report), io.Discard, false)
	if c := exitCode(t, err); c != exitStuck {
		t.Fatalf("exit %d (%v) after %d fresh empty pages, want %d", c, err, s.fresh.Load(), exitStuck)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	if rep.Outcome.Cause != stuckEmptyChain || !rep.Outcome.Resumable {
		t.Fatalf("outcome %+v, want cause %s, resumable", rep.Outcome, stuckEmptyChain)
	}
	// The bound's worth of empty pages, then the stall timeout's (one
	// each 5 ms at most), with slack for the watchdog's tick and the stop.
	chain := int64(keyrange.EmptyChainLive)
	if n := s.fresh.Load(); n <= chain || n > chain+2*int64(stall/(5*time.Millisecond)) {
		t.Fatalf("%d fresh empty pages, want over %d and within the stall timeout after", n, chain)
	}
}

// TestUnwritableReportAndStaleErrorJSON: an attempt that can write neither
// its report (the path a supervisor names is unwritable) nor error.json
// leaves the earlier attempt's error.json byte for byte; its exit code is
// its own (124, not the stale 75), and stderr names its attempt, in the
// report failure and in the outcome line, so a supervisor that accepts
// error.json only for the current attempt cannot accept the stale one
// (durability review).
func TestUnwritableReportAndStaleErrorJSON(t *testing.T) {
	datasettest.RequireJournal(t)
	stderr := captureStderr(t)
	s := newRowlessService(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "ds")
	s.set("hung")
	if c := exitCode(t, run(ctx, s.args(dir, "--stall-timeout", "1s"), io.Discard, false)); c != exitStuck {
		t.Fatalf("first attempt: exit %d", c)
	}
	var old outcome
	stale := readJSON(t, filepath.Join(dir, errorName), &old)
	if old.Attempt == nil || old.ExitCode != exitStuck {
		t.Fatalf("first attempt: error.json %+v", old)
	}
	// error.json cannot be replaced, nor the report written (its
	// destination passes the check before the run; the write fails at the
	// end).
	report := filepath.Join(t.TempDir(), "report.json")
	defer faults.SetIO(func(op, name string, b []byte) (int, error) {
		if base := filepath.Base(name); op == "write" && (strings.HasPrefix(base, "."+errorName+".") || strings.HasPrefix(base, ".report.json.")) {
			return 0, errors.New("injected: cannot be written")
		}
		return 0, nil
	})()
	s.set("empty")
	stderr.Reset()
	err := resume(ctx, []string{dir, "-v", "--stall-timeout", "1s", "--max-duration", "2s", "--report", report}, io.Discard)
	conclude(err) // -v: the outcome line
	if c := exitCode(t, err); c != exitDeadline {
		t.Fatalf("second attempt: exit %d (%v), want %d", c, err, exitDeadline)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, errorName)); !bytes.Equal(b, stale) {
		t.Fatalf("error.json changed: %q", b)
	}
	if _, err := os.Stat(report); err == nil {
		t.Fatal("the report was written")
	}
	m := regexp.MustCompile(`the report of attempt ([0-9a-f]+) is not written`).FindStringSubmatch(stderr.String())
	if m == nil || m[1] == old.Attempt.ID {
		t.Fatalf("stderr does not name the current attempt: %q", stderr.String())
	}
	var cur outcome
	for line := range strings.Lines(stderr.String()) {
		if j, ok := strings.CutPrefix(line, "swath: outcome: "); ok {
			json.Unmarshal([]byte(j), &cur)
		}
	}
	if cur.Attempt == nil || cur.Attempt.ID != m[1] || cur.ExitCode != exitDeadline {
		t.Fatalf("the outcome line %+v is not the current attempt %s", cur, m[1])
	}
	// The supervisor's check: error.json stands for this attempt only if
	// it names it.
	if old.Attempt.ID == m[1] {
		t.Fatal("the stale error.json names the current attempt")
	}
}

// TestDeadlineExitsAfterGrace: a run whose journal sync never returns (a
// goroutine blocked on I/O that no context reaches) cannot outlive
// --max-duration: shutdownGrace after the deadline the process exits 124,
// as it exits after a signal (before, only a signal had the forced exit and
// the process hung).
func TestDeadlineExitsAfterGrace(t *testing.T) {
	datasettest.RequireJournal(t)
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	s := newRowlessService(t)
	var stderr bytes.Buffer
	dir := filepath.Join(t.TempDir(), "ds")
	cmd := exec.Command(bin, append([]string{"list"}, s.args(dir, "--max-duration", "1s")...)...)
	cmd.Env = append(os.Environ(), "SWATH_TEST_HANG_AT=journal-sync:1")
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(shutdownGrace + 30*time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatalf("still running %s after the deadline: %s", time.Since(start), stderr.String())
	}
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != exitDeadline {
		t.Fatalf("exit %v, want %d: %s", err, exitDeadline, stderr.String())
	}
	if d := time.Since(start); d < shutdownGrace || !strings.Contains(stderr.String(), "still stopping (--max-duration)") {
		t.Fatalf("exited after %s without the forced exit: %s", d, stderr.String())
	}
	var o outcome
	readJSON(t, filepath.Join(dir, errorName), &o)
	if o.Class != "deadline" || o.ExitCode != exitDeadline {
		t.Fatalf("error.json after the forced exit: %+v", o)
	}
}

// failOutcomeWrites fails every write of error.json's temporary file
// until the returned restore.
func failOutcomeWrites() (restore func()) {
	return faults.SetIO(func(op, name string, b []byte) (int, error) {
		if op == "write" && strings.HasPrefix(filepath.Base(name), "."+errorName+".") {
			return 0, errors.New("injected: error.json cannot be written")
		}
		return 0, nil
	})
}
