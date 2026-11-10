// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// segReport is what TestSegmentsReplayDurableFiles reads of a run report.
type segReport struct {
	Output struct {
		RowsFetched  int64 `json:"rows_fetched"`
		RowsFiltered int64 `json:"rows_filtered"`
		RowsEmitted  int64 `json:"rows_emitted"`
	} `json:"output"`
	Resources struct {
		Spill *struct {
			Durable *struct {
				SealedFiles int64 `json:"sealed_files"`
				Resume      *struct {
					ReplayedRows int64 `json:"replayed_rows"`
				} `json:"resume"`
			} `json:"durable"`
		} `json:"spill"`
		Drain struct {
			Segments *struct {
				Reason            string `json:"reason"`
				Workers           int    `json:"workers"`
				DurableFilesRead  int    `json:"durable_files_read"`
				DurableBoundaries int    `json:"durable_boundaries"`
			} `json:"segments"`
		} `json:"drain"`
	} `json:"resources"`
}

func readSegReport(t *testing.T, at, path string) segReport {
	t.Helper()
	var r segReport
	if b, err := os.ReadFile(path); err != nil || json.Unmarshal(b, &r) != nil {
		t.Fatalf("%s: report: %v", at, err)
	}
	if r.Resources.Drain.Segments == nil || r.Resources.Spill == nil || r.Resources.Spill.Durable == nil {
		t.Fatalf("%s: no segments or durable spill in the report", at)
	}
	return r
}

// frontierGate holds the frontier of one run: its first listing request
// (the second from the start of the keyspace: the seed's is the first,
// as listing's frontierHold) goes unanswered while every other range
// lists, spills and seals behind a drainer that cannot move. It is
// released on the run's own signal (SWATH_TEST_LAST_RANGE_FILE,
// lastRangeFile): every range but the held one is done and a durable
// file is sealed. Listing then ends within the frontier's own range, the
// drainer still in it, so the sealed files ahead are the segments' to
// read. (A 50 ms delay on the first folders' pages left that to timing:
// under -race on CI the clean run's segments read no durable file.) A
// signal that does not come within two minutes is a failure of the
// fixture (late), not a release.
type frontierGate struct {
	signal  string // the file the run creates
	starts  atomic.Int64
	held    atomic.Bool // the frontier's request was held
	late    atomic.Bool // no signal within the cap
	release chan struct{}
	stop    chan struct{}
}

func newFrontierGate(signal string) *frontierGate {
	g := &frontierGate{signal: signal, release: make(chan struct{}), stop: make(chan struct{})}
	go g.watch()
	return g
}

// watch opens the gate once the frontier is held and the run has
// signalled; after two minutes it marks the gate late and opens it so
// the run ends (the test fails on late).
func (g *frontierGate) watch() {
	defer close(g.release)
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		select {
		case <-g.stop:
			return
		default:
		}
		if _, err := os.Stat(g.signal); err == nil && g.held.Load() {
			return
		}
	}
	g.late.Store(true)
}

// close stops the watch (the run has ended).
func (g *frontierGate) close() { close(g.stop) }

func (g *frontierGate) serve(h http.Handler, w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("delimiter") == "" && q.Get("start-after") == "" && q.Get("continuation-token") == "" && g.starts.Add(1) == 2 {
		g.held.Store(true)
		select {
		case <-g.release:
		case <-r.Context().Done(): // a killed run
			return
		}
	}
	h.ServeHTTP(w, r)
}

// TestSegmentsReplayDurableFiles: the segment tail replays durable spill
// files (order.Cache) by offset, beside run-private ones. A journaled
// sorted run on the S3 fake whose first folder is held until the rest has
// been served (frontierGate: the ranges ahead spill, capture and seal
// certificates) and whose row budget leaves
// room for several segment workers: the clean run engages segments that
// read durable files, with part boundaries inside their frames. Then runs
// killed after a seal, after a ledger append, and in a resume's recovery
// replay (frames of a reused certificate already handed on), each resumed:
// the resume engages segments over durable files too, its rows reconcile
// (fetched - filtered + replayed = emitted), and the dataset is the clean
// run's, byte for byte. A segment that could not read a durable file fails
// the run (the offset reader opens it through the output root and checks
// its version-2 CRC); nothing falls back to the serial drain silently.
func TestSegmentsReplayDurableFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	h := s3test.Handler(s3test.Backend(t, gcstest.Keys(150000)))
	var gate atomic.Pointer[frontierGate] // the run's; nil on a resume, which lists only gaps
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.URL.Query()["list-type"]; ok {
			if g := gate.Load(); g != nil {
				g.serve(h, w, r)
				return
			}
		}
		h.ServeHTTP(w, r)
	}))
	env := append(os.Environ(), "GOMAXPROCS=4")
	runtime := []string{"--part-interval=0", "--encoders", "4", "--sort-buffer", "12MiB", "--spill-budget", "512MiB"}
	args := func(out string, extra ...string) []string {
		a := []string{"list", "s3://bkt", "--endpoint-url", url, "--s3-path-style", "--anonymous", "--workers", "32", "--part-size", "16KiB", "-o", out}
		return append(append(a, runtime...), extra...)
	}
	run := func(at string, cmd *exec.Cmd, kill string, extra ...string) {
		t.Helper()
		var g *frontierGate
		cmd.Env = append(append(env, extra...), "SWATH_TEST_KILL_AT="+kill)
		if cmd.Args[1] == "list" {
			signal := filepath.Join(t.TempDir(), "last-range")
			g = newFrontierGate(signal)
			cmd.Env = append(cmd.Env, "SWATH_TEST_LAST_RANGE_FILE="+signal)
		}
		gate.Store(g)
		out, err := cmd.CombinedOutput()
		if g != nil {
			g.close()
			if kill == "" && (!g.held.Load() || g.late.Load()) {
				t.Fatalf("%s: the frontier held %v, the run's signal missing %v", at, g.held.Load(), g.late.Load())
			}
		}
		if kill == "" {
			if err != nil {
				t.Fatalf("%s: %v\n%s", at, err, out)
			}
			return
		}
		if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); err == nil || !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
			t.Fatalf("%s: not killed at %s (not reached): %v\n%s", at, kill, err, out)
		}
	}
	// engaged requires reconciled rows and segments over durable files,
	// or, on a resume that reuses certificates, the reason they could not
	// run: a reused certificate is a replay range the drainer alone
	// expands when it reaches it, so listing is not done (and the tail not
	// immutable) until the drainer has passed the last one.
	engaged := func(at string, r segReport) {
		t.Helper()
		sg, o := r.Resources.Drain.Segments, r.Output
		var replayed int64
		if res := r.Resources.Spill.Durable.Resume; res != nil {
			replayed = res.ReplayedRows
		}
		t.Logf("%s: segments %s, %d workers, %d durable files read, %d boundaries in them; %d fetched, %d replayed, %d emitted",
			at, sg.Reason, sg.Workers, sg.DurableFilesRead, sg.DurableBoundaries, o.RowsFetched, replayed, o.RowsEmitted)
		switch {
		case sg.Reason == "engaged" && sg.Workers >= 2 && sg.DurableFilesRead >= 1:
		case sg.Reason == "listing_active" && replayed > 0:
		default:
			t.Fatalf("%s: the segments did not replay durable files: %+v (%d rows replayed)", at, *sg, replayed)
		}
		if o.RowsFetched-o.RowsFiltered+replayed != o.RowsEmitted {
			t.Fatalf("%s: rows do not reconcile: %d fetched - %d filtered + %d replayed != %d emitted", at, o.RowsFetched, o.RowsFiltered, replayed, o.RowsEmitted)
		}
	}
	same := func(at, ref, out string) {
		t.Helper()
		sameParts(t, at, ref, out)
		if a, b := manifestBytes(t, ref), manifestBytes(t, out); !bytes.Equal(a, b) {
			t.Fatalf("%s: the manifest differs", at)
		}
	}

	ref := filepath.Join(t.TempDir(), "ref")
	report := filepath.Join(t.TempDir(), "ref.json")
	run("clean run", exec.Command(bin, args(ref, "--report", report)...), "")
	r := readSegReport(t, "clean run", report)
	engaged("clean run", r)
	if sg := r.Resources.Drain.Segments; sg.Reason != "engaged" || sg.DurableFilesRead < 1 {
		t.Fatalf("clean run: segments %+v: want engaged over durable files", *sg)
	}
	if r.Resources.Drain.Segments.DurableBoundaries < 1 || r.Resources.Spill.Durable.SealedFiles < 1 {
		t.Fatalf("clean run: %d segment boundaries inside durable frames, %d sealed files: want both", r.Resources.Drain.Segments.DurableBoundaries, r.Resources.Spill.Durable.SealedFiles)
	}

	resume := func(at, out, kill string, extra ...string) segReport {
		t.Helper()
		rep := filepath.Join(t.TempDir(), "resume.json")
		run(at+" resume", exec.Command(bin, append(append([]string{"resume", "--report", rep}, runtime...), out)...), kill, extra...)
		if kill != "" {
			return segReport{}
		}
		return readSegReport(t, at+" resume", rep)
	}
	// Each kill leaves the first certificate durable by construction:
	// under SWATH_TEST_SERIAL_SEALS seal n waits for the (n-1)-th
	// record cycle to start ("cycle-records", emitted before its append
	// and sync), and the one flusher starts a cycle only once the one
	// before has synced. So the third seal and the second append each
	// come after the first record's ledger sync. (A kill at the second
	// seal came after the first cycle started, not after it synced: on
	// macOS, whose fsync is slow, the kill landed before it, the resume
	// found no certificate and replayed nothing, #333.)
	for _, kill := range []string{"spill-seal:3", "spill-ledger-append:2"} {
		out := filepath.Join(t.TempDir(), "ds")
		run(kill, exec.Command(bin, args(out)...), kill, "SWATH_TEST_SERIAL_SEALS=1")
		engaged(kill, resume(kill, out, ""))
		same(kill, ref, out)
	}
	// Mid replay: certificates synced (the fourth ledger sync), the
	// resume killed in its recovery replay after its first frame, a second
	// resume replays them again.
	at := "spill-replay-frame"
	out := filepath.Join(t.TempDir(), "ds")
	run(at, exec.Command(bin, args(out)...), "spill-ledger-sync:4", "SWATH_TEST_SERIAL_SEALS=1")
	resume(at, out, "spill-replay-frame:2")
	r = resume(at+" again", out, "")
	engaged(at, r)
	if res := r.Resources.Spill.Durable.Resume; res == nil || res.ReplayedRows == 0 {
		t.Fatalf("%s: the second resume replayed no certificate", at)
	}
	same(at, ref, out)
}
