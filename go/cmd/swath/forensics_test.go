// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestStuckCause: a run stopped as stuck (75) names what rose while
// nothing progressed, in error.json and the report, and leaves the
// goroutines as they hung in DIR/.swath/stacks-stuck.txt (Java recorded
// stuck_throttle or stuck_api_timeouts and logged a forensic snapshot).
func TestStuckCause(t *testing.T) {
	captureStderr(t)
	for _, c := range []struct {
		mode, want string
		extra      []string
	}{
		{"throttle", stuckThrottle, nil},
		{"hang", stuckTimeouts, []string{"--request-timeout", "200ms"}},
		{"hang", stuckNoRows, nil}, // requests outlive the stall timeout: nothing rises
	} {
		t.Run(c.want, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.mode == "hang" {
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(429)
				fmt.Fprint(w, `{"error":{"code":429,"message":"The rate of change requests to the bucket is too high"}}`)
			}))
			t.Cleanup(srv.Close)
			dir := filepath.Join(t.TempDir(), "ds")
			report := filepath.Join(t.TempDir(), "report.json")
			args := append([]string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "2",
				"--stall-timeout", "1s", "--report", report, "-o", dir}, c.extra...)
			args = append(args, unjournaled...)
			if code := exitCode(t, run(context.Background(), args, io.Discard, diagList)); code != exitStuck {
				t.Fatalf("exit %d, want %d", code, exitStuck)
			}
			var o outcome
			readJSON(t, filepath.Join(dir, errorName), &o)
			var rep listing.Report
			readJSON(t, report, &rep)
			if o.Class != "stuck" || o.Cause != c.want || rep.Outcome.Cause != c.want {
				t.Fatalf("error.json %+v, report cause %q: want cause %s", o, rep.Outcome.Cause, c.want)
			}
			b, err := os.ReadFile(filepath.Join(dir, dataset.Workspace, "stacks-stuck.txt"))
			if err != nil || !bytes.Contains(b, []byte("goroutine ")) || !bytes.Contains(b, []byte(c.want)) {
				t.Fatalf("stacks: %v, %d bytes", err, len(b))
			}
		})
	}
}

// TestForcedExitCode: a run whose journal sync never returns cannot stop
// gracefully after a signal; shutdownGrace later the process exits with
// the signal's own code (143 after SIGTERM, 130 after SIGINT; it was 130
// for both), writes the goroutine dump to DIR/.swath, and records the
// outcome in error.json and the report (it wrote neither). The graceful
// codes are TestExitCodes'.
func TestForcedExitCode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sends SIGTERM and SIGINT, which Windows cannot deliver to a process")
	}
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	s := newRowlessService(t)
	for sig, want := range map[syscall.Signal]int{syscall.SIGTERM: exitTerminated, syscall.SIGINT: exitInterrupted} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "ds")
			report := filepath.Join(t.TempDir(), "report.json")
			var stderr bytes.Buffer
			cmd := exec.Command(bin, append([]string{"list"}, s.args(dir, "--report", report)...)...)
			cmd.Env = append(os.Environ(), "SWATH_TEST_HANG_AT=journal-sync:1")
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			time.Sleep(time.Second)
			start := time.Now()
			cmd.Process.Signal(sig)
			var err error
			select {
			case err = <-done:
			case <-time.After(shutdownGrace + 30*time.Second):
				cmd.Process.Kill()
				<-done
				t.Fatalf("still running after the grace: %s", stderr.String())
			}
			ee, ok := err.(*exec.ExitError)
			if !ok || ee.ExitCode() != want {
				t.Fatalf("exit %v, want %d: %s", err, want, stderr.String())
			}
			if d := time.Since(start); d < shutdownGrace || !strings.Contains(stderr.String(), "still stopping") {
				t.Fatalf("exited after %s without the forced exit: %s", d, stderr.String())
			}
			b, err := os.ReadFile(filepath.Join(dir, dataset.Workspace, "stacks-forced-exit.txt"))
			if err != nil || !bytes.Contains(b, []byte("goroutine ")) || !bytes.Contains(b, []byte(sig.String())) {
				t.Fatalf("stacks: %v %.200s", err, b)
			}
			var o outcome
			readJSON(t, filepath.Join(dir, errorName), &o)
			var rep listing.Report
			readJSON(t, report, &rep)
			if o.ExitCode != want || rep.Outcome.ExitCode != want || o.Class != rep.Outcome.Class || o.Attempt == nil ||
				rep.Attempt == nil || o.Attempt.ID != rep.Attempt.ID || !strings.Contains(o.Message, "forced exit") {
				t.Fatalf("error.json %+v and report %+v %+v", o, rep.Attempt, rep.Outcome)
			}
		})
	}
}

// TestOneTerminalAccount: run.json, the external report (--report) and
// error.json carry the same outcome. A committed dataset with partial
// coverage (a key limit) is "incomplete", exit 1, in all three (run.json
// said "completed", exit 0, before the command classified it); a run that
// fails after journaling parts reports those durable parts, their bytes
// and rows (they were reported as none).
func TestOneTerminalAccount(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	ctx := context.Background()
	same := func(t *testing.T, name string, a, b listing.Outcome) {
		t.Helper()
		if a.Class != b.Class || a.ExitCode != b.ExitCode || a.Resumable != b.Resumable || a.Cause != b.Cause {
			t.Fatalf("%s %+v disagrees with the report %+v", name, a, b)
		}
	}
	t.Run("partial coverage", func(t *testing.T) {
		keys := make([]string, 300)
		for i := range keys {
			keys[i] = fmt.Sprintf("k%04d", i)
		}
		url := s3test.Bucket(t, keys)
		dir := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "report.json")
		err := run(ctx, []string{"s3://bkt", "--endpoint-url", url, "--s3-path-style", "--anonymous", "--sequential", "--max-keys", "10",
			"--report", report, "-o", dir}, io.Discard, true)
		if c := exitCode(t, err); c != exitFatal {
			t.Fatalf("exit %d (%v)", c, err)
		}
		var rep, rj listing.Report
		readJSON(t, report, &rep)
		readJSON(t, filepath.Join(dir, dataset.ReportName), &rj)
		var o outcome
		readJSON(t, filepath.Join(dir, errorName), &o)
		if rep.Outcome.Class != "incomplete" || rep.Outcome.ExitCode != exitFatal {
			t.Fatalf("report outcome %+v", rep.Outcome)
		}
		same(t, "run.json", rj.Outcome, rep.Outcome)
		same(t, "error.json", listing.Outcome{Class: o.Class, ExitCode: o.ExitCode, Resumable: o.Resumable, Cause: o.Cause}, rep.Outcome)
		if rj.Output.RowsDurable != 10 || rep.Output.RowsDurable != 10 {
			t.Fatalf("durable rows: run.json %d, report %d", rj.Output.RowsDurable, rep.Output.RowsDurable)
		}
	})
	t.Run("failed after parts", func(t *testing.T) {
		// The failure must come after a part is journaled and published,
		// and nothing else orders the two: the 12 pages come from a local
		// server in a few milliseconds while each part's publication takes
		// several fsyncs (part, claim directory, journal, data directory),
		// F_FULLFSYNC on macOS, slow enough there that the 403 arrived
		// with no part published. The 13th request waits for one.
		published := make(chan struct{})
		var once sync.Once
		restore := faults.Set(func(p string) error {
			if p == "renamed" {
				once.Do(func() { close(published) })
			}
			return nil
		})
		defer restore()
		h := gcstest.Handler(gcstest.Keys(20000), 0)
		var n atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) > 12 {
				select { // without a part (a regression) the check below fails, not the wait
				case <-published:
				case <-r.Context().Done():
				case <-time.After(time.Minute):
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(403)
				fmt.Fprint(w, `{"error":{"code":403,"message":"denied","errors":[{"reason":"forbidden"}]}}`)
				return
			}
			h.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		dir := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "report.json")
		err := run(ctx, []string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "1",
			"--part-size", "4KiB", "--sort-buffer", "512KiB", "--report", report, "-o", dir}, io.Discard, false)
		if c := exitCode(t, err); c != exitFatal {
			t.Fatalf("exit %d (%v)", c, err)
		}
		var rep listing.Report
		readJSON(t, report, &rep)
		var o outcome
		readJSON(t, filepath.Join(dir, errorName), &o)
		same(t, "error.json", listing.Outcome{Class: o.Class, ExitCode: o.ExitCode, Resumable: o.Resumable, Cause: o.Cause}, rep.Outcome)
		kept, _ := filepath.Glob(filepath.Join(dir, dataset.DataDir, "*.parquet"))
		var size int64
		for _, p := range kept {
			fi, _ := os.Stat(p)
			size += fi.Size()
		}
		if len(kept) == 0 || rep.Output.PartsWritten != len(kept) || rep.Output.OutputBytes != size || rep.Output.RowsDurable <= 0 {
			t.Fatalf("report output %+v; %d parts of %d bytes kept", rep.Output, len(kept), size)
		}
		if _, err := os.Stat(filepath.Join(dir, dataset.ReportName)); err == nil {
			t.Fatal("run.json for an uncommitted run")
		}
	})
	// A fatal failure is settled (error.json written) and the deadline
	// passes before the command returns: the report and the exit code keep
	// the run's classification (fatal, 1). Before, the command classified
	// again with the deadline as the stop cause and returned 124 with a
	// "deadline" report beside a "fatal" error.json of the same attempt.
	t.Run("settlement crosses the deadline", func(t *testing.T) {
		h := gcstest.Handler(gcstest.Keys(20000), 0)
		var n atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) > 12 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(403)
				fmt.Fprint(w, `{"error":{"code":403,"message":"denied","errors":[{"reason":"forbidden"}]}}`)
				return
			}
			h.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		const deadline = 4 * time.Second // the 403 comes well before it, also under the race detector
		var failedAt time.Time
		restore := faults.Set(func(p string) error {
			if p == "run-returned" {
				failedAt = time.Now()
				time.Sleep(deadline + 500*time.Millisecond) // the run is settled; the deadline passes
			}
			return nil
		})
		defer restore()
		dir := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "report.json")
		start := time.Now()
		err := run(ctx, []string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "1",
			"--part-size", "4KiB", "--sort-buffer", "512KiB", "--max-duration", deadline.String(), "--report", report, "-o", dir}, io.Discard, false)
		if time.Since(start) < deadline || failedAt.Sub(start) >= deadline {
			t.Fatalf("the run failed after %s and the command returned after %s: not across the %s deadline",
				failedAt.Sub(start), time.Since(start), deadline)
		}
		var rep listing.Report
		readJSON(t, report, &rep)
		var o outcome
		readJSON(t, filepath.Join(dir, errorName), &o)
		if c := exitCode(t, err); c != exitFatal || o.Class != "fatal" {
			t.Fatalf("exit %d, error.json %+v, report %+v", c, o, rep.Outcome)
		}
		same(t, "error.json", listing.Outcome{Class: o.Class, ExitCode: o.ExitCode, Resumable: o.Resumable, Cause: o.Cause}, rep.Outcome)
	})
}

// TestStacksWholeAndBounded: the goroutine dump is the whole profile (it
// was cut at 1 MiB by runtime.Stack, which hundreds of workers exceed);
// only its stderr fallback is cut at stacksMax.
func TestStacksWholeAndBounded(t *testing.T) {
	park := make(chan struct{})
	defer close(park)
	for range 10000 {
		go func() { <-park }()
	}
	var whole bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&whole, 2); err != nil || whole.Len() <= stacksMax {
		t.Fatalf("profile %d bytes (%v): too small to show the bound", whole.Len(), err)
	}
	var b bytes.Buffer
	errOut = &b
	t.Cleanup(func() { errOut = os.Stderr })
	if where := dumpStacks("", nil, "stuck", "test", listing.ProgressSnapshot{}); where != "stderr" {
		t.Fatalf("dump went to %s", where)
	}
	if b.Len() > stacksMax+4096 || !strings.Contains(b.String(), "cut at 1 MiB") {
		t.Fatalf("stderr dump: %d bytes, cut marker %v", b.Len(), strings.Contains(b.String(), "cut at 1 MiB"))
	}
}
