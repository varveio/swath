// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package command

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
)

// waitExit waits for cmd at most limit, then kills it and fails.
func waitExit(t *testing.T, cmd *exec.Cmd, start time.Time, limit time.Duration) (code int, took time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Until(start.Add(limit + 10*time.Second))):
		cmd.Process.Kill()
		<-done
		t.Fatalf("still running %s after the start", time.Since(start).Round(time.Second))
	}
	took = time.Since(start)
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, took
	case errors.As(err, &ee):
		return ee.ExitCode(), took
	}
	t.Fatal(err)
	return 0, took
}

// fillPipe writes into w until the pipe is full (nobody reads it), then
// puts the descriptor back in blocking mode, so the child's next write to
// it blocks.
func fillPipe(t *testing.T, w *os.File) {
	t.Helper()
	fd := int(w.Fd())
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4096)
	n := 0
	for {
		k, err := syscall.Write(fd, b)
		if err != nil {
			break
		}
		n += k
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("the pipe took nothing")
	}
}

// sawLine reads r until the line marker and reports whether it came
// before r ended.
func sawLine(r io.Reader, marker string) bool {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if sc.Text() == marker {
			return true
		}
	}
	return false
}

// holdWatch is a child's stderr that closes held once the hang point
// spec's marker line (hangMarker) is written: the run is then held there.
type holdWatch struct {
	syncBuffer
	marker string
	held   chan struct{}
	once   sync.Once
}

func newHoldWatch(spec string) *holdWatch {
	return &holdWatch{marker: hangMarker(spec) + "\n", held: make(chan struct{})}
}

func (h *holdWatch) Write(p []byte) (int, error) {
	n, err := h.syncBuffer.Write(p)
	if strings.Contains(h.String(), h.marker) {
		h.once.Do(func() { close(h.held) })
	}
	return n, err
}

// TestForcedExitWithFullStderr (durability review): the
// process's stderr is a pipe nobody drains, filled after startup, and the
// journal sync hangs. A deadline (124) and a stall (75) still end the
// process with their code within the grace plus forcedAllowance, with the
// forced outcome in error.json: every write on the forced path to stderr
// (the notice, the warnings, a dump that falls back to stderr, the
// timeout line) is bounded, and the exit has a backstop. Before, the
// forced path's first stderr write blocked and the process never exited.
// The pipe is filled once the sync hangs (its marker read from the pipe),
// so the forced path, a grace after the stop, finds it full.
func TestForcedExitWithFullStderr(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-tags", "swathtest", "-o", bin, swathMain).CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	t.Parallel()
	s := newRowlessService(t)
	for _, tc := range []struct {
		name  string
		flags []string
		code  int
		class string
		wait  time.Duration // the stop's own time before the grace
	}{
		{"deadline", []string{"--max-duration", "1s"}, exitDeadline, "deadline", time.Second},
		{"stall", []string{"--stall-timeout", "1s"}, exitStuck, "stuck", time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			const hang = "journal-sync:1"
			dir := filepath.Join(t.TempDir(), "ds")
			cmd := exec.Command(bin, append([]string{"list"}, s.args(dir, tc.flags...)...)...)
			cmd.Env = graceEnv(hang)
			cmd.Stderr = w
			start := time.Now()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			limit := tc.wait + testGrace + forcedAllowance + 3*time.Second
			held := make(chan bool, 1)
			go func() { held <- sawLine(r, hangMarker(hang)) }()
			select {
			case ok := <-held:
				if !ok {
					t.Fatal("stderr closed before the journal sync hung")
				}
			case err := <-done:
				t.Fatalf("exited (%v) before the journal sync hung", err)
			case <-time.After(limit + 10*time.Second):
				cmd.Process.Kill()
				<-done
				t.Fatal("the journal sync never hung")
			}
			fillPipe(t, w)
			select {
			case err = <-done:
			case <-time.After(limit + 10*time.Second):
				cmd.Process.Kill()
				<-done
				t.Fatalf("still running %s after the stop, with stderr full", time.Since(start).Round(time.Second))
			}
			w.Close()
			ee, ok := err.(*exec.ExitError)
			if !ok || ee.ExitCode() != tc.code {
				t.Fatalf("exit %v, want %d", err, tc.code)
			}
			if d := time.Since(start); d < tc.wait+testGrace || d > limit {
				t.Fatalf("exited after %s, want between %s and %s", d.Round(time.Millisecond), tc.wait+testGrace, limit)
			}
			var o outcome
			readJSON(t, filepath.Join(dir, errorName), &o)
			if o.Class != tc.class || o.ExitCode != tc.code {
				t.Fatalf("error.json after the forced exit: %+v", o)
			}
		})
	}
}

// TestForcedExitAfterCommit (B2): a run that committed its dataset and
// then blocks writing run.json is stopped by SIGTERM and forced out after
// the grace. Its classification (ok) stands: the process exits 0,
// the report (--report) says committed and complete with the run's
// statistics, and no error.json is left. Before, the forced path built an
// unstarted report (status failed, complete false) and wrote error.json.
// SIGTERM is sent once the run report's write hangs, after the commit.
func TestForcedExitAfterCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-tags", "swathtest", "-o", bin, swathMain).CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	t.Parallel()
	s := newRowlessService(t)
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	const hang = "run-report:1"
	stderr := newHoldWatch(hang)
	cmd := exec.Command(bin, append([]string{"list"}, s.args(dir, "--report", report)...)...)
	cmd.Env = graceEnv(hang)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-stderr.held:
	case err := <-done:
		t.Fatalf("exited (%v) before the run report hung: %s", err, stderr.String())
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatalf("the run report did not hang within 30 s: %s", stderr.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		cmd.Process.Kill()
		<-done
		t.Fatalf("the run report hung before the commit: %v", err)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(testGrace + forcedAllowance + 10*time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("still running after the forced exit's limit")
	}
	if err != nil {
		t.Fatalf("exit %v, want 0 (the run committed and is complete)", err)
	}
	var r struct {
		Outcome struct {
			Status, Class, Error string
			Complete             bool
			ExitCode             int `json:"exit_code"`
		}
		Output struct {
			RowsCommitted int64 `json:"rows_committed"`
		}
	}
	readJSON(t, report, &r)
	if r.Outcome.Status != "committed" || !r.Outcome.Complete || r.Outcome.Class != "ok" || r.Outcome.ExitCode != 0 ||
		r.Outcome.Error != "" || r.Output.RowsCommitted != 20000 {
		t.Fatalf("report after the forced exit: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(dir, errorName)); !os.IsNotExist(err) {
		t.Fatalf("error.json after a completed run's forced exit: %v", err)
	}
	// The progress file's last snapshot is the committed run's.
	var p inventory.ProgressSnapshot
	readJSON(t, filepath.Join(dir, progressName), &p)
	if p.State != inventory.StateDone || p.Rows != 20000 {
		t.Fatalf("progress.json after a completed run's forced exit: state %q, rows_fetched %d", p.State, p.Rows)
	}
}

// TestForcedExitAtCommit: the run installs its manifest and then hangs (at
// the manifest fault point) past --max-duration and the grace, so it is
// forced out. The commit and its account are one step, so the forced exit
// takes the committed result: a complete dataset exits 0 with a complete
// report and no error.json; one that does not cover its scope (--max-keys)
// exits 3 as incomplete and never as successful. Before, the account came
// after the manifest's fault point: the forced exit found no result and
// wrote a current-attempt deadline error.json (124) beside the complete
// dataset (eighth review). The bucket is 100 keys, one part: the run must
// reach its commit inside the deadline, and listing 20,000 keys into
// 4 KiB parts did not always on a loaded runner (CI: 124, the deadline
// before the commit).
func TestForcedExitAtCommit(t *testing.T) {
	bin := buildSwath(t)
	url := gcstest.Bucket(t, gcstest.Keys(100), 0)
	for _, tc := range []struct {
		name     string
		command  []string
		flags    []string
		code     int
		class    string
		complete bool
	}{
		{"complete", []string{"list"}, nil, exitOK, "ok", true},
		{"partial", []string{"diag", "list"}, []string{"--sequential", "--max-keys", "10"}, exitIncomplete, "incomplete", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "ds")
			report := filepath.Join(t.TempDir(), "report.json")
			const deadline = 3 * time.Second
			args := append(append(tc.command, "gs://bkt", "--endpoint-url", url, "--anonymous", "-o", dir,
				"--max-duration", deadline.String(), "--report", report), tc.flags...)
			cmd := exec.Command(bin, args...)
			cmd.Env = append(os.Environ(), "SWATH_TEST_HANG_AT=manifest:1")
			start := time.Now()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			code, took := waitExit(t, cmd, start, deadline+shutdownGrace+forcedAllowance)
			if code != tc.code {
				t.Fatalf("exit %d, want %d", code, tc.code)
			}
			if took < deadline+shutdownGrace {
				t.Fatalf("exited after %s: not forced", took)
			}
			if !dataset.Committed(dir) {
				t.Fatal("no manifest")
			}
			var r inventory.Report
			readJSON(t, report, &r)
			if r.Outcome.Status != inventory.StatusCommitted || r.Outcome.Class != tc.class || r.Outcome.ExitCode != tc.code || r.Outcome.Complete != tc.complete {
				t.Fatalf("report after the forced exit: %+v", r.Outcome)
			}
			// The committed dataset, complete or partial, has no
			// error.json: its manifest's coverage says what is missing.
			if _, err := os.Stat(filepath.Join(dir, errorName)); !os.IsNotExist(err) {
				t.Fatalf("error.json beside the committed dataset: %v", err)
			}
		})
	}
}

// TestForcedExitBackstopFirst: the run's classification hangs holding the
// terminal result's lock (SWATH_TEST_HANG_AT=classify:1) when the deadline
// stops it. The forced exit cannot read the result, yet the process exits
// with the deadline's code within the grace plus forcedAllowance: the
// backstop is armed before anything that waits. Before, the forced exit
// waited for that lock before arming it and never exited (eighth review).
func TestForcedExitBackstopFirst(t *testing.T) {
	bin := buildSwath(t)
	t.Parallel()
	s := newRowlessService(t)
	s.set("hung")
	dir := filepath.Join(t.TempDir(), "ds")
	const deadline, hang = time.Second, "classify:1"
	var stderr syncBuffer
	cmd := exec.Command(bin, append([]string{"list"}, s.args(dir, "--max-duration", deadline.String())...)...)
	cmd.Env = graceEnv(hang)
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	limit := deadline + testGrace + forcedAllowance + 3*time.Second
	code, took := waitExit(t, cmd, start, limit)
	if !strings.Contains(stderr.String(), hangMarker(hang)) {
		t.Fatalf("the classification never hung: %s", stderr.String())
	}
	if code != exitDeadline || took < deadline+testGrace || took > limit {
		t.Fatalf("exit %d after %s, want %d between %s and %s", code, took.Round(time.Millisecond), exitDeadline, deadline+testGrace, limit)
	}
	if _, err := os.Stat(filepath.Join(dir, errorName)); !os.IsNotExist(err) {
		t.Fatalf("an outcome written without the run's result: %v", err)
	}
}

// TestForcedSnapshotIsTheResult: a forced exit takes one snapshot of the
// terminal result, and its exit code and report derive from it, whatever
// the run freezes concurrently. A commit that comes after the snapshot is
// refused, and a later classification gets the forced result; a commit
// under way when the forced exit comes is waited for (a barrier holds it),
// and its result is the forced exit's: completed with exit 0 for a
// complete dataset, incomplete and nonzero for a partial one. Before, the
// exit code was chosen from one read and the report from a later one, so a
// result frozen in between paired a completed report with a nonzero code
// (eighth review).
func TestForcedSnapshotIsTheResult(t *testing.T) {
	captureStderr(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errDeadline)
	committed := func(complete bool) *inventory.Report {
		return &inventory.Report{Outcome: inventory.Outcome{Status: inventory.StatusCommitted, Class: inventory.ClassOK, Complete: complete}}
	}
	target := func() (*forensicTarget, string) {
		dir := t.TempDir()
		att := newAttempt()
		report := filepath.Join(t.TempDir(), "report.json")
		return &forensicTarget{ctx: ctx, dir: dir, report: report, argv: []string{"list"}, att: att,
			t: &terminal{ctx: ctx, dir: dir, att: att}}, report
	}
	// check forces the exit as Main does and returns its code and report.
	check := func(f *forensicTarget, report string, s snapshot, ok bool) (int, inventory.Report) {
		t.Helper()
		if !ok {
			t.Fatal("no snapshot")
		}
		forcedAccount(f, s, "--max-duration")
		var r inventory.Report
		readJSON(t, report, &r)
		if r.Outcome.ExitCode != s.out.ExitCode || (r.Outcome.Class == inventory.ClassOK) != (s.out.ExitCode == exitOK) {
			t.Fatalf("exit %d with report %+v", s.out.ExitCode, r.Outcome)
		}
		return s.out.ExitCode, r
	}

	t.Run("freeze after the snapshot", func(t *testing.T) {
		f, report := target()
		s, ok := forcedSnapshot(f, exitDeadline, "--max-duration", nil)
		ran := false
		if err := f.t.commit(func() error { ran = true; return nil }); !errors.Is(err, errForced) || ran {
			t.Fatalf("a commit after the forced snapshot: %v, ran %v", err, ran)
		}
		f.t.account(committed(true), nil) // a classification after it
		if code, r := check(f, report, s, ok); code != exitDeadline || r.Outcome.Class != inventory.ClassDeadline {
			t.Fatalf("exit %d, %+v", code, r.Outcome)
		}
	})
	for _, tc := range []struct {
		name     string
		complete bool
		code     int
		class    string
	}{
		{"commit under way/complete", true, exitOK, inventory.ClassOK},
		{"commit under way/partial", false, exitIncomplete, inventory.ClassIncomplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, report := target()
			entered, release, committedErr := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				committedErr <- f.t.commit(func() error {
					close(entered)
					<-release // the barrier: the manifest is being installed
					f.t.account(committed(tc.complete), nil)
					return nil
				})
			}()
			<-entered
			type result struct {
				s  snapshot
				ok bool
			}
			got := make(chan result, 1)
			go func() {
				s, ok := f.t.claim(func() outcome { return outcome{Class: inventory.ClassDeadline, ExitCode: exitDeadline} }, time.Minute, nil)
				got <- result{s, ok}
			}()
			select {
			case <-got:
				t.Fatal("the snapshot did not wait for the commit under way")
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-committedErr; err != nil {
				t.Fatal(err)
			}
			res := <-got
			if code, r := check(f, report, res.s, res.ok); code != tc.code || r.Outcome.Class != tc.class {
				t.Fatalf("exit %d, %+v", code, r.Outcome)
			}
		})
	}
	t.Run("lock held past the wait", func(t *testing.T) {
		f, _ := target()
		f.t.mu.Lock() // a classification that does not return
		late := make(chan snapshot, 1)
		if _, ok := f.t.claim(func() outcome { return outcome{ExitCode: exitDeadline} }, 10*time.Millisecond, func(s snapshot) { late <- s }); ok {
			t.Fatal("a snapshot while the result is held")
		}
		f.t.out = &outcome{Class: inventory.ClassOK, ExitCode: exitOK}
		f.t.mu.Unlock()
		if s := <-late; s.out.ExitCode != exitOK || !s.run {
			t.Fatalf("the late snapshot: %+v", s)
		}
	})
}
