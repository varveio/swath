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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
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
		{"hang", stuckTimeouts, []string{"--gcs-request-timeout", "200ms"}},
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
			args := append([]string{"gs://bkt", "--gcs-endpoint", srv.URL + gcstest.Path, "--gcs-anonymous", "--workers", "2",
				"--stall-timeout", "1s", "--report", report, "-o", dir}, c.extra...)
			if code := exitCode(t, run(context.Background(), args, io.Discard, false)); code != exitStuck {
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
			if err != nil || !bytes.Contains(b, []byte("goroutine ")) || !bytes.Contains(b, []byte(c.want)) || len(b) > stacksMax+4096 {
				t.Fatalf("stacks: %v, %d bytes", err, len(b))
			}
		})
	}
}

// TestForcedExitCode: a run whose journal sync never returns cannot stop
// gracefully after a signal; shutdownGrace later the process exits with
// the signal's own code (143 after SIGTERM, 130 after SIGINT; it was 130
// for both), and writes the goroutine dump to DIR/.swath. The graceful
// codes are TestExitCodes'.
func TestForcedExitCode(t *testing.T) {
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
			var stderr bytes.Buffer
			cmd := exec.Command(bin, append([]string{"list"}, s.args(dir)...)...)
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
		})
	}
}
