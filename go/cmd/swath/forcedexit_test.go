// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

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

// TestForcedExitWithFullStderr (Sol, seventh review, Part 3 item 3): the
// process's stderr is a pipe nobody drains, filled after startup, and the
// journal sync hangs. A deadline (124) and a stall (75) still end the
// process with their code within the grace plus forcedAllowance, with the
// forced outcome in error.json: every write on the forced path to stderr
// (the notice, the warnings, a dump that falls back to stderr, the
// timeout line) is bounded, and the exit has a backstop. Before, the
// forced path's first stderr write blocked and the process never exited.
func TestForcedExitWithFullStderr(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
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
			dir := filepath.Join(t.TempDir(), "ds")
			cmd := exec.Command(bin, append([]string{"list"}, s.args(dir, tc.flags...)...)...)
			cmd.Env = append(os.Environ(), "SWATH_TEST_HANG_AT=journal-sync:1")
			cmd.Stderr = w
			start := time.Now()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(200 * time.Millisecond) // after startup
			fillPipe(t, w)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			limit := tc.wait + shutdownGrace + forcedAllowance + 3*time.Second
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
			if d := time.Since(start); d < shutdownGrace || d > limit {
				t.Fatalf("exited after %s, want between %s and %s", d.Round(time.Millisecond), shutdownGrace, limit)
			}
			var o outcome
			readJSON(t, filepath.Join(dir, errorName), &o)
			if o.Class != tc.class || o.ExitCode != tc.code {
				t.Fatalf("error.json after the forced exit: %+v", o)
			}
		})
	}
}
