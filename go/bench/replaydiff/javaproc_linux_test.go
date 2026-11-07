// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"
)

var childLine = regexp.MustCompile(`child (\d+)`)

// childOf reads the "child PID" line a test launcher wrote.
func childOf(t *testing.T, text []byte) int {
	t.Helper()
	m := childLine.FindSubmatch(text)
	if m == nil {
		t.Fatalf("no child pid in %q", text)
	}
	pid, _ := strconv.Atoi(string(m[1]))
	return pid
}

// gone waits up to five seconds for the process group led by leader and
// the process child to be gone (an orphaned member that died is reaped by
// init shortly after; until then it counts).
func gone(t *testing.T, leader, child int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		groupErr, childErr := syscall.Kill(-leader, 0), syscall.Kill(child, 0)
		if errors.Is(groupErr, syscall.ESRCH) && errors.Is(childErr, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group %d or child %d still there: %v, %v", leader, child, groupErr, childErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func script(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestServerThatNeverBecomesReadyIsStopped forces the failure path of the
// Java server helper with launchers that never print their startup line,
// under a one-second readiness deadline: the start fails within the stop's
// bounds, the helper's own check passes, and afterwards neither the
// launcher's process group nor its child exists. The second launcher exits
// on SIGTERM while its child ignores it, so the launcher's exit alone does
// not prove the group stopped.
func TestServerThatNeverBecomesReadyIsStopped(t *testing.T) {
	for name, body := range map[string]string{
		"child keeps running":                      "sleep 600 &\necho \"child $!\" >&2\nwait\n",
		"launcher exits on TERM, child ignores it": "trap 'exit 0' TERM\nsh -c 'trap \"\" TERM; exec sleep 3600' &\necho \"child $!\" >&2\nwait\n",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			start := time.Now()
			p, stopAndCheck, err := launch(script(t, dir, "never-ready", body), nil, filepath.Join(dir, "never-ready.log"), time.Second)
			if err == nil {
				t.Fatal("a server that never printed its startup line started")
			}
			if p == nil {
				t.Fatal("no process returned for a server that started")
			}
			if took := time.Since(start); took > 25*time.Second {
				t.Errorf("the failed start took %v", took)
			}
			log, err := os.ReadFile(filepath.Join(dir, "never-ready.log"))
			if err != nil {
				t.Fatal(err)
			}
			child := childOf(t, log)
			if err := stopAndCheck(); err != nil {
				t.Fatal(err)
			}
			gone(t, p.cmd.Process.Pid, child)
		})
	}
}

// TestRunBoundedKillsTheGroup runs one-shot commands whose descendant
// keeps the output pipe open and sleeps an hour: one still running at its
// deadline, one whose own process exits at once. Each returns within its
// bounds, and neither the group nor the descendant survives.
func TestRunBoundedKillsTheGroup(t *testing.T) {
	for name, c := range map[string]struct {
		body     string
		deadline time.Duration
	}{
		"running at its deadline":          {"sleep 3600 &\necho \"child $!\"\nsleep 3600\n", time.Second},
		"exits, descendant holds the pipe": {"sleep 3600 &\necho \"child $!\"\nexit 0\n", time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
			defer cancel()
			start := time.Now()
			out, _ := runBounded(ctx, script(t, dir, "one-shot", c.body))
			if took := time.Since(start); took > 20*time.Second {
				t.Errorf("returned after %v", took)
			}
			child := childOf(t, out)
			gone(t, child, child)
		})
	}
}
