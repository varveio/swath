// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	ownedSignals(t)
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
	ownedSignals(t)
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

// TestDriverStopsALauncherThatExitsEarly runs the driver against a Java
// launcher that forks a child ignoring SIGTERM and exits 0 before any
// startup line: the driver returns the start error, and the child does not
// outlive it (startServer stops the whole group on every path after Start).
func TestDriverStopsALauncherThatExitsEarly(t *testing.T) {
	ownedSignals(t)
	dir := t.TempDir()
	java := script(t, dir, "early-exit",
		"sh -c 'trap \"\" TERM; exec sleep 3600' >/dev/null 2>&1 &\necho \"child $!\" >&2\nexit 0\n")
	err := run(options{java: java, goBin: "unused", fixture: dir, bucket: "bkt", logDir: dir, protocol: "s3", parallel: 1})
	if err == nil || !strings.Contains(err.Error(), "exited before its startup line") {
		t.Fatalf("driver: %v", err)
	}
	log, rerr := os.ReadFile(filepath.Join(dir, "replaydiff-java.log"))
	if rerr != nil {
		t.Fatal(rerr)
	}
	child := childOf(t, log)
	gone(t, child, child)
}

// ownedSignals wraps killGroup for one test: every group signal is
// counted, and must target an id whose leader pid is still allocated (the
// leader alive or an unreaped zombie), so it cannot reach a group that
// reused the id.
func ownedSignals(t *testing.T) *atomic.Int64 {
	var n atomic.Int64
	orig := killGroup
	killGroup = func(pgid int, sig syscall.Signal) error {
		n.Add(1)
		if _, err := os.Stat("/proc/" + strconv.Itoa(pgid)); err != nil {
			t.Errorf("signal %v sent to group %d after its leader was reaped", sig, pgid)
		}
		return orig(pgid, sig)
	}
	t.Cleanup(func() { killGroup = orig })
	return &n
}

// leaderState is the leader's /proc state letter, "" once it is reaped.
func leaderState(pid int) string {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	i := strings.LastIndexByte(string(stat), ')')
	return strings.Fields(string(stat[i+1:]))[0]
}

// TestGroupCleanupOwnsItsIDAndRunsOnce pins the group's ownership of its
// id: the leader of a group whose processes have all exited stays an
// unreaped zombie until cleanup, so the id cannot be reused before then;
// cleanup reaps it and retires the group; stop called again, or
// concurrently, signals nothing; and every group signal, here and in
// runBounded's sweep, targets an id the group still owns.
func TestGroupCleanupOwnsItsIDAndRunsOnce(t *testing.T) {
	signals := ownedSignals(t)
	dir := t.TempDir()

	// A leader whose whole group has already exited: held, not reaped.
	quick, err := startGroup(exec.Command(script(t, dir, "quick", "exit 0\n")))
	if err != nil {
		t.Fatal(err)
	}
	<-quick.exited
	time.Sleep(200 * time.Millisecond)
	pid := quick.cmd.Process.Pid
	if st := leaderState(pid); st != "Z" {
		t.Fatalf("an exited leader is %q before cleanup, want an unreaped zombie", st)
	}
	quick.stop(time.Second)
	if !quick.clean() || leaderState(pid) != "" || signals.Load() != 0 {
		t.Fatalf("clean %v, leader %q, %d signals", quick.clean(), leaderState(pid), signals.Load())
	}

	// A running group: stopped once by any number of concurrent and later
	// calls.
	slow, err := startGroup(exec.Command(script(t, dir, "slow", "sleep 600 &\nwait\n")))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() { slow.stop(10 * time.Second) })
	}
	wg.Wait()
	sent := signals.Load()
	if !slow.clean() || sent == 0 {
		t.Fatalf("clean %v after %d signals", slow.clean(), sent)
	}
	slow.stop(10 * time.Second)
	slow.kill()
	if signals.Load() != sent {
		t.Errorf("%d signals after the group was retired", signals.Load()-sent)
	}

	// runBounded's kill on its deadline and its sweep.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, _ := runBounded(ctx, script(t, dir, "one-shot", "sleep 3600 &\necho \"child $!\"\nsleep 3600\n"))
	child := childOf(t, out)
	gone(t, child, child)
}
