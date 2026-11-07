// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestServerThatNeverBecomesReadyIsStopped forces the failure path of the
// Java server helper: a launcher that starts a child and never prints its
// startup line, with a one-second readiness deadline. The start fails,
// and stopping the returned process leaves neither the launcher nor its
// child running: the whole process group is gone.
func TestServerThatNeverBecomesReadyIsStopped(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "never-ready")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nsleep 600 &\necho \"child $!\" >&2\nwait\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	p, stopAndCheck, err := launch(bin, nil, filepath.Join(dir, "never-ready.log"), time.Second)
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("start: %v", err)
	}
	if p == nil {
		t.Fatal("no process returned for a server that started")
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Errorf("the failed start took %v", took)
	}
	if err := stopAndCheck(); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(filepath.Join(dir, "never-ready.log"))
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(string(log), "child ")))
	if err != nil {
		t.Fatalf("no child pid in the log %q", log)
	}
	// The group, the launcher's child included, is gone (an orphaned child
	// that died is reaped by init shortly after; until then it counts).
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(-p.cmd.Process.Pid, 0)
		childErr := syscall.Kill(child, 0)
		if errors.Is(err, syscall.ESRCH) && errors.Is(childErr, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group %d or child %d still there: %v, %v", p.cmd.Process.Pid, child, err, childErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
