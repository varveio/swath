// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestTraceFileThatNeverDrains: --trace to a pipe whose reader never reads
// (a 4 KiB pipe, far less than the run's events) does not hold up the
// run: the listing completes, the dataset and --report are written, and
// the end waits for the trace only traceCloseWait before abandoning it,
// with a warning that counts the events not written.
func TestTraceFileThatNeverDrains(t *testing.T) {
	stderr := captureStderr(t)
	defer func(w time.Duration) { traceCloseWait = w }(traceCloseWait)
	traceCloseWait = 300 * time.Millisecond
	url := s3test.Bucket(t, s3test.Keys(200, 3))
	dir := t.TempDir()
	fifo, report, out := filepath.Join(dir, "trace.fifo"), filepath.Join(dir, "r.json"), filepath.Join(dir, "ds")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skip("no FIFO:", err)
	}
	reader, err := os.OpenFile(fifo, os.O_RDONLY|syscall.O_NONBLOCK, 0) // held open, never read
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() }) // the abandoned writer then fails and ends
	if _, err := unix.FcntlInt(reader.Fd(), unix.F_SETPIPE_SZ, 4096); err != nil {
		t.Log("pipe size unchanged:", err)
	}
	args := append([]string{"s3://bkt", "--endpoint-url", url, "--s3-path-style", "--anonymous", "--page-size", "1", "--workers", "4",
		"--no-progress", "--report", report, "--trace", fifo, "-o", out}, unjournaled...)
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- run(context.Background(), args, io.Discard, true) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the run did not end: it waits on the trace file")
	}
	t.Logf("run took %s", time.Since(start))
	if _, err := os.Stat(report); err != nil {
		t.Fatalf("--report not written: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); err != nil {
		t.Fatalf("the dataset was not committed: %v", err)
	}
	m := regexp.MustCompile(`--trace \S+: the file took no write for 300ms: abandoned, with (\d+) events and run_end not written`).FindStringSubmatch(stderr.String())
	if m == nil || m[1] == "0" {
		t.Fatalf("no warning counting the abandoned events: %q", stderr.String())
	}
	if strings.Count(stderr.String(), "--trace") != 1 {
		t.Fatalf("the trace was warned about more than once: %q", stderr.String())
	}
}
