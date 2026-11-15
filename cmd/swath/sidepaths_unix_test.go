// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"

	"github.com/varveio/swath/internal/listing"
)

// TestReportToDescriptorOrPipe: --report naming a descriptor (/dev/fd/N,
// here onto a regular file) or a named pipe is written there, not renamed
// over: before, the temporary file beside /dev/fd/N could not be created
// and the run failed after listing (exit 1), and a FIFO was replaced by a
// regular file (C-S4).
func TestReportToDescriptorOrPipe(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/dev/fd and mkfifo as on Linux")
	}
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "r3.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fd := "/dev/fd/" + strconv.Itoa(int(f.Fd()))
	if err := runDu(context.Background(), []string{src, "--format=tsv", "--report", fd}, io.Discard); err != nil {
		t.Fatalf("--report %s: %v", fd, err)
	}
	var rep listing.Report
	readJSON(t, f.Name(), &rep)
	if rep.Outcome.Class != listing.ClassOK {
		t.Fatalf("report through %s: %+v", fd, rep.Outcome)
	}
	fifo := filepath.Join(t.TempDir(), "report.fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	got := make(chan []byte, 1)
	go func() {
		b, _ := os.ReadFile(fifo) // blocks until the run opens it for writing
		got <- b
	}()
	if err := runDu(context.Background(), []string{src, "--format=tsv", "--report", fifo}, io.Discard); err != nil {
		t.Fatalf("--report FIFO: %v", err)
	}
	rep = listing.Report{}
	if err := json.Unmarshal(<-got, &rep); err != nil || rep.Outcome.Class != listing.ClassOK {
		t.Fatalf("report through the FIFO: %v %+v", err, rep.Outcome)
	}
	if fi, err := os.Lstat(fifo); err != nil || fi.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("the FIFO was replaced: %v %v", fi.Mode(), err)
	}
}
