// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package main

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestProfilerAccountDoesNotWait: a phase switch blocked on a file (a FIFO
// with no reader at MEM.listing) does not block the report's account,
// which the commit's Finalize takes; wait does, until the write is done.
func TestProfilerAccountDoesNotWait(t *testing.T) {
	dir := t.TempDir()
	mem := filepath.Join(dir, "mem")
	if err := syscall.Mkfifo(mem+phaseListing, 0o644); err != nil {
		t.Skip("no FIFO:", err)
	}
	before := runtime.MemProfileRate
	defer func() { runtime.MemProfileRate = before }()
	p, err := startProfiles("", mem, defaultMemProfileRate, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.listingDone()
	accounted := make(chan []string)
	go func() { accounted <- statuses(p.account()) }()
	select {
	case got := <-accounted:
		if len(got) != 3 || got[0] != "mem.listing planned" {
			t.Fatalf("account while the switch writes: %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the account waited for a blocked profile write")
	}
	waited := make(chan struct{})
	go func() { p.wait(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("wait returned while the switch was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	r, err := os.OpenFile(mem+phaseListing, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	r.Close()
	<-waited
	p.stop()
	if got := statuses(p.account()); got[0] != "mem.listing written" {
		t.Fatalf("after the reader: %q", got)
	}
}
