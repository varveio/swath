// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// TestProfileFanoutFailure: a profile written to two files, the first of
// which fails every write (/dev/full: ENOSPC), still writes the second
// whole; the first is failed, with its error, the second written. CPU and
// allocation profiles alike.
func TestProfileFanoutFailure(t *testing.T) {
	if f, err := os.OpenFile("/dev/full", os.O_WRONLY, 0); err != nil {
		t.Skip("no /dev/full:", err)
	} else {
		f.Close()
	}
	dir := t.TempDir()
	cpu, mem := filepath.Join(dir, "cpu"), filepath.Join(dir, "mem")
	var warned []string
	p := &profiler{files: map[string][]*profFile{}, warn: func(m string) { warned = append(warned, m) }}
	if err := p.startCPU(p.plan(kindCPUListing, "/dev/full", cpu)); err != nil {
		t.Fatal(err)
	}
	spin(20 * time.Millisecond)
	p.stopCPU()
	p.writeMem(p.plan(kindMemExit, "/dev/full", mem))
	a := p.account()
	got := []string{}
	for _, f := range a.Files {
		got = append(got, filepath.Base(f.Path)+" "+f.Status)
		if (f.Status == statusFailed) != (f.Error != "") {
			t.Errorf("%s: status %s, error %q", f.Path, f.Status, f.Error)
		}
	}
	if want := []string{"full failed", "cpu written", "full failed", "mem written"}; !slices.Equal(got, want) || len(warned) != 2 {
		t.Fatalf("files %q, warned %q, want %q", got, warned, want)
	}
	profileSpan(t, cpu) // whole: a complete gzipped profile
	profileSpan(t, mem)
}
