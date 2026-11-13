// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/order"
)

// TestTmpfsNoSpillNoWarning: with $TMPDIR on tmpfs, a small listing to
// stdout (which spills, if at all, under $TMPDIR) prints no tmpfs warning,
// and a usage error found when the schema is bound comes alone.
func TestTmpfsNoSpillNoWarning(t *testing.T) {
	tmp, err := os.MkdirTemp("/dev/shm", "swath-tmpfs-")
	if err != nil {
		t.Skipf("no /dev/shm: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })
	if fs := order.FSType(tmp); !order.InMemory(fs) {
		t.Skipf("/dev/shm is %q, not tmpfs", fs)
	}
	t.Setenv("TMPDIR", tmp)
	src := t.TempDir()
	for i := range 14 {
		if err := os.WriteFile(filepath.Join(src, string(rune('a'+i))), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stderr := captureSyncStderr(t)
	setLog(termOptions{logFormat: logText}, "")
	if err := run(context.Background(), []string{src}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	err = run(context.Background(), []string{src, "--columns", "nope"}, io.Discard, false)
	if code := exitCode(t, err); code != exitUsage {
		t.Fatalf("exit %d (%v)", code, err)
	}
	if s := stderr.String(); strings.Contains(s, "tmpfs") {
		t.Fatalf("a tmpfs warning without a spill: %q", s)
	}
}
