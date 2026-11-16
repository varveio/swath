// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestDirSyncFlushes: a directory the user created is flushed by path and
// through a root (the flush accepts the directory reopened with
// FILE_APPEND_DATA), the probe of an output not yet created flushes its
// nearest existing parent, and a file or a missing directory fails rather
// than passing for a synced directory.
func TestDirSyncFlushes(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syncDir(dir); err != nil {
		t.Fatal(err)
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, name := range []string{".", "sub"} {
		if err := SyncDirIn(r, name); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if err := CheckDirSync(filepath.Join(dir, "not", "yet")); err != nil {
		t.Fatal(err)
	}
	if err := syncDir(filepath.Join(dir, "f")); err == nil {
		t.Fatal("a file was synced as a directory")
	}
	if err := SyncDirIn(r, "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a missing directory: %v", err)
	}
}
