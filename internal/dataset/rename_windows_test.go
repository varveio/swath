// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// holdOpen opens path without delete sharing, as a reader that is not Go
// does, so a rename over it fails until the returned close.
func holdOpen(t *testing.T, path string) (close func()) {
	t.Helper()
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	return func() { windows.CloseHandle(h) }
}

// TestReplaceWaitsForAHeldTarget: a replacement whose target another
// handle holds without delete sharing waits for the handle to close, then
// installs the new file; nothing is left beside it.
func TestReplaceWaitsForAHeldTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.json")
	if err := Replace(path, []byte("old"), Durable); err != nil {
		t.Fatal(err)
	}
	release := holdOpen(t, path)
	done := make(chan error, 1)
	go func() { done <- Replace(path, []byte("new"), Durable) }()
	select {
	case err := <-done:
		t.Fatalf("the replacement ended (%v) while the target was held", err)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "new" {
		t.Fatalf("after the handle closed: %q", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Fatalf("%d entries left", len(ents))
	}
}

// TestReplaceGivesUpOnAHeldTarget: a handle held past the retry budget
// fails the replacement: the error surfaces, the old target is intact and
// the temporary file is removed.
func TestReplaceGivesUpOnAHeldTarget(t *testing.T) {
	defer func(d time.Duration) { replaceRetry = d }(replaceRetry)
	replaceRetry = 200 * time.Millisecond
	dir := t.TempDir()
	path := filepath.Join(dir, "f.json")
	if err := Replace(path, []byte("old"), Durable); err != nil {
		t.Fatal(err)
	}
	release := holdOpen(t, path)
	defer release()
	start := time.Now()
	err := Replace(path, []byte("lost"), Durable)
	if err == nil || !strings.Contains(err.Error(), "another program may hold it") {
		t.Fatalf("a held target: %v, want the refusal with its hint", err)
	}
	if d := time.Since(start); d < replaceRetry/2 || d > 10*replaceRetry {
		t.Fatalf("gave up after %s, want about %s: %v", d, replaceRetry, err)
	}
	if b, _ := os.ReadFile(path); string(b) != "old" {
		t.Fatalf("the old target became %q", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Fatalf("%d entries left (a temporary file?)", len(ents))
	}
}

// TestReplaceLongPath: a replacement whose path is longer than MAX_PATH
// is installed (MoveFileEx takes it under the \\?\ prefix).
func TestReplaceLongPath(t *testing.T) {
	dir := t.TempDir()
	for len(dir) < 300 {
		dir = filepath.Join(dir, "a-directory-name-of-some-length")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "f.json")
	for _, b := range []string{"old", "new"} {
		if err := Replace(path, []byte(b), Durable); err != nil {
			t.Fatal(err)
		}
	}
	if b, _ := os.ReadFile(path); string(b) != "new" {
		t.Fatalf("read %q", b)
	}
}

// TestRenameHeldDirectoryNamesTheCause: a directory holding a file another
// program has open (a reader of a part while --overwrite retires the old
// dataset) is not renamed, and the error says why.
func TestRenameHeldDirectoryNamesTheCause(t *testing.T) {
	defer func(d time.Duration) { replaceRetry = d }(replaceRetry)
	replaceRetry = 200 * time.Millisecond
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	if err := os.Mkdir(data, 0o755); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(data, "part-00000.parquet")
	if err := os.WriteFile(part, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	release := holdOpen(t, part)
	defer release()
	err := renameFile(data, filepath.Join(dir, "retired"))
	if err == nil || !strings.Contains(err.Error(), "another program may hold it, or a file in it, open") {
		t.Fatalf("renaming a held directory: %v", err)
	}
	if _, err := os.Stat(part); err != nil {
		t.Fatalf("the held part moved: %v", err)
	}
}
