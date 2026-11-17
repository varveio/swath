// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package command

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/varveio/swath/internal/dataset"
)

// TestOverwriteWithAReaderHoldingAPart: a reader of the old dataset holds
// a part open without delete sharing (as a DuckDB session does), so
// --overwrite cannot retire the old data/: the run fails naming the cause,
// the previous dataset is left as it was and still opens, and once the
// reader is gone swath resume installs the replacement.
func TestOverwriteWithAReaderHoldingAPart(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{src, "-o", dir, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var was strings.Builder
	if err := show([]string{dir}, &was); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "b"), []byte("22"), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := windows.UTF16PtrFromString(filepath.Join(dir, dataset.DataDir, dataset.PartName(0)))
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			windows.CloseHandle(h)
		}
	}()
	err = run(context.Background(), []string{src, "-o", dir, "--overwrite", "--no-stats"}, io.Discard, false)
	if err == nil || !strings.Contains(err.Error(), "another program may hold it, or a file in it, open") {
		t.Fatalf("an overwrite past a held part: %v", err)
	}
	// The previous dataset stands as it was: its manifest and parts.
	if b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName)); err != nil || !bytes.Equal(b, manifest) {
		t.Fatalf("the previous manifest: %v", err)
	}
	var now strings.Builder
	if err := show([]string{dir}, &now); err != nil || !strings.HasPrefix(now.String(), was.String()) {
		t.Fatalf("show after the refused overwrite: %v\n%s\nwas\n%s", err, now.String(), was.String())
	}
	if got, err := duRun(t, "--from-dataset", dir, "--format", "tsv"); err != nil || !strings.Contains(got, "\ntotal\t0\t1\t") {
		t.Fatalf("du --from-dataset after the refused overwrite: %v\n%q", err, got)
	}
	windows.CloseHandle(h)
	held = false
	if err := dispatch(context.Background(), []string{"resume", dir}, io.Discard); err != nil {
		t.Fatalf("resume after the reader closed: %v", err)
	}
	now.Reset()
	if err := show([]string{dir}, &now); err != nil || !strings.Contains(now.String(), "2 entries") || dataset.NextFound(dir) {
		t.Fatalf("after the resume (next left %v): %v\n%s", dataset.NextFound(dir), err, now.String())
	}
}
