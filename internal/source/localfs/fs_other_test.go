// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package localfs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestOpenCheckedRejectsSymlinkedChild: where a directory is opened by its
// path, one below the root that is a symbolic link is refused
// (unsafe_dirs); the root may be one.
func TestOpenCheckedRejectsSymlinkedChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on Windows")
	}
	base := t.TempDir()
	real := filepath.Join(base, "real")
	link := filepath.Join(base, "link")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	var st Stats
	p := &Provider{}
	if f, ok := p.openChecked(Range{path: link, prefix: "x/"}, &st); ok {
		f.Close()
		t.Fatal("symlinked child directory was opened")
	}
	if st.Unsafe != 1 {
		t.Fatalf("unsafe count %d", st.Unsafe)
	}
	if f, ok := p.openChecked(Range{path: link}, &st); !ok { // the root may be a symlink
		t.Fatal("symlinked root refused")
	} else {
		f.Close()
	}
}
