// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package localfs

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// TestStatxMissingFields: a value statx did not return (its mask bit
// clear: a filesystem that keeps no blocks) is null, not the zero statx
// leaves in its place; the values it did return stay present.
func TestStatxMissingFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	var s unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_ALL, &s); err != nil {
		t.Skipf("statx: %v", err)
	}
	var whole Entry
	fillStatx(&whole, &s)
	if !whole.Has(HasStat) || whole.Size != 5 || whole.Kind != KindFile {
		t.Fatalf("every field returned: %+v", whole)
	}
	s.Mask &^= unix.STATX_BLOCKS | unix.STATX_NLINK | unix.STATX_BTIME
	s.Blocks = 0
	var r Entry
	fillStatx(&r, &s)
	for _, p := range []Presence{HasBlocks, HasNlink, HasBtime} {
		if r.Has(p) {
			t.Errorf("presence %b set without its mask bit", p)
		}
	}
	if !r.Has(HasSize|HasMtime|HasMode|HasInode|HasDev) || r.Size != 5 || r.Kind != KindFile {
		t.Fatalf("the returned values: %+v", r)
	}
	for _, c := range Schema.Columns {
		if c.Name != "allocated_bytes" {
			continue
		}
		if v, ok := c.Int(&r); ok {
			t.Fatalf("allocated_bytes %d: want null", v)
		}
		return
	}
	t.Fatal("no allocated_bytes column")
}
