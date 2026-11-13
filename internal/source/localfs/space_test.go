// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source"
)

// TestBlockSpace: block counts become bytes in the filesystem's unit; a
// unit that is not positive, a product past an int64, or a total of 0
// measures nothing, and zero free space is a measurement.
func TestBlockSpace(t *testing.T) {
	at := time.Date(2026, 11, 13, 10, 0, 0, 0, time.FixedZone("x", 3600))
	got := blockSpace(1000, 400, 300, 4096, at)
	want := source.FilesystemSpace{TotalBytes: 4_096_000, FreeBytes: 1_638_400, AvailableBytes: 1_228_800, MeasuredAt: at.UTC()}
	if got == nil || *got != want || got.MeasuredAt.Location() != time.UTC {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if got := blockSpace(1000, 0, 0, 4096, at); got == nil || got.FreeBytes != 0 || got.AvailableBytes != 0 {
		t.Fatalf("a full filesystem: %+v", got)
	}
	for _, c := range []struct {
		total, free, avail uint64
		unit               int64
	}{
		{1000, 400, 300, 0},
		{1000, 400, 300, -1},
		{0, 0, 0, 4096},
		{math.MaxUint64, 1, 1, 2},
		{1 << 52, 1, 1, 4096},          // 2^64: past a uint64
		{1 << 51, 1 << 51, 1, 4096},    // 2^63: past an int64
		{1 << 40, 1 << 40, 1 << 62, 4}, // available alone overflows
	} {
		if got := blockSpace(c.total, c.free, c.avail, c.unit, at); got != nil {
			t.Errorf("%+v: %+v, want nothing", c, got)
		}
	}
}

// TestByteSpace: byte counts are kept as reported, free above total
// included (a Windows quota bounds the total, not the volume's free
// space); a total of 0 or a count past an int64 measures nothing.
func TestByteSpace(t *testing.T) {
	at := time.Now()
	if got := byteSpace(100, 500, 80, at); got == nil || got.TotalBytes != 100 || got.FreeBytes != 500 || got.AvailableBytes != 80 {
		t.Fatalf("a quota: %+v", got)
	}
	for _, c := range [][3]uint64{{0, 5, 5}, {math.MaxInt64 + 1, 1, 1}, {1, math.MaxInt64 + 1, 1}, {1, 1, math.MaxInt64 + 1}} {
		if got := byteSpace(c[0], c[1], c[2], at); got != nil {
			t.Errorf("%v: %+v, want nothing", c, got)
		}
	}
}

// TestFilesystemSpace: the provider measures its root's filesystem on the
// platforms that can (a file root included) and keeps the first
// measurement, a failed one included.
func TestFilesystemSpace(t *testing.T) {
	measures := runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows"
	dir := t.TempDir()
	file := filepath.Join(dir, "f")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{dir, file} {
		p, err := New(root, Options{})
		if err != nil {
			t.Fatal(err)
		}
		s := p.filesystemSpace()
		if !measures {
			if s != nil {
				t.Fatalf("%s: measured %+v on %s", root, s, runtime.GOOS)
			}
			continue
		}
		if s == nil || s.TotalBytes <= 0 || s.AvailableBytes < 0 || s.AvailableBytes > s.TotalBytes || s.MeasuredAt.IsZero() {
			t.Fatalf("%s: %+v", root, s)
		}
		if again := p.filesystemSpace(); again != s {
			t.Fatalf("%s: measured again: %+v, then %+v", root, s, again)
		}
	}

	t.Run("symlink file root", func(t *testing.T) { symlinkFileRoot(t, dir) })
	t.Run("replaced root", func(t *testing.T) { replacedRoot(t, dir) })

	gone := filepath.Join(dir, "gone")
	if err := os.Mkdir(gone, 0o700); err != nil {
		t.Fatal(err)
	}
	p, err := New(gone, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	if s := p.filesystemSpace(); s != nil {
		t.Fatalf("a root that is gone: %+v", s)
	}
	if err := os.Mkdir(gone, 0o700); err != nil {
		t.Fatal(err)
	}
	if s := p.filesystemSpace(); s != nil {
		t.Fatalf("a failed measurement was retried: %+v", s)
	}
}

// replacedRoot: a root replaced between New and the measurement (here a
// link retargeted to another directory) is not measured, as the path may
// now lead to another filesystem.
func replacedRoot(t *testing.T, dir string) {
	first, second := filepath.Join(dir, "first"), filepath.Join(dir, "second")
	for _, d := range []string{first, second} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(dir, "root")
	if err := os.Symlink(first, link); err != nil {
		t.Skipf("no symbolic links: %v", err)
	}
	p, err := New(link, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, link); err != nil {
		t.Fatal(err)
	}
	if s := p.filesystemSpace(); s != nil {
		t.Fatalf("a retargeted root measured %+v", s)
	}
}

// symlinkFileRoot lists a link, in dir, to a file on another filesystem
// (Linux's /dev/shm): the link is the row, so the space is dir's.
func symlinkFileRoot(t *testing.T, dir string) {
	if runtime.GOOS != "linux" {
		t.Skip("needs Linux's /dev/shm")
	}
	target, err := os.CreateTemp("/dev/shm", "swath-space-*")
	if err != nil {
		t.Skipf("no /dev/shm: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(target.Name()) })
	_ = target.Close()
	here, there := measureSpace(dir, false), measureSpace(target.Name(), true)
	if here == nil || there == nil || here.TotalBytes == there.TotalBytes {
		t.Skipf("/dev/shm is not told apart from %s by its size", dir)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target.Name(), link); err != nil {
		t.Fatal(err)
	}

	p, err := New(link, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if s := p.filesystemSpace(); s == nil || s.TotalBytes != here.TotalBytes {
		t.Fatalf("a link to a file on another filesystem measured %+v, want the link's filesystem (%d bytes), not the target's (%d)", s, here.TotalBytes, there.TotalBytes)
	}
}
