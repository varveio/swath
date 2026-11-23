// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDarwinSystemDataBoundary(t *testing.T) {
	root, err := os.Open("/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := os.Open("/System/Volumes/Data")
	if err != nil {
		t.Skipf("System/Data volume group unavailable: %v", err)
	}
	defer data.Close()
	a, err := filesystemID(int(root.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	b, err := filesystemID(int(data.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Skip("System and Data do not expose distinct filesystem IDs")
	}
	o := mustOpenDir(t, root, Options{})
	for _, key := range []string{"System/Volumes/Data/", "Users/"} {
		names, err := keyNames(key, true)
		if err != nil {
			t.Fatal(err)
		}
		fd, _, err := o.p.openLevel(context.Background(), names)
		if err == nil {
			unix.Close(fd)
			t.Fatalf("entered Data through %q", key)
		}
		if !errors.Is(err, errOtherVolume) && !errors.Is(err, errOtherDevice) {
			t.Fatalf("%q: %v", key, err)
		}
	}
	selected := mustOpenDir(t, data, Options{})
	fd, _, err := selected.p.openLevel(context.Background(), nil)
	if err != nil {
		t.Fatalf("selected Data root rejected: %v", err)
	}
	unix.Close(fd)
	t.Logf("System fsid=%v, Data fsid=%v", a, b)
}

// A mounted HFS+ image checks f_fsid on a supported non-APFS filesystem.
func TestDarwinHFSBoundary(t *testing.T) {
	tool, err := exec.LookPath("hdiutil")
	if err != nil {
		t.Skip("hdiutil unavailable")
	}
	root := t.TempDir()
	mount := filepath.Join(root, "mounted")
	if err := os.Mkdir(mount, 0o755); err != nil {
		t.Fatal(err)
	}
	image := filepath.Join(t.TempDir(), "volume.sparseimage")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, tool, "create", "-size", "32m", "-fs", "HFS+", "-type", "SPARSE", "-volname", "swath-boundary-test", image).CombinedOutput(); err != nil {
		t.Skipf("HFS+ fixture unavailable: %v: %s", err, out)
	}
	if out, err := exec.CommandContext(ctx, tool, "attach", "-nobrowse", "-mountpoint", mount, image).CombinedOutput(); err != nil {
		t.Skipf("HFS+ attach unavailable: %v: %s", err, out)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if out, err := exec.CommandContext(ctx, tool, "detach", mount).CombinedOutput(); err != nil {
			t.Errorf("detach fixture: %v: %s", err, out)
		}
	})
	mk(t, root, "mounted/hidden", false)
	for _, portable := range []bool{false, true} {
		o, err := Open(root, Options{OneFilesystem: true, Portable: portable}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if keys := countKeys(t, o, 2); !slices.Equal(keys, []string{"mounted/"}) {
			t.Fatalf("keys %q", keys)
		}
		selected, err := Open(mount, Options{OneFilesystem: true, Portable: portable}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if keys := countKeys(t, selected, 2); !slices.Contains(keys, "hidden") {
			t.Fatalf("selected volume not scanned: %q", keys)
		}
	}
}

func BenchmarkDarwinFilesystemCheck(b *testing.B) {
	f, err := os.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	var s unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &s); err != nil {
		b.Fatal(err)
	}
	id, err := filesystemID(int(f.Fd()))
	if err != nil {
		b.Fatal(err)
	}
	p := &Provider{dev: int64(s.Dev), fsid: id}
	for _, volume := range []bool{false, true} {
		name := "device"
		if volume {
			name = "device_and_fsid"
		}
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				if err := unix.Fstat(int(f.Fd()), &s); err != nil {
					b.Fatal(err)
				}
				if volume {
					if err := p.checkFilesystem(int(f.Fd()), int64(s.Dev)); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// Record the added per-directory lookup cost on a warm, branching tree.
// Timings are evidence, not a threshold: shared CI runners are noisy.
func TestDarwinVolumeWalkCost(t *testing.T) {
	root := t.TempDir()
	for i := range 500 {
		mk(t, root, fmt.Sprintf("d%04d/a", i), false)
		mk(t, root, fmt.Sprintf("d%04d/b", i), false)
	}
	f := holdDir(t, root)
	id, err := filesystemID(int(f.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	old := readFilesystemID
	t.Cleanup(func() { readFilesystemID = old })
	for _, checked := range []bool{false, true} {
		readFilesystemID = func(int) ([2]int32, error) { return id, nil }
		if checked {
			readFilesystemID = old
		}
		var elapsed time.Duration
		for range 3 {
			start := time.Now()
			rows, _ := list(t, root, Options{OneFilesystem: true}, 4, 1<<20)
			elapsed += time.Since(start)
			if len(rows) != 1500 {
				t.Fatalf("volume_check=%v: rows=%d, want 1500", checked, len(rows))
			}
		}
		t.Logf("volume_check=%v: 500 dirs, 1000 files, 4 workers, 3 warm runs, mean=%s", checked, elapsed/3)
	}
}
