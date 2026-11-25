// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/varveio/swath/reader"
)

// junction makes a junction at link to the directory target: mklink /J
// needs no privilege.
func junction(t *testing.T, link, target string) {
	t.Helper()
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput(); err != nil {
		t.Fatalf("mklink /J %s %s: %v: %s", link, target, err, out)
	}
}

// volumeMount mounts the volume that holds dir at the empty directory at,
// and unmounts it when the test ends. It needs an administrator (as the CI
// runner is); elsewhere it skips, reporting false.
func volumeMount(t *testing.T, at, dir string) bool {
	t.Helper()
	root, err := windows.UTF16PtrFromString(filepath.VolumeName(dir) + `\`)
	if err != nil {
		t.Fatal(err)
	}
	guid := make([]uint16, windows.MAX_PATH)
	if err := windows.GetVolumeNameForVolumeMountPoint(root, &guid[0], uint32(len(guid))); err != nil {
		t.Logf("no volume name for %s: %v", dir, err)
		return false
	}
	if err := os.Mkdir(at, 0o755); err != nil {
		t.Fatal(err)
	}
	mp, err := windows.UTF16PtrFromString(at + `\`)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetVolumeMountPoint(mp, &guid[0]); err != nil {
		t.Logf("cannot mount a volume (not an administrator?): %v", err)
		if err := os.Remove(at); err != nil { // else listed as m/
			t.Fatal(err)
		}
		return false
	}
	// Before t.TempDir's own cleanup, which runs later.
	t.Cleanup(func() {
		if err := windows.DeleteVolumeMountPoint(mp); err != nil {
			t.Errorf("unmount %s: %v", at, err)
		}
	})
	return true
}

// TestJunctionAndMountPoint: a junction, to a directory outside the tree
// or inside it, and a volume mount point list as kind symlink with their
// targets, at every tier (no target at the names tier, nor without
// LinkTarget), and the walk enters none, with or without OneFilesystem,
// which Windows accepts: the coverage records the filter and the report
// names the mechanism. Before, each was kind unknown, and OneFilesystem
// was refused for want of a device.
func TestJunctionAndMountPoint(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "a/x", false)
	mk(t, root, "z", false)
	outside := t.TempDir()
	mk(t, outside, "inner", false)
	links := map[string]string{"j": outside, "k": filepath.Join(root, "a")}
	junction(t, filepath.Join(root, "j"), links["j"])
	junction(t, filepath.Join(root, "k"), links["k"])
	want := []string{"a/", "a/x", "j", "k", "z"}
	if volumeMount(t, filepath.Join(root, "m"), root) {
		links["m"] = filepath.VolumeName(root) + `\`
		want = []string{"a/", "a/x", "j", "k", "m", "z"}
	}
	for _, opt := range []Options{
		{LinkTarget: true},
		{LinkTarget: true, OneFilesystem: true},
		{OneFilesystem: true},
		{Fields: Names},
	} {
		p, err := New(root, opt)
		if err != nil {
			t.Fatalf("%+v: %v", opt, err)
		}
		rows, st := walk(t, p, 4, 1<<12)
		var keys []string
		for _, r := range rows {
			keys = append(keys, r.Key)
			target, ok := links[r.Key]
			if !ok {
				continue
			}
			if r.Data.Kind != KindSymlink {
				t.Errorf("%+v: %s is %v, want symlink", opt, r.Key, r.Data.Kind)
			}
			if !opt.LinkTarget || opt.Fields == Names {
				if r.Data.Has(HasLink) {
					t.Errorf("%+v: %s has a target %q", opt, r.Key, r.Data.LinkTarget)
				}
				continue
			}
			if !r.Data.Has(HasLink) {
				t.Errorf("%+v: %s has no target", opt, r.Key)
				continue
			}
			// The target as stored: a path, or the volume's \\?\Volume{GUID}\,
			// each the same directory as the one linked to.
			got, err1 := os.Stat(filepath.FromSlash(r.Data.LinkTarget))
			exp, err2 := os.Stat(target)
			if err1 != nil || err2 != nil || !os.SameFile(got, exp) {
				t.Errorf("%+v: %s's target %q is not %s (%v, %v)", opt, r.Key, r.Data.LinkTarget, target, err1, err2)
			}
		}
		if !slices.Equal(keys, want) {
			t.Errorf("%+v: keys %q, want %q", opt, keys, want)
		}
		if st.Errors() != 0 || st.OtherDevice != 0 {
			t.Errorf("%+v: errors %d (%q), other_device_dirs %d", opt, st.Errors(), st.Samples, st.OtherDevice)
		}
		var wantFilters []reader.Filter
		wantBoundary := ""
		if opt.OneFilesystem {
			wantFilters, wantBoundary = []reader.Filter{oneFilesystemFilter}, "reparse_point"
		}
		if got := (&Opened{p: p}).Report().Coverage.Filters; !slices.Equal(got, wantFilters) || st.FilesystemBoundary != wantBoundary {
			t.Errorf("%+v: filters %q, filesystem_boundary %q", opt, got, st.FilesystemBoundary)
		}
	}
}
