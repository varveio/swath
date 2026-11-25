// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"
)

// spaceTree builds, below a new directory, an ordinary file of 10,000
// bytes with a second name (a hard link), a sparse file of 64 MiB with 64
// KiB written in its middle, a compressed file of 4 MiB of repeated text,
// a read-only file, an empty directory, a directory of 600 files (whose
// index has outgrown its MFT record), and a junction; each file's data is
// flushed, so that compression and sparse allocation are done. It returns
// the directory.
func spaceTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "ordinary"), compressible(10000))
	if err := os.Link(filepath.Join(root, "ordinary"), filepath.Join(root, "ordinary-link")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "readonly"), compressible(300))
	if err := os.Chmod(filepath.Join(root, "readonly"), 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(root, "readonly"), 0o644) })
	for _, name := range []string{"sparse", "compressed"} {
		f, err := os.Create(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if name == "sparse" {
			if err := ioctl(f, windows.FSCTL_SET_SPARSE, nil); err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(64 << 20); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(compressible(64<<10), 32<<20); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := ioctl(f, windows.FSCTL_SET_COMPRESSION, []byte{1, 0}); err != nil { // COMPRESSION_FORMAT_DEFAULT
				t.Fatal(err)
			}
			if _, err := f.Write(compressible(4 << 20)); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 600 {
		mk(t, root, "many/f"+strconv.Itoa(i), false)
	}
	junction(t, filepath.Join(root, "j"), filepath.Join(root, "many"))
	return root
}

// TestReadDirIsReadDir: readDir gives each entry the name, kind, mode,
// length, times and attributes os.File.ReadDir gives it, with and without
// file IDs, and only adds what Go drops.
func TestReadDirIsReadDir(t *testing.T) {
	root := spaceTree(t)
	for _, ids := range []bool{true, false} {
		readDirFileIDs = ids
		f, err := os.Open(root)
		if err != nil {
			t.Fatal(err)
		}
		got, err := readDir(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		var gotNames, wantNames []string
		for _, d := range got {
			gotNames = append(gotNames, d.Name())
		}
		for _, d := range want {
			wantNames = append(wantNames, d.Name())
		}
		slices.Sort(gotNames)
		if !slices.Equal(gotNames, wantNames) { // os.ReadDir sorts by name
			t.Fatalf("ids=%v: names %q, want %q", ids, gotNames, wantNames)
		}
		byName := map[string]*winEntry{}
		for _, d := range got {
			byName[d.Name()] = d.(*winEntry)
		}
		for _, w := range want {
			g := byName[w.Name()]
			wi, err := w.Info()
			if err != nil {
				t.Fatal(err)
			}
			gi, _ := g.Info()
			wd := wi.Sys().(*syscall.Win32FileAttributeData)
			if g.Type() != w.Type() || g.IsDir() != w.IsDir() || gi.Mode() != wi.Mode() || gi.Size() != wi.Size() ||
				!gi.ModTime().Equal(wi.ModTime()) || g.info.Win32FileAttributeData != *wd {
				t.Errorf("ids=%v: %s: %v %v %d %v %+v, want %v %v %d %v %+v", ids, w.Name(), g.Type(), gi.Mode(), gi.Size(), gi.ModTime(), g.info.Win32FileAttributeData,
					w.Type(), wi.Mode(), wi.Size(), wi.ModTime(), *wd)
			}
			if (g.info.FileID != 0) != ids {
				t.Errorf("ids=%v: %s: file ID %d", ids, w.Name(), g.info.FileID)
			}
		}
	}
	readDirFileIDs = true
}

// TestAllocatedBytes: on Windows a stat tier fills allocated_bytes for
// every entry, files and directories, and inode with the file ID, each
// what a handle to the entry says once its data is on disk: a sparse and
// a compressed file take less than their length, and two names of one file
// have one ID. A file root's are read through a handle (fillRoot). On a
// filesystem without file IDs (FileFullDirectoryInfo) inode is null and
// the space on disk still read. The names tier reads neither. Before, both
// were null on Windows.
func TestAllocatedBytes(t *testing.T) {
	root := spaceTree(t)
	check := func(t *testing.T, ids bool) {
		rows, st := list(t, root, Options{LinkTarget: true}, 4, 1<<12)
		if st.Errors() != 0 {
			t.Fatalf("errors %d: %q", st.Errors(), st.Samples)
		}
		byKey := map[string]Entry{}
		for _, r := range rows {
			byKey[r.Key] = r.Data
			path := filepath.Join(root, filepath.FromSlash(r.Key))
			s, idx, err := byHandle(path)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Data.Has(HasBlocks) || r.Data.Blocks != s.AllocationSize {
				t.Errorf("%s: allocated %d (present %v), the handle's %d", r.Key, r.Data.Blocks, r.Data.Has(HasBlocks), s.AllocationSize)
			}
			if ids && (!r.Data.Has(HasInode) || r.Data.Inode != idx) || !ids && r.Data.Has(HasInode) {
				t.Errorf("ids=%v: %s: inode %d (present %v), the handle's %d", ids, r.Key, r.Data.Inode, r.Data.Has(HasInode), idx)
			}
		}
		if e := byKey["sparse"]; e.Blocks <= 0 || e.Blocks >= e.Size {
			t.Errorf("sparse: %d allocated of %d", e.Blocks, e.Size)
		}
		if e := byKey["compressed"]; e.Blocks <= 0 || e.Blocks >= e.Size {
			t.Errorf("compressed: %d allocated of %d", e.Blocks, e.Size)
		}
		if a, b := byKey["ordinary"], byKey["ordinary-link"]; ids && a.Inode != b.Inode {
			t.Errorf("one file, two IDs: %d, %d", a.Inode, b.Inode)
		}
		if _, ok := byKey["many/"]; !ok || len(rows) != 608 { // 8 entries and many/'s 600 files
			t.Errorf("%d rows, many/ among them: %v", len(rows), ok)
		}
	}
	t.Run("file IDs", func(t *testing.T) { check(t, true) })
	t.Run("no file IDs", func(t *testing.T) {
		readDirFileIDs = false
		defer func() { readDirFileIDs = true }()
		check(t, false)
	})
	t.Run("file root", func(t *testing.T) {
		for _, ids := range []bool{true, false} {
			readDirFileIDs = ids
			path := filepath.Join(root, "sparse")
			rows, _ := list(t, path, Options{}, 1, 1<<12)
			s, idx, err := byHandle(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || !rows[0].Data.Has(HasBlocks) || rows[0].Data.Blocks != s.AllocationSize ||
				ids != rows[0].Data.Has(HasInode) || ids && rows[0].Data.Inode != idx {
				t.Errorf("ids=%v: rows %+v, the handle's %d, %d", ids, rows, s.AllocationSize, idx)
			}
		}
		readDirFileIDs = true
	})
	t.Run("names", func(t *testing.T) {
		rows, _ := list(t, root, Options{Fields: Names}, 4, 1<<12)
		for _, r := range rows {
			if r.Data.Present != 0 {
				t.Errorf("%s: names tier filled %b", r.Key, r.Data.Present)
			}
		}
	})
	if !BlocksRead(Stat) || BlocksRead(Names) || LinksRead(Stat) {
		t.Errorf("BlocksRead stat %v names %v, LinksRead %v", BlocksRead(Stat), BlocksRead(Names), LinksRead(Stat))
	}
}
