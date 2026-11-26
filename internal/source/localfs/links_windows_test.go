// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"maps"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// linksTree builds, below a new directory, a file a/f with a second name
// b/f-link (a hard link), grown by 1 MiB through a/f after the link was
// made, which leaves b/f-link's directory entry behind
// (docs/internals/measurements.md); a file c; a symbolic link s to a/f; a
// junction j to a/; and an empty directory e/. It returns the directory
// and its volume's serial number.
func linksTree(t *testing.T) (string, uint32) {
	t.Helper()
	root := t.TempDir()
	mk(t, root, "a", true)
	mk(t, root, "b", true)
	mk(t, root, "e", true)
	mk(t, root, "c", false)
	f := filepath.Join(root, "a", "f")
	writeFile(t, f, compressible(10000))
	if err := os.Link(f, filepath.Join(root, "b", "f-link")); err != nil {
		t.Fatal(err)
	}
	w, err := os.OpenFile(f, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(f, filepath.Join(root, "s")); err != nil {
		t.Fatal(err)
	}
	junction(t, filepath.Join(root, "j"), filepath.Join(root, "a"))
	vp, err := windows.UTF16PtrFromString(filepath.VolumeName(root) + `\`)
	if err != nil {
		t.Fatal(err)
	}
	var serial uint32
	if err := windows.GetVolumeInformation(vp, nil, 0, &serial, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	return root, serial
}

// TestLinksTier: at the links tier a Windows listing reads each entry
// through a handle of its own. Both names of a hard-linked file have nlink
// 2, one dev (the volume's serial number) and one inode, and the length and
// space on disk a handle to the file gives, though the file grew through
// one name after the other was made; every other entry has nlink 1 and the
// same dev. A symbolic link and a junction are read as themselves, never
// followed. --one-file-system stops no directory for its dev. The stat tier
// opens no file (Handles.Files 0) and leaves nlink and dev null, and the
// names tier opens nothing.
func TestLinksTier(t *testing.T) {
	root, serial := linksTree(t)
	const grown = 10000 + 1<<20
	for _, onefs := range []bool{false, true} {
		rows, st := list(t, root, Options{Fields: Links, LinkTarget: true, OneFilesystem: onefs}, 4, 1<<12)
		if st.Errors() != 0 || st.OtherDevice != 0 {
			t.Fatalf("onefs=%v: errors %d, other devices %d: %q", onefs, st.Errors(), st.OtherDevice, st.Samples)
		}
		by := map[string]Entry{}
		for _, r := range rows {
			by[r.Key] = r.Data
			s, idx, err := handleInfo(filepath.Join(root, filepath.FromSlash(r.Key)))
			if err != nil {
				t.Fatal(err)
			}
			d := r.Data
			if !d.Has(HasNlink|HasDev|HasInode|HasBlocks) || d.Dev != int64(serial) || d.Nlink != s.NumberOfLinks || d.Inode != idx ||
				d.Blocks != s.AllocationSize || d.Kind != KindDir && d.Size != s.EndOfFile {
				t.Errorf("onefs=%v: %s: nlink %d dev %d inode %d allocated %d size %d (present %b); the handle's %d, %d, %d, %d, %d",
					onefs, r.Key, d.Nlink, d.Dev, d.Inode, d.Blocks, d.Size, d.Present, s.NumberOfLinks, serial, idx, s.AllocationSize, s.EndOfFile)
			}
		}
		want := map[string]uint32{"a/": 1, "a/f": 2, "b/": 1, "b/f-link": 2, "c": 1, "e/": 1, "j": 1, "s": 1}
		if len(rows) != len(want) {
			t.Fatalf("onefs=%v: %d rows, want %d", onefs, len(rows), len(want))
		}
		for k, n := range want {
			if e, ok := by[k]; !ok || e.Nlink != n {
				t.Errorf("onefs=%v: %s: nlink %d (listed %v), want %d", onefs, k, e.Nlink, ok, n)
			}
		}
		if a, b := by["a/f"], by["b/f-link"]; a.Inode != b.Inode || a.Size != grown || b.Size != grown || a.Blocks != b.Blocks {
			t.Errorf("onefs=%v: a/f inode %d size %d allocated %d, b/f-link %d %d %d", onefs, a.Inode, a.Size, a.Blocks, b.Inode, b.Size, b.Blocks)
		}
		if by["s"].Kind != KindSymlink || by["j"].Kind != KindSymlink {
			t.Errorf("onefs=%v: s %v, j %v", onefs, by["s"].Kind, by["j"].Kind)
		}
		if st.Handles.Files != 5 || st.Handles.Dirs != 3 || len(st.Handles.Failed) != 0 {
			t.Errorf("onefs=%v: handles %+v, want 5 files and 3 directories", onefs, st.Handles)
		}
	}

	rows, st := list(t, root, Options{LinkTarget: true}, 4, 1<<12)
	if st.Handles.Files != 0 || st.Handles.Dirs != 3 {
		t.Errorf("stat tier: handles %+v, want no file and 3 directories", st.Handles)
	}
	for _, r := range rows {
		if r.Data.Has(HasNlink) || r.Data.Has(HasDev) {
			t.Errorf("stat tier: %s: nlink %d dev %d (present %b)", r.Key, r.Data.Nlink, r.Data.Dev, r.Data.Present)
		}
	}
	if _, st := list(t, root, Options{Fields: Names}, 4, 1<<12); st.Handles.Files != 0 || st.Handles.Dirs != 0 {
		t.Errorf("names tier: handles %+v", st.Handles)
	}
	if !LinksRead(Links) || LinksRead(Stat) || LinksRead(Names) || !BlocksRead(Links) || !BlocksMayLack(Links) {
		t.Errorf("LinksRead links %v stat %v names %v, BlocksRead %v, BlocksMayLack %v",
			LinksRead(Links), LinksRead(Stat), LinksRead(Names), BlocksRead(Links), BlocksMayLack(Links))
	}
}

// TestLinksTierFileRoot: a file root at the links tier has its link count,
// dev and current length too (fillRoot); at the stat tier it has neither.
func TestLinksTierFileRoot(t *testing.T) {
	root, serial := linksTree(t)
	path := filepath.Join(root, "b", "f-link")
	s, idx, err := handleInfo(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, st := list(t, path, Options{Fields: Links}, 1, 1<<12)
	if len(rows) != 1 || st.Errors() != 0 {
		t.Fatalf("rows %+v, errors %d", rows, st.Errors())
	}
	if d := rows[0].Data; !d.Has(HasNlink|HasDev|HasInode|HasBlocks) || d.Nlink != 2 || d.Dev != int64(serial) || d.Inode != idx ||
		d.Size != s.EndOfFile || d.Blocks != s.AllocationSize {
		t.Errorf("links tier: %+v, the handle's %d links, inode %d, %d bytes, %d allocated", d, s.NumberOfLinks, idx, s.EndOfFile, s.AllocationSize)
	}
	rows, _ = list(t, path, Options{}, 1, 1<<12)
	if d := rows[0].Data; d.Has(HasNlink) || d.Has(HasDev) {
		t.Errorf("stat tier: %+v", d)
	}
}

// TestLinksTierHandleFails: an entry the links tier cannot read through a
// handle (denied, or gone since the directory was listed), or whose handle
// finds a directory where the listing had a file, is listed with what the
// listing said of it, nlink and dev null (a directory's space on disk
// too), and counted by reason, not as an error; a directory is still
// walked, and the rest of the listing is read.
func TestLinksTierHandleFails(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "a/f", false)
	mk(t, root, "c", false)
	mk(t, root, "x", false)
	mk(t, root, "y", false)
	fail := map[string]error{
		filepath.Join(root, "c"): windows.ERROR_ACCESS_DENIED,
		filepath.Join(root, "a"): windows.ERROR_FILE_NOT_FOUND,
	}
	openHandle = func(path string, id bool) (handleRead, error) {
		if err, ok := fail[path]; ok {
			return handleRead{}, err
		}
		if path == filepath.Join(root, "x") {
			return handleRead{dir: true, links: 1}, nil
		}
		return readHandle(path, id)
	}
	defer func() { openHandle = readHandle }()
	rows, st := list(t, root, Options{Fields: Links, LinkTarget: true}, 2, 1<<12)
	if st.Errors() != 0 || len(st.Samples) != 0 {
		t.Fatalf("errors %d: %q", st.Errors(), st.Samples)
	}
	if want := map[string]int64{"permission": 1, "not_found": 1, "changed": 1}; !maps.Equal(st.Handles.Failed, want) {
		t.Errorf("failed %v, want %v", st.Handles.Failed, want)
	}
	by := map[string]Entry{}
	for _, r := range rows {
		by[r.Key] = r.Data
	}
	if len(rows) != 5 {
		t.Fatalf("%d rows: %v", len(rows), rows)
	}
	for _, k := range []string{"c", "x"} {
		if d := by[k]; d.Kind != KindFile || d.Size != 5 || !d.Has(HasSize|HasBlocks|HasInode) || d.Has(HasNlink) || d.Has(HasDev) {
			t.Errorf("%s: %+v, want the listing's values, nlink and dev null", k, d)
		}
	}
	if d := by["a/"]; d.Kind != KindDir || d.Has(HasBlocks) || d.Has(HasNlink) || d.Has(HasDev) {
		t.Errorf("a/: %+v, want no space on disk, nlink or dev", d)
	}
	for _, k := range []string{"a/f", "y"} {
		if d := by[k]; d.Nlink != 1 || !d.Has(HasNlink|HasDev) {
			t.Errorf("%s: %+v, want nlink 1 and dev", k, d)
		}
	}
}
