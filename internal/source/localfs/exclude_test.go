// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

// TestExcludeResolvesTheParent: a text output's file is left out by its
// name in the directory a write through its path lands in, not the one
// its spelling cleans to. On POSIX a symbolic link followed by ".." is
// the parent of the link's target: TREE/link/../rows.jsonl, with link
// pointing outside the tree, is written outside and leaves the user's
// TREE/rows.jsonl listed; OUTSIDE/back/../rows.jsonl, with back pointing
// into the tree, is written there and left out. Windows removes ".." by
// the spelling first, so there the same paths land beside the links. The
// want is where a write through the path lands, checked against the
// platform's rule.
func TestExcludeResolvesTheParent(t *testing.T) {
	sep := string(filepath.Separator)
	for _, c := range []struct {
		name  string
		path  func(base string) string
		posix []string // the key on POSIX; Windows' is windows
		win   []string
	}{
		{"link out and back", func(b string) string { return b + sep + "TREE" + sep + "link" + sep + ".." + sep + "rows.jsonl" }, nil, []string{"rows.jsonl"}},
		{"link in and back", func(b string) string { return b + sep + "OUTSIDE" + sep + "back" + sep + ".." + sep + "rows.jsonl" }, []string{"sub/rows.jsonl"}, nil},
		{"plain", func(b string) string { return b + sep + "TREE" + sep + "rows.jsonl" }, []string{"rows.jsonl"}, []string{"rows.jsonl"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "TREE")
			for _, d := range []string{filepath.Join(root, "sub", "deep"), filepath.Join(base, "OUTSIDE", "child")} {
				if err := os.MkdirAll(d, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(filepath.Join("..", "OUTSIDE", "child"), filepath.Join(root, "link")); err != nil {
				t.Skipf("no symbolic links here: %v", err)
			}
			if err := os.Symlink(filepath.Join("..", "TREE", "sub", "deep"), filepath.Join(base, "OUTSIDE", "back")); err != nil {
				t.Fatal(err)
			}
			want := c.posix
			if runtime.GOOS == "windows" {
				want = c.win
			}
			// Where a write through the path lands: the key want names.
			path := c.path(base)
			if err := os.WriteFile(path, []byte("probe"), 0o644); err != nil {
				t.Fatal(err)
			}
			var landed []string
			for _, k := range []string{"rows.jsonl", "sub/rows.jsonl"} {
				if b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(k))); err == nil && string(b) == "probe" {
					landed = append(landed, k)
				}
			}
			if !slices.Equal(landed, want) {
				t.Fatalf("%s: a write landed at %q in the tree; the test wants %q", path, landed, want)
			}
			for _, portable := range []bool{false, true} {
				o := pruneOpen(t, root, Options{Portable: portable}).(*opened)
				if err := o.Exclude(path); err != nil {
					t.Fatalf("%s: %v", path, err)
				}
				if got := slices.Sorted(maps.Keys(o.p.opt.Skip)); !slices.Equal(got, want) {
					t.Errorf("%s (portable %v): left out %q, want %q", path, portable, got, want)
				}
			}
		})
	}
}

// TestExcludeStoredSpelling: -o TREE/ROWS.jsonl over an existing
// TREE/rows.jsonl leaves the entry out under the spelling the listing
// emits where the directory ignores case (it is the same entry, by
// identity, and the only one); where it does not, the other spelling is
// another file, even a hard link to the same one, and stays listed.
func TestExcludeStoredSpelling(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "rows.jsonl"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := os.Lstat(filepath.Join(root, "ROWS.jsonl"))
	folds := err == nil
	if !folds {
		// Case-sensitive: a hard link under the other spelling is another
		// name, never taken for the destination's.
		if err := os.Link(filepath.Join(root, "rows.jsonl"), filepath.Join(root, "ROWS.jsonl")); err != nil {
			t.Skipf("no hard links here: %v", err)
		}
	}
	o := pruneOpen(t, root, Options{}).(*opened)
	if err := o.Exclude(filepath.Join(root, "ROWS.jsonl")); err != nil {
		t.Fatal(err)
	}
	want := []string{"ROWS.jsonl"}
	if folds {
		want = []string{"rows.jsonl"}
	}
	if got := slices.Sorted(maps.Keys(o.p.opt.Skip)); !slices.Equal(got, want) {
		t.Fatalf("case-insensitive %v: left out %q, want %q", folds, got, want)
	}
	// A name equal without case that does not exist yet is created by the
	// commit; the existing file of another spelling stays listed.
	o = pruneOpen(t, root, Options{}).(*opened)
	if err := o.Exclude(filepath.Join(root, "Rows.JSONL.new")); err != nil {
		t.Fatal(err)
	}
	if got := slices.Sorted(maps.Keys(o.p.opt.Skip)); !slices.Equal(got, []string{"Rows.JSONL.new"}) {
		t.Fatalf("a new name: left out %q", got)
	}
}
