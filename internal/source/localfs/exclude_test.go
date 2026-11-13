// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestExcludeResolvesTheParent: a text output's file is left out by its
// name in the directory the system resolves its path to, not the one its
// spelling cleans to: through a symbolic link followed by "..", the file
// lands beside the link's target. TREE/link/../rows.jsonl, with link
// pointing outside the tree, is written outside and leaves the user's
// TREE/rows.jsonl listed; OUTSIDE/back/../rows.jsonl, with back pointing
// into the tree, is written there and left out.
func TestExcludeResolvesTheParent(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "TREE")
	for _, d := range []string{filepath.Join(root, "sub", "deep"), filepath.Join(base, "OUTSIDE", "child")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "rows.jsonl"), []byte("the user's\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..", "OUTSIDE", "child"), filepath.Join(root, "link")); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	if err := os.Symlink(filepath.Join("..", "TREE", "sub", "deep"), filepath.Join(base, "OUTSIDE", "back")); err != nil {
		t.Fatal(err)
	}
	sep := string(filepath.Separator)
	for path, want := range map[string][]string{
		root + sep + "link" + sep + ".." + sep + "rows.jsonl":                   nil,
		base + sep + "OUTSIDE" + sep + "back" + sep + ".." + sep + "rows.jsonl": {"sub/rows.jsonl"},
		root + sep + "rows.jsonl":                                               {"rows.jsonl"},
	} {
		for _, portable := range []bool{false, true} {
			o := pruneOpen(t, root, Options{Portable: portable}).(*opened)
			if err := o.Exclude(path); err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if got := slices.Sorted(maps.Keys(o.p.opt.Skip)); !slices.Equal(got, want) {
				t.Errorf("%s (portable %v): left out %q, want %q", path, portable, got, want)
			}
		}
	}
}
