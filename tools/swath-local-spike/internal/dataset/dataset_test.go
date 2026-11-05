// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func part(i int, lo, hi string, n int64) Part {
	return Part{Path: "/x/data/" + PartName(i) + ".tmp", MinKey: lo, MaxKey: hi, Rows: n}
}

func TestCheck(t *testing.T) {
	if err := Check([]Part{part(0, "a", "c", 2), part(1, "d", "f", 3)}, 5); err != nil {
		t.Fatal(err)
	}
	for name, parts := range map[string][]Part{
		"overlap":  {part(0, "a", "d", 2), part(1, "d", "f", 3)},
		"gap":      {part(0, "a", "c", 2), part(2, "d", "f", 3)},
		"rowcount": {part(0, "a", "c", 2), part(1, "d", "f", 2)},
		"inverted": {part(0, "c", "a", 2), part(1, "d", "f", 3)},
	} {
		if err := Check(parts, 5); err == nil {
			t.Errorf("%s: gate passed", name)
		}
	}
}

func touch(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s should survive: %v", p, err)
		}
	}
}

const id = "swath-local/v0"

func TestPrepareNeverDeletesTheTree(t *testing.T) {
	T := t.TempDir()
	src := filepath.Join(T, "src")
	keep := filepath.Join(src, "keep", "file.txt")
	touch(t, keep)
	for name, tc := range map[string]struct {
		out     string
		replace bool
		want    error
	}{
		"ancestor":            {T, true, ErrOutIsRoot},
		"root":                {src, true, ErrOutIsRoot},
		"inside, new":         {filepath.Join(src, "out"), true, ErrInsideRoot},
		"inside, not empty":   {filepath.Join(src, "keep"), false, ErrNotEmpty},
		"foreign, replace":    {filepath.Join(T, "foreign"), true, ErrNotDataset},
		"foreign, no replace": {filepath.Join(T, "foreign"), false, ErrNotEmpty},
	} {
		touch(t, filepath.Join(T, "foreign", "precious.txt"))
		err := Prepare(tc.out, src, id, tc.replace)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
		exists(t, keep, filepath.Join(T, "foreign", "precious.txt"))
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(T, alias); err == nil {
		if err := Prepare(T, filepath.Join(alias, "src"), id, true); !errors.Is(err, ErrOutIsRoot) {
			t.Errorf("ancestor via alias: %v", err)
		}
		exists(t, keep)
	}
}

func TestReplaceRemovesOnlyOwnEntries(t *testing.T) {
	out := filepath.Join(t.TempDir(), "ds")
	for _, f := range []string{"data/part-00000.parquet", ManifestName, SuccessName, SymlinkName, "notes.txt"} {
		touch(t, filepath.Join(out, f))
	}
	if err := Prepare(out, t.TempDir(), id, true); err != nil {
		t.Fatal(err)
	}
	exists(t, filepath.Join(out, "notes.txt"), filepath.Join(out, DataDir))
	for _, gone := range []string{"data/part-00000.parquet", ManifestName, SuccessName} {
		if _, err := os.Stat(filepath.Join(out, gone)); !os.IsNotExist(err) {
			t.Errorf("%s survived", gone)
		}
	}
}

func TestPublishFailureLeavesNoSuccess(t *testing.T) {
	out := t.TempDir()
	touch(t, TempPath(out, 0))
	// A non-empty directory where manifest.json goes makes the commit fail.
	touch(t, filepath.Join(out, ManifestName, "blocker"))
	err := Publish(out, Meta{SchemaID: id}, []Part{{Path: TempPath(out, 0), Rows: 1, MinKey: "a", MaxKey: "a"}}, map[string]int{})
	if err == nil {
		t.Fatal("publish succeeded")
	}
	if _, err := os.Stat(filepath.Join(out, SuccessName)); !os.IsNotExist(err) {
		t.Fatal("_SUCCESS written after a failed publish")
	}
}
