// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
	"github.com/varveio/swath/tools/swath-local-spike/sortout"
)

func mkTree(t *testing.T, base string, files ...string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(base, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func mustExist(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected %s to survive: %v", p, err)
		}
	}
}

func datasetKeys(t *testing.T, out string) []string {
	t.Helper()
	parts, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
	var keys []string
	for _, p := range parts {
		rd, err := pqout.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]record.Row, 0, 256)
		for {
			buf, err = rd.Read(buf)
			for _, r := range buf {
				keys = append(keys, r.Key)
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		rd.Close()
	}
	return keys
}

// Review H1: --overwrite must never delete the listed tree or foreign content.
func TestOverwriteSafety(t *testing.T) {
	T := t.TempDir()
	src := filepath.Join(T, "src")
	mkTree(t, src, "keep/file.txt", "b.txt")
	keep := filepath.Join(src, "keep", "file.txt")

	// --out is an ancestor of the root.
	if err := list([]string{src, "--out", T, "--overwrite"}); err == nil {
		t.Fatal("--out ancestor of root with --overwrite must fail")
	}
	mustExist(t, keep, filepath.Join(src, "b.txt"))
	// --out is the root itself.
	if err := list([]string{src, "--out", src, "--overwrite"}); err == nil {
		t.Fatal("--out == root must fail")
	}
	mustExist(t, keep)
	// --out is inside the root: --overwrite refused (existing or not).
	if err := list([]string{src, "--out", filepath.Join(src, "out"), "--overwrite"}); err == nil {
		t.Fatal("--out inside root with --overwrite must fail")
	}
	mkTree(t, src, "out2/data/x.parquet", "out2/_SUCCESS")
	if err := list([]string{src, "--out", filepath.Join(src, "out2"), "--overwrite"}); err == nil {
		t.Fatal("--out (a dataset) inside root with --overwrite must fail")
	}
	mustExist(t, keep, filepath.Join(src, "out2", "data", "x.parquet"))
	// Same identity through a symlinked alias of an ancestor.
	if runtime.GOOS != "windows" {
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(T, alias); err != nil {
			t.Fatal(err)
		}
		if err := list([]string{filepath.Join(alias, "src"), "--out", T, "--overwrite"}); err == nil {
			t.Fatal("--out ancestor (via alias) must fail")
		}
		mustExist(t, keep)
	}
	// A non-empty --out that is not a swath-local dataset is refused, untouched.
	foreign := filepath.Join(T, "foreign")
	mkTree(t, foreign, "precious.txt")
	if err := list([]string{src, "--out", foreign, "--overwrite"}); err == nil {
		t.Fatal("overwrite of a foreign directory must fail")
	}
	mustExist(t, filepath.Join(foreign, "precious.txt"))
	// A previous dataset is replaced; unrelated entries beside it survive.
	ds := filepath.Join(T, "ds")
	if err := list([]string{src, "--out", ds}); err != nil {
		t.Fatal(err)
	}
	mkTree(t, ds, "notes.txt")
	if err := list([]string{src, "--out", ds}); err == nil {
		t.Fatal("non-empty --out without --overwrite must fail")
	}
	if err := list([]string{src, "--out", ds, "--overwrite"}); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(ds, "notes.txt"), filepath.Join(ds, "_SUCCESS"), filepath.Join(ds, sortout.SummaryName))
	// A missing root fails before --out is created.
	missingOut := filepath.Join(T, "never")
	if err := list([]string{filepath.Join(T, "nope"), "--out", missingOut}); err == nil {
		t.Fatal("missing root must fail")
	}
	if _, err := os.Stat(missingOut); !os.IsNotExist(err) {
		t.Fatal("--out was created for a missing root")
	}
}

// Review M2: an --out inside the root is skipped by identity: no rows for it,
// even while spill runs are being written there (all engines and orders).
func TestOutInsideRootIsSkipped(t *testing.T) {
	engines := []string{"parallel-lstat", "portable", "walkdir"}
	if runtime.GOOS == "linux" {
		engines = append(engines, "getdents", "fstatat")
	}
	for _, eng := range engines {
		for _, order := range []string{"none", "dfs-sequential", "dfs-parallel"} {
			if eng == "walkdir" && order != "none" {
				continue
			}
			src := t.TempDir()
			mkTree(t, src, "a/1", "a/2", "b", "z/y/x")
			out := filepath.Join(src, "a", "out")
			if err := list([]string{src, "--out", out, "--engine", eng, "--order", order, "--sort-buffer-rows", "2"}); err != nil {
				t.Fatalf("%s/%s: %v", eng, order, err)
			}
			keys := datasetKeys(t, out)
			want := []string{"a/", "a/1", "a/2", "b", "z/", "z/y/", "z/y/x"}
			if strings.Join(keys, ",") != strings.Join(want, ",") {
				t.Fatalf("%s/%s: keys %q, want %q", eng, order, keys, want)
			}
		}
	}
}

// Review L1: --split-entries 1 used to loop forever (chunk 0).
func TestSplitEntriesValidation(t *testing.T) {
	src := t.TempDir()
	mkTree(t, src, "d/1", "d/2", "d/3", "d/4", "d/5")
	if err := list([]string{src, "--count-only", "--split-entries", "1"}); err == nil {
		t.Fatal("--split-entries 1 must be rejected")
	}
	if err := list([]string{src, "--count-only", "--split-entries", "-3"}); err == nil {
		t.Fatal("negative --split-entries must be rejected")
	}
	out := filepath.Join(t.TempDir(), "o")
	if err := list([]string{src, "--out", out, "--split-entries", "2"}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(datasetKeys(t, out), ","); got != "d/,d/1,d/2,d/3,d/4,d/5" {
		t.Fatalf("split=2 keys %q", got)
	}
}
