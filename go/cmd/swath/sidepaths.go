// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/varveio/swath/go/internal/dataset"
)

// checkSidePaths refuses, before the source is opened, a file the command
// writes beside its output (--report, the SWATH_CPUPROFILE and
// SWATH_MEMPROFILE profiles) where it would replace managed output: the
// text output file; inside the dataset directory anything but an ordinary
// name directly in it (not data/, the manifest, run.json, error.json, the
// .swath workspace with its lock and journal, nor an earlier layout's
// entry); and anything inside another directory this tool manages (a
// dataset or an interrupted run, whose lock this run does not hold). Paths
// are compared cleaned and absolute, after resolving the symbolic links of
// the part that exists.
func checkSidePaths(o options) error {
	for _, side := range []struct{ what, path string }{
		{"--report", o.report},
		{"SWATH_CPUPROFILE", os.Getenv("SWATH_CPUPROFILE")},
		{"SWATH_MEMPROFILE", os.Getenv("SWATH_MEMPROFILE")},
	} {
		if side.path == "" {
			continue
		}
		if err := checkSidePath(o, side.path); err != nil {
			return fmt.Errorf("%s %s: %w", side.what, side.path, err)
		}
	}
	return nil
}

func checkSidePath(o options, path string) error {
	p := resolvePath(path)
	if o.text != nil && o.text.Path != "" && samePath(p, resolvePath(o.text.Path)) {
		return fmt.Errorf("is the text output file")
	}
	out := ""
	if o.text == nil && !o.countOnly && o.out != "" {
		out = resolvePath(o.out)
	}
	if out != "" {
		if rel, err := filepath.Rel(out, p); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			switch {
			case rel == ".":
				return fmt.Errorf("is the output directory")
			case strings.ContainsRune(rel, filepath.Separator):
				return fmt.Errorf("is inside the output's %s: only an ordinary name directly in the output directory may be written there", strings.SplitN(rel, string(filepath.Separator), 2)[0])
			case dataset.Reserved(rel):
				return fmt.Errorf("is the output's own %s", rel)
			}
		}
	}
	for d := filepath.Dir(p); ; d = filepath.Dir(d) {
		if d != out && dataset.Managed(d) {
			return fmt.Errorf("is inside %s, a dataset or run of this tool that this run does not own", d)
		}
		if filepath.Dir(d) == d {
			return nil
		}
	}
}

// samePath reports whether a and b (resolved) name one file: the same
// path, or, when both exist, the same file by identity (a hard link, or a
// case-insensitive filesystem's other spelling).
func samePath(a, b string) bool {
	if a == b {
		return true
	}
	fa, ea := os.Stat(a)
	fb, eb := os.Stat(b)
	return ea == nil && eb == nil && os.SameFile(fa, fb)
}

// resolvePath is path absolute and cleaned, with the symbolic links of its
// longest existing prefix resolved: what a later open of path would reach.
func resolvePath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	rest := ""
	for cur := abs; ; {
		if r, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(r, rest)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return abs
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}
