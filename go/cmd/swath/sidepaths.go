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
// dataset or an interrupted run, whose lock this run does not hold).
//
// Each path is resolved once (sideDest) and o then names the resolved
// destination: every writer (the report's, the profiles') writes there,
// the pathname that was checked, not a second resolution of the original.
func checkSidePaths(o *options) error {
	o.cpuProfile, o.memProfile = os.Getenv("SWATH_CPUPROFILE"), os.Getenv("SWATH_MEMPROFILE")
	for _, side := range []struct {
		what string
		path *string
	}{
		{"--report", &o.report},
		{"SWATH_CPUPROFILE", &o.cpuProfile},
		{"SWATH_MEMPROFILE", &o.memProfile},
	} {
		if *side.path == "" {
			continue
		}
		dest, err := checkSidePath(*o, *side.path)
		if err != nil {
			return fmt.Errorf("%s %s: %w", side.what, *side.path, err)
		}
		*side.path = dest
	}
	return nil
}

// checkSidePath checks path (checkSidePaths) and returns its destination.
// The destination keeps its last name as given (sideDest), so a reserved
// slot is refused by its name whatever a symbolic link there points at;
// where that name is such a link, what it reaches (a profile's open
// follows it) is checked too.
func checkSidePath(o options, path string) (string, error) {
	dest, through := sideDest(path)
	for _, p := range []string{dest, through} {
		if err := checkDest(o, p); err != nil {
			return "", err
		}
	}
	return dest, nil
}

func checkDest(o options, p string) error {
	if o.text != nil && o.text.Path != "" {
		td, tt := sideDest(o.text.Path)
		if samePath(p, td) || samePath(p, tt) {
			return fmt.Errorf("is the text output file")
		}
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

// seps are the path separators of this platform.
const seps = string(filepath.Separator) + "/"

// absRaw is path made absolute without cleaning it: filepath.Abs would
// apply a ".." before the symbolic link it follows is resolved, while an
// open climbs from the link's target.
func absRaw(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	if wd, err := os.Getwd(); err == nil {
		if abs := wd + string(filepath.Separator) + path; filepath.IsAbs(abs) {
			return abs
		}
	}
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
}

// resolvePath is what an open of path reaches: absolute, the symbolic
// links of its longest existing prefix resolved component by component
// (filepath.EvalSymlinks, which applies a ".." to the resolved target),
// and the part that does not exist yet joined lexically.
func resolvePath(path string) string {
	raw := absRaw(path)
	rest := ""
	for cur := raw; ; {
		if r, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(r, rest)
		}
		i := strings.LastIndexAny(cur, seps)
		if i < 0 {
			return filepath.Clean(raw)
		}
		rest = filepath.Join(cur[i+1:], rest)
		next := cur[:i]
		if i <= len(filepath.VolumeName(cur)) {
			next = cur[:i+1] // the root
		}
		if next == cur {
			return filepath.Clean(raw)
		}
		cur = next
	}
}

// sideDest is where a file at path is written by a writer that replaces
// it (a rename over its name): its directory resolved (resolvePath) and
// its last name as given, so a symbolic link of that name is replaced,
// not followed. through is path resolved whole: what a writer that opens
// the name reaches.
func sideDest(path string) (dest, through string) {
	raw := absRaw(path)
	trimmed := strings.TrimRight(raw, seps)
	i := strings.LastIndexAny(trimmed, seps)
	if i < 0 || trimmed[i+1:] == "." || trimmed[i+1:] == ".." || trimmed[i+1:] == "" {
		d := resolvePath(raw)
		return d, d
	}
	dest = filepath.Join(resolvePath(trimmed[:i+1]), trimmed[i+1:])
	return dest, resolvePath(dest)
}
