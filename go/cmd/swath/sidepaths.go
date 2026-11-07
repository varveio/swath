// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

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
		if fi, lerr := os.Lstat(dest); err == nil && side.what != "--report" && lerr == nil && fi.Mode()&os.ModeSymlink != 0 {
			// A profile is opened, which would follow the link to wherever
			// it points by then (createNoFollow refuses it there too); the
			// report is renamed over its name, which replaces a link.
			err = fmt.Errorf("is a symbolic link: a profile is not written through one")
		}
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
		if samePlace(p, td) || samePlace(p, tt) {
			return fmt.Errorf("is the text output file")
		}
	}
	out := ""
	if o.text == nil && !o.countOnly && o.out != "" {
		out = resolvePath(o.out)
	}
	// The output and its entries are found by identity (samePlace), not by
	// spelling: on a case-insensitive filesystem base/out is base/OUT.
	isOut := func(d string) bool { return out != "" && samePlace(d, out) }
	var below []string // the names of p below the output, if p is in it
	for d := p; ; d = filepath.Dir(d) {
		if isOut(d) {
			switch {
			case len(below) == 0:
				return fmt.Errorf("is the output directory")
			case len(below) > 1:
				return fmt.Errorf("is inside the output's %s: only an ordinary name directly in the output directory may be written there", below[0])
			case dataset.Reserved(below[0]):
				return fmt.Errorf("is the output's own %s", below[0])
			}
			break
		}
		if filepath.Dir(d) == d {
			break
		}
		below = append([]string{filepath.Base(d)}, below...)
	}
	for d := filepath.Dir(p); ; d = filepath.Dir(d) {
		if !isOut(d) && dataset.Managed(d) {
			return fmt.Errorf("is inside %s, a dataset or run of this tool that this run does not own", d)
		}
		if filepath.Dir(d) == d {
			return nil
		}
	}
}

// samePlace reports whether a and b (resolved, absolute) name one place:
// the same path; the same existing file by identity (a hard link, or a
// case-insensitive filesystem's other spelling); or, below the same
// existing directory, the same names yet to be created, compared without
// case where that directory ignores it (foldsCase). A symbolic link is
// compared as itself, not its target: a rename replaces the link.
func samePlace(a, b string) bool {
	if a == b {
		return true
	}
	pa, ra := existingPrefix(a)
	pb, rb := existingPrefix(b)
	if len(ra) != len(rb) {
		return false
	}
	if pa != pb {
		fa, ea := os.Lstat(pa)
		fb, eb := os.Lstat(pb)
		if ea != nil || eb != nil || !os.SameFile(fa, fb) {
			return false
		}
	}
	fold := len(ra) > 0 && foldsCase(pa)
	for i := range ra {
		if ra[i] != rb[i] && !(fold && strings.EqualFold(ra[i], rb[i])) {
			return false
		}
	}
	return true
}

// existingPrefix splits p into its longest existing prefix and the names
// below it.
func existingPrefix(p string) (string, []string) {
	var rest []string
	for {
		if _, err := os.Lstat(p); err == nil {
			return p, rest
		}
		parent := filepath.Dir(p)
		if parent == p {
			return p, rest
		}
		rest = append([]string{filepath.Base(p)}, rest...)
		p = parent
	}
}

// foldsCase reports whether dir looks names up without case. A variable
// for tests (caseFS is the filesystem it asks).
var foldsCase = func(dir string) bool { return caseFS.folds(dir) }

// caseProbe is the filesystem the case probe asks: the names in a
// directory (at most n), an entry by path (Lstat), and whether two entries
// are one file.
type caseProbe struct {
	names func(dir string, n int) []string
	lstat func(path string) (os.FileInfo, error)
	same  func(a, b os.FileInfo) bool
}

var caseFS = caseProbe{
	names: func(dir string, n int) []string {
		f, err := os.Open(dir)
		if err != nil {
			return nil
		}
		defer f.Close()
		names, _ := f.Readdirnames(n)
		return names
	},
	lstat: os.Lstat,
	same:  os.SameFile,
}

// maxProbeNames bounds the names the probe reads from one directory, and
// maxProbeCandidates the entries it examines for a directory to try.
const (
	maxProbeNames      = 1 << 16
	maxProbeCandidates = 256
)

// folds probes inside dir itself, so the answer is dir's filesystem's
// even at a mount point (a case-insensitive volume at /mnt/Drive under a
// case-sensitive /mnt): an entry with a letter, respelled in the other
// case, must reach the same file, and the listing must not hold that
// other spelling as an entry of its own (a hard link A beside a is listed
// under both names; a case-folded alias is not). A directory entry is
// preferred: directories cannot be hard-linked, so for one the listing
// check is redundant. Where the listing is too long to read whole, a file
// entry's other spelling past the read counts as unlisted: true, the
// answer that refuses. With no entry to try (an empty directory, or names
// without letters) it falls back to dir's own name in its parent
// (parentFolds); a mount point at an empty directory is the limit that
// remains.
func (c caseProbe) folds(dir string) bool {
	names := c.names(dir, maxProbeNames)
	listed := make(map[string]bool, len(names))
	for _, n := range names {
		listed[n] = true
	}
	var file string
	for _, n := range names[:min(len(names), maxProbeCandidates)] {
		other := swapCase(n)
		if other == n {
			continue
		}
		fa, err := c.lstat(filepath.Join(dir, n))
		if err != nil {
			continue
		}
		if fa.IsDir() {
			fb, err := c.lstat(filepath.Join(dir, other))
			return err == nil && c.same(fa, fb)
		}
		if file == "" {
			file = n
		}
	}
	if file != "" {
		other := swapCase(file)
		fa, ea := c.lstat(filepath.Join(dir, file))
		fb, eb := c.lstat(filepath.Join(dir, other))
		if ea != nil || eb != nil || !c.same(fa, fb) {
			return false
		}
		return !listed[other]
	}
	return c.parentFolds(dir)
}

// parentFolds: dir's own name, respelled in the other case, reaches dir in
// its parent (the parent's lookup rules, which are dir's unless dir is a
// mount point); a name without letters defers to the nearest ancestor's,
// and with none to try the answer is true, the answer that refuses.
func (c caseProbe) parentFolds(dir string) bool {
	for d := dir; ; d = filepath.Dir(d) {
		parent, name := filepath.Dir(d), filepath.Base(d)
		if parent == d {
			return true
		}
		other := swapCase(name)
		if other == name {
			continue
		}
		fa, ea := c.lstat(d)
		if ea != nil || !fa.IsDir() {
			continue
		}
		fb, eb := c.lstat(filepath.Join(parent, other))
		return eb == nil && c.same(fa, fb)
	}
}

// swapCase is name with each letter's case swapped.
func swapCase(name string) string {
	return strings.Map(func(r rune) rune {
		if u := unicode.ToUpper(r); u != r {
			return u
		}
		return unicode.ToLower(r)
	}, name)
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

// maxLinks bounds the symbolic links resolvePath follows (Linux's
// MAXSYMLINKS is 40); past it an open fails, and the path is checked as
// far as it was resolved.
const maxLinks = 40

// resolvePath is what an open of path reaches, walked component by
// component as the kernel does: a symbolic link is read and its target
// walked in its place, also when the target does not exist yet (a
// dangling link to OUT/manifest.json reaches OUT/manifest.json once the
// run publishes it), a ".." climbs from what is resolved so far, and the
// names below the first missing one are joined lexically.
func resolvePath(path string) string {
	raw := absRaw(path)
	vol := filepath.VolumeName(raw)
	cur := vol + string(filepath.Separator)
	todo := splitPath(raw[len(vol):])
	for links := 0; len(todo) > 0; {
		c := todo[0]
		todo = todo[1:]
		switch c {
		case "", ".":
			continue
		case "..":
			cur = filepath.Dir(cur)
			continue
		}
		next := filepath.Join(cur, c)
		fi, err := os.Lstat(next)
		if err != nil {
			return filepath.Join(append([]string{canonical(cur), c}, todo...)...)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			cur = next
			continue
		}
		target, err := os.Readlink(next)
		if links++; err != nil || links > maxLinks {
			return filepath.Join(append([]string{canonical(cur), c}, todo...)...)
		}
		if tv := filepath.VolumeName(target); filepath.IsAbs(target) {
			cur = tv + string(filepath.Separator)
			target = target[len(tv):]
		} else if tv != "" {
			return filepath.Join(append([]string{canonical(cur), c}, todo...)...) // a volume-relative link: not followed here
		}
		todo = append(splitPath(target), todo...)
	}
	return canonical(cur)
}

// canonical is dir, an existing directory resolved so far, as
// filepath.EvalSymlinks names it: the same on Unix; on Windows also through
// what Lstat does not report as a link (a junction, a short name).
func canonical(dir string) string {
	if r, err := filepath.EvalSymlinks(dir); err == nil {
		return r
	}
	return dir
}

// splitPath is path's names, split at every separator.
func splitPath(path string) []string {
	return strings.FieldsFunc(path, func(r rune) bool { return strings.ContainsRune(seps, r) })
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
