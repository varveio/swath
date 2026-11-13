// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/listing"
)

// exitOf is the exit code main gives err.
func exitOf(err error) int {
	var ee *exitError
	var u usageError
	switch {
	case err == nil:
		return exitCompleted
	case errors.As(err, &ee):
		return ee.code
	case errors.As(err, &u):
		return exitUsage
	}
	return exitFatal
}

// TestReportCannotReplaceManagedOutput (R8): --report naming the output's
// manifest, its journal or another managed entry, the text output file, or
// a path inside another dataset is refused with exit 2 before the source is
// opened, and what it names is left as it was. An ordinary name directly in
// the output directory is allowed.
func TestReportCannotReplaceManagedOutput(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	out := filepath.Join(base, "OUT")
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(out, "manifest.json")
	committed, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "LINK")
	if err := os.Symlink(out, link); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(base, "OTHER")
	if err := run(context.Background(), []string{src, "-o", other, "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	textOut := filepath.Join(base, "rows.tsv")
	if err := os.WriteFile(textOut, []byte("rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"manifest", []string{src, "-o", out, "--overwrite", "--report", manifest}},
		{"manifest, unclean", []string{src, "-o", out, "--overwrite", "--report", out + "/data/../manifest.json"}},
		{"manifest through a link", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(link, "manifest.json")}},
		{"manifest, other case", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(out, "MANIFEST.json")}},
		{"journal", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(out, ".swath", "journal")}},
		{"lock", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(out, ".swath", "lock")}},
		{"a part", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(out, "data", "part-00000.parquet")}},
		{"run.json", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(out, "run.json")}},
		{"error.json", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(out, "error.json")}},
		{"the output directory", []string{src, "-o", out, "--overwrite", "--report", out}},
		{"another dataset", []string{"count", src, "--report", filepath.Join(other, "report.json")}},
		{"another dataset's manifest", []string{"count", src, "--report", filepath.Join(other, "manifest.json")}},
		{"the text output", []string{"s3://bucket", "-o", textOut, "--report", textOut}},
		{"the text output, unclean", []string{"s3://bucket", "-o", textOut, "--report", filepath.Join(base, ".", "rows.tsv")}},
	} {
		buf.Reset()
		err := listOrCount(tc.args, &buf)
		if c := exitOf(err); c != exitUsage {
			t.Errorf("%s: exit %d (%v), want %d", tc.name, c, err, exitUsage)
		} else if !strings.Contains(err.Error(), "--report") {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if b, err := os.ReadFile(manifest); err != nil || !bytes.Equal(b, committed) {
		t.Fatalf("the manifest changed: %v\n%s", err, b)
	}
	if b, _ := os.ReadFile(textOut); string(b) != "rows\n" {
		t.Fatalf("the text output changed: %q", b)
	}
	if _, err := os.Stat(filepath.Join(other, "report.json")); !os.IsNotExist(err) {
		t.Fatalf("a report was written inside another dataset: %v", err)
	}
	// Allowed: an ordinary name in the output directory, and outside it.
	for _, report := range []string{filepath.Join(out, "report.json"), filepath.Join(base, "report.json")} {
		if err := run(context.Background(), []string{src, "-o", out, "--overwrite", "--no-progress", "--no-stats", "--report", report}, &buf, false); err != nil {
			t.Fatalf("--report %s: %v", report, err)
		}
		if b, err := os.ReadFile(report); err != nil || !strings.Contains(string(b), `"report_version"`) {
			t.Fatalf("--report %s: %v %s", report, err, b)
		}
		if b, err := os.ReadFile(manifest); err != nil || !strings.Contains(string(b), `"parts"`) {
			t.Fatalf("manifest after --report %s: %v %s", report, err, b)
		}
	}
}

// TestProfilePathsCannotReplaceManagedOutput: the profile files are
// checked as --report is.
func TestProfilePathsCannotReplaceManagedOutput(t *testing.T) {
	src := t.TempDir()
	out := filepath.Join(t.TempDir(), "OUT")
	t.Setenv("SWATH_CPUPROFILE", filepath.Join(out, "manifest.json"))
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out}, &buf, false); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "SWATH_CPUPROFILE") {
		t.Fatalf("profile onto the manifest: %v", err)
	}
}

// TestOpenedSidePathsNotInOutput: a profile, which is opened before the
// run, may not be an ordinary name directly in an existing empty output
// directory either: the run would find the directory not empty and refuse
// its own file. It is refused up front (exit 2) and nothing is created;
// --report, written after the run, may go there. (--trace is checked as a
// profile is: TestTraceFlag.)
func TestOpenedSidePathsNotInOutput(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for _, env := range []string{"SWATH_CPUPROFILE", "SWATH_MEMPROFILE"} {
		out := filepath.Join(t.TempDir(), "OUT")
		if err := os.Mkdir(out, 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv(env, filepath.Join(out, "run.prof"))
		err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats"}, &buf, false)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "is in the output directory") {
			t.Fatalf("%s in OUT: %v, want a refusal", env, err)
		}
		if ents, _ := os.ReadDir(out); len(ents) != 0 {
			t.Fatalf("%s in OUT: the refused run left %d entries", env, len(ents))
		}
		t.Setenv(env, "")
	}
	out := filepath.Join(t.TempDir(), "OUT")
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{src, "-o", out, "--report", filepath.Join(out, "r.json"), "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatalf("--report in an empty OUT: %v", err)
	}
}

// TestResumeCannotReplaceManagedOutput (R8): swath resume checks --report
// before any refusal or completion settles, as a list command does. On a
// committed dataset, a resume with --report OUT/manifest.json is refused
// with exit 2 and the manifest stays; so are the early refusals' reports
// (a text run, whose file --report names; a directory with no run, whose
// --report names another dataset's manifest) and a negative duration.
func TestResumeCannotReplaceManagedOutput(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	out := filepath.Join(base, "OUT")
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(out, "manifest.json")
	committed, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	textOut := filepath.Join(base, "rows.tsv")
	if err := os.WriteFile(textOut, []byte("rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(base, "EMPTY")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"completed, its manifest", []string{"resume", out, "--report", manifest}},
		{"completed, its journal", []string{"resume", out, "--report", filepath.Join(out, ".swath", "journal")}},
		{"completed, negative duration", []string{"resume", out, "--max-duration=-1s"}},
		{"a text run, its file", []string{"resume", textOut, "--report", textOut}},
		{"no run, another dataset's manifest", []string{"resume", empty, "--report", manifest}},
	} {
		buf.Reset()
		if c := exitOf(dispatch(context.Background(), tc.args, &buf)); c != exitUsage {
			t.Errorf("%s: exit %d, want %d", tc.name, c, exitUsage)
		}
	}
	if b, err := os.ReadFile(manifest); err != nil || !bytes.Equal(b, committed) {
		t.Fatalf("the manifest changed: %v\n%s", err, b)
	}
	if b, _ := os.ReadFile(textOut); string(b) != "rows\n" {
		t.Fatalf("the text output changed: %q", b)
	}
	if _, err := os.Stat(filepath.Join(out, ".swath", "journal")); !os.IsNotExist(err) {
		t.Fatalf("a report was written as the journal: %v", err)
	}
	// Allowed: an ordinary report beside the dataset, which says complete.
	report := filepath.Join(out, "report.json")
	if err := dispatch(context.Background(), []string{"resume", out, "--report", report}, &buf); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(report); err != nil || !strings.Contains(string(b), `"report_version"`) {
		t.Fatalf("report: %v %s", err, b)
	}
}

// TestSidePathResolvedAsWritten (R8): a side path is checked at the
// pathname its writer reaches. With B/link -> A/sub, B/link/../D is A/D
// (an open climbs from the link's target), not B/D as a lexical clean of
// the path says: a report there, onto the dataset A/D's manifest or
// beside another dataset, is refused, and the writer of an allowed path
// writes the destination that was checked.
func TestSidePathResolvedAsWritten(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	A, B := filepath.Join(base, "A"), filepath.Join(base, "B")
	for _, d := range []string{filepath.Join(A, "sub"), filepath.Join(B, "D")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(A, "sub"), filepath.Join(B, "link")); err != nil {
		t.Skip("no symbolic links:", err)
	}
	out := filepath.Join(A, "D")
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(out, "manifest.json")
	committed, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	via := filepath.Join(B, "link") + string(filepath.Separator) + ".." + string(filepath.Separator) + "D"
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"the dataset's manifest", []string{src, "-o", out, "--overwrite", "--no-progress", "--no-stats", "--report", via + string(filepath.Separator) + "manifest.json"}},
		{"inside another dataset", []string{"count", src, "--report", via + string(filepath.Separator) + "report.json"}},
	} {
		if c := exitOf(listOrCount(tc.args, &buf)); c != exitUsage {
			t.Errorf("%s: exit %d, want %d", tc.name, c, exitUsage)
		}
	}
	if b, err := os.ReadFile(manifest); err != nil || !bytes.Equal(b, committed) {
		t.Fatalf("the manifest changed: %v\n%s", err, b)
	}
	for _, p := range []string{filepath.Join(out, "report.json"), filepath.Join(B, "D", "report.json")} {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Fatalf("%s written: %v", p, err)
		}
	}
	// Allowed: the same climb out of an ordinary directory lands where it
	// was checked (A/other/report.json), never in B.
	if err := os.Mkdir(filepath.Join(A, "other"), 0o755); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(B, "link") + string(filepath.Separator) + ".." + string(filepath.Separator) + "other" + string(filepath.Separator) + "report.json"
	if err := runCount(context.Background(), []string{src, "--no-progress", "--no-stats", "--report", report}, &buf); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(A, "other", "report.json")); err != nil {
		t.Fatal(err)
	}
}

// TestReservedSlotRefusedWhateverItsLinkTarget (R8): OUT/manifest.json is
// the manifest's slot even while a symbolic link there points outside the
// output: an overwrite removes the link and publishes a manifest in it, so
// a report naming it is refused, the link and its target left as they
// were.
func TestReservedSlotRefusedWhateverItsLinkTarget(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	out := filepath.Join(base, "OUT")
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	manifest, ext := filepath.Join(out, "manifest.json"), filepath.Join(base, "ext.json")
	if err := os.Rename(manifest, ext); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(ext, manifest); err != nil {
		t.Skip("no symbolic links:", err)
	}
	before, _ := os.ReadFile(ext)
	t.Setenv("SWATH_MEMPROFILE", "")
	for _, env := range []string{"", "SWATH_MEMPROFILE"} {
		args := []string{src, "-o", out, "--overwrite", "--no-progress", "--no-stats"}
		if env == "" {
			args = append(args, "--report", manifest)
		} else {
			t.Setenv(env, manifest)
		}
		if c := exitOf(run(context.Background(), args, &buf, false)); c != exitUsage {
			t.Errorf("%q: exit %d, want %d", env, c, exitUsage)
		}
		t.Setenv("SWATH_MEMPROFILE", "")
	}
	if l, err := os.Readlink(manifest); err != nil || l != ext {
		t.Fatalf("the link changed: %q %v", l, err)
	}
	if b, _ := os.ReadFile(ext); !bytes.Equal(b, before) {
		t.Fatalf("the link's target changed")
	}
}

// TestSamePlace: the comparison the side-path check finds the output and
// its entries by. Identity decides for what exists (a hard link is the
// same file, another directory is not); for names yet to be created below
// the same existing directory, case counts only where that directory
// ignores it.
func TestSamePlace(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "OUT")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(base, "OTHER")
	if err := os.Mkdir(other, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "f")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(base, "hard")
	if err := os.Link(file, hard); err != nil {
		t.Fatal(err)
	}
	fold := false
	defer func(f func(string) bool) { foldsCase = f }(foldsCase)
	foldsCase = func(string) bool { return fold }
	for _, tc := range []struct {
		a, b      string
		plain, ci bool
	}{
		{dir, dir, true, true},
		{file, hard, true, true},
		{dir, other, false, false},
		{filepath.Join(base, "NEW", "x"), filepath.Join(base, "new", "x"), false, true},
		{filepath.Join(base, "NEW", "x"), filepath.Join(base, "NEW", "X"), false, true},
		{filepath.Join(base, "NEW", "x"), filepath.Join(base, "NEW"), false, false},
		{filepath.Join(base, "NEW"), filepath.Join(other, "NEW"), false, false},
		{filepath.Join(dir, "x"), filepath.Join(other, "x"), false, false},
	} {
		for _, f := range []bool{false, true} {
			fold = f
			want := tc.plain
			if f {
				want = tc.ci
			}
			if got := samePlace(tc.a, tc.b); got != want {
				t.Errorf("samePlace(%s, %s) with fold %v = %v, want %v", tc.a, tc.b, f, got, want)
			}
		}
	}
	// The check finds a fresh output by this comparison: base/new/manifest.json
	// is the manifest of the output base/NEW where base ignores case.
	o := options{out: filepath.Join(base, "NEW")}
	for _, f := range []bool{false, true} {
		fold = f
		if _, err := checkSidePath(o, filepath.Join(base, "new", "manifest.json"), false); (err != nil) != f {
			t.Errorf("a fresh output's manifest, other case, fold %v: %v", f, err)
		}
		if _, err := checkSidePath(o, filepath.Join(base, "new", "report.json"), false); err != nil {
			t.Errorf("an ordinary report, fold %v: %v", f, err)
		}
	}
	if got := swapCase("Ab-1é"); got != "aB-1É" {
		t.Errorf("swapCase: %q", got)
	}
}

// TestSidePathsCaseInsensitiveAlias (R8): on a case-insensitive
// filesystem base/out is the output base/OUT. A report onto its manifest
// spelled so, existing or not yet, is refused; an ordinary report in it so
// spelled is not taken for another dataset's. Skipped where the
// filesystem of the temporary directory distinguishes case.
func TestSidePathsCaseInsensitiveAlias(t *testing.T) {
	captureStderr(t)
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "Probe"), 0o755); err != nil {
		t.Fatal(err)
	}
	fa, ea := os.Stat(filepath.Join(base, "Probe"))
	fb, eb := os.Stat(filepath.Join(base, "probe"))
	if ea != nil || eb != nil || !os.SameFile(fa, fb) {
		t.Skip("the temporary directory's filesystem distinguishes case")
	}
	if !foldsCase(base) {
		t.Fatal("foldsCase: false on a case-insensitive directory")
	}
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(base, "OUT")
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(out, "manifest.json")
	committed, _ := os.ReadFile(manifest)
	fresh := filepath.Join(base, "FRESH")
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"the manifest, other case", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(base, "out", "manifest.json")}},
		{"the output, other case", []string{src, "-o", out, "--overwrite", "--report", filepath.Join(base, "out")}},
		{"a fresh output's manifest, other case", []string{src, "-o", fresh, "--report", filepath.Join(base, "fresh", "manifest.json")}},
	} {
		if c := exitOf(listOrCount(tc.args, &buf)); c != exitUsage {
			t.Errorf("%s: exit %d, want %d", tc.name, c, exitUsage)
		}
	}
	if b, _ := os.ReadFile(manifest); !bytes.Equal(b, committed) {
		t.Fatal("the manifest changed")
	}
	if _, err := os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatalf("the refused run wrote %s: %v", fresh, err)
	}
	report := filepath.Join(base, "out", "report.json")
	if err := run(context.Background(), []string{src, "-o", out, "--overwrite", "--no-progress", "--no-stats", "--report", report}, &buf, false); err != nil {
		t.Fatalf("an ordinary report in the output, other case: %v", err)
	}
}

// TestSidePathsBesideUnrelatedDirectories: a directory is refused as
// another dataset only by this tool's marks; a report inside a directory
// with _SUCCESS and an unrelated data/, or another tool's manifest with a
// schema id, is written.
func TestSidePathsBesideUnrelatedDirectories(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()
	spark, other := filepath.Join(base, "spark"), filepath.Join(base, "other")
	for _, f := range []struct{ path, body string }{
		{filepath.Join(spark, "_SUCCESS"), ""},
		{filepath.Join(spark, "data", "part-0"), "x"},
		{filepath.Join(other, "manifest.json"), `{"schema_id":"other/v1"}`},
	} {
		if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f.path, []byte(f.body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	for _, dir := range []string{spark, other} {
		report := filepath.Join(dir, "report.json")
		if err := runCount(context.Background(), []string{src, "--no-progress", "--no-stats", "--report", report}, &buf); err != nil {
			t.Fatalf("--report %s: %v", report, err)
		}
		if _, err := os.Stat(report); err != nil {
			t.Fatal(err)
		}
	}
}

// TestFoldsCaseProbe: the probe respells a directory's own name, so two
// case-distinct hard links in it (a and A, one file) are not taken for
// case folding, nor are distinct fresh names that differ only by case
// then refused as one. A directory whose name has no letter defers to an
// ancestor's.
func TestFoldsCaseProbe(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "Dir")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "dir")); err == nil {
		t.Skip("the temporary directory's filesystem ignores case")
	}
	if err := os.WriteFile(filepath.Join(dir, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dir, "a"), filepath.Join(dir, "A")); err != nil {
		t.Skip("no hard links:", err)
	}
	if foldsCase(dir) {
		t.Error("foldsCase: case-distinct hard links taken for case folding")
	}
	digits := filepath.Join(dir, "123")
	if err := os.Mkdir(digits, 0o755); err != nil {
		t.Fatal(err)
	}
	if foldsCase(digits) {
		t.Error("foldsCase: a letterless name not deferred to its parent's")
	}
	o := options{out: filepath.Join(dir, "x")}
	if _, err := checkSidePath(o, filepath.Join(dir, "X", "manifest.json"), false); err != nil {
		t.Errorf("a distinct fresh directory differing by case refused: %v", err)
	}
}

// TestDanglingLinkSidePaths (R8): a side path that is a symbolic link to a
// managed name that does not exist yet (mem -> OUT/manifest.json, before
// the run publishes it) is checked where an open would land once it
// exists, and refused with exit 2 before the run; the manifest the run
// would publish is never written through it. A profile is never written
// through a link at all.
func TestDanglingLinkSidePaths(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for _, tc := range []struct{ name, env string }{{"--report", ""}, {"cpu", "SWATH_CPUPROFILE"}, {"mem", "SWATH_MEMPROFILE"}} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			out := filepath.Join(base, "OUT")
			if err := os.Mkdir(out, 0o755); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(base, "mem")
			if err := os.Symlink(filepath.Join(out, "manifest.json"), link); err != nil {
				t.Skip("no symbolic links:", err)
			}
			args := []string{src, "-o", out, "--no-progress", "--no-stats"}
			if tc.env == "" {
				args = append(args, "--report", link)
			} else {
				t.Setenv(tc.env, link)
			}
			err := run(context.Background(), args, &buf, false)
			b, rerr := os.ReadFile(filepath.Join(out, "manifest.json"))
			if rerr == nil && !strings.Contains(string(b), `"coverage"`) {
				t.Fatalf("the manifest was overwritten through the link: %d bytes", len(b))
			}
			if c := exitOf(err); c != exitUsage {
				t.Fatalf("exit %d (%v), want %d", c, err, exitUsage)
			}
		})
	}
	base := t.TempDir()
	// A link to an ordinary file is refused as a profile (an open would
	// follow it); its target is untouched.
	target := filepath.Join(base, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(base, "plain")
	if err := os.Symlink(target, plain); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SWATH_MEMPROFILE", plain)
	if c := exitOf(runCount(context.Background(), []string{src}, &buf)); c != exitUsage {
		t.Errorf("a profile through a link: exit %d, want %d", c, exitUsage)
	}
	if f, err := createNoFollow(plain); err == nil {
		f.Close()
		t.Error("createNoFollow opened a link")
	}
	if b, _ := os.ReadFile(target); string(b) != "keep" {
		t.Fatalf("the link's target changed: %d bytes", len(b))
	}
}

// fakeEntry is an entry of fakeCaseFS: a file identity and its kind.
type fakeEntry struct {
	id  int
	dir bool
}

func (e fakeEntry) Name() string       { return "" }
func (e fakeEntry) Size() int64        { return 0 }
func (e fakeEntry) Mode() os.FileMode  { return map[bool]os.FileMode{true: os.ModeDir}[e.dir] }
func (e fakeEntry) ModTime() time.Time { return time.Time{} }
func (e fakeEntry) IsDir() bool        { return e.dir }
func (e fakeEntry) Sys() any           { return nil }

// fakeCaseFS is a filesystem for the case probe: entries by exact path,
// and the directories whose lookups fold case (a case-insensitive volume
// mounted there; its parent's rules are its parent's own).
func fakeCaseFS(entries map[string]fakeEntry, folding ...string) caseProbe {
	lookup := func(p string) (fakeEntry, bool) {
		if e, ok := entries[p]; ok {
			return e, true
		}
		if slices.Contains(folding, filepath.Dir(p)) {
			for q, e := range entries {
				if filepath.Dir(q) == filepath.Dir(p) && strings.EqualFold(filepath.Base(q), filepath.Base(p)) {
					return e, true
				}
			}
		}
		return fakeEntry{}, false
	}
	return caseProbe{
		names: func(dir string, n int) []string {
			var names []string
			for q := range entries {
				if filepath.Dir(q) == dir && q != dir {
					names = append(names, filepath.Base(q))
				}
			}
			slices.Sort(names)
			return names[:min(len(names), n)]
		},
		lstat: func(p string) (os.FileInfo, error) {
			if e, ok := lookup(p); ok {
				return e, nil
			}
			return nil, os.ErrNotExist
		},
		same: func(a, b os.FileInfo) bool { return a.(fakeEntry).id == b.(fakeEntry).id },
	}
}

// TestFoldsCaseProbeInside: the probe asks the directory itself, so a
// case-insensitive volume mounted under a case-sensitive parent folds
// (its entries do, though its own name does not fold in the parent); two
// case-distinct hard links do not; an empty directory falls back to its
// own name in its parent.
func TestFoldsCaseProbeInside(t *testing.T) {
	root := string(filepath.Separator)
	p := func(names ...string) string { return filepath.Join(append([]string{root}, names...)...) }
	fs := fakeCaseFS(map[string]fakeEntry{
		p("mnt"):                 {1, true},
		p("mnt", "Links"):        {2, true},
		p("mnt", "Links", "a"):   {3, false},
		p("mnt", "Links", "A"):   {3, false}, // a hard link, listed under its own spelling
		p("mnt", "Drive"):        {4, true},  // a case-insensitive volume
		p("mnt", "Drive", "a"):   {5, false},
		p("mnt", "Drive", "Sub"): {6, true},
		p("mnt", "Files"):        {7, true}, // another, holding a file only
		p("mnt", "Files", "f"):   {8, false},
		p("mnt", "Empty"):        {9, true},
		p("ci"):                  {10, true},
		p("ci", "Empty"):         {11, true},
	}, p("mnt", "Drive"), p("mnt", "Files"), p("ci"))
	for _, tc := range []struct {
		name, dir string
		want      bool
	}{
		{"hard links a and A", p("mnt", "Links"), false},
		{"a folding volume, a directory entry", p("mnt", "Drive"), true},
		{"a folding volume, a file entry with no other-case entry listed", p("mnt", "Files"), true},
		{"an empty directory in a case-sensitive parent", p("mnt", "Empty"), false},
		{"an empty directory in a folding parent", p("ci", "Empty"), true},
	} {
		if got := fs.folds(tc.dir); got != tc.want {
			t.Errorf("%s: folds(%s) = %v, want %v", tc.name, tc.dir, got, tc.want)
		}
	}
}

// listOrCount runs swath diag count for args that start with "count", else
// swath list.
func listOrCount(args []string, stdout io.Writer) error {
	if len(args) > 0 && args[0] == "count" {
		return runCount(context.Background(), args[1:], stdout)
	}
	return run(context.Background(), args, stdout, false)
}

// TestReportDestination: --report naming a directory, or a name whose
// directory does not exist, is refused before listing (exit 2); a report
// that cannot be written at the end fails a run that succeeded (exit 1),
// its error shown under -q; a refusal of a command line that parsed (a
// provider flag on another provider's source) writes its report.
func TestReportDestination(t *testing.T) {
	stderr := captureStderr(t)
	src := localSource(t)
	base := t.TempDir()
	for _, report := range []string{base, filepath.Join(base, "missing", "r.json")} {
		if err := run(context.Background(), []string{src, "--columns", "key", "--report", report}, io.Discard, false); exitOf(err) != exitUsage {
			t.Errorf("--report %s: %v", report, err)
		}
	}
	report := filepath.Join(base, "r.json")
	err := run(context.Background(), []string{src, "--s3-region", "x", "--report", report}, io.Discard, false)
	var rep listing.Report
	if exitOf(err) != exitUsage {
		t.Fatalf("--s3-region on a local source: %v", err)
	}
	readJSON(t, report, &rep)
	if rep.Outcome.ExitCode != exitUsage || !strings.Contains(rep.Outcome.Error, "--s3-region") {
		t.Errorf("report outcome %+v", rep.Outcome)
	}
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		return // a read-only directory is writable there
	}
	ro := filepath.Join(base, "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(ro, 0o700) })
	stderr.Reset()
	setLog(termOptions{quiet: 1}, "")
	t.Cleanup(func() { logs.Store(nil) })
	err = run(context.Background(), []string{src, "--columns", "key", "-q", "--report", filepath.Join(ro, "r.json")}, io.Discard, false)
	if code := conclude(err); code != exitFatal || !strings.Contains(stderr.String(), "is not written to") {
		t.Errorf("unwritable report: exit %d (%v), stderr %q", code, err, stderr.String())
	}
}
