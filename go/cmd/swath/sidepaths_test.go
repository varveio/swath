// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		{"another dataset", []string{src, "--count-only", "--report", filepath.Join(other, "report.json")}},
		{"another dataset's manifest", []string{src, "--count-only", "--report", filepath.Join(other, "manifest.json")}},
		{"the text output", []string{"s3://bucket", "-o", textOut, "--report", textOut}},
		{"the text output, unclean", []string{"s3://bucket", "-o", textOut, "--report", filepath.Join(base, ".", "rows.tsv")}},
	} {
		buf.Reset()
		err := run(context.Background(), tc.args, &buf, false)
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
		{"inside another dataset", []string{src, "--count-only", "--report", via + string(filepath.Separator) + "report.json"}},
	} {
		if c := exitOf(run(context.Background(), tc.args, &buf, false)); c != exitUsage {
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
	if err := run(context.Background(), []string{src, "--count-only", "--no-progress", "--no-stats", "--report", report}, &buf, false); err != nil {
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
