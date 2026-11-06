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
