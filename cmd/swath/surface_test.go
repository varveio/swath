// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source"
)

// localSource is a directory with two files.
func localSource(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	for _, n := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(src, n), []byte("xyz"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return src
}

// TestHelpGroupsCoverFlags: every flag of the list commands is in exactly
// one help group (or hidden, or an alias shown on its flag's line), and
// the diagnostic group is exactly the flags swath list refuses.
func TestHelpGroupsCoverFlags(t *testing.T) {
	if err := checkHelpGroups(newFlags(&options{}, true)); err != nil {
		t.Fatal(err)
	}
}

// TestMovedFlagsNameDiag: each flag moved to swath diag is refused on
// swath list and swath du with exit 2 and a message naming swath diag,
// and accepted by swath diag list.
func TestMovedFlagsNameDiag(t *testing.T) {
	src := localSource(t)
	moved := map[string]string{
		"--no-seed": "s3", "--structure=off": "s3", "--sequential": "s3", "--max-keys=5": "s3",
		"--adaptive-limit-slow-start": "s3", "--spill-plain": "", "--no-spill-compress": "", "--spill-reserve=1GiB": "",
		"--no-spill": "", "--no-journal": "",
	}
	for fl, scheme := range moved {
		root := src
		if scheme == "s3" {
			root = "s3://bkt"
		}
		name, _, _ := strings.Cut(fl, "=")
		if !diagOnly()[strings.TrimLeft(name, "-")] {
			t.Errorf("%s is not a diagnostic flag", name)
		}
		for _, cmd := range []command{{}, {count: true, du: true}} {
			var buf bytes.Buffer
			err := runWith(context.Background(), cmd, []string{root, "-o", filepath.Join(t.TempDir(), "ds"), fl}, &buf, nil, false, nil)
			if cmd.count {
				err = runWith(context.Background(), cmd, []string{root, fl}, &buf, nil, false, nil)
			}
			want := "swath diag " + cmd.name()
			switch {
			case cmd.count && !takes(strings.TrimLeft(name, "-"), cmdDiagCount):
				want = "swath diag list takes it" // no count command takes an output flag
			case cmd.du:
				want = "swath diag count or swath diag list" // swath du has no diagnostic form
			}
			if exitOf(err) != exitUsage || !strings.Contains(err.Error(), want) {
				t.Errorf("swath %s %s: exit %d (%v), want %d naming swath diag", cmd.name(), fl, exitOf(err), err, exitUsage)
			}
		}
		var o options
		if err := newFlags(&o, true).Parse([]string{fl}); err != nil {
			t.Errorf("swath diag list %s: %v", fl, err)
		}
	}
	// The flags that stayed on swath list are not diagnostic.
	for _, n := range []string{"workers", "sort-buffer", "spill-budget", "encoders", "part-size", "seed-from", "no-adaptive-limit", "request-rate", "max-duration", "stall-timeout", "part-interval", "part-rotation-max-rows"} {
		if diagOnly()[n] {
			t.Errorf("--%s is diagnostic", n)
		}
	}
}

// takes reports whether the list command cmd takes flag name (its table
// entry's Commands).
func takes(name, cmd string) bool {
	cmds := source.Described(newFlags(&options{}, true).Lookup(name)).Commands
	return cmds == nil || slices.Contains(cmds, cmd)
}

// TestCountRefusesOutputFlags: swath du and swath diag count write no
// rows, so the output, part, reorder and spill flags are refused (exit 2),
// naming a command that takes them; swath du takes --format alone, for its
// totals, and the totals' flags are swath du's.
func TestCountRefusesOutputFlags(t *testing.T) {
	src := localSource(t)
	for _, fl := range []string{"-o=x", "--overwrite", "--part-size=1MiB", "--sort-buffer=1MiB", "--spill-budget=1GiB", "--encoders=2"} {
		var buf bytes.Buffer
		err := runDu(context.Background(), []string{src, fl}, &buf)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "has no meaning for swath du, which writes totals") || !strings.Contains(err.Error(), "swath list takes it") {
			t.Errorf("swath du %s: exit %d (%v)", fl, exitOf(err), err)
		}
		err = runCount(context.Background(), []string{src, fl}, &buf)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "has no meaning for swath diag count, which writes nothing") {
			t.Errorf("swath diag count %s: exit %d (%v)", fl, exitOf(err), err)
		}
	}
	var buf bytes.Buffer
	if err := runCount(context.Background(), []string{src, "--format=tsv"}, &buf); exitOf(err) != exitUsage {
		t.Errorf("swath diag count --format: exit %d (%v)", exitOf(err), err)
	}
	for _, fl := range []string{"--depth=1", "-d=1", "--max-prefixes=5", "--allocated", "--count-links", "-l", "--max-inodes=5"} {
		for name, run := range map[string]func() error{
			"list":       func() error { return run(context.Background(), []string{src, fl}, &buf, false) },
			"diag count": func() error { return runCount(context.Background(), []string{src, fl}, &buf) },
		} {
			if err := run(); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "shapes swath du's totals") {
				t.Errorf("swath %s %s: exit %d (%v)", name, fl, exitOf(err), err)
			}
		}
	}
	for _, fl := range []string{"--no-spill-compress", "--spill-plain", "--spill-reserve=1GiB", "--no-spill", "--no-journal", "--part-interval=1s"} {
		var buf bytes.Buffer
		err := runCount(context.Background(), []string{src, fl}, &buf)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "has no meaning for swath") || strings.Contains(err.Error(), "count takes it") {
			t.Errorf("swath diag count %s: exit %d (%v)", fl, exitOf(err), err)
		}
		err = runDu(context.Background(), []string{src, fl}, &buf)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "swath du") && !strings.Contains(err.Error(), "swath diag count or swath diag list") {
			t.Errorf("swath du %s: exit %d (%v)", fl, exitOf(err), err)
		}
	}
}

// TestCount: swath diag count lists and counts, writes nothing, prints
// the rows and object bytes, and its report says so (status listed, no
// ordering, the object bytes summed).
func TestCount(t *testing.T) {
	src := localSource(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var buf bytes.Buffer
	if err := runCount(context.Background(), []string{src, "--no-stats", "--report", report}, &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "rows=2 object_bytes=6 ") {
		t.Fatalf("summary %q", buf.String())
	}
	var r listing.Report
	b, err := os.ReadFile(report)
	if err != nil || json.Unmarshal(b, &r) != nil {
		t.Fatal(err)
	}
	if r.Outcome.Status != listing.StatusListed || r.Resources.Features["order"] || r.Output.RowsEmitted != 2 || r.Output.RowsFetched != 2 ||
		r.Output.ObjectBytes == nil || *r.Output.ObjectBytes != 6 || !slices.Equal(r.Invocation.Argv[:2], []string{"diag", "count"}) || r.ReportVersion != listing.ReportVersion {
		t.Fatalf("report %+v %+v %v", r.Outcome, r.Output, r.Invocation.Argv)
	}
	if r.History == nil || len(r.History.Samples) < 2 {
		t.Fatalf("history %+v", r.History)
	}
}

// TestOldNamesRejected: the names of earlier builds and Java's aliases are
// unknown flags (exit 2, the flag parser's own error): docs/upgrade.md maps
// them, the runtime does not.
func TestOldNamesRejected(t *testing.T) {
	src := localSource(t)
	for _, fl := range []string{"--concurrency=7", "--spill-max=2GiB", "--force", "--count-only", "--endpoint=http://x", "--report-format=json", "--one-filesystem", "--limiter=false", "--region=x", "--azblob-sas=x"} {
		var buf bytes.Buffer
		err := run(context.Background(), []string{src, "-o", filepath.Join(t.TempDir(), "ds"), fl}, &buf, false)
		name, _, _ := strings.Cut(strings.TrimLeft(fl, "-"), "=")
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "unknown flag --"+name) {
			t.Errorf("%s: exit %d (%v)", fl, exitOf(err), err)
		}
	}
}

// TestEveryHelpExitsZero: --help on every command, diag included, prints
// to stdout and exits 0 (main treats pflag.ErrHelp as success).
func TestEveryHelpExitsZero(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"help"}, {"list", "--help"}, {"du", "--help"}, {"resume", "--help"}, {"show", "--help"},
		{"diag", "--help"}, {"diag", "list", "--help"}, {"diag", "count", "-h"}} {
		var out bytes.Buffer
		err := dispatch(context.Background(), args, &out)
		if err != nil && !errors.Is(err, pflag.ErrHelp) {
			t.Errorf("%v: %v", args, err)
		}
		if out.Len() == 0 {
			t.Errorf("%v: no help on stdout", args)
		}
	}
	var out bytes.Buffer
	if err := dispatch(context.Background(), []string{"list", "--help"}, &out); !errors.Is(err, pflag.ErrHelp) {
		t.Fatal(err)
	}
	for _, want := range []string{"Output:", "Filters:", "Object stores:", "S3:", "GCS:", "Azure Blob:", "Local:", "Performance:", "Limits:", "Reporting:", "Raise it (128, 256)", "-j, --workers N", "-o, --output PATH", "Examples:"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("swath list --help lacks %q", want)
		}
	}
	for _, hidden := range []string{"--spill-max", "--count-only", "--concurrency", "--no-seed", "Diagnostics:", "=false", "Java"} {
		if strings.Contains(out.String(), hidden) {
			t.Errorf("swath list --help shows %q", hidden)
		}
	}
	if err := dispatch(context.Background(), []string{"diag", "nothing"}, &out); exitOf(err) != exitUsage {
		t.Errorf("diag nothing: %v", err)
	}
}

// TestHistoryEndsDurable: a committed run's history ends with the
// committed rows durable, also without a journal (a local dataset, or
// --no-journal), whose parts the journal never acknowledged.
func TestHistoryEndsDurable(t *testing.T) {
	src := localSource(t)
	for _, extra := range [][]string{nil, {"--no-journal"}} {
		report := filepath.Join(t.TempDir(), "r.json")
		args := append([]string{src, "-o", filepath.Join(t.TempDir(), "ds"), "--no-stats", "--report", report}, extra...)
		if err := run(context.Background(), args, io.Discard, extra != nil); err != nil {
			t.Fatal(err)
		}
		var r listing.Report
		b, err := os.ReadFile(report)
		if err != nil || json.Unmarshal(b, &r) != nil {
			t.Fatal(err)
		}
		if r.History == nil || len(r.History.Samples) == 0 || r.Output.RowsCommitted != 2 {
			t.Fatalf("%v: history %+v, committed %d", extra, r.History, r.Output.RowsCommitted)
		}
		if last := r.History.Samples[len(r.History.Samples)-1]; last.RowsDurable != r.Output.RowsCommitted {
			t.Fatalf("%v: last sample durable %d, committed %d", extra, last.RowsDurable, r.Output.RowsCommitted)
		}
	}
}
