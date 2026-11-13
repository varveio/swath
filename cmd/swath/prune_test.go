// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"slices"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/localfs"
)

type commandPruneReport struct {
	Output  listing.Output
	Outcome listing.Outcome
	Source  struct {
		Name  string
		Stats localfs.Stats
	}
}

func checkCommandPruning(t *testing.T, path string, pruned, fetched, filtered, emitted int64) commandPruneReport {
	t.Helper()
	var report commandPruneReport
	readJSON(t, path, &report)
	out, stats := report.Output, report.Source.Stats
	if out.RowsFetched != fetched || out.RowsFiltered != filtered || out.RowsEmitted != emitted || out.RowsFetched != out.RowsFiltered+out.RowsEmitted {
		t.Fatalf("row account: fetched=%d filtered=%d emitted=%d, want %d/%d/%d", out.RowsFetched, out.RowsFiltered, out.RowsEmitted, fetched, filtered, emitted)
	}
	if report.Source.Name != "local" || stats.PrunedDirs != pruned || stats.Pruning == nil ||
		stats.Pruning.RecursiveGlobs != 1 || stats.Pruning.RowOnlyGlobs != 1 || stats.Pruning.RegexRules != 1 {
		t.Fatalf("source %q stats %+v pruning %+v", report.Source.Name, stats, stats.Pruning)
	}
	if stats.Errors() != 0 || report.Outcome.EntryErrors != 0 || report.Outcome.Unreadable {
		t.Fatalf("intentional pruning counted as unreadable: %+v", report)
	}
	return report
}

// A committed Parquet dataset retains a/'s row for a/** while never reading
// its descendants. Coverage continues to describe the original user filters;
// pruning is a traversal optimization recorded separately in the run report.
func TestPruneCommandParquet(t *testing.T) {
	root, _ := depthTree(t)
	for _, engine := range []string{"", "portable"} {
		t.Run("engine="+engine, func(t *testing.T) {
			t.Setenv("SWATH_LOCALFS_ENGINE", engine)
			dir := filepath.Join(t.TempDir(), "dataset")
			args := []string{root, "-o", dir, "--no-stats", "--exclude", "a/**", "--exclude", "x", "--exclude-regex", "^never$"}
			if err := run(context.Background(), append(args, unjournaled...), io.Discard, diagList); err != nil {
				t.Fatal(err)
			}
			want := []string{"a/", "c/", "c/h", "e/"}
			if got := datasetKeys(t, dir); !slices.Equal(got, want) {
				t.Fatalf("Parquet keys %q, want %q", got, want)
			}
			manifest := readManifest(t, dir)
			filters := []string{"exclude=a/**", "exclude=x", "exclude_regex=^never$"}
			if !slices.Equal(manifest.Coverage.Filters, filters) || manifest.Coverage.SkippedEntries != 0 ||
				!manifest.Coverage.Complete || manifest.Coverage.StopReason != "completed" {
				t.Fatalf("coverage %+v, want original filters %q and complete coverage", manifest.Coverage, filters)
			}
			report := checkCommandPruning(t, filepath.Join(dir, dataset.ReportName), 1, 5, 1, 4)
			if report.Output.RowsCommitted != 4 || report.Source.Stats.DirsRead != 3 {
				t.Fatalf("committed=%d directories read=%d, want 4 and 3", report.Output.RowsCommitted, report.Source.Stats.DirsRead)
			}
		})
	}
}

// du shares the count traversal: a/ survives as an empty prefix, its
// descendants contribute no objects or bytes, and an ordinary row filter
// still participates in the fetched/filtered/emitted account.
func TestPruneCommandDu(t *testing.T) {
	root, _ := depthTree(t)
	for _, engine := range []string{"", "portable"} {
		t.Run("engine="+engine, func(t *testing.T) {
			t.Setenv("SWATH_LOCALFS_ENGINE", engine)
			reportPath := filepath.Join(t.TempDir(), "report.json")
			var out bytes.Buffer
			args := []string{root, "--depth", "2", "--format", "tsv", "--no-stats", "--report", reportPath,
				"--exclude", "a/**", "--exclude", "x", "--exclude-regex", "^never$"}
			if err := runDu(context.Background(), args, &out); err != nil {
				t.Fatal(err)
			}
			const older = "2026-01-02T03:04:05.000000Z"
			want := "depth\tobjects\tdirs\tsize\tunsized\tlast_modified\tprefix\n" +
				"0\t1\t3\t1\t0\t" + older + "\t\n" +
				"1\t0\t0\t0\t0\t\\N\ta/\n" +
				"1\t1\t0\t1\t0\t" + older + "\tc/\n" +
				"1\t0\t0\t0\t0\t\\N\te/\n"
			if out.String() != want {
				t.Fatalf("du totals:\n%s\nwant:\n%s", out.String(), want)
			}
			report := checkCommandPruning(t, reportPath, 1, 5, 1, 4)
			totals := report.Output.Totals
			if totals == nil || totals.Objects != 1 || totals.Dirs != 3 || totals.Bytes != 1 || totals.Prefixes != 4 {
				t.Fatalf("report totals %+v", totals)
			}
		})
	}
}

// The same recursive exclude configured on a file root has no child to
// prune, and does not match the file's own row.
func TestPruneCommandFileRoot(t *testing.T) {
	root, _ := depthTree(t)
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var out bytes.Buffer
	args := []string{filepath.Join(root, "a", "g"), "--format", "tsv", "--columns", "key,size,kind", "--no-stats", "--report", reportPath,
		"--exclude", "g/**", "--exclude", "x", "--exclude-regex", "^never$"}
	if err := run(context.Background(), args, &out, false); err != nil {
		t.Fatal(err)
	}
	if out.String() != "key\tsize\tkind\ng\t2\tFILE\n" {
		t.Fatalf("file-root rows %q", out.String())
	}
	checkCommandPruning(t, reportPath, 0, 1, 0, 1)
}
