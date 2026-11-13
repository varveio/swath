// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/reader"
)

// duRun runs swath du with args and returns its stdout.
func duRun(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runDu(context.Background(), append(args, "--no-stats"), &out)
	return out.String(), err
}

// liveAndSaved runs swath du on a source (live) and on its dataset (saved)
// and fails unless their rows are the same: TSV byte for byte, JSONL with
// the saved rows' dataset member, last, removed.
func liveAndSaved(t *testing.T, live, saved []string) {
	t.Helper()
	want, err := duRun(t, live...)
	if err != nil {
		t.Fatalf("%q: %v", live, err)
	}
	got, err := duRun(t, saved...)
	if err != nil {
		t.Fatalf("%q: %v", saved, err)
	}
	var stripped strings.Builder
	for l := range strings.Lines(got) {
		if i := strings.Index(l, `,"dataset":{`); i >= 0 {
			l = l[:i] + "}\n"
		}
		stripped.WriteString(l)
	}
	if stripped.String() != want || want == "" {
		t.Fatalf("%q:\n%s\n%q, live:\n%s", saved, got, live, want)
	}
}

// TestDuFromDatasetLocal: swath du --from-dataset prints what swath du of
// the tree prints, for every depth, order and format, with the bytes
// columns, hard links counted once and filters; plain swath du of the
// dataset directory still totals its files.
func TestDuFromDatasetLocal(t *testing.T) {
	stderr := captureStderr(t)
	root, _ := depthTree(t)
	if err := os.Link(filepath.Join(root, "a", "g"), filepath.Join(root, "c", "g-link")); err != nil {
		t.Fatal(err)
	}
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{root, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	for _, extra := range [][]string{nil, {"--allocated"}, {"--count-links-once"}, {"--exclude", "a/**"}, {"--min-size", "2", "--include", "*/*"}, {"--modified-since", "2026-01-02T03:30:00Z"}} {
		for depth := range 4 {
			for _, sort := range []string{"name", "size"} {
				for _, format := range []string{"tsv", "jsonl", "table"} {
					flags := append([]string{"-d", strconv.Itoa(depth), "--sort", sort, "--format", format}, extra...)
					liveAndSaved(t, append([]string{root}, flags...), append([]string{"--from-dataset", ds}, flags...))
				}
			}
		}
	}
	// PREFIX restricts the totals to its keys, the depth counted from it;
	// the rows keep the dataset's keys.
	got, err := duRun(t, "--from-dataset", ds, "a/", "-d", "1", "--format", "tsv")
	if err != nil || !strings.Contains(got, "\t2\t1\t7\t0\t") || !strings.HasSuffix(got, "\ta/b/\n") || strings.Count(got, "\n") != 3 {
		t.Fatalf("PREFIX a/: %v\n%s", err, got)
	}
	// Stderr says it is the dataset, its source and coverage, not a live
	// listing; JSONL rows carry it.
	if s := stderr.String(); !strings.Contains(s, "swath: totals of the dataset "+ds+", not a live listing: "+root+" as listed ") || !strings.Contains(s, "; complete\n") {
		t.Fatalf("stderr: %s", s)
	}
	got, err = duRun(t, "--from-dataset", ds, "--format", "jsonl")
	var row struct {
		Dataset provenance `json:"dataset"`
	}
	if err != nil || json.Unmarshal([]byte(got), &row) != nil || row.Dataset.Path != ds || *row.Dataset.Source != root || !*row.Dataset.Complete ||
		row.Dataset.SchemaID != "swath/local-entries/v1" || row.Dataset.ListingFinishedAt == nil {
		t.Fatalf("JSONL: %v %s", err, got)
	}
	// -q leaves stderr empty.
	stderr.Reset()
	if _, err := duRun(t, "--from-dataset", ds, "-q"); err != nil || stderr.Len() != 0 {
		t.Fatalf("-q: %v %q", err, stderr.String())
	}
	// A dataset directory given as SOURCE is a tree: its own files.
	var files int
	if err := filepath.WalkDir(ds, func(_ string, e fs.DirEntry, err error) error {
		if err == nil && e.Type().IsRegular() {
			files++
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err = duRun(t, ds, "-d", "1", "--format", "tsv")
	if err != nil || !strings.HasSuffix(got, "\tdata/\n") || !strings.Contains(got, "\n0\t"+strconv.Itoa(files)+"\t") {
		t.Fatalf("du DIR: %v %d files\n%s", err, files, got)
	}
}

// TestDuFromDatasetObjectStore: an object store's dataset totals as swath
// du of the source and of a prefix of it (PREFIX), filters included; a
// PREFIX outside the dataset's scope is refused, and the local totals'
// options are refused on a schema without their columns.
func TestDuFromDatasetObjectStore(t *testing.T) {
	captureStderr(t)
	keys := []string{"logs/2026/01/a", "logs/2026/01/b/c", "logs/2026/02/d", "logs/2026/top", "logs/2027/x", "other/y"}
	end := s3test.Bucket(t, keys)
	store := []string{"--endpoint-url", end, "--anonymous"}
	ds, logs := filepath.Join(t.TempDir(), "ds"), filepath.Join(t.TempDir(), "logs")
	for _, l := range []struct{ src, dir string }{{"s3://bkt", ds}, {"s3://bkt/logs/", logs}} {
		if err := run(context.Background(), append([]string{l.src, "-o", l.dir, "--no-stats"}, store...), io.Discard, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		src, dir, prefix string
		flags            []string
	}{
		{"s3://bkt", ds, "", []string{"-d", "2"}},
		{"s3://bkt", ds, "", []string{"-d", "3", "--sort", "size", "--format", "jsonl"}},
		{"s3://bkt/logs/2026/", ds, "logs/2026/", []string{"-d", "1"}},
		{"s3://bkt/logs/2026/0", ds, "logs/2026/0", []string{"-d", "1"}},
		{"s3://bkt/logs", ds, "logs", []string{"-d", "1"}},
		{"s3://bkt", ds, "", []string{"-d", "2", "--include", "logs/**", "--exclude", "*/02/*"}},
		{"s3://bkt", ds, "", []string{"-d", "1", "--storage-class", "STANDARD"}},
		{"s3://bkt/logs/", logs, "", []string{"-d", "2", "--sort", "size"}},
		{"s3://bkt/logs/2027/", logs, "logs/2027/", []string{"-d", "1", "--max-size", "11"}},
	} {
		saved := []string{"--from-dataset", tc.dir}
		if tc.prefix != "" {
			saved = append(saved, tc.prefix)
		}
		flags := append([]string{"--format", "tsv"}, tc.flags...) // the last --format wins
		liveAndSaved(t, append(append([]string{tc.src}, store...), flags...), append(saved, flags...))
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--from-dataset", logs, "other/"}, `PREFIX "other/" is not inside the dataset's scope`},
		{[]string{"--from-dataset", logs, "lo"}, `PREFIX "lo" is not inside the dataset's scope`},
		{[]string{"--from-dataset", ds, "--allocated"}, "--allocated: the dataset's schema swath/s3-objects/v1 has no allocated_bytes column"},
		{[]string{"--from-dataset", ds, "--count-links-once"}, "--count-links-once: the dataset's schema swath/s3-objects/v1 has no nlink column"},
	} {
		out, err := duRun(t, tc.args...)
		if u := (usageError{}); !errors.As(err, &u) || !strings.Contains(err.Error(), tc.want) || out != "" {
			t.Errorf("%q: %v, stdout %q", tc.args, err, out)
		}
	}
}

// TestDuFromDatasetRefusals: an option that shapes a listing, or that the
// dataset cannot answer, is a usage error naming it, as is a dataset in a
// bucket, a URL as PREFIX, and a directory that holds no committed
// dataset (an interrupted run's says how to finish it); nothing is
// printed on stdout.
func TestDuFromDatasetRefusals(t *testing.T) {
	captureStderr(t)
	root, _ := depthTree(t)
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{root, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	interrupted := t.TempDir()
	if err := os.MkdirAll(filepath.Join(interrupted, dataset.JournalDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(interrupted, dataset.JournalDir, "journal"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--anonymous"}, "--anonymous: not with --from-dataset"},
		{[]string{"--endpoint-url", "http://127.0.0.1:9"}, "--endpoint-url: not with --from-dataset"},
		{[]string{"--report", filepath.Join(t.TempDir(), "r.json")}, "--report: not with --from-dataset"},
		{[]string{"--workers", "2"}, "--workers: not with --from-dataset"},
		{[]string{"--max-duration", "1m"}, "--max-duration: not with --from-dataset"},
		{[]string{"--metadata", "names"}, "--metadata: not with --from-dataset"},
		{[]string{"--all-versions"}, "--all-versions: not with --from-dataset"},
		{[]string{"--seed-from", ds}, "--seed-from: not with --from-dataset"},
		{[]string{"--stats"}, "--stats: not with --from-dataset"},
		{[]string{"-o", "x"}, "--output: not with --from-dataset"},
		{[]string{"--storage-class", "STANDARD"}, "--storage-class: the dataset's schema swath/local-entries/v1 has no storage class column"},
		{[]string{"--format", "csv"}, `--format "csv": want auto, table, tsv or jsonl`},
		{[]string{"a/", "b/"}, `unexpected argument "b/"`},
		{[]string{"s3://bkt/logs/"}, `PREFIX "s3://bkt/logs/": a key prefix within the dataset`},
	} {
		out, err := duRun(t, append([]string{"--from-dataset", ds}, tc.args...)...)
		if u := (usageError{}); !errors.As(err, &u) || !strings.Contains(err.Error(), tc.want) || out != "" {
			t.Errorf("%q: %v, stdout %q", tc.args, err, out)
		}
	}
	for dir, want := range map[string]string{
		"s3://bkt/ds":                      "a dataset in a bucket is not supported yet",
		filepath.Join(root, "nonexistent"): "no such directory",
		root:                               "no dataset (no manifest.json)",
		interrupted:                        "an interrupted run, not a committed dataset: swath show " + interrupted + " describes it, swath resume " + interrupted + " finishes it",
	} {
		out, err := duRun(t, "--from-dataset", dir)
		if u := (usageError{}); !errors.As(err, &u) || !strings.Contains(err.Error(), want) || out != "" {
			t.Errorf("%s: %v, stdout %q", dir, err, out)
		}
	}
	// --from-dataset is swath du's.
	if err := run(context.Background(), []string{"--from-dataset", ds, root}, io.Discard, false); err == nil || !strings.Contains(err.Error(), "--from-dataset has no meaning for swath list") {
		t.Errorf("swath list --from-dataset: %v", err)
	}
}

// TestDuFromDatasetIncomplete: a dataset whose listing did not complete (a
// key limit) prints its totals, says INCOMPLETE on stderr and in the JSONL
// rows, and exits 1 (incomplete), as the run that wrote it; one that
// skipped entries exits 0 and warns.
func TestDuFromDatasetIncomplete(t *testing.T) {
	stderr := captureStderr(t)
	end := s3test.Bucket(t, s3test.Keys(30, 3))
	ds := filepath.Join(t.TempDir(), "ds")
	err := run(context.Background(), []string{"s3://bkt", "--endpoint-url", end, "--anonymous", "--sequential", "--max-keys", "10", "-o", ds, "--no-stats"}, io.Discard, true)
	if c := exitCode(t, err); c != exitFatal {
		t.Fatalf("capped run: exit %d (%v)", c, err)
	}
	stderr.Reset()
	got, err := duRun(t, "--from-dataset", ds, "--format", "tsv")
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != exitFatal || ee.out.Class != listing.ClassIncomplete || !strings.Contains(err.Error(), "the dataset is a partial listing: its listing did not complete (max_keys)") {
		t.Fatalf("exit: %v", err)
	}
	if !strings.HasPrefix(got, "depth\tobjects\tsize\tlast_modified\tprefix\n0\t10\t") {
		t.Fatalf("stdout: %q", got)
	}
	if s := stderr.String(); !strings.Contains(s, "; INCOMPLETE (max_keys)\n") {
		t.Fatalf("stderr: %s", s)
	}
	got, _ = duRun(t, "--from-dataset", ds, "--format", "jsonl")
	var row struct {
		Dataset provenance `json:"dataset"`
	}
	if json.Unmarshal([]byte(got), &row) != nil || *row.Dataset.Complete || *row.Dataset.StopReason != "max_keys" {
		t.Fatalf("JSONL: %s", got)
	}

	// Skipped entries, as the manifest records them.
	root, _ := depthTree(t)
	local := filepath.Join(t.TempDir(), "local")
	if err := run(context.Background(), []string{root, "-o", local, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(local, dataset.ManifestName)
	var m map[string]any
	readJSON(t, path, &m)
	m["extensions"].(map[string]any)["swath"].(map[string]any)["coverage"].(map[string]any)["skipped_entries"] = 2
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	if _, err := duRun(t, "--from-dataset", local); err != nil {
		t.Fatal(err)
	}
	if s := stderr.String(); !strings.Contains(s, "; complete, 2 entries skipped") ||
		!strings.Contains(s, "swath: warning: 2 entries could not be read when the dataset was listed: the totals leave them out\n") {
		t.Fatalf("stderr: %s", s)
	}
}

// noKindRow is a swath/local-entries/v1 row without the kind column,
// which every part of that schema has.
type noKindRow struct {
	Key          []byte `parquet:"key"`
	Size         *int64 `parquet:"size,optional"`
	LastModified *int64 `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
}

type parquetRows struct{ rows []parquet.Row }

func (r *parquetRows) ReadRows(out []parquet.Row) (int, error) {
	n := copy(out, r.rows)
	if r.rows = r.rows[n:]; len(r.rows) == 0 {
		return n, io.EOF
	}
	return n, nil
}

// TestDuFromDatasetRefusesBrokenSchema: a committed dataset whose parts
// break its schema id's columns (a local tree's without kind, so a
// directory would count as a file) is refused before any row is totalled:
// exit 1, the column named, nothing on stdout.
func TestDuFromDatasetRefusesBrokenSchema(t *testing.T) {
	captureStderr(t)
	size, at := int64(4096), int64(1_767_000_000_000_000)
	s := parquet.SchemaOf(new(noKindRow))
	rows := &parquetRows{rows: []parquet.Row{s.Deconstruct(nil, &noKindRow{Key: []byte("dir/"), Size: &size, LastModified: &at})}}
	ext, err := json.Marshal(map[string]any{reader.Extension: map[string]any{"version": reader.MetadataVersion,
		"source": map[string]any{"kind": "local-fs", "uri": "/data"}, "scope": map[string]any{"kind": "whole"},
		"coverage": map[string]any{"complete": true, "stop_reason": "completed", "skipped_entries": 0, "exclusions": []string{}, "filters": []string{}}}})
	if err != nil {
		t.Fatal(err)
	}
	ds := filepath.Join(t.TempDir(), "ds")
	if err := local.WriteDir(context.Background(), ds, s, rows, local.DatasetOptions{Extensions: ext, AllowUnsynced: true,
		Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: string(reader.LocalV1)}}); err != nil {
		t.Fatal(err)
	}
	out, err := duRun(t, "--from-dataset", ds, "--format", "tsv")
	if exitOf(err) != exitFatal || !strings.Contains(err.Error(), "does not hold schema swath/local-entries/v1") || !strings.Contains(err.Error(), "kind") || out != "" {
		t.Fatalf("exit %d (%v), stdout %q", exitOf(err), err, out)
	}
}

// TestDuSaveHint: a live swath du that listed hintRows entries or more ends
// its statistics on a terminal with the commands that save its SOURCE
// (quoted) once and total that, at its depth and order; not below the
// threshold, with -q, --no-stats, json logs or a stderr that is no
// terminal, nor after --from-dataset or swath list.
func TestDuSaveHint(t *testing.T) {
	stderr := captureSyncStderr(t)
	was, rows := stderrTerminal, hintRows
	tty := true
	stderrTerminal = func() (bool, int) { return tty, 120 }
	t.Cleanup(func() { stderrTerminal, hintRows = was, rows })
	parent := t.TempDir()
	root := filepath.Join(parent, "my tree")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"a/x", "a/y", "b/z"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(f)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, f), []byte("1"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ds := filepath.Join(parent, "ds")
	if err := run(context.Background(), []string{root, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	hintRows = 5 // the tree's 5 entries: 3 files, 2 directories
	want := "hint: this run listed 5 entries; to explore them again without relisting, save an inventory once and total that:\n" +
		"hint:   swath list '" + root + "' -o DIR\n" +
		"hint:   swath du --from-dataset DIR -d 1 --sort size\n"
	try := func(name string, shown bool, cmd func() error) {
		t.Helper()
		stderr.b.Reset()
		if err := cmd(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := strings.Contains(stderr.String(), "hint:"); got != shown || shown && !strings.HasSuffix(stderr.String(), want) {
			t.Errorf("%s: hint %v, want %v:\n%s", name, got, shown, stderr.String())
		}
	}
	du := func(args ...string) func() error {
		return func() error {
			return runDu(context.Background(), append([]string{root, "-d", "1", "--sort", "size", "--format", "tsv", "--color", "never"}, args...), io.Discard)
		}
	}
	try("at the threshold", true, du("--stats", "--no-progress"))
	try("-q", false, du("--stats", "-q"))
	try("--no-stats", false, du("--no-stats"))
	try("json logs", false, du("--stats", "--log-format", "json"))
	try("from a dataset", false, func() error {
		_, err := duRun(t, "--from-dataset", ds, "-d", "1", "--sort", "size")
		return err
	})
	try("swath list", false, func() error {
		return run(context.Background(), []string{root, "--stats", "--no-progress", "--color", "never"}, io.Discard, false)
	})
	tty = false
	try("not a terminal", false, du("--stats"))
	tty, hintRows = true, 6
	try("below the threshold", false, du("--stats", "--no-progress"))
}

// TestDuFromDatasetManyParts: a dataset of many parts, scanned by parallel
// shards, totals as the live listing does, whole and under a PREFIX.
func TestDuFromDatasetManyParts(t *testing.T) {
	captureStderr(t)
	end := s3test.Bucket(t, s3test.Keys(3000, 7))
	store := []string{"--endpoint-url", end, "--anonymous"}
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), append([]string{"s3://bkt", "-o", ds, "--part-size", "4KiB", "--no-stats"}, store...), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	if readJSON(t, filepath.Join(ds, dataset.ManifestName), &m); len(m.Parts) < 8 {
		t.Fatalf("%d parts", len(m.Parts))
	}
	for _, flags := range [][]string{{"-d", "1"}, {"-d", "2", "--sort", "size", "--format", "jsonl"}, {"-d", "1", "--exclude", "p03/**"}} {
		liveAndSaved(t, append(append([]string{"s3://bkt"}, store...), flags...), append([]string{"--from-dataset", ds}, flags...))
	}
	liveAndSaved(t, append([]string{"s3://bkt/p04/", "-d", "1"}, store...), []string{"--from-dataset", ds, "p04/", "-d", "1"})
}
