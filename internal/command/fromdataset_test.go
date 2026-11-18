// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// unnamedTotal is a table with its total row's name left out.
func unnamedTotal(table string) string {
	var b strings.Builder
	for l := range strings.Lines(table) {
		if strings.HasSuffix(l, " (total)\n") {
			l = l[:strings.LastIndex(strings.TrimSuffix(l, " (total)\n"), "  ")] + "  (total)\n"
		}
		b.WriteString(l)
	}
	return b.String()
}

// duRun runs swath du with args and returns its stdout.
func duRun(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runDu(context.Background(), append(args, "--no-stats"), &out)
	return out.String(), err
}

// liveAndSaved runs swath du on a source (live) and on its dataset (saved)
// and fails unless their rows are the same: TSV byte for byte, JSONL with
// the saved rows' dataset member, last, removed, the table but for what
// its total row is named by (the SOURCE or DIR typed).
func liveAndSaved(t *testing.T, live, saved []string) {
	t.Helper()
	want, err := duRun(t, live...)
	if err != nil {
		t.Fatalf("%q: %v", live, err)
	}
	want = unnamedTotal(want)
	got, err := duRun(t, saved...)
	if err != nil {
		t.Fatalf("%q: %v", saved, err)
	}
	var stripped strings.Builder
	for l := range strings.Lines(unnamedTotal(got)) {
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
// the tree prints, for every depth, order and format, with either primary
// figure, hard links counted once and filters, and from a dataset listed
// at the names tier (no sizes, blocks or links) what the live names-tier
// run prints; plain swath du of the dataset directory still totals its
// files.
func TestDuFromDatasetLocal(t *testing.T) {
	stderr := captureStderr(t)
	root, _ := depthTree(t)
	if err := os.Link(filepath.Join(root, "a", "g"), filepath.Join(root, "c", "g-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a", "gg"), []byte("123"), 0o644); err != nil { // a/g is a prefix of its key
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Symlink("b", filepath.Join(root, "a", "to-b")); err != nil {
			t.Fatal(err)
		}
	}
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{root, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	extras := [][]string{nil, {"--exclude", "a/**"}, {"--min-size", "2", "--include", "*/*"}, {"--modified-since", "2026-01-02T03:30:00Z"}, {"--apparent-size"}}
	if runtime.GOOS != "windows" { // Windows reads no allocated bytes, link count or inode
		extras = append(extras, []string{"--allocated"}, []string{"-l"})
	}
	for _, extra := range extras {
		for depth := range 4 {
			for _, sort := range []string{"name", "size"} {
				for _, format := range []string{"tsv", "jsonl", "table"} {
					flags := append([]string{"-d", strconv.Itoa(depth), "--sort", sort, "--format", format}, extra...)
					liveAndSaved(t, append([]string{root}, flags...), append([]string{"--from-dataset", ds}, flags...))
				}
			}
		}
	}
	names := filepath.Join(t.TempDir(), "names")
	if err := run(context.Background(), []string{root, "-o", names, "--metadata", "names", "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	for depth := range 3 {
		for _, format := range []string{"tsv", "jsonl", "table"} {
			flags := []string{"-d", strconv.Itoa(depth), "--format", format}
			liveAndSaved(t, append([]string{root, "--metadata", "names"}, flags...), append([]string{"--from-dataset", names}, flags...))
		}
	}
	// With nothing counted (an empty tree, every object filtered out) the
	// names tier's links_once is still the live run's: the manifest
	// records the tier.
	empty, emptyNames := t.TempDir(), filepath.Join(t.TempDir(), "empty")
	if err := run(context.Background(), []string{empty, "-o", emptyNames, "--metadata", "names", "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var em dataset.Manifest
	if readJSON(t, filepath.Join(emptyNames, dataset.ManifestName), &em); em.Collected.Metadata != "names" {
		t.Fatalf("manifest collected %+v", em.Collected)
	}
	for _, format := range []string{"tsv", "jsonl"} {
		liveAndSaved(t, []string{empty, "--metadata", "names", "--format", format}, []string{"--from-dataset", emptyNames, "--format", format})
		liveAndSaved(t, []string{root, "--metadata", "names", "--include", "none", "--format", format},
			[]string{"--from-dataset", names, "--include", "none", "--format", format})
	}
	liveAndSaved(t, []string{root, "-d", "1", "--no-header"}, []string{"--from-dataset", ds, "-d", "1", "--no-header"})
	liveAndSaved(t, []string{root, "--max-depth", "1"}, []string{"--from-dataset", ds, "--max-depth", "1"}) // GNU du's spelling
	// SOURCE, a path below the tree, totals as swath du of that directory:
	// its keys below it, the depth counted from it, its filters anchored
	// there. The tree itself, in any spelling of its path, is the whole
	// dataset.
	sub := filepath.Join(root, "a")
	below := []string{sub, sub + string(filepath.Separator)}
	whole := []string{root, filepath.Join(sub, "..")}
	if runtime.GOOS == "windows" { // the same paths, as Windows takes them: the root in any case, the names below it as listed
		below = append(below, filepath.ToSlash(sub), filepath.Join(strings.ToUpper(root), "a"))
		whole = append(whole, filepath.ToSlash(root), strings.ToUpper(root))
	}
	for _, a := range below {
		for _, format := range []string{"table", "tsv", "jsonl"} {
			for _, extra := range [][]string{{"-d", "0"}, {"-d", "2"}, {"-d", "1", "--sort", "size"}, {"-d", "2", "--exclude", "b/**"}} {
				flags := append([]string{"--format", format}, extra...)
				liveAndSaved(t, append([]string{sub}, flags...), append([]string{"--from-dataset", ds, a}, flags...))
			}
		}
	}
	for _, a := range whole {
		liveAndSaved(t, []string{root, "-d", "2"}, []string{"--from-dataset", ds, a, "-d", "2"})
	}
	// A file below the tree totals as swath du of that file: its row alone,
	// not the keys its name is a prefix of.
	for _, file := range []string{filepath.Join(root, "a", "g"), filepath.Join(root, "x"), filepath.Join(root, "c", "g-link")} {
		for _, format := range []string{"table", "tsv", "jsonl"} {
			for _, extra := range [][]string{{"-d", "0"}, {"-d", "2"}, {"--include", "[gx]*"}} { // a filter that keeps the file: on Windows an empty live total has no link counts
				flags := append([]string{"--format", format}, extra...)
				liveAndSaved(t, append([]string{file}, flags...), append([]string{"--from-dataset", ds, file}, flags...))
			}
		}
	}
	// A symbolic link is refused: a live swath du follows it to a
	// directory, whose rows the dataset holds under the target's name.
	if runtime.GOOS != "windows" {
		link := filepath.Join(root, "a", "to-b")
		if out, err := duRun(t, "--from-dataset", ds, link); !errors.As(err, new(usageError)) || !strings.Contains(err.Error(), "a symbolic link in the dataset") || out != "" {
			t.Fatalf("symbolic link SOURCE: %v\n%s", err, out)
		}
	}
	// A relative path is taken from the working directory, as a live
	// swath du takes it: the listed tree's own name there is the whole.
	t.Chdir(filepath.Dir(root))
	liveAndSaved(t, []string{root, "-d", "1"}, []string{"--from-dataset", ds, filepath.Base(root), "-d", "1"})
	// A directory below the tree that holds no key warns and exits 0, as a
	// live listing of an empty prefix.
	stderr.Reset()
	if got, err := duRun(t, "--from-dataset", ds, filepath.Join(root, "gone"), "--format", "tsv"); err != nil || !strings.Contains(got, "\ntotal\t0\t0\t0\t") ||
		!strings.Contains(stderr.String(), `swath: warning: no key of the dataset starts with "gone/"`) {
		t.Fatalf("empty SOURCE: %v\n%s\nstderr: %s", err, got, stderr.String())
	}
	// Stderr says it is the dataset, its source (as its manifest records
	// the root) and coverage, not a live listing; JSONL rows carry it.
	var m dataset.Manifest
	readJSON(t, filepath.Join(ds, dataset.ManifestName), &m)
	src := m.Source.URI
	if s := stderr.String(); src == "" || !strings.Contains(s, "swath: totals of the dataset "+ds+", not a live listing: "+src+" as listed ") || !strings.Contains(s, "; complete\n") {
		t.Fatalf("stderr: %s", s)
	}
	got, err := duRun(t, "--from-dataset", ds, "--format", "jsonl")
	var row struct {
		Dataset provenance `json:"dataset"`
	}
	if err != nil || json.Unmarshal([]byte(got), &row) != nil || row.Dataset.Path != ds || *row.Dataset.Source != src || !*row.Dataset.Complete ||
		row.Dataset.SchemaID != "swath/local-entries/v1" || row.Dataset.ListingFinishedAt == nil {
		t.Fatalf("JSONL: %v %s", err, got)
	}
	// A filtered dataset's rows carry its filters as its manifest records
	// them, one object each; stderr names them.
	filtered := filepath.Join(t.TempDir(), "filtered")
	if err := run(context.Background(), []string{root, "-o", filtered, "--exclude", "x", "--no-dirs", "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	got, err = duRun(t, "--from-dataset", filtered, "--format", "jsonl")
	if want := `"filters":[{"filter":"no_dirs","value":null},{"filter":"exclude","value":"x"}]`; err != nil || !strings.Contains(got, want) ||
		!strings.Contains(stderr.String(), "; filtered: no_dirs, exclude=x") {
		t.Fatalf("filtered JSONL: %v %s\nstderr: %s", err, got, stderr.String())
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
	if err != nil || !strings.HasSuffix(got, "\tdata/\n") || !strings.Contains(got, "\ntotal\t0\t"+strconv.Itoa(files)+"\t") {
		t.Fatalf("du DIR: %v %d files\n%s", err, files, got)
	}
}

// TestDuFromDatasetObjectStore: an object store's dataset totals as swath
// du of the source and of a URL below it (SOURCE), filters included; any
// other SOURCE is refused naming the dataset's, and the local totals'
// options are refused on a schema without their columns.
func TestDuFromDatasetObjectStore(t *testing.T) {
	stderr := captureStderr(t)
	keys := []string{"logs/2026/01/a", "logs/2026/01/b/c", "logs/2026/02/d", "logs/2026/top", "logs/2027/x", "other/y"}
	end := s3test.Bucket(t, keys)
	store := []string{"--endpoint-url", end, "--anonymous"}
	ds, logs := filepath.Join(t.TempDir(), "ds"), filepath.Join(t.TempDir(), "logs")
	for _, l := range []struct{ src, dir string }{{"s3://bkt", ds}, {"s3://bkt/logs/", logs}} {
		if err := run(context.Background(), append([]string{l.src, "-o", l.dir, "--no-stats"}, store...), io.Discard, false); err != nil {
			t.Fatal(err)
		}
	}
	listed := map[string]string{ds: "s3://bkt", logs: "s3://bkt/logs/"}
	for _, tc := range []struct {
		src, dir string
		flags    []string
	}{
		{"s3://bkt", ds, []string{"-d", "2"}},
		{"s3://bkt", ds, []string{"-d", "3", "--sort", "size", "--format", "jsonl"}},
		{"s3://bkt/logs/2026/", ds, []string{"-d", "1"}},
		{"s3://bkt/logs/2026/0", ds, []string{"-d", "1"}},
		{"s3://bkt/logs", ds, []string{"-d", "1"}},
		{"S3://bkt/logs/", ds, []string{"-d", "1"}},
		{"s3://bkt", ds, []string{"-d", "2", "--include", "logs/**", "--exclude", "*/02/*"}},
		{"s3://bkt", ds, []string{"-d", "1", "--storage-class", "STANDARD"}},
		{"s3://bkt/logs/", logs, []string{"-d", "2", "--sort", "size"}},
		{"s3://bkt/logs/2027/", logs, []string{"-d", "1", "--max-size", "11"}},
	} {
		flags := append([]string{"--format", "tsv"}, tc.flags...) // the last --format wins
		live := append(append([]string{tc.src}, store...), flags...)
		liveAndSaved(t, live, append([]string{"--from-dataset", tc.dir, tc.src}, flags...))
		if tc.src == listed[tc.dir] {
			liveAndSaved(t, live, append([]string{"--from-dataset", tc.dir}, flags...))
		}
	}
	// A URL below the source that holds no key warns and exits 0, as a
	// live listing of an empty prefix.
	stderr.Reset()
	if got, err := duRun(t, "--from-dataset", logs, "s3://bkt/logs/2028/", "--format", "tsv"); err != nil || !strings.Contains(got, "\ntotal\t0\t0\t0\t") ||
		!strings.Contains(stderr.String(), `swath: warning: no key of the dataset starts with "logs/2028/"`) {
		t.Fatalf("empty SOURCE: %v\n%s\nstderr: %s", err, got, stderr.String())
	}
	refused := func(arg string) string {
		return fmt.Sprintf("%q: %s was listed from s3://bkt/logs/; give a URL below it, or no argument", arg, logs)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--from-dataset", logs, "s3://bkt/other/"}, refused("s3://bkt/other/")},
		{[]string{"--from-dataset", logs, "s3://bkt/lo"}, refused("s3://bkt/lo")},
		{[]string{"--from-dataset", logs, "s3://bkt"}, refused("s3://bkt")},
		{[]string{"--from-dataset", logs, "s3://bkt2/logs/"}, refused("s3://bkt2/logs/")},
		{[]string{"--from-dataset", logs, "gs://bkt/logs/"}, refused("gs://bkt/logs/")},
		{[]string{"--from-dataset", logs, "logs/2026/"}, refused("logs/2026/")}, // a key prefix is not SOURCE's form
		{[]string{"--from-dataset", ds, "--allocated"}, "--allocated: the dataset's schema swath/s3-objects/v1 has no allocated_bytes column"},
		{[]string{"--from-dataset", ds, "--count-links"}, "--count-links: the dataset's schema swath/s3-objects/v1 has no nlink column"},
	} {
		out, err := duRun(t, tc.args...)
		if u := (usageError{}); !errors.As(err, &u) || !strings.Contains(err.Error(), tc.want) || out != "" {
			t.Errorf("%q: %v, stdout %q", tc.args, err, out)
		}
	}
}

// TestDuFromDatasetRefusals: an option that shapes a listing, or that the
// dataset cannot answer, is a usage error naming it, as is a dataset in a
// bucket, a SOURCE that is not the dataset's tree or below it, and a directory that holds no committed
// dataset (an interrupted run's says how to finish it); nothing is
// printed on stdout.
func TestDuFromDatasetRefusals(t *testing.T) {
	captureStderr(t)
	root, _ := depthTree(t)
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{root, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	outside := func(arg string) string {
		return fmt.Sprintf("%q: %s was listed from %s; give a path below it, or no argument", arg, ds, root)
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
		{[]string{"--allocated", "--apparent-size"}, "--allocated and --apparent-size each choose the figure that comes first"},
		{[]string{"--no-header", "--format", "jsonl"}, "not with JSONL"},
		{[]string{"a/", "b/"}, `unexpected argument "b/"`},
		// SOURCE is the tree or a path below it: the dataset's own
		// directory, the tree's parent, a sibling sharing its name's start,
		// a URL and a key prefix (outside the tree) are refused, naming it.
		{[]string{ds}, outside(ds)},
		{[]string{filepath.Dir(root)}, outside(filepath.Dir(root))},
		{[]string{root + "x"}, outside(root + "x")},
		{[]string{"s3://bkt/logs/"}, outside("s3://bkt/logs/")},
		{[]string{"a/"}, outside("a/")},
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
// key limit) prints its totals, says partial on stderr and in the JSONL
// rows, and exits 3 (incomplete), as the run that wrote it; so does one
// whose listing left out entries it could not read (it exited 0 and
// warned).
func TestDuFromDatasetIncomplete(t *testing.T) {
	stderr := captureStderr(t)
	end := s3test.Bucket(t, s3test.Keys(30, 3))
	ds := filepath.Join(t.TempDir(), "ds")
	err := run(context.Background(), []string{"s3://bkt", "--endpoint-url", end, "--anonymous", "--sequential", "--max-keys", "10", "-o", ds, "--no-stats"}, io.Discard, true)
	if c := exitCode(t, err); c != exitIncomplete {
		t.Fatalf("capped run: exit %d (%v)", c, err)
	}
	stderr.Reset()
	got, err := duRun(t, "--from-dataset", ds, "--format", "tsv")
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != exitIncomplete || ee.out.Class != inventory.ClassIncomplete || !strings.Contains(err.Error(), "the dataset is a partial listing: its listing did not complete (max_keys)") {
		t.Fatalf("exit: %v", err)
	}
	if !strings.HasPrefix(got, "row_type\tdepth\tobjects\tsize\tsize_missing\tallocated_bytes\tallocated_bytes_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\ntotal\t0\t10\t") {
		t.Fatalf("stdout: %q", got)
	}
	if s := stderr.String(); !strings.Contains(s, "; partial: --max-keys reached\n") {
		t.Fatalf("stderr: %s", s)
	}
	got, _ = duRun(t, "--from-dataset", ds, "--format", "jsonl")
	var row struct {
		Dataset provenance `json:"dataset"`
	}
	if json.Unmarshal([]byte(got), &row) != nil || *row.Dataset.Complete || *row.Dataset.StopReason != "max_keys" {
		t.Fatalf("JSONL: %s", got)
	}

	// Unreadable entries, as the manifest records them.
	root, _ := depthTree(t)
	local := filepath.Join(t.TempDir(), "local")
	if err := run(context.Background(), []string{root, "-o", local, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(local, dataset.ManifestName)
	var m map[string]any
	readJSON(t, path, &m)
	m["extensions"].(map[string]any)["swath"].(map[string]any)["coverage"].(map[string]any)["unreadable"] = 2
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	got, err = duRun(t, "--from-dataset", local, "--format", "tsv")
	if !errors.As(err, &ee) || ee.code != exitIncomplete || ee.out.Class != inventory.ClassIncomplete ||
		err.Error() != "2 entries could not be read when the dataset was listed: the totals leave them out" || !strings.HasPrefix(got, "row_type\t") {
		t.Fatalf("unreadable: %v, stdout %q", err, got)
	}
	if s := stderr.String(); !strings.Contains(s, "; partial: 2 unreadable entries") {
		t.Fatalf("stderr: %s", s)
	}
	got, _ = duRun(t, "--from-dataset", local, "--format", "jsonl")
	if json.Unmarshal([]byte(strings.SplitN(got, "\n", 2)[0]), &row) != nil || row.Dataset.Unreadable == nil || *row.Dataset.Unreadable != 2 {
		t.Fatalf("JSONL: %s", got)
	}
}

// cancelWriter is stdout that stops the command (cancel, as SIGINT does)
// on its first write, then takes the rows.
type cancelWriter struct {
	bytes.Buffer
	cancel context.CancelCauseFunc
}

func (w *cancelWriter) Write(p []byte) (int, error) {
	w.cancel(&signalError{syscall.SIGINT})
	return w.Buffer.Write(p)
}

// TestDuFromDatasetInterruptedOutput: a signal while the totals are being
// written ends swath du --from-dataset interrupted (130), as a live swath
// du of the same tree, not completed.
func TestDuFromDatasetInterruptedOutput(t *testing.T) {
	captureStderr(t)
	root, _ := depthTree(t)
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{root, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"--from-dataset", ds}, {root}} {
		ctx, cancel := context.WithCancelCause(context.Background())
		w := &cancelWriter{cancel: cancel}
		err := runDu(ctx, append(args, "-d", "1", "--format", "tsv", "--no-stats"), w)
		if exitOf(err) != exitInterrupted || !strings.HasPrefix(w.String(), "row_type\t") {
			t.Errorf("%q: exit %d (%v), stdout %q", args, exitOf(err), err, w.String())
		}
		cancel(nil)
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
		"source": map[string]any{"kind": "local", "uri": "/data"}, "scope": map[string]any{"kind": "whole"},
		"coverage": map[string]any{"complete": true, "stop_reason": "end_of_scope", "unreadable": 0, "exclusions": []string{}, "filters": []string{}}}})
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

// unblockedRow is a local tree's row as a listing that reads no blocks,
// links or inodes writes it (Windows): every column present, those null.
type unblockedRow struct {
	Key            []byte  `parquet:"key"`
	Size           *int64  `parquet:"size,optional"`
	LastModified   *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	Kind           string  `parquet:"kind"`
	Mode           *uint32 `parquet:"mode,optional,uint(32)"`
	UID            *uint32 `parquet:"uid,optional,uint(32)"`
	GID            *uint32 `parquet:"gid,optional,uint(32)"`
	Nlink          *uint32 `parquet:"nlink,optional,uint(32)"`
	Inode          *uint64 `parquet:"inode,optional,uint(64)"`
	Dev            *uint64 `parquet:"dev,optional,uint(64)"`
	AllocatedBytes *int64  `parquet:"allocated_bytes,optional"`
	ChangedAt      *int64  `parquet:"metadata_changed_at,optional,timestamp(microsecond:utc)"`
	AccessedAt     *int64  `parquet:"accessed_at,optional,timestamp(microsecond:utc)"`
	CreatedAt      *int64  `parquet:"created_at,optional,timestamp(microsecond:utc)"`
	LinkTarget     []byte  `parquet:"link_target,optional"`
}

// TestDuFromDatasetWithoutBlocks: a local tree's dataset listed at a stat
// tier where blocks are not read (Windows: the column there, its values
// null) totals by size, as the live run there does, the whole count by
// the one figure: also where some files have blocks and others do not, and
// where the filters keep no file but a directory, which has none either.
// --allocated, which requires them, fails.
func TestDuFromDatasetWithoutBlocks(t *testing.T) {
	captureStderr(t)
	at, five, three, block := int64(1_767_000_000_000_000), int64(5), int64(3), int64(4096)
	s := parquet.SchemaOf(new(unblockedRow))
	ext, err := json.Marshal(map[string]any{reader.Extension: map[string]any{"version": reader.MetadataVersion,
		"source": map[string]any{"kind": "local", "uri": "/data"}, "scope": map[string]any{"kind": "whole"},
		"collected": map[string]any{"metadata": "stat", "columns": []string{}},
		"coverage":  map[string]any{"complete": true, "stop_reason": "end_of_scope", "unreadable": 0, "exclusions": []string{}, "filters": []string{}}}})
	if err != nil {
		t.Fatal(err)
	}
	for name, blocks := range map[string]*int64{"none": nil, "mixed": &block} {
		rows := &parquetRows{rows: []parquet.Row{
			s.Deconstruct(nil, &unblockedRow{Key: []byte("a/"), Kind: "dir", LastModified: &at}),
			s.Deconstruct(nil, &unblockedRow{Key: []byte("a/f"), Kind: "file", Size: &five, LastModified: &at, AllocatedBytes: blocks}),
			s.Deconstruct(nil, &unblockedRow{Key: []byte("b"), Kind: "file", Size: &three, LastModified: &at}),
		}}
		ds := filepath.Join(t.TempDir(), name)
		if err := local.WriteDir(context.Background(), ds, s, rows, local.DatasetOptions{Extensions: ext, AllowUnsynced: true,
			Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: string(reader.LocalV1)}}); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"-d", "1"}, {"-d", "1", "--sort", "size"}, {"-d", "1", "-A"}} {
			out, err := duRun(t, append([]string{"--from-dataset", ds, "--format", "tsv"}, args...)...)
			rows := duTSV(t, out)
			if err != nil || len(rows) != 2 { // the total and a/; b is the total's alone
				t.Fatalf("%s %q: %v\n%s", name, args, err, out)
			}
			for _, r := range rows {
				if r["primary"] != "size" || r["prefix"] == "" && r["size"] != "8" {
					t.Fatalf("%s %q: %v", name, args, r)
				}
			}
		}
		var nb *totals.NoAllocatedError
		if out, err := duRun(t, "--from-dataset", ds, "--allocated", "--format", "tsv"); !errors.As(err, &nb) || out != "" {
			t.Fatalf("%s --allocated: %v, stdout %q", name, err, out)
		}
		// Every file filtered out, its directories still say so: no blocks.
		if out, err := duRun(t, "--from-dataset", ds, "--exclude", "a/f", "--exclude", "b", "--format", "tsv"); err != nil ||
			duTSV(t, out)[0]["primary"] != "size" || duTSV(t, out)[0]["dirs"] != "1" {
			t.Fatalf("%s, every file filtered out: %v\n%s", name, err, out)
		}
	}
}

// TestDuFromDatasetStatxLowerBound: a tree listed with statx, which a
// filesystem may answer without blocks for some entries, leads with the
// space on disk where its entries have it, those without counted missing:
// a lower bound (">= " in the table), as its live run counts them, never
// the size instead; --allocated still requires every entry's.
func TestDuFromDatasetStatxLowerBound(t *testing.T) {
	captureStderr(t)
	at, five, block := int64(1_767_000_000_000_000), int64(5), int64(4096)
	s := parquet.SchemaOf(new(unblockedRow))
	ext, err := json.Marshal(map[string]any{reader.Extension: map[string]any{"version": reader.MetadataVersion,
		"source": map[string]any{"kind": "local", "uri": "/data"}, "scope": map[string]any{"kind": "whole"},
		"collected": map[string]any{"metadata": "statx", "columns": []string{}},
		"coverage":  map[string]any{"complete": true, "stop_reason": "end_of_scope", "unreadable": 0, "exclusions": []string{}, "filters": []string{}}}})
	if err != nil {
		t.Fatal(err)
	}
	rows := &parquetRows{rows: []parquet.Row{
		s.Deconstruct(nil, &unblockedRow{Key: []byte("a/"), Kind: "dir", LastModified: &at, AllocatedBytes: &block}),
		s.Deconstruct(nil, &unblockedRow{Key: []byte("a/f"), Kind: "file", Size: &five, LastModified: &at, AllocatedBytes: &block}),
		s.Deconstruct(nil, &unblockedRow{Key: []byte("b"), Kind: "file", Size: &five, LastModified: &at}),
	}}
	ds := filepath.Join(t.TempDir(), "statx")
	if err := local.WriteDir(context.Background(), ds, s, rows, local.DatasetOptions{Extensions: ext, AllowUnsynced: true,
		Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: string(reader.LocalV1)}}); err != nil {
		t.Fatal(err)
	}
	out, err := duRun(t, "--from-dataset", ds, "--format", "tsv")
	if r := duTSV(t, out); err != nil || r[0]["primary"] != "allocated_bytes" || r[0]["allocated_bytes"] != "8192" || r[0]["allocated_bytes_missing"] != "1" {
		t.Fatalf("default: %v\n%s", err, out)
	}
	if out, err := duRun(t, "--from-dataset", ds, "--format", "table"); err != nil || !strings.HasPrefix(tableCells(strings.SplitN(out, "\n", 3)[1])[0], ">= ") {
		t.Fatalf("table: %v\n%s", err, out)
	}
	var nb *totals.NoAllocatedError
	if out, err := duRun(t, "--from-dataset", ds, "--allocated", "--format", "tsv"); !errors.As(err, &nb) || out != "" {
		t.Fatalf("--allocated: %v, stdout %q", err, out)
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
		"hint:   swath list " + shellQuote(root) + " -o DIR\n" +
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
// shards, totals as the live listing does, whole and below the source.
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
	liveAndSaved(t, append([]string{"s3://bkt/p04/", "-d", "1"}, store...), []string{"--from-dataset", ds, "s3://bkt/p04/", "-d", "1"})
}
