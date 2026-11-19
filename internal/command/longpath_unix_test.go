// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package command

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/reader"
)

// longTree builds below root, through directory handles, a chain of levels
// directories with names of 200 bytes (paths past PATH_MAX), each holding a
// file s of 5 bytes and the next; the last holds a file f, a symbolic link
// l to it and a directory d holding a file g. It returns the keys, sorted,
// and the last directory's key.
func longTree(t *testing.T, root string, levels int) (keys []string, last string) {
	t.Helper()
	dir, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	put := func(dir int, name string) {
		fd, err := unix.Openat(dir, name, unix.O_CREAT|unix.O_WRONLY|unix.O_CLOEXEC, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		unix.Write(fd, []byte("hello"))
		unix.Close(fd)
	}
	down := func(dir int, name string) int {
		if err := unix.Mkdirat(dir, name, 0o755); err != nil {
			t.Fatal(err)
		}
		next, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		unix.Close(dir)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	for i := range levels {
		name := fmt.Sprintf("%03d", i) + strings.Repeat("x", 197)
		put(dir, "s")
		dir = down(dir, name)
		keys = append(keys, last+"s", last+name+"/")
		last += name + "/"
	}
	put(dir, "f")
	if err := unix.Symlinkat("f", dir, "l"); err != nil {
		t.Fatal(err)
	}
	dir = down(dir, "d")
	put(dir, "g")
	unix.Close(dir)
	keys = append(keys, last+"f", last+"l", last+"d/", last+"d/g")
	slices.Sort(keys)
	return keys, last
}

// TestLongPathsEndToEnd: a tree whose paths pass PATH_MAX, to 30,000 bytes,
// lists whole and exactly with every output: a dataset read back through
// the typed reader (sizes and link targets included, coverage complete),
// TSV, JSONL and du's totals; the run report counts the directories opened
// below the root. A root itself past PATH_MAX lists too, with its output
// beside it and replaced with --overwrite.
func TestLongPathsEndToEnd(t *testing.T) {
	const levels = 150
	root := t.TempDir()
	keys, last := longTree(t, root, levels)
	ctx := context.Background()

	out := filepath.Join(t.TempDir(), "ds")
	if err := run(ctx, []string{root, "-o", out, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	d, err := reader.OpenDir(ctx, out, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	m := d.Metadata()
	if c := m.Coverage; c == nil || c.Complete == nil || !*c.Complete || c.Unreadable == nil || *c.Unreadable != 0 {
		t.Fatalf("coverage %+v", m.Coverage)
	}
	spec, _ := reader.Schema(m.SchemaID)
	var got []string
	err = d.SeekParquet().Scan(ctx, seekparquet.Query{Columns: spec.ColumnNames()}, nil, func(b *seekparquet.Batch) error {
		for i := range b.Len() {
			e, err := reader.DecodeLocalEntry(b.Row(i))
			if err != nil {
				return err
			}
			k := string(e.Key)
			got = append(got, k)
			switch {
			case k == last+"l" && (e.Kind != "symlink" || e.LinkTarget == nil || string(*e.LinkTarget) != "f"):
				return fmt.Errorf("l: %+v", e)
			case (k == last+"f" || k == last+"d/g") && (e.Kind != "file" || e.Size == nil || *e.Size != 5):
				return fmt.Errorf("%s: %+v", k[len(last):], e)
			}
		}
		return nil
	})
	if err != nil || !slices.Equal(got, keys) {
		t.Fatalf("dataset: %d keys of %d, %v", len(got), len(keys), err)
	}
	var report commandPruneReport
	readJSON(t, filepath.Join(out, dataset.ReportName), &report)
	if lp := report.Diagnostics.Source.Stats.LongPaths; lp.Attempts < levels-20 || lp.Opened != lp.Attempts || lp.Failed != nil {
		t.Fatalf("long_paths %+v", lp)
	}

	var tsv bytes.Buffer
	if err := run(ctx, []string{root, "--no-stats", "--no-header"}, &tsv, false); err != nil {
		t.Fatal(err)
	}
	got = got[:0]
	for line := range strings.Lines(tsv.String()) {
		got = append(got, line[:strings.IndexByte(line, '\t')])
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("TSV: %d keys of %d", len(got), len(keys))
	}

	jsonl := filepath.Join(t.TempDir(), "rows.jsonl")
	if err := run(ctx, []string{root, "-o", jsonl, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(jsonl)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got = got[:0]
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var row struct{ Key string }
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		got = append(got, row.Key)
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("JSONL: %d keys of %d", len(got), len(keys))
	}

	var du bytes.Buffer
	if err := runDu(ctx, []string{root, "--no-stats", "--format=tsv", "--no-header"}, &du); err != nil {
		t.Fatal(err)
	}
	// Objects: s at each level, f, l and g; directories: the chain and d;
	// bytes: 5 a file and 1 for l (its target's length).
	want := strings.Join([]string{"total", "0", strconv.Itoa(levels + 3), strconv.Itoa(levels + 1), strconv.Itoa(5*(levels+2) + 1), "0"}, "\t")
	if f := strings.Split(du.String(), "\t"); len(f) < 6 || strings.Join(f[:6], "\t") != want {
		t.Fatalf("du: %q, want %q", du.String(), want)
	}

	deep := filepath.Join(root, filepath.FromSlash(strings.TrimSuffix(last, "/")))
	beside := filepath.Join(t.TempDir(), "deep")
	for _, again := range [][]string{nil, {"--overwrite"}} {
		if err := run(ctx, append([]string{deep, "-o", beside, "--no-stats"}, again...), io.Discard, false); err != nil {
			t.Fatalf("a root of %d bytes %q: %v", len(deep), again, err)
		}
	}
	if got := datasetKeys(t, beside); !slices.Equal(got, []string{"d/", "d/g", "f", "l"}) {
		t.Fatalf("a long root: keys %q", got)
	}
}
