// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
)

// listS3 lists root on the fake at url into out (seeded from seedFrom when
// set) with small parts, so a dataset has several.
func listS3(t *testing.T, url, root, out, seedFrom string) (Report, error) {
	t.Helper()
	return Run(context.Background(), Config{Source: SourceConfig{URI: root, SeedFrom: seedFrom,
		Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Seed: true}}},
		Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 16 << 10, Workers: 8}})
}

func manifestOf(t *testing.T, dir string) dataset.Manifest {
	t.Helper()
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	return m
}

// TestSeedFromDataset: a run seeded from its predecessor's dataset lists
// the same rows into the same parts, from tiles cut at the predecessor's
// key pages (no delimiter seed request), and its report names the prior.
func TestSeedFromDataset(t *testing.T) {
	url := fakeS3(t, 6000)
	dir := t.TempDir()
	first, second := filepath.Join(dir, "first"), filepath.Join(dir, "second")
	if _, err := listS3(t, url, "s3://bkt", first, ""); err != nil {
		t.Fatal(err)
	}
	sum, err := listS3(t, url, "s3://bkt", second, first)
	if err != nil {
		t.Fatal(err)
	}
	m1, m2 := manifestOf(t, first), manifestOf(t, second)
	if len(m1.Parts) < 2 || len(m1.Parts) != len(m2.Parts) {
		t.Fatalf("%d parts, then %d", len(m1.Parts), len(m2.Parts))
	}
	for i := range m1.Parts {
		if m1.Parts[i].PartInfo != m2.Parts[i].PartInfo {
			t.Fatalf("part %d differs: %+v, %+v", i, m1.Parts[i].PartInfo, m2.Parts[i].PartInfo)
		}
	}
	c := s3Stats(sum).Counters
	if c.Value("seed.from.parts") != int64(len(m1.Parts)) || c.Value("seed.from.indexed_parts") == 0 || c.Value("seed.from.index_failed") != 0 ||
		c.Value("seed.from.tiles") < 2 || c.Value("seed.requests") != 0 || c.Value("seed.from.expected_keys") != 6000 {
		t.Fatalf("counters %v", c)
	}
	if p := sum.Invocation.Prior; p == nil || p.Source != "dataset" || p.Path != first || !p.Counted || p.Cuts < 2 {
		t.Fatalf("report prior %+v", p)
	}
}

// TestSeedFromCutFile: a cut file seeds the run (with and without
// counts); one out of order, outside the scope, or mixing counted and
// uncounted lines is refused before anything is listed.
func TestSeedFromCutFile(t *testing.T) {
	url := fakeS3(t, 3000)
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, body := range map[string]string{
		"plain.cuts":   "logs/03/\nlogs/08/obj-01000\nlogs/12/\n",
		"counted.cuts": "logs/03/\t500\nlogs/08/obj-01000\t900\nlogs/12/\t700\n",
	} {
		sum, err := listS3(t, url, "s3://bkt", filepath.Join(dir, name+".out"), write(name, body))
		if err != nil {
			t.Fatal(err)
		}
		c := s3Stats(sum).Counters
		if sum.Output.RowsEmitted != 3000 || c.Value("seed.from.cuts") != 3 || c.Value("seed.from.tiles") != 4 || c.Value("seed.requests") != 0 {
			t.Fatalf("%s: rows %d, counters %v", name, sum.Output.RowsEmitted, c)
		}
		if p := sum.Invocation.Prior; p == nil || p.Source != "cuts" || p.Counted != strings.Contains(body, "\t") {
			t.Fatalf("%s: report prior %+v", name, p)
		}
	}
	for name, body := range map[string]string{
		"unsorted.cuts": "logs/08/\nlogs/03/\n",
		"repeated.cuts": "logs/03/\nlogs/03/\n",
		"mixed.cuts":    "logs/03/\t5\nlogs/08/\n",
		"empty.cuts":    "",
		"blank.cuts":    "logs/03/\n\nlogs/08/\n",
	} {
		out := filepath.Join(dir, name+".out")
		if _, err := listS3(t, url, "s3://bkt", out, write(name, body)); !errors.Is(err, ErrPrior) {
			t.Fatalf("%s: %v, want ErrPrior", name, err)
		}
		if _, err := os.Stat(filepath.Join(out, dataset.ManifestName)); err == nil {
			t.Fatalf("%s: a refused prior published a dataset", name)
		}
	}
	if _, err := listS3(t, url, "s3://bkt/logs/05/", filepath.Join(dir, "outside.out"), write("outside.cuts", "logs/03/\n")); !errors.Is(err, ErrPrior) {
		t.Fatalf("a cut outside the scope: %v, want ErrPrior", err)
	}
}

// TestSeedFromCutFileBounded: a cut file of more than 262,144 cuts, or
// larger than 16 MiB, is refused before it is read into memory (ErrPrior,
// nothing listed); one at the cut bound seeds the run.
func TestSeedFromCutFileBounded(t *testing.T) {
	url := fakeS3(t, 300)
	dir := t.TempDir()
	cuts := func(n int) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, "logs/%07d\n", i)
		}
		return b.String()
	}
	for name, tc := range map[string]struct {
		body string
		size int64 // if set, the file is extended (sparse) to this size
		ok   bool
	}{
		"at-bound":   {body: cuts(maxCutFileCuts), ok: true},
		"over-cuts":  {body: cuts(maxCutFileCuts + 1)},
		"over-bytes": {body: "logs/03/\n", size: maxCutFileBytes + 1},
	} {
		p := filepath.Join(dir, name+".cuts")
		if err := os.WriteFile(p, []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		if tc.size > 0 {
			if err := os.Truncate(p, tc.size); err != nil {
				t.Fatal(err)
			}
		}
		out := filepath.Join(dir, name+".out")
		_, err := listS3(t, url, "s3://bkt", out, p)
		switch {
		case tc.ok && err != nil:
			t.Fatalf("%s: %v", name, err)
		case !tc.ok && !errors.Is(err, ErrPrior):
			t.Fatalf("%s: %v, want ErrPrior", name, err)
		case !tc.ok && !strings.Contains(err.Error(), "keep fewer"):
			t.Fatalf("%s: %v: the refusal says what to do", name, err)
		}
	}
}

// TestSeedFromIdentity: a dataset of another scope or another bucket is
// refused, so is the output directory itself and a source that does not
// list in key ranges.
func TestSeedFromIdentity(t *testing.T) {
	url := fakeS3(t, 2000)
	dir := t.TempDir()
	scoped := filepath.Join(dir, "scoped")
	if _, err := listS3(t, url, "s3://bkt/logs/03/", scoped, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := listS3(t, url, "s3://bkt", filepath.Join(dir, "a"), scoped); !errors.Is(err, ErrPrior) || !strings.Contains(err.Error(), "prefix") {
		t.Fatalf("another scope: %v, want ErrPrior", err)
	}
	if _, err := listS3(t, url, "s3://bkt/logs/03/", filepath.Join(dir, "b"), scoped); err != nil {
		t.Fatalf("the same scope: %v", err)
	}
	// Another bucket: the same dataset, its manifest naming another one.
	other := filepath.Join(dir, "other")
	if err := os.CopyFS(other, os.DirFS(scoped)); err != nil {
		t.Fatal(err)
	}
	m := manifestOf(t, other)
	m.Source.Container = "elsewhere"
	b, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(other, dataset.ManifestName), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := listS3(t, url, "s3://bkt/logs/03/", filepath.Join(dir, "c"), other); !errors.Is(err, ErrPrior) {
		t.Fatalf("another bucket: %v, want ErrPrior", err)
	}
	if _, err := listS3(t, url, "s3://bkt/logs/03/", scoped, scoped); !errors.Is(err, ErrPrior) {
		t.Fatalf("the output itself: %v, want ErrPrior", err)
	}
	tree := t.TempDir()
	if err := os.WriteFile(filepath.Join(tree, "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cuts := filepath.Join(dir, "x.cuts")
	if err := os.WriteFile(cuts, []byte("f\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: tree, SeedFrom: cuts}, Output: OutputConfig{Dir: filepath.Join(dir, "fs")}}); !errors.Is(err, ErrPrior) {
		t.Fatalf("a filesystem source: %v, want ErrPrior", err)
	}
}

// TestDatasetPriorCoalescesFinalVersionGroup: A final key group crossing row groups
// contributes all its rows to the prior weights.
func TestDatasetPriorCoalescesFinalVersionGroup(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, dataset.DataDir), 0755); err != nil {
		t.Fatal(err)
	}
	r, err := parquetout.NewRouter(parquetout.Options{Out: dir, PartSize: 64 << 20, Encoders: 1}, &s3.VersionsSchema)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]s3.VersionRow, 5000)
	for i := range rows {
		rows[i] = s3.VersionRow{Key: "k", Data: s3.Version{VersionID: "v"}}
	}
	if err := r.Write(context.Background(), [][]s3.VersionRow{rows}); err != nil {
		t.Fatal(err)
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	meta := dataset.Meta{SchemaID: s3.VersionsSchema.ID, Groups: true, Source: dataset.Source{Kind: "s3", URI: "s3://b", Container: "b"}, Scope: dataset.Scope{Kind: "whole"}}
	if err := dataset.Publish(dir, meta, parts, nil); err != nil {
		t.Fatal(err)
	}
	pr, err := datasetPrior(dir, source.Locator{Kind: "s3", Root: "s3://b", Container: "b"})
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, c := range pr.Cuts {
		total += c.Rows
	}
	if total != 5000 {
		t.Fatalf("prior weights=%d, want 5000; cuts=%+v", total, pr.Cuts)
	}
}
