// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// checkContract reads a committed dataset the way a reader does, from the
// manifest only, and checks the format-2 contract: the manifest is the
// commit (no _SUCCESS, no symlink.txt) and its parts are exactly data/'s
// files in name order; each part's bytes and MD5 match; its rows equal the
// footer's; its bounds are its first and last key, exact bytes; keys rise
// strictly within and across parts; every footer carries exactly the
// profile, its version, the schema id and the index bound under swath.*,
// and sorting_columns says key ascending. It returns every key in order.
func checkContract(t *testing.T, out string) []string {
	t.Helper()
	var m dataset.Manifest
	readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
	if m.Format != seekparquet.Format || m.FormatVersion != int(seekparquet.FormatVersion) || m.SchemaID == "" || m.PublishedAt.IsZero() {
		t.Fatalf("manifest header %+v", m)
	}
	for _, gone := range []string{"_SUCCESS", "symlink.txt"} {
		if _, err := os.Stat(filepath.Join(out, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s present", gone)
		}
	}
	ents, _ := os.ReadDir(filepath.Join(out, dataset.DataDir))
	var files []string
	for _, e := range ents {
		files = append(files, dataset.DataDir+"/"+e.Name())
	}
	var paths []string
	for _, p := range m.Parts {
		paths = append(paths, p.Path)
	}
	if !slices.Equal(files, paths) {
		t.Fatalf("data/ holds %v, the manifest lists %v", files, paths)
	}
	var keys []string
	for i, p := range m.Parts {
		b, err := os.ReadFile(filepath.Join(out, p.Path))
		if err != nil {
			t.Fatal(err)
		}
		sum := md5.Sum(b)
		if int64(len(b)) != p.Bytes || hex.EncodeToString(sum[:]) != p.MD5 {
			t.Fatalf("part %d: bytes or md5 differ from the manifest", i)
		}
		f, err := os.Open(filepath.Join(out, p.Path))
		if err != nil {
			t.Fatal(err)
		}
		pf, err := pq.OpenFile(f, int64(len(b)))
		if err != nil {
			t.Fatal(err)
		}
		kv := map[string]string{}
		for _, e := range pf.Metadata().KeyValueMetadata {
			kv[e.Key] = e.Value
		}
		want := map[string]string{seekparquet.StampFormatVersion: strconv.FormatInt(seekparquet.FormatVersion, 10), seekparquet.StampDraftRevision: strconv.FormatInt(seekparquet.DraftRevision, 10),
			seekparquet.StampSchemaID: m.SchemaID, seekparquet.StampKeyColumn: m.Key.Column, seekparquet.StampKeyUnique: strconv.FormatBool(m.Key.Unique)}
		if !m.Current() || m.Key.Column != dataset.KeyColumn {
			t.Fatalf("manifest %s %d draft %d, key %+v", m.Format, m.FormatVersion, m.DraftRevision, m.Key)
		}
		if !maps.Equal(kv, want) {
			t.Fatalf("part %d: footer stamps %v, want %v", i, kv, want)
		}
		for _, rg := range pf.Metadata().RowGroups {
			if len(rg.SortingColumns) != 1 || rg.SortingColumns[0].ColumnIdx != 0 || rg.SortingColumns[0].Descending {
				t.Fatalf("part %d: sorting_columns %+v", i, rg.SortingColumns)
			}
		}
		if pf.NumRows() != p.Rows {
			t.Fatalf("part %d: %d rows in the footer, %d in the manifest", i, pf.NumRows(), p.Rows)
		}
		rows := make([]pq.Row, pf.NumRows())
		r := pq.NewReader(pf)
		n, _ := r.ReadRows(rows)
		r.Close()
		f.Close()
		first := len(keys)
		for _, row := range rows[:n] {
			keys = append(keys, string(row[0].ByteArray()))
		}
		if int64(n) != p.Rows || keys[first] != string(p.MinKey) || keys[len(keys)-1] != string(p.MaxKey) {
			t.Fatalf("part %d: %d rows read; bounds %q..%q, manifest %q..%q", i, n, keys[first], keys[len(keys)-1], p.MinKey, p.MaxKey)
		}
	}
	for i := 1; i < len(keys); i++ {
		if !(keys[i-1] < keys[i]) {
			t.Fatalf("keys not strictly ascending at %d: %q, %q", i, keys[i-1], keys[i])
		}
	}
	return keys
}

// TestDatasetContract: multi-part S3 datasets (sequential, parallel,
// seeded) and a local dataset with a name that is not UTF-8 meet the
// format contract, and the S3 ones hold the same keys.
func TestDatasetContract(t *testing.T) {
	url := fakeS3(t, 3000)
	var ref []string
	for i, o := range []S3Config{{Policy: keyrange.Policy{Sequential: true}}, {}, {Policy: keyrange.Policy{Seed: true}}} {
		o.Endpoint, o.PathStyle = url, true
		out := filepath.Join(t.TempDir(), "ds")
		if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", Options: &o}, Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()},
			Limits: Limits{PartSize: 16 << 10, Workers: 8}}); err != nil {
			t.Fatal(err)
		}
		keys := checkContract(t, out)
		if i == 0 {
			ref = keys
		} else if !slices.Equal(keys, ref) {
			t.Fatalf("%+v: %d keys, sequential %d", o, len(keys), len(ref))
		}
	}
	if len(ref) != 3000 {
		t.Fatalf("%d keys", len(ref))
	}
	src := t.TempDir()
	names := []string{"a/1", "a/2", "b", "c/d/e"}
	if runtime.GOOS != "windows" && runtime.GOOS != "darwin" { // NTFS and APFS refuse names that are not UTF-8
		names = append(names, "z\xff")
	}
	mk(t, src, names...)
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}, Limits: Limits{PartSize: 1}}); err != nil {
		t.Fatal(err)
	}
	checkContract(t, out)
}
