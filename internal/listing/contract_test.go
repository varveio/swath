// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3"
)

// checkContract reads a committed dataset the way a reader does, from the
// manifest only, and checks the SeekParquet contract: the manifest is the
// commit (no _SUCCESS, no symlink.txt) and its parts are exactly data/'s
// files in name order; each part's bytes and SHA-256 match; its rows equal
// the footer's; its bounds are its first and last key, exact bytes; keys
// rise strictly within and across parts; every footer carries exactly the
// seekparquet.* stamps (format version, draft revision, schema id, key
// column, key uniqueness), and sorting_columns says key ascending. It
// returns every key in order.
func checkContract(t testing.TB, out string) []string {
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
		sum := sha256.Sum256(b)
		if int64(len(b)) != p.Bytes || hex.EncodeToString(sum[:]) != p.SHA256 {
			t.Fatalf("part %d: bytes or sha256 differ from the manifest", i)
		}
		// Read from the bytes already read: no handle is left open when a
		// check below fails (TestCheckContractRefuses fails it on purpose).
		pf, err := pq.OpenFile(bytes.NewReader(b), int64(len(b)))
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

// oracleT runs checkContract as a test would and records its first fatal
// failure instead of failing the test: the oracle's own test.
type oracleT struct {
	testing.TB
	failed string
}

func (o *oracleT) Helper() {}
func (o *oracleT) Fatal(args ...any) {
	o.failed = fmt.Sprint(args...)
	runtime.Goexit()
}
func (o *oracleT) Fatalf(format string, args ...any) {
	o.failed = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// TestCheckContractRefuses: the oracle the crash and resume tests check
// their datasets with (TestCrashMatrix takes its reference keys from it)
// refuses a dataset that breaks the contract in one way each, and accepts
// the dataset each was made from. Inside a part, through its own key
// check: a key repeated, a pair out of order (the publication gate checks
// only part bounds, so such parts publish). Across parts, through the
// manifest it reads: a key repeated at a part boundary, parts out of
// order. And a part listed but gone, a file in data/ the manifest does not
// list, a part whose bytes, row count or bounds are not the manifest's.
func TestCheckContractRefuses(t *testing.T) {
	// build publishes keys, in the order given, maxRows to a part.
	build := func(maxRows int64, keys ...string) string {
		rows := make([]schema.Row[s3.Object], len(keys))
		for i, k := range keys {
			rows[i] = schema.Row[s3.Object]{Key: k, Data: s3.Object{Size: int64(i)}}
		}
		out := filepath.Join(t.TempDir(), "ds")
		writeSeekFixture(t, out, "s3", &s3.Schema, rows, maxRows)
		return out
	}
	check := func(out string) string {
		o := &oracleT{TB: t}
		done := make(chan struct{})
		go func() {
			defer close(done)
			checkContract(o, out)
		}()
		<-done
		return o.failed
	}
	// manifest edits the manifest's JSON as written, past dataset.Manifest's
	// own validation, which would refuse to write most of these.
	manifest := func(out string, edit func(parts []any) []any) {
		path := filepath.Join(out, dataset.ManifestName)
		var m map[string]any
		readJSON(t, path, &m)
		m["parts"] = edit(m["parts"].([]any))
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	part := func(parts []any, i int) map[string]any { return parts[i].(map[string]any) }
	partPath := func(i int) string { return dataset.DataDir + "/" + dataset.PartName(i) }
	// rewrite re-encodes part 0 with its rows edited, its schema, footer
	// stamps and sorting_columns kept, past the writer's own key check
	// (KEY-3), and records it in the manifest: bytes, digest, rows and its
	// first and last key as bounds. What a writer that lost its order check
	// would publish.
	rewrite := func(out string, edit func([]pq.Row) []pq.Row) string {
		path := filepath.Join(out, partPath(0))
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		pf, err := pq.OpenFile(bytes.NewReader(b), int64(len(b)))
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]pq.Row, pf.NumRows())
		r := pq.NewReader(pf)
		n, _ := r.ReadRows(rows)
		r.Close()
		rows = edit(rows[:n])
		opts := []pq.WriterOption{pf.Schema(), pq.SortingWriterConfig(pq.SortingColumns(pq.Ascending(dataset.KeyColumn)))}
		for _, kv := range pf.Metadata().KeyValueMetadata {
			opts = append(opts, pq.KeyValueMetadata(kv.Key, kv.Value))
		}
		var buf bytes.Buffer
		w := pq.NewWriter(&buf, opts...)
		if _, err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(buf.Bytes())
		manifest(out, func(parts []any) []any {
			p := part(parts, 0)
			p["bytes"], p["sha256"], p["rows"] = buf.Len(), hex.EncodeToString(sum[:]), len(rows)
			p["min_key_base64"] = base64.StdEncoding.EncodeToString(rows[0][0].ByteArray())
			p["max_key_base64"] = base64.StdEncoding.EncodeToString(rows[len(rows)-1][0].ByteArray())
			return parts
		})
		return out
	}
	for _, tc := range []struct {
		name, why string // why: the refusal names this
		out       func() string
	}{
		{"key repeated inside a part", "not strictly ascending", func() string {
			return rewrite(build(4, "a", "b", "c"), func(rows []pq.Row) []pq.Row { return slices.Insert(rows, 1, rows[1]) })
		}},
		{"keys out of order inside a part", "not strictly ascending", func() string {
			return rewrite(build(4, "a", "b", "c", "d"), func(rows []pq.Row) []pq.Row { rows[1], rows[2] = rows[2], rows[1]; return rows })
		}},
		{"key repeated at a part boundary", "does not start after part 1 ends", func() string {
			// Another dataset's part holding "b" follows this one's
			// last part, "b": each part sound, their order not.
			out, other := build(1, "a", "b"), build(1, "b", "c")
			b, err := os.ReadFile(filepath.Join(other, partPath(0)))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, partPath(2)), b, 0o644); err != nil {
				t.Fatal(err)
			}
			var om map[string]any
			readJSON(t, filepath.Join(other, dataset.ManifestName), &om)
			manifest(out, func(parts []any) []any {
				p := part(om["parts"].([]any), 0)
				p["path"] = partPath(2)
				return append(parts, p)
			})
			return out
		}},
		{"parts out of order", "does not start after part 0 ends", func() string {
			out := build(2, "a", "b", "c", "d")
			a, b := filepath.Join(out, partPath(0)), filepath.Join(out, partPath(1))
			if err := errors.Join(os.Rename(a, a+".swap"), os.Rename(b, a), os.Rename(a+".swap", b)); err != nil {
				t.Fatal(err)
			}
			manifest(out, func(parts []any) []any {
				parts[0], parts[1] = parts[1], parts[0]
				part(parts, 0)["path"], part(parts, 1)["path"] = partPath(0), partPath(1)
				return parts
			})
			return out
		}},
		{"part listed but missing", "the manifest lists", func() string {
			out := build(2, "a", "b", "c", "d")
			os.Remove(filepath.Join(out, partPath(1)))
			return out
		}},
		{"file in data/ not in the manifest", "the manifest lists", func() string {
			out := build(2, "a", "b", "c", "d")
			os.WriteFile(filepath.Join(out, partPath(2)), []byte("x"), 0o644)
			return out
		}},
		{"part bytes altered", "bytes or sha256 differ", func() string {
			out := build(2, "a", "b", "c", "d")
			p := filepath.Join(out, partPath(1))
			b, _ := os.ReadFile(p)
			b[len(b)/2] ^= 1
			os.WriteFile(p, b, 0o644)
			return out
		}},
		{"row count not the footer's", "rows in the footer", func() string {
			out := build(2, "a", "b", "c", "d")
			manifest(out, func(parts []any) []any { part(parts, 1)["rows"] = part(parts, 1)["rows"].(float64) + 1; return parts })
			return out
		}},
		{"bounds not the part's keys", "bounds", func() string {
			out := build(2, "a", "b", "c", "d")
			manifest(out, func(parts []any) []any {
				part(parts, 1)["max_key_base64"] = base64.StdEncoding.EncodeToString([]byte("dd"))
				return parts
			})
			return out
		}},
	} {
		if got := check(tc.out()); !strings.Contains(got, tc.why) || got == "" {
			t.Errorf("%s: refused for %q, want %q", tc.name, got, tc.why)
		}
	}
	for _, maxRows := range []int64{1, 2, 4} {
		if got := check(build(maxRows, "a", "b", "c", "d")); got != "" {
			t.Fatalf("the clean dataset, %d rows a part, refused: %s", maxRows, got)
		}
	}
	// The re-encoding alone changes nothing the oracle checks.
	if got := check(rewrite(build(4, "a", "b", "c", "d"), func(rows []pq.Row) []pq.Row { return rows })); got != "" {
		t.Fatalf("the clean dataset, re-encoded, refused: %s", got)
	}
}

// TestDatasetContract: multi-part S3 datasets (sequential, parallel,
// seeded) and a local dataset with a name that is not UTF-8 meet the
// format contract, and the S3 ones hold the same keys.
func TestDatasetContract(t *testing.T) {
	url := fakeS3(t, 3000)
	var ref []string
	for i, o := range []S3Config{{Policy: keyrange.Policy{Sequential: true}}, {}, {Policy: keyrange.Policy{Seed: true}}} {
		o.Endpoint, o.PathStyle, o.Anonymous = url, true, true
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
