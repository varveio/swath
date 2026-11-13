// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// s3Row is a swath/s3-objects/v1 row as swath writes it, and one column
// the schema id does not define.
type s3Row struct {
	Key               string  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	ETag              *string `parquet:"etag,optional"`
	StorageClass      *string `parquet:"storage_class,optional"`
	OwnerID           *string `parquet:"owner_id,optional"`
	OwnerDisplayName  *string `parquet:"owner_display_name,optional"`
	ChecksumAlgorithm *string `parquet:"checksum_algorithm,optional"`
	ChecksumType      *string `parquet:"checksum_type,optional"`
	Extra             *string `parquet:"extra,optional"`
}

// s3RowNoETag lacks etag, a column every part has.
type s3RowNoETag struct {
	Key               string  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	StorageClass      *string `parquet:"storage_class,optional"`
	OwnerID           *string `parquet:"owner_id,optional"`
	OwnerDisplayName  *string `parquet:"owner_display_name,optional"`
	ChecksumAlgorithm *string `parquet:"checksum_algorithm,optional"`
	ChecksumType      *string `parquet:"checksum_type,optional"`
}

// s3RowUntimed stores last_modified as a plain integer.
type s3RowUntimed struct {
	Key               string  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional"`
	ETag              *string `parquet:"etag,optional"`
	StorageClass      *string `parquet:"storage_class,optional"`
	OwnerID           *string `parquet:"owner_id,optional"`
	OwnerDisplayName  *string `parquet:"owner_display_name,optional"`
	ChecksumAlgorithm *string `parquet:"checksum_algorithm,optional"`
	ChecksumType      *string `parquet:"checksum_type,optional"`
}

// renamedKey names its key column name.
type renamedKey struct {
	Name string `parquet:"name"`
}

// swathExtension is extensions.swath as a whole-bucket listing of bucket
// b writes it, with an exclusion, which does not refuse it.
func swathExtension() map[string]any {
	return map[string]any{
		"version":      MetadataVersion,
		"published_at": "2026-11-09T10:00:00Z",
		"source":       map[string]any{"kind": "s3", "uri": "s3://b", "container": "b"},
		"scope":        map[string]any{"kind": "whole"},
		"coverage": map[string]any{"complete": true, "stop_reason": "completed", "skipped_entries": 0,
			"exclusions": []string{"out/"}, "filters": []string{}, "snapshot": false},
	}
}

type spec struct {
	id  SchemaID
	key seekparquet.Key
	ext map[string]any // extensions.swath; nil for none
}

func s3Spec() spec {
	return spec{id: S3ObjectsV1, key: seekparquet.Key{Column: "key", Unique: true}, ext: swathExtension()}
}

// writeDataset writes rows (structs of one type, in key order) as a new
// dataset with the module's writer and returns its directory.
func writeDataset[T any](t *testing.T, s spec, rows []T) string {
	t.Helper()
	return writeGeometry(t, s, rows, 100, 0)
}

// writeGeometry writes rows as writeDataset does, in pages of pageRows
// rows and row groups of groupRows rows (0: the writer's default).
func writeGeometry[T any](t testing.TB, s spec, rows []T, pageRows int, groupRows int64) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ds")
	schema := parquet.SchemaOf(new(T))
	var buf []parquet.Row
	for i := range rows {
		buf = append(buf, schema.Deconstruct(nil, &rows[i]))
	}
	var ext json.RawMessage
	if s.ext != nil {
		var err error
		if ext, err = json.Marshal(map[string]any{Extension: s.ext}); err != nil {
			t.Fatal(err)
		}
	}
	err := local.WriteDir(context.Background(), dir, schema, &rowReader{rows: buf},
		local.DatasetOptions{Part: write.PartOptions{Key: s.key, SchemaID: string(s.id), PageRows: pageRows, RowGroupRows: groupRows}, Extensions: ext, AllowUnsynced: true})
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

type rowReader struct{ rows []parquet.Row }

func (r *rowReader) ReadRows(out []parquet.Row) (int, error) {
	n := copy(out, r.rows)
	r.rows = r.rows[n:]
	if len(r.rows) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func openDir(t *testing.T, dir string) *Dataset {
	t.Helper()
	d, err := OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func ptr[T any](v T) *T { return &v }

// semanticAt fails unless err is a SemanticError of kind naming column.
func semanticAt(t *testing.T, err error, kind Kind, column string) *SemanticError {
	t.Helper()
	var se *SemanticError
	if !errors.As(err, &se) || se.Kind != kind || se.Column != column {
		t.Fatalf("error %v, want a %s error naming %q", err, kind, column)
	}
	return se
}

// TestOpenRecordsCoverage: a partial, filtered listing with no publication
// time opens through the manifest alone, and Metadata hands on how the
// listing ran for the application to judge.
func TestOpenRecordsCoverage(t *testing.T) {
	ext := swathExtension()
	delete(ext, "published_at")
	ext["source"] = map[string]any{"kind": "local-fs", "uri": "/data"}
	ext["scope"] = map[string]any{"kind": "prefix", "prefix": "d/"}
	ext["coverage"] = map[string]any{"complete": false, "stop_reason": "max_keys", "skipped_entries": 3,
		"exclusions": []string{}, "filters": []string{"one_filesystem"}, "snapshot": false}
	s := spec{id: LocalV1, key: s3Spec().key, ext: ext}
	rows := make([]localRow, len(localTree))
	for i, e := range localTree {
		rows[i] = localRow{Key: []byte(e.key), Kind: e.kind, Size: ptr(e.size)}
	}
	dir := writeGeometry(t, s, rows[:3], 2, 2)
	appendPart(t, dir, writeGeometry(t, s, rows[3:], 2, 2))

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	src := &openRecorder{root: root}
	d, err := Open(context.Background(), src, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if !reflect.DeepEqual(src.opens, []string{seekparquet.ManifestName}) {
		t.Fatalf("opened %q, want the manifest alone", src.opens)
	}
	m := d.Metadata()
	c := m.Coverage
	if !m.Swath || m.PublishedAt != nil || *m.Source.Kind != "local-fs" || *m.Scope.Kind != "prefix" || *m.Scope.Prefix != "d/" ||
		*c.Complete || *c.StopReason != "max_keys" || *c.SkippedEntries != 3 || !reflect.DeepEqual(c.Filters, []string{"one_filesystem"}) ||
		len(m.Manifest.Parts) != 2 {
		t.Fatalf("metadata %+v, coverage %+v", m, c)
	}
}

func TestOpenUnknownSchemaID(t *testing.T) {
	s := s3Spec()
	s.id = "swath/canonical"
	_, err := OpenDir(context.Background(), writeDataset[s3Row](t, s, nil), seekparquet.OpenOptions{})
	if !errors.Is(err, ErrSchema) {
		t.Fatalf("open: %v, want a schema error", err)
	}
}

func TestOpenRefusalUnchanged(t *testing.T) {
	_, err := OpenDir(context.Background(), t.TempDir(), seekparquet.OpenOptions{})
	if seekparquet.RefusalCodeOf(err) != seekparquet.CodeNoManifest {
		t.Fatalf("open an empty directory: %v", err)
	}
}

func s3Rows(n int) []s3Row {
	rows := make([]s3Row, n)
	for i := range rows {
		rows[i] = s3Row{Key: fmt.Sprintf("k%05d", i), Size: ptr(int64(i)), LastModified: ptr(int64(i) * 1_000_000), StorageClass: ptr("STANDARD")}
		if i%3 == 0 {
			rows[i].ETag = ptr(fmt.Sprintf("%032x", i))
		}
	}
	return rows
}

func TestCheckSchema(t *testing.T) {
	ctx := context.Background()
	if err := openDir(t, writeDataset(t, s3Spec(), s3Rows(10))).CheckSchema(ctx); err != nil {
		t.Fatalf("a dataset as swath writes it: %v", err)
	}
	t.Run("keys not unique", func(t *testing.T) {
		s := s3Spec()
		s.key.Unique = false
		semanticAt(t, openDir(t, writeDataset(t, s, s3Rows(3))).CheckSchema(ctx), KindSchema, "key")
	})
	t.Run("key column", func(t *testing.T) {
		s := s3Spec()
		s.key.Column = "name"
		semanticAt(t, openDir(t, writeDataset(t, s, []renamedKey{{"a"}})).CheckSchema(ctx), KindSchema, "name")
	})
	t.Run("column absent", func(t *testing.T) {
		semanticAt(t, openDir(t, writeDataset(t, s3Spec(), []s3RowNoETag{{Key: "a"}})).CheckSchema(ctx), KindSchema, "etag")
	})
	t.Run("column type", func(t *testing.T) {
		semanticAt(t, openDir(t, writeDataset(t, s3Spec(), []s3RowUntimed{{Key: "a"}})).CheckSchema(ctx), KindSchema, "last_modified")
	})
	t.Run("a later part lacks a column", func(t *testing.T) {
		dir := writeDataset(t, s3Spec(), s3Rows(5))
		other := writeDataset(t, s3Spec(), []s3RowNoETag{{Key: "z1"}, {Key: "z2"}})
		appendPart(t, dir, other)
		d := openDir(t, dir)
		if err := d.SeekParquet().Verify(ctx, nil); err != nil {
			t.Fatalf("the module accepts the parts: %v", err)
		}
		se := semanticAt(t, d.CheckSchema(ctx), KindSchema, "etag")
		if !errors.Is(se, ErrSchema) {
			t.Fatal(se)
		}
	})
}

// appendPart adds the one part of the dataset at from to the dataset at
// dir, as its last part, keeping dir's extensions.
func appendPart(t testing.TB, dir, from string) {
	t.Helper()
	read := func(dir string) seekparquet.Manifest {
		raw, err := os.ReadFile(filepath.Join(dir, seekparquet.ManifestName))
		if err != nil {
			t.Fatal(err)
		}
		m, err := seekparquet.ParseManifest(raw, seekparquet.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	m, add := read(dir), read(from).Parts[0]
	raw, err := os.ReadFile(filepath.Join(from, add.Path))
	if err != nil {
		t.Fatal(err)
	}
	add.Path = fmt.Sprintf("data/part-%05d.parquet", len(m.Parts))
	if err := os.WriteFile(filepath.Join(dir, add.Path), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	m.Parts = append(m.Parts, add)
	out, err := seekparquet.MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, seekparquet.ManifestName), out, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCheckSchemaReadsTheVerifiedPart: CheckSchema checks the columns of the
// part file the module verified and reads, not of a file put at its path
// since: a part whose last_modified is not a timestamp, replaced after
// Verify by one whose is, is still refused.
func TestCheckSchemaReadsTheVerifiedPart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("an open file cannot be renamed over on Windows")
	}
	dir := writeDataset(t, s3Spec(), []s3RowUntimed{{Key: "a", Size: ptr(int64(1))}})
	good := writeDataset(t, s3Spec(), []s3Row{{Key: "a", Size: ptr(int64(1))}})
	d := openDir(t, dir)
	ctx := context.Background()
	if err := d.SeekParquet().Verify(ctx, nil); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join("data", "part-00000.parquet")
	if err := os.Rename(filepath.Join(good, part), filepath.Join(dir, part)); err != nil {
		t.Fatal(err)
	}
	semanticAt(t, d.CheckSchema(ctx), KindSchema, "last_modified")
}

// openRecorder records files opened while reading the manifest.
type openRecorder struct {
	root  *os.Root
	opens []string
}

func (s *openRecorder) Open(ctx context.Context, name string) (seekparquet.SourceFile, error) {
	s.opens = append(s.opens, name)
	f, err := s.root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return openRecordedFile{File: f, size: info.Size()}, nil
}

type openRecordedFile struct {
	*os.File
	size int64
}

func (f openRecordedFile) Size() int64 { return f.size }
