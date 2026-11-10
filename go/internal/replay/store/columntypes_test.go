// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/write"
)

// seekPart is one part's rows, in key order, with the schema they were
// built from.
type seekPart struct {
	schema *parquet.Schema
	rows   []parquet.Row
}

// partOf returns rows (a struct with a required string key column) as a
// seekPart of T's schema.
func partOf[T any](rows []T) seekPart {
	schema := parquet.SchemaOf(new(T))
	part := seekPart{schema: schema, rows: make([]parquet.Row, len(rows))}
	for i := range rows {
		part.rows[i] = schema.Deconstruct(nil, &rows[i])
	}
	return part
}

// writeSeek writes parts, in order, as a SeekParquet dataset of schemaID
// through the module's part writer (pages of at most pageRows rows; 0 is
// its default), and returns the dataset directory. Parts may differ in
// their value columns.
func writeSeek(t *testing.T, schemaID string, pageRows int, parts ...seekPart) string {
	t.Helper()
	return writeSeekWith(t, seekparquet.Key{Column: "key", Unique: true}, write.PartOptions{PageRows: pageRows}, schemaID, parts...)
}

// writeSeekWith is writeSeek with the key declaration and the part options
// (whose Key and SchemaID it sets).
func writeSeekWith(t testing.TB, key seekparquet.Key, opts write.PartOptions, schemaID string, parts ...seekPart) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	opts.Key, opts.SchemaID = key, schemaID
	m := seekparquet.Manifest{Key: key, SchemaID: schemaID,
		Version: seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision}}
	for i, part := range parts {
		name := fmt.Sprintf("data/part-%05d.parquet", i)
		f, err := os.Create(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		w, err := write.NewPart(f, part.schema, opts)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if _, err := w.WriteRows(part.rows); err != nil {
			_ = w.Abort()
			t.Fatal(err)
		}
		info, err := w.Seal()
		if err != nil {
			t.Fatal(err)
		}
		info.Path = name
		m.Parts = append(m.Parts, info)
	}
	raw, err := seekparquet.MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, seekparquet.ManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// writeTyped writes rows (two keys, a and b) as the one part of a
// SeekParquet dataset of schemaID and returns the dataset directory.
func writeTyped[T any](t *testing.T, schemaID string, rows []T) string {
	t.Helper()
	return writeSeek(t, schemaID, 0, partOf(rows))
}

// TestProjectedColumnTypesAreChecked opens datasets whose projected
// columns have the wrong physical type, a repetition or a conflicting
// annotation, for the GCS schema's native columns and the S3-shaped
// columns: each is refused at open with unsupported_schema naming the
// column. Before, a GCS fixture with content_type stored as INT64 opened,
// and the GCS route's read of it as a string panicked.
func TestProjectedColumnTypesAreChecked(t *testing.T) {
	const gcs, s3 = "swath/gcs-objects/v1", "swath/s3-objects/v1"
	cases := []struct {
		name, column, schemaID string
		dir                    func(t *testing.T, schemaID string) string
	}{
		{"content_type as INT64", "content_type", gcs, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key         string `parquet:"key"`
				ContentType int64  `parquet:"content_type"`
			}{{"a", 7}, {"b", 8}})
		}},
		{"repeated md5", "md5", gcs, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key string   `parquet:"key"`
				MD5 []string `parquet:"md5"`
			}{{"a", []string{"x", "y"}}, {"b", nil}})
		}},
		{"generation as a string", "generation", gcs, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key        string `parquet:"key"`
				Generation string `parquet:"generation"`
			}{{"a", "1"}, {"b", "2"}})
		}},
		{"metageneration as unsigned", "metageneration", gcs, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key            string `parquet:"key"`
				Metageneration uint64 `parquet:"metageneration"`
			}{{"a", 1}, {"b", 2}})
		}},
		{"crc32c as JSON", "crc32c", gcs, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key    string `parquet:"key"`
				CRC32C string `parquet:"crc32c,json"`
			}{{"a", `"x"`}, {"b", `"y"`}})
		}},
		{"etag as INT64", "etag", s3, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key  string `parquet:"key"`
				ETag int64  `parquet:"etag"`
			}{{"a", 1}, {"b", 2}})
		}},
		{"repeated storage_class", "storage_class", s3, func(t *testing.T, id string) string {
			return writeTyped(t, id, []struct {
				Key          string   `parquet:"key"`
				StorageClass []string `parquet:"storage_class"`
			}{{"a", []string{"STANDARD"}}, {"b", nil}})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _, err := Open(context.Background(), c.dir(t, c.schemaID), Options{})
			if err == nil {
				_ = s.Close()
			}
			if Refusal(err) != "unsupported_schema" || !strings.Contains(err.Error(), "column "+c.column) {
				t.Fatalf("open: %v (reason %q)", err, Refusal(err))
			}
		})
	}
	// The right types open.
	dir := writeTyped(t, gcs, []struct {
		Key         string  `parquet:"key"`
		Generation  int64   `parquet:"generation"`
		MD5         *string `parquet:"md5,optional"`
		ContentType string  `parquet:"content_type"`
	}{{Key: "a", Generation: 1, ContentType: "text/plain"}, {Key: "b", Generation: 2, ContentType: "text/plain"}})
	s, _, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
}
