// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
)

// writeTypedFormat2 writes rows (two keys, a and b) as the one part of a
// format-2 dataset of schemaID, with the profile's stamps and sorting
// metadata, and returns the dataset directory.
func writeTypedFormat2[T any](t *testing.T, schemaID string, rows []T) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := legacytest.WriteFormat2(dir, legacytest.Dataset{SchemaID: schemaID,
		Parts: []legacytest.Part{{Rows: objects("a", "b")}}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data", "part-00000.parquet")
	options := []parquet.WriterOption{parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("key")))}
	for k, v := range legacytest.Format2Stamps(schemaID, 1024) {
		options = append(options, parquet.KeyValueMetadata(k, v))
	}
	if err := parquet.WriteFile(path, rows, options...); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m["parts"].([]any)[0].(map[string]any)["bytes"] = info.Size()
	raw, _ = json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
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
			return writeTypedFormat2(t, id, []struct {
				Key         string `parquet:"key"`
				ContentType int64  `parquet:"content_type"`
			}{{"a", 7}, {"b", 8}})
		}},
		{"repeated md5", "md5", gcs, func(t *testing.T, id string) string {
			return writeTypedFormat2(t, id, []struct {
				Key string   `parquet:"key"`
				MD5 []string `parquet:"md5"`
			}{{"a", []string{"x", "y"}}, {"b", nil}})
		}},
		{"generation as a string", "generation", gcs, func(t *testing.T, id string) string {
			return writeTypedFormat2(t, id, []struct {
				Key        string `parquet:"key"`
				Generation string `parquet:"generation"`
			}{{"a", "1"}, {"b", "2"}})
		}},
		{"metageneration as unsigned", "metageneration", gcs, func(t *testing.T, id string) string {
			return writeTypedFormat2(t, id, []struct {
				Key            string `parquet:"key"`
				Metageneration uint64 `parquet:"metageneration"`
			}{{"a", 1}, {"b", 2}})
		}},
		{"crc32c as JSON", "crc32c", gcs, func(t *testing.T, id string) string {
			return writeTypedFormat2(t, id, []struct {
				Key    string `parquet:"key"`
				CRC32C string `parquet:"crc32c,json"`
			}{{"a", `"x"`}, {"b", `"y"`}})
		}},
		{"etag as INT64", "etag", s3, func(t *testing.T, id string) string {
			return writeTypedFormat2(t, id, []struct {
				Key  string `parquet:"key"`
				ETag int64  `parquet:"etag"`
			}{{"a", 1}, {"b", 2}})
		}},
		{"repeated storage_class", "storage_class", s3, func(t *testing.T, id string) string {
			return writeTypedFormat2(t, id, []struct {
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
	dir := writeTypedFormat2(t, gcs, []struct {
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
