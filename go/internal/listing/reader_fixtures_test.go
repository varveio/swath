// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
)

// TestVerifierFixtures writes real Go Parquet parts for the independent
// Python verifier tests. Mutations preserve profile stamps, manifest bounds,
// digests and all other columns, so each refusal exercises the schema check.
func TestVerifierFixtures(t *testing.T) {
	base := os.Getenv("SWATH_VERIFIER_FIXTURES")
	if base == "" {
		t.Skip("Python verifier fixtures not requested")
	}
	writeVerifierFixtures(t, filepath.Join(base, "current"), gcs.Schema, gcs.Object{Size: 7, Mtime: 10, Generation: 2, TimeCreated: 5})
	writeVerifierFixtures(t, filepath.Join(base, "generations"), gcs.GenerationSchema,
		gcs.Generation{Object: gcs.Object{Size: 7, Mtime: 10, Generation: 2, TimeCreated: 5}, TimeDeleted: 20})
}

func writeVerifierFixtures[P any](t *testing.T, base string, original schema.Schema[P], value P) {
	t.Helper()
	variants := map[string]func(*schema.Schema[P]){"valid": func(*schema.Schema[P]) {}}
	for _, col := range original.Columns {
		name := col.Name
		variants["missing_"+name] = func(s *schema.Schema[P]) {
			s.Columns = slices.DeleteFunc(s.Columns, func(c schema.Column[P]) bool { return c.Name == name })
		}
		variants["required_"+name] = func(s *schema.Schema[P]) {
			for i := range s.Columns {
				if s.Columns[i].Name == name {
					s.Columns[i].Required = true
				}
			}
		}
		if col.Type == schema.TimestampMicros || col.Type == schema.Int64 {
			variants["wrong_type_"+name] = func(s *schema.Schema[P]) {
				for i := range s.Columns {
					if s.Columns[i].Name == name {
						if col.Type == schema.TimestampMicros {
							s.Columns[i].Type = schema.Int64
						} else {
							s.Columns[i].Type = schema.Int32
						}
					}
				}
			}
		}
	}
	for name, mutate := range variants {
		dir := filepath.Join(base, name)
		var parts []dataset.Part
		for i, key := range []string{"a", "b"} {
			s := original
			s.Columns = slices.Clone(original.Columns)
			// A valid first part must not hide a malformed later part.
			// Omit size from both parts to check a consistently incomplete schema.
			if i == 1 || name == "missing_size" {
				mutate(&s)
			}
			router, err := parquetout.NewRouter(parquetout.Options{Out: t.TempDir(), PartSize: 64 << 20, Encoders: 1}, &s)
			if err != nil {
				t.Fatal(err)
			}
			r := schema.Row[P]{Key: key, Data: value}
			if err := router.Write(context.Background(), [][]schema.Row[P]{{r}}); err != nil {
				t.Fatal(err)
			}
			written, err := router.Close()
			if err != nil {
				t.Fatal(err)
			}
			if len(written) != 1 {
				t.Fatalf("parts = %d", len(written))
			}
			part := written[0]
			path := dataset.TempPath(dir, i)
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(part.Path, path); err != nil {
				t.Fatal(err)
			}
			part.Path = path
			parts = append(parts, part)
		}
		if err := os.MkdirAll(filepath.Join(dir, "data"), 0755); err != nil {
			t.Fatal(err)
		}
		meta := dataset.Meta{SchemaID: original.ID, Source: dataset.Source{Kind: "gcs", URI: "gs://b", Container: "b"}, Scope: dataset.Scope{Kind: "whole"},
			Coverage: dataset.Coverage{Complete: true, StopReason: "completed", Exclusions: []string{}, Filters: []string{}}}
		if err := os.WriteFile(filepath.Join(dir, "run.json"), []byte("{}\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := dataset.Publish(dir, meta, parts, nil); err != nil {
			t.Fatal(err)
		}
	}
}
