// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/azblob"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
)

// TestVerifierFixtures writes real Go Parquet parts for the independent
// Python verifier tests. Mutations preserve profile stamps, manifest bounds,
// digests and all other columns, so each refusal exercises the schema check.
func TestVerifierFixtures(t *testing.T) {
	base := os.Getenv("SWATH_VERIFIER_FIXTURES")
	if base == "" {
		t.Skip("Python verifier fixtures not requested")
	}
	writeVerifierFixtures(t, filepath.Join(base, "current"), "gcs", gcs.Schema, gcs.Object{Size: 7, Mtime: 10, Generation: 2, TimeCreated: 5})
	writeVerifierFixtures(t, filepath.Join(base, "generations"), "gcs", gcs.GenerationSchema,
		gcs.Generation{Object: gcs.Object{Size: 7, Mtime: 10, Generation: 2, TimeCreated: 5}, TimeDeleted: 20})
	writeVerifierFixtures(t, filepath.Join(base, "azblob"), "azblob", azblob.Schema, azblob.Object{Size: 7, Mtime: 10, CreationTime: 5})
	writeVerifierFixtures(t, filepath.Join(base, "azblobversions"), "azblob", azblob.VersionSchema,
		azblob.Version{Object: azblob.Object{Size: 7, Mtime: 10, CreationTime: 5}, VersionID: "2026-01-01T00:00:00.0000000Z", IsLatest: true})
}

func writeVerifierFixtures[P any](t *testing.T, base, kind string, original schema.Schema[P], value P) {
	t.Helper()
	variants := map[string]func(*schema.Schema[P]){"valid": func(*schema.Schema[P]) {}}
	for _, col := range original.Columns {
		name := col.Name
		variants["missing_"+name] = func(s *schema.Schema[P]) {
			s.Columns = slices.DeleteFunc(s.Columns, func(c schema.Column[P]) bool { return c.Name == name })
		}
		if col.Required {
			continue // nothing to mutate: the contract requires it already
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
		meta := dataset.Meta{SchemaID: original.ID, Groups: original.Groups, Source: dataset.Source{Kind: kind, URI: "fixture://b", Container: "b"}, Scope: dataset.Scope{Kind: "whole"},
			Coverage: dataset.Coverage{Complete: true, StopReason: "completed", Exclusions: []string{}, Filters: []string{}}}
		if err := os.WriteFile(filepath.Join(dir, "run.json"), []byte("{}\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := dataset.Publish(dir, meta, parts, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSeekParquetFixtures writes datasets with this build's writer for the
// SeekParquet conformance check (scripts/ci/seekparquet-conformance.sh):
// S3 current objects over several parts and row groups, whose keys roll up
// at several depths; S3 versions, whose keys repeat in groups that span
// pages and row groups (unique false); and filesystem keys longer than the
// 1,024 bytes, half of them led by 0xFF bytes, with link targets as long,
// whose exact bounds parquet-go would have shortened below the value.
func TestSeekParquetFixtures(t *testing.T) {
	base := os.Getenv("SWATH_SEEKPARQUET_FIXTURES")
	if base == "" {
		t.Skip("SeekParquet conformance fixtures not requested")
	}
	var current []s3.Row
	for i := range 9000 {
		key := fmt.Sprintf("logs/%02d/%03d/obj-%05d.json", i%13, i%97, i)
		if i%5 == 0 {
			key = fmt.Sprintf("top-%05d", i)
		}
		current = append(current, s3.Row{Key: key, Data: s3.Object{Size: int64(i), Mtime: int64(i) * 1_000_000, ETag: fmt.Sprintf("%032x", i), StorageClass: "STANDARD"}})
	}
	slices.SortFunc(current, func(a, b s3.Row) int { return strings.Compare(a.Key, b.Key) })
	current = slices.CompactFunc(current, func(a, b s3.Row) bool { return a.Key == b.Key })
	writeSeekFixture(t, filepath.Join(base, "s3-current"), "s3", &s3.Schema, current, 2500)

	var versions []s3.VersionRow
	for k := range 400 {
		n := 1 + k%4
		if k%50 == 7 {
			n = 3000 // a group over several pages and row groups
		}
		for v := range n {
			versions = append(versions, s3.VersionRow{Key: fmt.Sprintf("v/%02d/key-%04d", k%9, k),
				Data: s3.Version{Object: s3.Object{Size: int64(v), Mtime: int64(n - v)}, VersionID: fmt.Sprintf("v%05d", v), IsLatest: v == 0}})
		}
	}
	slices.SortStableFunc(versions, func(a, b s3.VersionRow) int { return strings.Compare(a.Key, b.Key) })
	writeSeekFixture(t, filepath.Join(base, "s3-versions"), "s3", &s3.VersionsSchema, versions, 4000)

	var long []localfs.Row
	for i := range 3000 {
		lead := strings.Repeat("a", 1100)
		if i >= 1500 {
			lead = strings.Repeat("\xff", 1100)
		}
		entry := localfs.Entry{Kind: localfs.KindFile}
		if i%3 == 0 { // link targets over 1,024 bytes, some all 0xFF
			entry = localfs.Entry{Kind: localfs.KindSymlink, LinkTarget: lead + strings.Repeat("\xff", i%5), Present: localfs.HasLink}
		}
		long = append(long, localfs.Row{Key: fmt.Sprintf("%s/%02d/%05d", lead, i%7, i), Data: entry})
	}
	slices.SortFunc(long, func(a, b localfs.Row) int { return strings.Compare(a.Key, b.Key) })
	writeSeekFixture(t, filepath.Join(base, "local-long-keys"), "local-fs", &localfs.Schema, long, 1000)
}

func writeSeekFixture[P any](t *testing.T, dir, kind string, s *schema.Schema[P], rows []schema.Row[P], maxRows int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	router, err := parquetout.NewRouter(parquetout.Options{Out: dir, PartSize: 64 << 20, Encoders: 2, MaxRows: maxRows}, s)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += 1000 {
		if err := router.Write(context.Background(), [][]schema.Row[P]{rows[i:min(i+1000, len(rows))]}); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := router.Close()
	if err != nil {
		t.Fatal(err)
	}
	meta := dataset.Meta{SchemaID: s.ID, Groups: s.Groups, Source: dataset.Source{Kind: kind, URI: "fixture://" + filepath.Base(dir)},
		Scope: dataset.Scope{Kind: "whole"}, Coverage: dataset.Coverage{Complete: true, StopReason: "completed"}}
	if err := dataset.Publish(dir, meta, parts, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, dataset.ReportName), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
