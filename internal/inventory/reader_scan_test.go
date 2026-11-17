// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/reader"
)

// openDataset writes rows with this build's writer and opens the dataset
// through the reader module: its schema checked, and its metadata the
// source, scope and coverage the writer recorded.
func openDataset[P any](t *testing.T, s *schema.Schema[P], kind string, rows []schema.Row[P], maxRows int64) *reader.Dataset {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ds")
	writeSeekFixture(t, dir, kind, s, rows, maxRows)
	d, err := reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatalf("%s: check schema: %v", s.ID, err)
	}
	m := d.Metadata()
	if !m.Swath || m.PublishedAt == nil || *m.Source.Kind != kind || *m.Source.URI != "fixture://"+filepath.Base(dir) ||
		*m.Scope.Kind != "whole" || !*m.Coverage.Complete || *m.Coverage.StopReason != "end_of_scope" ||
		*m.Coverage.Unreadable != 0 || m.Coverage.Filters == nil || len(m.Coverage.Filters) != 0 {
		t.Fatalf("%s: metadata %+v, coverage %+v", s.ID, m, m.Coverage)
	}
	return d
}

func scanDecoded[T any](d *reader.Dataset, decode func(seekparquet.Row) (T, error)) ([]T, seekparquet.ReadStats, error) {
	var out []T
	var st seekparquet.ReadStats
	spec, _ := reader.Schema(d.Metadata().SchemaID)
	err := d.SeekParquet().Scan(context.Background(), seekparquet.Query{Columns: spec.ColumnNames()}, &st, func(b *seekparquet.Batch) error {
		for i := range b.Len() {
			v, err := decode(b.Row(i))
			if err != nil {
				return err
			}
			out = append(out, v)
		}
		return nil
	})
	return out, st, err
}

// TestReaderScansS3Objects: the current objects of a dataset over
// several parts, in key order, with their nulls.
func TestReaderScansS3Objects(t *testing.T) {
	var rows []s3.Row
	for i := range 3000 {
		o := s3.Object{Size: int64(i), Mtime: int64(i) * 1_000_003}
		if i%4 != 0 {
			o.ETag, o.StorageClass = fmt.Sprintf("%032x", i), "STANDARD"
		}
		rows = append(rows, s3.Row{Key: fmt.Sprintf("p/%05d", i), Data: o})
	}
	d := openDataset(t, &s3.Schema, "s3", rows, 1000)
	if n := len(d.Metadata().Manifest.Parts); n != 3 {
		t.Fatalf("%d parts, want 3", n)
	}
	got, st, err := scanDecoded(d, reader.DecodeS3Object)
	if err != nil {
		t.Fatal(err)
	}
	var want []reader.S3Object
	for _, r := range rows {
		want = append(want, s3Want(r.Key, 0, r.Data))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
	if len(got) != 3000 || st.PartsOpened != 0 {
		t.Fatalf("stats %+v: every part was opened by CheckSchema", st)
	}
}

// TestReaderScansGCSObjects: the production writer's parts pass the
// schema check and decode; a part without one of the schema's columns
// fails the check.
func TestReaderScansGCSObjects(t *testing.T) {
	rows := []gcs.Row{
		{Key: "a", Data: gcs.Object{Size: 5, Mtime: 1_700_000_000_000_001, TimeCreated: 1, ETag: "CJ/r", Generation: 3, StorageClass: "NEARLINE"}},
		{Key: "b", Data: gcs.Object{Size: 0}},
	}
	want := []reader.GCSObject{gcsWant("a", 0, rows[0].Data), gcsWant("b", 0, rows[1].Data)}
	got, _, err := scanDecoded(openDataset(t, &gcs.Schema, "gcs", rows, 0), reader.DecodeGCSObject)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
	older := gcs.Schema
	older.Columns = slices.DeleteFunc(slices.Clone(older.Columns), func(c schema.Column[gcs.Object]) bool { return c.Name == "created_at" })
	dir := filepath.Join(t.TempDir(), "ds")
	writeSeekFixture(t, dir, "gcs", &older, rows, 0)
	d, err := reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.CheckSchema(context.Background()); !errors.Is(err, reader.ErrSchema) {
		t.Fatalf("a part without created_at: %v, want a schema error", err)
	}
}

// TestReaderScansLocalEntries: concrete decoding keeps kind, binary keys,
// link targets and nulls from the production writer.
func TestReaderScansLocalEntries(t *testing.T) {
	rows := []localfs.Row{
		{Key: "d/", Data: localfs.Entry{Kind: localfs.KindDir, Size: 4096, Mtime: 7, Present: localfs.HasSize | localfs.HasMtime}},
		{Key: "d/l", Data: localfs.Entry{Kind: localfs.KindSymlink, LinkTarget: "../x", Present: localfs.HasLink}},
		{Key: "d/\xff", Data: localfs.Entry{Kind: localfs.KindFile, Size: 3, Present: localfs.HasSize}},
		{Key: "e", Data: localfs.Entry{Kind: localfs.KindUnknown}},
	}
	d := openDataset(t, &localfs.Schema, "local", rows, 0)
	got, _, err := scanDecoded(d, reader.DecodeLocalEntry)
	if err != nil {
		t.Fatal(err)
	}
	want := []reader.LocalEntry{
		{Position: reader.Position{Key: []byte("d/")}, Kind: "dir", Size: ptr(int64(4096)), LastModified: micros(7)},
		{Position: reader.Position{Key: []byte("d/l")}, Kind: "symlink", LinkTarget: ptr([]byte("../x"))},
		{Position: reader.Position{Key: []byte("d/\xff")}, Kind: "file", Size: ptr(int64(3))},
		{Position: reader.Position{Key: []byte("e")}, Kind: "unknown"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
}

// TestReaderChecksVersionSchemas: version datasets pass the swath schema check.
func TestReaderChecksVersionSchemas(t *testing.T) {
	s3d := openDataset(t, &s3.VersionsSchema, "s3", []s3.VersionRow{
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1}, VersionID: "v2", IsLatest: true}},
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 2}, VersionID: "v1"}},
	}, 0)
	gcsd := openDataset(t, &gcs.GenerationSchema, "gcs", []gcs.GenerationRow{
		{Key: "n", Data: gcs.Generation{Object: gcs.Object{Size: 1, Generation: 1}, TimeDeleted: 2}},
	}, 0)
	if s3d.Metadata().SchemaID != reader.S3VersionsV1 || gcsd.Metadata().SchemaID != reader.GCSGenerationsV1 {
		t.Fatal("wrong version schema")
	}
}
