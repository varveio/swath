// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/reader"
)

// openDataset writes rows with this build's writer and opens the dataset
// through the reader module: its schema checked, and its metadata the
// source, scope and coverage the writer recorded.
func openDataset[P any](t *testing.T, s *schema.Schema[P], kind string, rows []schema.Row[P], maxRows int64) *reader.Dataset {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ds")
	writeSeekFixture(t, dir, kind, s, rows, maxRows)
	d, err := reader.OpenDir(context.Background(), dir, seekparquet.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatalf("%s: check schema: %v", s.ID, err)
	}
	m := d.Metadata()
	if !m.Swath || m.PublishedAt == nil || *m.Source.Kind != kind || *m.Source.URI != "fixture://"+filepath.Base(dir) ||
		*m.Scope.Kind != "whole" || !*m.Coverage.Complete || *m.Coverage.StopReason != "completed" ||
		*m.Coverage.SkippedEntries != 0 || m.Coverage.Filters == nil || len(m.Coverage.Filters) != 0 {
		t.Fatalf("%s: metadata %+v, coverage %+v", s.ID, m, m.Coverage)
	}
	return d
}

func streamAll(d *reader.Dataset) ([]reader.Object, error) {
	var out []reader.Object
	err := d.Stream(context.Background(), func(o reader.Object) error {
		out = append(out, o)
		return nil
	})
	return out, err
}

func sameObjects(t *testing.T, got, want []reader.Object) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("streamed %d objects, want %d", len(got), len(want))
	}
	for i := range got {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("object %d: %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestReaderStreamsS3Objects: the current objects of a dataset over
// several parts, in key order, with their nulls.
func TestReaderStreamsS3Objects(t *testing.T) {
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
	got, err := streamAll(d)
	if err != nil {
		t.Fatal(err)
	}
	var want []reader.Object
	for _, r := range rows {
		w := s3Want(r.Key, 0, r.Data)
		want = append(want, reader.Object{Position: w.Position, Size: w.Size, LastModified: w.LastModified, ETag: w.ETag, StorageClass: w.StorageClass})
	}
	sameObjects(t, got, want)
	if st := d.StreamStats(); st.Objects != 3000 || st.PartsOpened != 0 {
		t.Fatalf("stats %+v: every part was opened by CheckSchema", st)
	}
}

// TestReaderStreamsGCSObjects: the parts written before time_created was
// added pass the schema check; their objects stream alike.
func TestReaderStreamsGCSObjects(t *testing.T) {
	rows := []gcs.Row{
		{Key: "a", Data: gcs.Object{Size: 5, Mtime: 1_700_000_000_000_001, TimeCreated: 1, ETag: "CJ/r", Generation: 3, StorageClass: "NEARLINE"}},
		{Key: "b", Data: gcs.Object{Size: 0}},
	}
	want := []reader.Object{
		{Position: reader.Position{Key: []byte("a")}, Size: ptr(int64(5)), LastModified: micros(1_700_000_000_000_001), ETag: ptr("CJ/r"), StorageClass: ptr("NEARLINE")},
		{Position: reader.Position{Key: []byte("b")}, Size: ptr(int64(0))},
	}
	older := gcs.Schema
	older.Columns = slices.DeleteFunc(slices.Clone(older.Columns), func(c schema.Column[gcs.Object]) bool { return c.Name == "time_created" })
	for _, s := range []*schema.Schema[gcs.Object]{&gcs.Schema, &older} {
		got, err := streamAll(openDataset(t, s, "gcs", rows, 0))
		if err != nil {
			t.Fatal(err)
		}
		sameObjects(t, got, want)
	}
}

// TestReaderStreamsLocalEntries: a filesystem's entries stream through
// StreamLocal with their kind, link target and nulls: one listed without
// a size keeps it nil. Stream refuses them: they are entries, not objects.
func TestReaderStreamsLocalEntries(t *testing.T) {
	rows := []localfs.Row{
		{Key: "d/", Data: localfs.Entry{Kind: localfs.KindDir, Size: 4096, Mtime: 7, Present: localfs.HasSize | localfs.HasMtime}},
		{Key: "d/l", Data: localfs.Entry{Kind: localfs.KindSymlink, LinkTarget: "../x", Present: localfs.HasLink}},
		{Key: "d/\xff", Data: localfs.Entry{Kind: localfs.KindFile, Size: 3, Present: localfs.HasSize}},
		{Key: "e", Data: localfs.Entry{Kind: localfs.KindUnknown}},
	}
	d := openDataset(t, &localfs.Schema, "local-fs", rows, 0)
	var got []reader.LocalEntry
	if err := d.StreamLocal(context.Background(), func(e reader.LocalEntry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []reader.LocalEntry{
		{Position: reader.Position{Key: []byte("d/")}, Kind: "DIR", Size: ptr(int64(4096)), LastModified: micros(7)},
		{Position: reader.Position{Key: []byte("d/l")}, Kind: "SYMLINK", LinkTarget: ptr([]byte("../x"))},
		{Position: reader.Position{Key: []byte("d/\xff")}, Kind: "FILE", Size: ptr(int64(3))},
		{Position: reader.Position{Key: []byte("e")}, Kind: "UNKNOWN"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
	if _, err := streamAll(d); !errors.Is(err, reader.ErrSchema) {
		t.Fatalf("stream entries as objects: %v", err)
	}
}

// TestReaderRefusesVersionsStream: the versions schemas pass admission,
// and Stream refuses them: their rows are history, not current objects.
func TestReaderRefusesVersionsStream(t *testing.T) {
	s3d := openDataset(t, &s3.VersionsSchema, "s3", []s3.VersionRow{
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1}, VersionID: "v2", IsLatest: true}},
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 2}, VersionID: "v1"}},
	}, 0)
	gcsd := openDataset(t, &gcs.GenerationSchema, "gcs", []gcs.GenerationRow{
		{Key: "n", Data: gcs.Generation{Object: gcs.Object{Size: 1, Generation: 1}, TimeDeleted: 2}},
	}, 0)
	for _, d := range []*reader.Dataset{s3d, gcsd} {
		if _, err := streamAll(d); !errors.Is(err, reader.ErrSchema) {
			t.Fatalf("%s: stream: %v, want a schema error", d.Metadata().SchemaID, err)
		}
	}
}
