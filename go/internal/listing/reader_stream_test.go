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

// admitDataset writes rows with this build's writer and opens the dataset through
// the reader module, which must admit it as swath wrote it: its schema
// checked, and accepted as the whole of its source.
func admitDataset[P any](t *testing.T, s *schema.Schema[P], kind string, rows []schema.Row[P], maxRows int64) *reader.Dataset {
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
	if err := d.AcceptWholeSource(reader.WholeSource{Kind: kind, URI: "fixture://" + filepath.Base(dir)}); err != nil {
		t.Fatalf("%s: accept: %v", s.ID, err)
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
// several parts and Range pages, in key order, with their nulls.
func TestReaderStreamsS3Objects(t *testing.T) {
	var rows []s3.Row
	for i := range 3000 {
		o := s3.Object{Size: int64(i), Mtime: int64(i) * 1_000_003}
		if i%4 != 0 {
			o.ETag, o.StorageClass = fmt.Sprintf("%032x", i), "STANDARD"
		}
		rows = append(rows, s3.Row{Key: fmt.Sprintf("p/%05d", i), Data: o})
	}
	d := admitDataset(t, &s3.Schema, "s3", rows, 1000)
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
		got, err := streamAll(admitDataset(t, s, "gcs", rows, 0))
		if err != nil {
			t.Fatal(err)
		}
		sameObjects(t, got, want)
	}
}

// TestReaderStreamsLocalEntries: a filesystem entry streams as an object
// without an ETag or storage class; one listed without a size ends the
// stream.
func TestReaderStreamsLocalEntries(t *testing.T) {
	rows := []localfs.Row{
		{Key: "d", Data: localfs.Entry{Kind: localfs.KindDir, Size: 4096, Mtime: 7, Present: localfs.HasSize | localfs.HasMtime}},
		{Key: "d/\xff", Data: localfs.Entry{Kind: localfs.KindFile, Size: 3, Present: localfs.HasSize}},
	}
	got, err := streamAll(admitDataset(t, &localfs.Schema, "local-fs", rows, 0))
	if err != nil {
		t.Fatal(err)
	}
	sameObjects(t, got, []reader.Object{
		{Position: reader.Position{Key: []byte("d")}, Size: ptr(int64(4096)), LastModified: micros(7)},
		{Position: reader.Position{Key: []byte("d/\xff")}, Size: ptr(int64(3))},
	})
	rows = append(rows, localfs.Row{Key: "e", Data: localfs.Entry{Kind: localfs.KindUnknown}})
	got, err = streamAll(admitDataset(t, &localfs.Schema, "local-fs", rows, 0))
	var se *reader.SemanticError
	if !errors.As(err, &se) || se.Kind != reader.KindRow || se.Column != "size" || string(se.Row.Key) != "e" || len(got) != 2 {
		t.Fatalf("an entry without a size: %v after %d objects", err, len(got))
	}
}

// TestReaderRefusesVersionsStream: the versions schemas pass admission,
// and Stream refuses them: their rows are history, not current objects.
func TestReaderRefusesVersionsStream(t *testing.T) {
	s3d := admitDataset(t, &s3.VersionsSchema, "s3", []s3.VersionRow{
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1}, VersionID: "v2", IsLatest: true}},
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 2}, VersionID: "v1"}},
	}, 0)
	gcsd := admitDataset(t, &gcs.GenerationSchema, "gcs", []gcs.GenerationRow{
		{Key: "n", Data: gcs.Generation{Object: gcs.Object{Size: 1, Generation: 1}, TimeDeleted: 2}},
	}, 0)
	for _, d := range []*reader.Dataset{s3d, gcsd} {
		if _, err := streamAll(d); !errors.Is(err, reader.ErrSchema) {
			t.Fatalf("%s: stream: %v, want a schema error", d.Metadata().SchemaID, err)
		}
	}
}
