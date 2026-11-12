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
	"strings"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/reader"
)

// readAll writes rows with this build's writer and reads them back through
// the SeekParquet module in pages of limit rows, each continuing after the
// previous page's last decoded row (its reader.Position), projecting the
// columns of the schema id's reader spec.
func readAll[P, T any](t *testing.T, s *schema.Schema[P], kind string, rows []schema.Row[P], maxRows int64, limit int,
	decode func(seekparquet.Row) (T, error), pos func(*T) reader.Position) ([]T, reader.Metadata) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ds")
	writeSeekFixture(t, dir, kind, s, rows, maxRows)
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	meta, err := reader.ParseMetadata(d.Metadata())
	if err != nil {
		t.Fatal(err)
	}
	spec, ok := reader.Schema(meta.SchemaID)
	if !ok {
		t.Fatalf("schema id %q: no reader spec", meta.SchemaID)
	}
	var out []T
	var cur *seekparquet.Cursor
	for {
		page, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Cursor: cur}, Limit: int64(limit), Columns: spec.ColumnNames()}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return out, meta
		}
		for _, r := range page {
			v, err := decode(r)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, v)
		}
		cur = pos(&out[len(out)-1]).Cursor()
	}
}

func ptr[T any](v T) *T { return &v }

func micros(us int64) *time.Time { return ptr(time.UnixMicro(us).UTC()) }

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func s3Want(key string, offset int64, o s3.Object) reader.S3Object {
	return reader.S3Object{Position: reader.Position{Key: []byte(key), Offset: offset}, Size: ptr(o.Size), LastModified: micros(o.Mtime),
		ETag: nonEmpty(o.ETag), StorageClass: nonEmpty(o.StorageClass), OwnerID: nonEmpty(o.OwnerID), OwnerDisplayName: nonEmpty(o.OwnerDisplayName),
		ChecksumAlgorithm: nonEmpty(o.ChecksumAlgorithm), ChecksumType: nonEmpty(o.ChecksumType)}
}

// TestReaderDecodesS3Objects: every column of swath/s3-objects/v1 as the
// writer stores it, nulls (S3 omitted the value) kept nil, and the
// manifest's extensions.swath as Publish writes it.
func TestReaderDecodesS3Objects(t *testing.T) {
	rows := []s3.Row{
		{Key: "a/b/été.txt", Data: s3.Object{Size: 12, Mtime: 1_700_000_000_123_456, ETag: "0123456789abcdef0123456789abcdef", StorageClass: "STANDARD",
			OwnerID: "owner", OwnerDisplayName: "name", ChecksumAlgorithm: "CRC64NVME", ChecksumType: "FULL_OBJECT"}},
		{Key: "a/c", Data: s3.Object{Size: 0, Mtime: -1}},
		{Key: "z", Data: s3.Object{Size: 1 << 40, Mtime: 0, ETag: "abc-3", StorageClass: "GLACIER"}},
	}
	got, meta := readAll(t, &s3.Schema, "s3", rows, 0, 2, reader.DecodeS3Object, func(o *reader.S3Object) reader.Position { return o.Position })
	var want []reader.S3Object
	for _, r := range rows {
		want = append(want, s3Want(r.Key, 0, r.Data))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded\n%+v\nwant\n%+v", got, want)
	}
	if meta.SchemaID != reader.S3ObjectsV1 || !meta.Swath || meta.PublishedAt == nil || *meta.Source.Kind != "s3" || meta.Source.Container != nil ||
		*meta.Scope.Kind != "whole" || meta.Scope.Prefix != nil || !*meta.Coverage.Complete || *meta.Coverage.StopReason != "completed" ||
		*meta.Coverage.SkippedEntries != 0 || meta.Coverage.Filters == nil || len(meta.Coverage.Filters) != 0 || *meta.Coverage.Snapshot {
		t.Fatalf("metadata %+v", meta)
	}
	objects, _ := readAll(t, &s3.Schema, "s3", rows, 0, 10, func(r seekparquet.Row) (reader.Object, error) { return reader.DecodeObject(reader.S3ObjectsV1, r) },
		func(o *reader.Object) reader.Position { return o.Position })
	if o := objects[0]; string(o.Key) != rows[0].Key || *o.Size != 12 || *o.ETag != rows[0].Data.ETag || *o.StorageClass != "STANDARD" || objects[1].ETag != nil {
		t.Fatalf("objects %+v", objects)
	}
}

// TestReaderPagesS3VersionGroups: a key's versions are separate rows in
// the writer's order, each with its offset in the key's group, and a page
// that ends inside a group continues from the decoded row's position with
// no row lost or repeated, across part boundaries too. A delete marker's
// size is nil, never zero.
func TestReaderPagesS3VersionGroups(t *testing.T) {
	var rows []s3.VersionRow
	for k, n := range []int{1, 23, 3, 9} {
		for v := range n {
			ver := s3.Version{Object: s3.Object{Size: int64(100*k + v), Mtime: int64(1_000_000 * (n - v)), ETag: fmt.Sprintf("%032x", v), StorageClass: "STANDARD"},
				VersionID: fmt.Sprintf("v%03d", v), IsLatest: v == 0}
			if v%4 == 1 {
				ver = s3.Version{Object: s3.Object{Mtime: int64(1_000_000 * (n - v))}, VersionID: fmt.Sprintf("v%03d", v), IsLatest: v == 0, DeleteMarker: true}
			}
			if v == 2 {
				ver.VersionID = "null"
			}
			rows = append(rows, s3.VersionRow{Key: fmt.Sprintf("key-%d", k), Data: ver})
		}
	}
	var want []reader.S3Version
	offset := int64(0)
	for i, r := range rows {
		if i > 0 && rows[i-1].Key == r.Key {
			offset++
		} else {
			offset = 0
		}
		v := reader.S3Version{S3Object: s3Want(r.Key, offset, r.Data.Object), VersionID: ptr(r.Data.VersionID), IsLatest: r.Data.IsLatest, IsDeleteMarker: r.Data.DeleteMarker}
		if r.Data.DeleteMarker {
			v.Size = nil
		}
		want = append(want, v)
	}
	for _, limit := range []int{1, 4, 7, 100} {
		got, meta := readAll(t, &s3.VersionsSchema, "s3", rows, 8, limit, reader.DecodeS3Version, func(v *reader.S3Version) reader.Position { return v.Position })
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("limit %d: decoded\n%+v\nwant\n%+v", limit, got, want)
		}
		if meta.Manifest.Key.Unique || len(meta.Manifest.Parts) < 2 {
			t.Fatalf("limit %d: key %+v, %d parts: want repeated keys over several parts", limit, meta.Manifest.Key, len(meta.Manifest.Parts))
		}
	}
	dir := filepath.Join(t.TempDir(), "ds")
	writeSeekFixture(t, dir, "s3", &s3.VersionsSchema, rows, 0)
	d, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	page, err := d.Rows(context.Background(), seekparquet.Query{Limit: 1, Columns: []string{"size"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.DecodeObject(reader.S3VersionsV1, page[0]); !errors.Is(err, reader.ErrSchema) {
		t.Fatalf("a versions row as a current object: %v, want a schema error", err)
	}
	if _, err := reader.DecodeS3Version(page[0]); !errors.Is(err, reader.ErrSchema) {
		t.Fatalf("a row without the projected columns: %v, want a schema error", err)
	}
}

func gcsWant(key string, offset int64, o gcs.Object) reader.GCSObject {
	g := reader.GCSObject{Position: reader.Position{Key: []byte(key), Offset: offset}, Size: ptr(o.Size), ETag: nonEmpty(o.ETag),
		StorageClass: nonEmpty(o.StorageClass), MD5: nonEmpty(o.MD5), CRC32C: nonEmpty(o.CRC32C), ContentType: nonEmpty(o.ContentType)}
	if o.Mtime != 0 {
		g.LastModified = micros(o.Mtime)
	}
	if o.Generation != 0 {
		g.Generation = ptr(o.Generation)
	}
	if o.Metageneration != 0 {
		g.Metageneration = ptr(o.Metageneration)
	}
	if o.TimeCreated != 0 {
		g.TimeCreated = micros(o.TimeCreated)
	}
	return g
}

// TestReaderDecodesGCS: the GCS current-object and generations schemas,
// nulls kept; time_created, the column a part may lack, reads as nil from
// a part without it, where any other column's absence is a schema error.
func TestReaderDecodesGCS(t *testing.T) {
	full := gcs.Object{Size: 5, Mtime: 1_700_000_000_000_001, TimeCreated: 1_700_000_000_000_000, ETag: "CJ/r", Generation: 1700000000000001, Metageneration: 2,
		StorageClass: "NEARLINE", MD5: "1B2M2Y8AsgTpgAmY7PhCfg==", CRC32C: "AAAAAA==", ContentType: "text/plain"}
	rows := []gcs.Row{{Key: "dir/a", Data: full}, {Key: "dir/b", Data: gcs.Object{Size: 0}}}
	got, meta := readAll(t, &gcs.Schema, "gcs", rows, 0, 1, reader.DecodeGCSObject, func(o *reader.GCSObject) reader.Position { return o.Position })
	want := []reader.GCSObject{gcsWant("dir/a", 0, full), gcsWant("dir/b", 0, rows[1].Data)}
	if !reflect.DeepEqual(got, want) || meta.SchemaID != reader.GCSObjectsV1 {
		t.Fatalf("decoded\n%+v\nwant\n%+v (%s)", got, want, meta.SchemaID)
	}

	without := func(s schema.Schema[gcs.Object], name string) *schema.Schema[gcs.Object] {
		s.Columns = slices.DeleteFunc(slices.Clone(s.Columns), func(c schema.Column[gcs.Object]) bool { return c.Name == name })
		return &s
	}
	older, _ := readAll(t, without(gcs.Schema, "time_created"), "gcs", rows, 0, 10, reader.DecodeGCSObject, func(o *reader.GCSObject) reader.Position { return o.Position })
	if want[0].TimeCreated = nil; !reflect.DeepEqual(older, want) {
		t.Fatalf("a part without time_created:\n%+v\nwant\n%+v", older, want)
	}
	dir := filepath.Join(t.TempDir(), "ds")
	writeSeekFixture(t, dir, "gcs", without(gcs.Schema, "md5"), rows, 0)
	d, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	spec, _ := reader.Schema(reader.GCSObjectsV1)
	page, err := d.Rows(context.Background(), seekparquet.Query{Limit: 1, Columns: spec.ColumnNames()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var se *reader.SemanticError
	if _, err := reader.DecodeGCSObject(page[0]); !errors.As(err, &se) || se.Kind != reader.KindSchema || se.Column != "md5" || string(se.Row.Key) != "dir/a" {
		t.Fatalf("a part without md5: %v, want a schema error naming md5 and the row", err)
	}

	gens := []gcs.GenerationRow{
		{Key: "n", Data: gcs.Generation{Object: gcs.Object{Size: 1, Mtime: 10, Generation: 1, Metageneration: 1}, TimeDeleted: 20}},
		{Key: "n", Data: gcs.Generation{Object: gcs.Object{Size: 2, Mtime: 20, Generation: 2, Metageneration: 1}, TimeDeleted: 30}},
		{Key: "n", Data: gcs.Generation{Object: full}},
	}
	gotGens, _ := readAll(t, &gcs.GenerationSchema, "gcs", gens, 0, 2, reader.DecodeGCSGeneration, func(g *reader.GCSGeneration) reader.Position { return g.Position })
	wantGens := []reader.GCSGeneration{
		{GCSObject: gcsWant("n", 0, gens[0].Data.Object), TimeDeleted: micros(20)},
		{GCSObject: gcsWant("n", 1, gens[1].Data.Object), TimeDeleted: micros(30)},
		{GCSObject: gcsWant("n", 2, full)},
	}
	if !reflect.DeepEqual(gotGens, wantGens) {
		t.Fatalf("generations\n%+v\nwant\n%+v", gotGens, wantGens)
	}
}

// TestReaderDecodesLocalEntries: keys and link targets are bytes (not
// UTF-8, 0xFF), unsigned identifiers keep their full range, and a value
// the entry does not have is nil.
func TestReaderDecodesLocalEntries(t *testing.T) {
	target := "\xff\xfe/" + strings.Repeat("\xff", 40)
	rows := []localfs.Row{
		{Key: "d", Data: localfs.Entry{Kind: localfs.KindDir, Size: 4096, Mtime: 7, Mode: 0o40755, Present: localfs.HasSize | localfs.HasMtime | localfs.HasMode}},
		{Key: "d/\xc3\x28-not-utf8", Data: localfs.Entry{Kind: localfs.KindSymlink, LinkTarget: target, Size: 3, Mtime: 8, Ctime: 9, Atime: 10, Btime: 11,
			Inode: -1, Dev: -2, Blocks: 512, Mode: 0xffffffff, UID: 0xfffffffe, GID: 1 << 31, Nlink: 1, Present: localfs.HasStat | localfs.HasBtime | localfs.HasLink}},
		{Key: "d/\xff", Data: localfs.Entry{Kind: localfs.KindUnknown}},
	}
	got, meta := readAll(t, &localfs.Schema, "local-fs", rows, 0, 2, reader.DecodeLocalEntry, func(e *reader.LocalEntry) reader.Position { return e.Position })
	tb := []byte(target)
	want := []reader.LocalEntry{
		{Position: reader.Position{Key: []byte("d")}, Kind: "DIR", Size: ptr(int64(4096)), LastModified: micros(7), Mode: ptr(uint32(0o40755))},
		{Position: reader.Position{Key: []byte("d/\xc3\x28-not-utf8")}, Kind: "SYMLINK", Size: ptr(int64(3)), LastModified: micros(8), MetadataChangedAt: micros(9),
			Atime: micros(10), BirthTime: micros(11), Inode: ptr(uint64(1<<64 - 1)), Dev: ptr(uint64(1<<64 - 2)), AllocatedBytes: ptr(int64(512)),
			Mode: ptr(uint32(0xffffffff)), UID: ptr(uint32(0xfffffffe)), GID: ptr(uint32(1 << 31)), Nlink: ptr(uint32(1)), LinkTarget: &tb},
		{Position: reader.Position{Key: []byte("d/\xff")}, Kind: "UNKNOWN"},
	}
	if !reflect.DeepEqual(got, want) || meta.SchemaID != reader.LocalV1 || meta.Manifest.Key.Unique != true {
		t.Fatalf("decoded\n%+v\nwant\n%+v (%s)", got, want, meta.SchemaID)
	}
}
