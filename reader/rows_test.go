// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"errors"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

// row builds a projected row of the schema id's columns from values by
// name; a column not given is null.
func row(t *testing.T, id SchemaID, key string, offset int64, values map[string]seekparquet.Value) seekparquet.Row {
	t.Helper()
	spec, _ := Schema(id)
	r := seekparquet.Row{Key: []byte(key), Offset: offset, Columns: spec.ColumnNames()}
	for _, name := range r.Columns {
		v, ok := values[name]
		if !ok {
			v = seekparquet.NullValue()
		}
		r.Values = append(r.Values, v)
	}
	return r
}

var absent = seekparquet.AbsentValue()

func TestDecodeKeepsNullsAndPosition(t *testing.T) {
	r := row(t, S3VersionsV1, "k", 3, map[string]seekparquet.Value{
		"last_modified": seekparquet.Int64Value(-1), "version_id": seekparquet.ByteArrayValue([]byte("null")),
		"is_latest": seekparquet.BooleanValue(false), "is_delete_marker": seekparquet.BooleanValue(true),
	})
	v, err := DecodeS3Version(r)
	if err != nil {
		t.Fatal(err)
	}
	if v.Size != nil || v.ETag != nil || v.StorageClass != nil || *v.VersionID != "null" || v.IsLatest || !v.IsDeleteMarker ||
		v.LastModified.UnixMicro() != -1 || v.LastModified.Location().String() != "UTC" {
		t.Fatalf("decoded %+v", v)
	}
	if c := v.Cursor(); string(c.Key) != "k" || c.Offset != 3 || c.Prefix != nil || &c.Key[0] == &r.Key[0] {
		t.Fatalf("cursor %+v", c)
	}
}

func TestDecodeUnsignedAndBinary(t *testing.T) {
	target := []byte{0xff, 0x00, 0xfe}
	r := row(t, LocalV1, "\xff\x00", 0, map[string]seekparquet.Value{
		"kind": seekparquet.ByteArrayValue([]byte("SYMLINK")), "mode": seekparquet.Int32Value(-1), "gid": seekparquet.Int32Value(0),
		"inode": seekparquet.Int64Value(-1), "dev": seekparquet.Int64Value(1 << 62), "link_target": seekparquet.ByteArrayValue(target),
	})
	e, err := DecodeLocalEntry(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.Key, []byte("\xff\x00")) || e.Kind != "SYMLINK" || *e.Mode != 0xffffffff || *e.GID != 0 || e.UID != nil ||
		*e.Inode != 1<<64-1 || *e.Dev != 1<<62 || !bytes.Equal(*e.LinkTarget, target) || e.Size != nil {
		t.Fatalf("decoded %+v", e)
	}
}

func TestDecodeErrors(t *testing.T) {
	ok := map[string]seekparquet.Value{"kind": seekparquet.ByteArrayValue([]byte("FILE"))}
	with := func(name string, v seekparquet.Value) map[string]seekparquet.Value {
		m := map[string]seekparquet.Value{"kind": ok["kind"]}
		m[name] = v
		return m
	}
	for _, tc := range []struct {
		name   string
		row    seekparquet.Row
		target error
		column string
	}{
		{"null in a required column", row(t, LocalV1, "k", 0, nil), ErrRow, "kind"},
		{"absent column", row(t, LocalV1, "k", 0, with("uid", absent)), ErrSchema, "uid"},
		{"absent required column", row(t, LocalV1, "k", 0, with("kind", absent)), ErrSchema, "kind"},
		{"another type", row(t, LocalV1, "k", 0, with("size", seekparquet.Int32Value(1))), ErrSchema, "size"},
		{"another type, a time", row(t, LocalV1, "k", 0, with("atime", seekparquet.ByteArrayValue([]byte("x")))), ErrSchema, "atime"},
		{"not projected", seekparquet.Row{Key: []byte("k"), Columns: []string{"kind"}, Values: []seekparquet.Value{ok["kind"]}}, ErrSchema, "size"},
	} {
		_, err := DecodeLocalEntry(tc.row)
		var se *SemanticError
		if !errors.Is(err, tc.target) || !errors.As(err, &se) || se.Column != tc.column || se.Row == nil || string(se.Row.Key) != "k" || se.Err == nil {
			t.Errorf("%s: %v", tc.name, err)
		}
		for _, other := range []error{ErrSchema, ErrMetadata, ErrRow} {
			if other != tc.target && errors.Is(err, other) {
				t.Errorf("%s: %v also matches %v", tc.name, err, other)
			}
		}
	}

	g, err := DecodeGCSGeneration(row(t, GCSGenerationsV1, "n", 1, map[string]seekparquet.Value{"time_created": absent}))
	if err != nil || g.TimeCreated != nil || g.TimeDeleted != nil || g.Offset != 1 {
		t.Fatalf("time_created absent from a part: %+v, %v", g, err)
	}
	if _, err := DecodeS3Object(row(t, S3ObjectsV1, "k", 0, map[string]seekparquet.Value{"etag": absent})); !errors.Is(err, ErrSchema) {
		t.Fatalf("etag absent: %v", err)
	}
}

// TestDecodeAzblobObject: every column of an Azure row, nulls kept nil.
func TestDecodeAzblobObject(t *testing.T) {
	r := row(t, AzblobObjectsV1, "dir/blob", 0, map[string]seekparquet.Value{
		"size": seekparquet.Int64Value(42), "last_modified": seekparquet.Int64Value(1_767_225_600_000_000),
		"etag": seekparquet.ByteArrayValue([]byte("0x8DE")), "content_type": seekparquet.ByteArrayValue([]byte("text/plain")),
		"blob_type": seekparquet.ByteArrayValue([]byte("BlockBlob")), "creation_time": seekparquet.Int64Value(1_767_225_000_000_000),
	})
	o, err := DecodeAzblobObject(r)
	if err != nil || *o.Size != 42 || *o.ETag != "0x8DE" || *o.ContentType != "text/plain" || *o.BlobType != "BlockBlob" ||
		o.ContentMD5 != nil || o.AccessTier != nil || o.LastModified.Unix() != 1_767_225_600 || o.CreationTime.Unix() != 1_767_225_000 ||
		string(o.Key) != "dir/blob" {
		t.Fatalf("decoded %+v, %v", o, err)
	}
	if _, err := DecodeAzblobObject(row(t, AzblobObjectsV1, "k", 0, map[string]seekparquet.Value{"blob_type": absent})); !errors.Is(err, ErrSchema) {
		t.Fatalf("blob_type absent: %v", err)
	}
	// The columns added later: read where present, nil where a part
	// written before lacks them.
	r = row(t, AzblobObjectsV1, "k", 0, map[string]seekparquet.Value{
		"access_tier_inferred": seekparquet.BooleanValue(true), "lease_state": seekparquet.ByteArrayValue([]byte("leased")),
		"tag_count": seekparquet.Int64Value(3), "metadata": seekparquet.ByteArrayValue([]byte(`{"a":"1"}`)),
		"legal_hold": seekparquet.BooleanValue(false), "last_access_time": seekparquet.Int64Value(1_767_225_600_000_000),
		"archive_status": absent, "server_encrypted": absent,
	})
	o, err = DecodeAzblobObject(r)
	if err != nil || !*o.AccessTierInferred || *o.LeaseState != "leased" || *o.TagCount != 3 || *o.Metadata != `{"a":"1"}` ||
		*o.LegalHold || o.LastAccessTime.Unix() != 1_767_225_600 || o.ArchiveStatus != nil || o.ServerEncrypted != nil || o.Tags != nil {
		t.Fatalf("decoded %+v, %v", o, err)
	}
}

// TestDecodeAzblobVersion: an entry's identity and the current columns.
func TestDecodeAzblobVersion(t *testing.T) {
	v, err := DecodeAzblobVersion(row(t, AzblobVersionsV1, "a", 2, map[string]seekparquet.Value{
		"size": seekparquet.Int64Value(5), "snapshot": seekparquet.ByteArrayValue([]byte("2026-01-01T00:00:00.0000000Z")),
		"is_latest": seekparquet.BooleanValue(false), "is_deleted": seekparquet.BooleanValue(true),
		"deleted_time": seekparquet.Int64Value(1_767_225_600_000_000), "remaining_retention_days": seekparquet.Int64Value(0),
	}))
	if err != nil || *v.Size != 5 || v.VersionID != nil || *v.Snapshot != "2026-01-01T00:00:00.0000000Z" || v.IsLatest || !v.IsDeleted ||
		v.DeletedTime.Unix() != 1_767_225_600 || *v.RemainingRetentionDays != 0 || v.Offset != 2 {
		t.Fatalf("decoded %+v, %v", v, err)
	}
	if _, err := DecodeAzblobVersion(row(t, AzblobVersionsV1, "a", 0, nil)); !errors.Is(err, ErrRow) {
		t.Fatalf("null is_latest: %v", err)
	}
}
