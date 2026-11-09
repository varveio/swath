// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"errors"
	"testing"

	"github.com/parquet-go/parquet-go"
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
			v = seekparquet.Value{Value: parquet.NullValue()}
		}
		r.Values = append(r.Values, v)
	}
	return r
}

func val(v parquet.Value) seekparquet.Value { return seekparquet.Value{Value: v} }

var absent = seekparquet.Value{Absent: true, Value: parquet.NullValue()}

func TestDecodeKeepsNullsAndPosition(t *testing.T) {
	r := row(t, S3VersionsV1, "k", 3, map[string]seekparquet.Value{
		"last_modified": val(parquet.Int64Value(-1)), "version_id": val(parquet.ByteArrayValue([]byte("null"))),
		"is_latest": val(parquet.BooleanValue(false)), "is_delete_marker": val(parquet.BooleanValue(true)),
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
		"kind": val(parquet.ByteArrayValue([]byte("SYMLINK"))), "mode": val(parquet.Int32Value(-1)), "gid": val(parquet.Int32Value(0)),
		"inode": val(parquet.Int64Value(-1)), "dev": val(parquet.Int64Value(1 << 62)), "link_target": val(parquet.ByteArrayValue(target)),
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
	ok := map[string]seekparquet.Value{"kind": val(parquet.ByteArrayValue([]byte("FILE")))}
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
		{"another type", row(t, LocalV1, "k", 0, with("size", val(parquet.Int32Value(1)))), ErrSchema, "size"},
		{"another type, a time", row(t, LocalV1, "k", 0, with("atime", val(parquet.ByteArrayValue([]byte("x"))))), ErrSchema, "atime"},
		{"not projected", seekparquet.Row{Key: []byte("k"), Columns: []string{"kind"}, Values: []seekparquet.Value{ok["kind"]}}, ErrSchema, "size"},
	} {
		_, err := DecodeLocalEntry(tc.row)
		var se *SemanticError
		if !errors.Is(err, tc.target) || !errors.As(err, &se) || se.Column != tc.column || se.Row == nil || string(se.Row.Key) != "k" || se.Err == nil {
			t.Errorf("%s: %v", tc.name, err)
		}
		for _, other := range []error{ErrSchema, ErrCoverage, ErrRow} {
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

func TestDecodeObject(t *testing.T) {
	values := map[string]seekparquet.Value{"size": val(parquet.Int64Value(7)), "storage_class": val(parquet.ByteArrayValue([]byte("STANDARD")))}
	for _, id := range []SchemaID{S3ObjectsV1, GCSObjectsV1} {
		o, err := DecodeObject(id, row(t, id, "k", 0, values))
		if err != nil || *o.Size != 7 || *o.StorageClass != "STANDARD" || o.ETag != nil || o.LastModified != nil || string(o.Key) != "k" {
			t.Fatalf("%s: %+v, %v", id, o, err)
		}
	}
	for _, id := range []SchemaID{S3VersionsV1, GCSGenerationsV1, LocalV1, "swath/canonical"} {
		if _, err := DecodeObject(id, row(t, S3ObjectsV1, "k", 0, values)); !errors.Is(err, ErrSchema) {
			t.Errorf("%s: %v, want a schema error", id, err)
		}
	}
}
