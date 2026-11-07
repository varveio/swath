// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/sortedparquet"
)

// NativeSchemaID is the schema id of a dataset the lister wrote from a
// Cloud Storage bucket: its rows carry generation, metageneration, the
// checksums and the content type, which the route renders instead of the
// synthetic values.
const NativeSchemaID = "swath/gcs-objects/v1"

// Item is one listed object as the route renders it. Size and Updated are
// 0 when the fixture stores a null. Native is nil for a fixture without
// GCS metadata; within it a nil field is a null column value.
type Item struct {
	Key     []byte
	Size    int64
	Updated int64 // epoch microseconds
	Native  *Native
}

// Native is the GCS metadata of a NativeSchemaID row.
type Native struct {
	ETag, StorageClass, ContentType, MD5, CRC32C *string
	Generation, Metageneration                   *int64
}

// Entry is one entry of a rollup: a common prefix (Item nil) or an item.
type Entry struct {
	Prefix []byte
	Item   *Item
}

// source reads items by range: store.Store's two reads in the route's own
// row type.
type source interface {
	rows(ctx context.Context, lower boundary, upper []byte, limit int) ([]Item, error)
	rollup(ctx context.Context, lower boundary, upper, prefix, delimiter []byte, limit int) ([]Entry, bool, error)
}

// objectSource reads any store through its Object projection.
type objectSource struct{ st store.Store }

func itemOf(o *store.Object) Item {
	it := Item{Key: o.Key}
	if o.Size != nil {
		it.Size = *o.Size
	}
	if o.LastModified != nil {
		it.Updated = o.LastModified.UnixMicro()
	}
	return it
}

func (s objectSource) rows(ctx context.Context, lower boundary, upper []byte, limit int) ([]Item, error) {
	objs, err := s.st.Rows(ctx, lower.key, lower.inclusive, upper, limit, store.Projection{})
	if err != nil {
		return nil, err
	}
	out := make([]Item, len(objs))
	for i := range objs {
		out[i] = itemOf(&objs[i])
	}
	return out, nil
}

func (s objectSource) rollup(ctx context.Context, lower boundary, upper, prefix, delimiter []byte, limit int) ([]Entry, bool, error) {
	entries, ok, err := s.st.DelimitedRollup(ctx, lower.key, lower.inclusive, upper, prefix, delimiter, limit, store.Projection{}, false)
	if !ok || err != nil {
		return nil, ok, err
	}
	out := make([]Entry, len(entries))
	for i, e := range entries {
		if e.Object == nil {
			out[i] = Entry{Prefix: e.CommonPrefix}
			continue
		}
		it := itemOf(e.Object)
		out[i] = Entry{Item: &it}
	}
	return out, true, nil
}

// nativeSource reads a NativeSchemaID fixture's GCS columns by name
// through the sorted store's reader.
type nativeSource struct{ st *store.Sorted }

// nativeColumns are the NativeSchemaID columns the route renders.
var nativeColumns = []string{"size", "last_modified", "etag", "generation", "metageneration", "storage_class", "md5", "crc32c", "content_type"}

func nativeItem(row *sortedparquet.Row) Item {
	it := Item{Key: row.Key, Native: &Native{}}
	if v, ok := present(row, "size"); ok {
		it.Size = v.Int64()
	}
	if v, ok := present(row, "last_modified"); ok {
		it.Updated = v.Int64()
	}
	n := it.Native
	n.ETag, n.StorageClass, n.ContentType = text(row, "etag"), text(row, "storage_class"), text(row, "content_type")
	n.MD5, n.CRC32C = text(row, "md5"), text(row, "crc32c")
	n.Generation, n.Metageneration = integer(row, "generation"), integer(row, "metageneration")
	return it
}

func present(row *sortedparquet.Row, name string) (parquet.Value, bool) {
	v, ok := row.Value(name)
	return v, ok && !v.IsNull()
}

func text(row *sortedparquet.Row, name string) *string {
	v, ok := present(row, name)
	if !ok {
		return nil
	}
	s := string(v.ByteArray())
	return &s
}

func integer(row *sortedparquet.Row, name string) *int64 {
	v, ok := present(row, name)
	if !ok {
		return nil
	}
	n := v.Int64()
	return &n
}

func (s nativeSource) rows(ctx context.Context, lower boundary, upper []byte, limit int) ([]Item, error) {
	rows, err := s.st.ReadRows(ctx, lower.key, lower.inclusive, upper, limit, nativeColumns)
	if err != nil {
		return nil, err
	}
	out := make([]Item, len(rows))
	for i := range rows {
		out[i] = nativeItem(&rows[i])
	}
	return out, nil
}

func (s nativeSource) rollup(ctx context.Context, lower boundary, upper, prefix, delimiter []byte, limit int) ([]Entry, bool, error) {
	entries, ok, err := s.st.RollupRows(ctx, lower.key, lower.inclusive, upper, prefix, delimiter, limit, nativeColumns, false)
	if !ok || err != nil {
		return nil, ok, err
	}
	out := make([]Entry, len(entries))
	for i, e := range entries {
		if e.Row == nil {
			out[i] = Entry{Prefix: e.CommonPrefix}
			continue
		}
		it := nativeItem(e.Row)
		out[i] = Entry{Item: &it}
	}
	return out, true, nil
}
