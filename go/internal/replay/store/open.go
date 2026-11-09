// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	seekparquet "github.com/varveio/seekparquet/go"
	"golang.org/x/sync/semaphore"

	"github.com/varveio/swath/go/internal/replay/legacy"
	"github.com/varveio/swath/go/internal/replay/legacy/javaera"
)

// expect states the type of every column a projection reads, by name,
// wherever a schema has it: the Object projection's (the S3-shaped
// schemas' nine columns) and the GCS route's native columns of
// swath/gcs-objects/v1 (generation, metageneration, md5, crc32c,
// content_type, time_created; md5 also gives the S3 route its ETag, and
// time_created its LastModified). A part with such a column of another physical type, a
// conflicting logical annotation or a repeated one is refused at open
// (unsupported_schema, naming the column), so no read decodes a value as a
// type it is not. No schema the store serves uses one of these names for
// another meaning.
var expect = []legacy.Column{
	{Name: "size", Kind: parquet.Int64, Check: signedInt64},
	{Name: "last_modified", Kind: parquet.Int64, Check: timestampMicros},
	{Name: "etag", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "storage_class", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "owner_id", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "owner_display_name", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "checksum_algorithm", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "checksum_type", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "generation", Kind: parquet.Int64, Check: signedInt64},
	{Name: "metageneration", Kind: parquet.Int64, Check: signedInt64},
	{Name: "md5", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "crc32c", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "content_type", Kind: parquet.ByteArray, Check: stringColumn},
	{Name: "time_created", Kind: parquet.Int64, Check: timestampMicros},
}

// stringColumn accepts a byte array column the projections render as a string:
// annotated as a string (or an enum), or not annotated.
func stringColumn(node parquet.Node) error {
	logical := node.Type().LogicalType()
	if logical == nil || logical.Value == nil {
		return nil
	}
	switch logical.Value.(type) {
	case *format.StringType, *format.EnumType:
		return nil
	}
	return fmt.Errorf("annotated %s, not a string", logical)
}

// signedInt64 accepts an INT64 column the projections read as a signed
// count: not annotated, or annotated as a signed 64-bit integer.
func signedInt64(node parquet.Node) error {
	logical := node.Type().LogicalType()
	if logical == nil || logical.Value == nil {
		return nil
	}
	if i, ok := logical.Value.(*format.IntType); ok && i.BitWidth == 64 && i.IsSigned {
		return nil
	}
	return fmt.Errorf("annotated %s, not a signed 64-bit integer", logical)
}

// timestampMicros requires a time column (last_modified, time_created) to
// be a microsecond timestamp, the unit Object.LastModified is decoded in.
func timestampMicros(node parquet.Node) error {
	logical := node.Type().LogicalType()
	if logical == nil {
		return errors.New("not a timestamp")
	}
	timestamp, ok := logical.Value.(*format.TimestampType)
	if !ok {
		return errors.New("not a timestamp")
	}
	if _, ok := timestamp.Unit.Value.(*format.MicroSeconds); !ok {
		return errors.New("not in microseconds")
	}
	return nil
}

func open(ctx context.Context, path string, opts Options) (Store, Metadata, error) {
	readers := opts.Readers
	if readers <= 0 {
		readers = DefaultReaders()
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, Metadata{}, err
	}
	s := &Sorted{sem: semaphore.NewWeighted(int64(readers)), readers: readers, files: legacy.NewFileCache(opts.MaxOpenFiles)}
	meta := Metadata{Ordering: OrderingProfile}
	if info.IsDir() && isSeekParquet(path) {
		dataset, err := openSeek(ctx, path, expect, s.files)
		if err != nil {
			return nil, Metadata{}, fmt.Errorf("open %s: %w", path, err)
		}
		s.reader = dataset
		meta.Kind, meta.SchemaID = KindSeekParquet, dataset.meta.SchemaID
		if meta.Identity, err = seekIdentity(path, dataset.meta); err != nil {
			_ = s.reader.Close()
			return nil, Metadata{}, err
		}
	} else {
		fixture, err := javaera.Open(ctx, path, expect, s.files)
		if err != nil {
			return nil, Metadata{}, fmt.Errorf("open %s: %w", path, err)
		}
		s.reader = fixture.Reader
		meta.Kind, meta.SchemaID = KindJavaEra, javaera.SchemaID
		if meta.Identity, err = javaEraIdentity(path, fixture.Files, fixture.Stamps); err != nil {
			_ = s.reader.Close()
			return nil, Metadata{}, err
		}
	}
	meta.Parts, meta.RowGroups, meta.Rows = s.reader.Parts(), s.reader.RowGroups(), s.reader.Rows()
	s.schemaID = meta.SchemaID
	s.objects = objectProjectionFor(meta.SchemaID)
	if !opts.Prefetch.Off {
		s.prefetch = newPrefetcher(opts.Prefetch, s.packedRows, s.objectRows)
	}
	return s, meta, nil
}

// Refusal returns the reason Open refused a fixture as not servable sorted
// (no_stamp, unsupported_mode, unknown_format_version, incomplete_multifile,
// mixed_row_types, sanity_failed, unsupported_schema, no_page_index) for a
// Java-era fixture, row_group_disorder
// for a read that found a row group's keys out of order, a SeekParquet
// refusal code (spec/refusal-codes.json: invalid_manifest, stamp_mismatch,
// key_order, ...) for a SeekParquet dataset, or "" when err is none of
// these.
func Refusal(err error) string {
	if code := seekparquet.CodeOf(err); code != "" {
		return string(code)
	}
	return legacy.ReasonOf(err)
}
