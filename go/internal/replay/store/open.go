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
	"golang.org/x/sync/semaphore"

	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/format2"
	"github.com/varveio/swath/go/sortedparquet/javaera"
)

// expect states the types of the columns the Object projection reads,
// wherever a schema has them.
var expect = []sortedparquet.Column{
	{Name: "size", Kind: parquet.Int64},
	{Name: "last_modified", Kind: parquet.Int64, Check: timestampMicros},
	{Name: "etag", Kind: parquet.ByteArray},
	{Name: "storage_class", Kind: parquet.ByteArray},
	{Name: "owner_id", Kind: parquet.ByteArray},
	{Name: "owner_display_name", Kind: parquet.ByteArray},
	{Name: "checksum_algorithm", Kind: parquet.ByteArray},
	{Name: "checksum_type", Kind: parquet.ByteArray},
}

// timestampMicros requires last_modified to be a microsecond timestamp, the
// unit Object.LastModified is decoded in.
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
	s := &Sorted{sem: semaphore.NewWeighted(int64(readers)), files: sortedparquet.NewFileCache(opts.MaxOpenFiles)}
	meta := Metadata{Ordering: OrderingProfile}
	if info.IsDir() && format2.IsDataset(path) {
		dataset, err := format2.Open(ctx, path, expect, s.files)
		if err != nil {
			return nil, Metadata{}, fmt.Errorf("open %s: %w", path, err)
		}
		s.reader = dataset.Reader
		meta.Kind, meta.SchemaID = KindFormat2, dataset.Manifest.SchemaID
		if meta.Identity, err = format2Identity(path, dataset); err != nil {
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
	return s, meta, nil
}

// Refusal returns the reason Open refused a fixture as not servable sorted
// (no_stamp, unsupported_mode, unknown_format_version, incomplete_multifile,
// mixed_row_types, sanity_failed, and for manifest datasets
// unsupported_schema, invalid_manifest, no_page_index), row_group_disorder
// for a read that found a row group's keys out of order, or "" when err is
// none of these.
func Refusal(err error) string {
	return sortedparquet.ReasonOf(err)
}
