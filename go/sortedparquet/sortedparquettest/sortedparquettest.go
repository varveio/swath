// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortedparquettest writes small Java-era sorted parts for tests:
// the 13-column canonical schema, the footer sortedness stamps, and page
// and row-group geometry chosen by the test. The files are synthetic: the
// writer is parquet-go, not the Java tool.
package sortedparquettest

import (
	"fmt"
	"os"
	"strconv"

	"github.com/parquet-go/parquet-go"
)

// Row is one canonical row. Nil pointers are nulls.
type Row struct {
	Key               string  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	ETag              *string `parquet:"etag,optional"`
	StorageClass      *string `parquet:"storage_class,optional"`
	VersionID         *string `parquet:"version_id,optional"`
	IsLatest          *bool   `parquet:"is_latest,optional"`
	IsDeleteMarker    bool    `parquet:"is_delete_marker"`
	OwnerID           *string `parquet:"owner_id,optional"`
	OwnerDisplayName  *string `parquet:"owner_display_name,optional"`
	ChecksumAlgorithm *string `parquet:"checksum_algorithm,optional"`
	ChecksumType      *string `parquet:"checksum_type,optional"`
	RowType           string  `parquet:"row_type"`
}

// Object returns a current-object row for key with every fact derived from
// i: size i, time i seconds after 2026-01-01, an etag, a storage class,
// owner columns and, for odd i, checksum columns.
func Object(key string, i int) Row {
	size := int64(i)
	micros := int64(1_767_225_600_000_000) + int64(i)*1_000_000
	str := func(s string) *string { return &s }
	row := Row{
		Key: key, Size: &size, LastModified: &micros,
		ETag: str(fmt.Sprintf("%032x", i)), StorageClass: str("STANDARD"),
		OwnerID: str("owner-" + strconv.Itoa(i%7)), OwnerDisplayName: str("name-" + strconv.Itoa(i%7)),
		RowType: "OBJECT",
	}
	if i%2 == 1 {
		row.ChecksumAlgorithm, row.ChecksumType = str("CRC32"), str("FULL_OBJECT")
	}
	return row
}

// Stamps returns the footer stamps of part index (1-based) of a sorted
// objects output, final on its last part.
func Stamps(index int, final bool) map[string]string {
	stamps := map[string]string{
		"swath.sort.order":          "key_bytes_unsigned,version_id_null_first,row_type_rank",
		"swath.sort.mode":           "objects",
		"swath.sort.format_version": "1",
		"swath.sort.file_index":     strconv.Itoa(index),
	}
	if final {
		stamps["swath.sort.file_final"] = "true"
	}
	return stamps
}

// Part describes one part file.
type Part struct {
	Rows []Row
	// Stamps are the footer key/value pairs; see Stamps.
	Stamps map[string]string
	// PageBytes is the page buffer size (pages are cut by bytes); 0 is 1 KiB.
	PageBytes int
	// GroupRows rows per row group; 0 is one group.
	GroupRows int
	// BoundBytes is the ColumnIndex truncation; 0 is 64, the Java writer's
	// library default.
	BoundBytes int
}

// Write writes part to path. Rows are written in the order given.
func Write(path string, part Part) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	options := []parquet.WriterOption{
		parquet.NewSchema("swath_listing", parquet.SchemaOf(Row{})),
		parquet.PageBufferSize(max(part.PageBytes, 0)),
		parquet.ColumnIndexSizeLimit(func([]string) int { return cmpOr(part.BoundBytes, 64) }),
		parquet.DataPageStatistics(true),
	}
	if part.PageBytes == 0 {
		options[1] = parquet.PageBufferSize(1024)
	}
	for k, v := range part.Stamps {
		options = append(options, parquet.KeyValueMetadata(k, v))
	}
	w := parquet.NewGenericWriter[Row](f, options...)
	group := part.GroupRows
	if group <= 0 {
		group = len(part.Rows) + 1
	}
	for i := 0; i < len(part.Rows); i += group {
		if _, err := w.Write(part.Rows[i:min(i+group, len(part.Rows))]); err != nil {
			_ = f.Close()
			return err
		}
		if err := w.Flush(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func cmpOr(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}
