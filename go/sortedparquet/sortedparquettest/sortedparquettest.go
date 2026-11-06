// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortedparquettest writes small sorted fixtures for tests: Java-era
// parts (the 13-column canonical schema and the footer sortedness stamps)
// and format-2 datasets (a manifest and parts carrying the profile stamps),
// with page and row-group geometry chosen by the test. The files are
// synthetic: the writer is parquet-go, not either tool.
package sortedparquettest

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	// Sorted records the key column ascending as every row group's sorting
	// column, as the format-2 writer does.
	Sorted bool
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
	if part.Sorted {
		options = append(options, parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("key"))))
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

// Format-2 names and stamps, spelled out here so that a test of the
// profile does not take them from the code under test.
const (
	Format2SchemaID = "swath/s3-objects/v1"
	format2Format   = "swath-dataset"
)

// Format2Stamps returns the footer stamps of a format-2 part.
func Format2Stamps(schemaID string, boundBytes int) map[string]string {
	return map[string]string{
		"swath.profile":           format2Format,
		"swath.profile_version":   "2",
		"swath.schema_id":         schemaID,
		"swath.index_bound_bytes": strconv.Itoa(boundBytes),
	}
}

// Dataset describes a format-2 dataset.
type Dataset struct {
	// SchemaID is the manifest's and the footers' schema id; "" is
	// Format2SchemaID.
	SchemaID string
	// Parts are written in order as data/part-NNNNN.parquet. Each part's
	// Stamps are added to the format-2 stamps (a value of "" removes
	// one); BoundBytes defaults to 1,024 bytes.
	Parts []Part
	// Unsorted leaves out the sorting-column metadata the format-2 writer
	// records on every row group.
	Unsorted bool
}

// ManifestPart is one part entry of a written manifest.
type ManifestPart struct {
	Path   string `json:"path"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	MinKey string `json:"min_key_base64,omitempty"`
	MaxKey string `json:"max_key_base64,omitempty"`
	MD5    string `json:"md5"`
}

// Manifest is the manifest WriteFormat2 writes: the fields a reader of the
// format checks, and a complete coverage.
type Manifest struct {
	Format        string         `json:"format"`
	FormatVersion int            `json:"format_version"`
	SchemaID      string         `json:"schema_id"`
	Coverage      map[string]any `json:"coverage"`
	Parts         []ManifestPart `json:"parts"`
}

// WriteFormat2 writes d into dir (created if needed) and returns its
// manifest. Every non-empty part records its exact first and last keys.
func WriteFormat2(dir string, d Dataset) (Manifest, error) {
	schemaID := d.SchemaID
	if schemaID == "" {
		schemaID = Format2SchemaID
	}
	m := Manifest{Format: format2Format, FormatVersion: 2, SchemaID: schemaID,
		Coverage: map[string]any{"complete": true, "stop_reason": "completed"}, Parts: []ManifestPart{}}
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		return m, err
	}
	for i, part := range d.Parts {
		bound := cmpOr(part.BoundBytes, 1024)
		stamps := Format2Stamps(schemaID, bound)
		for k, v := range part.Stamps {
			if v == "" {
				delete(stamps, k)
			} else {
				stamps[k] = v
			}
		}
		part.Stamps, part.BoundBytes, part.Sorted = stamps, bound, !d.Unsorted
		rel := fmt.Sprintf("data/part-%05d.parquet", i)
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := Write(path, part); err != nil {
			return m, err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return m, err
		}
		sum := md5.Sum(raw)
		entry := ManifestPart{Path: rel, Rows: int64(len(part.Rows)), Bytes: int64(len(raw)), MD5: hex.EncodeToString(sum[:])}
		if n := len(part.Rows); n > 0 {
			entry.MinKey = base64.StdEncoding.EncodeToString([]byte(part.Rows[0].Key))
			entry.MaxKey = base64.StdEncoding.EncodeToString([]byte(part.Rows[n-1].Key))
		}
		m.Parts = append(m.Parts, entry)
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	return m, os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644)
}
