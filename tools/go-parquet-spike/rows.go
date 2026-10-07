// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// Row preserves the canonical column order and distinguishes null from zero
// and empty string. Timestamps are already parsed UTC epoch microseconds.
type Row struct {
	Key               string  `json:"key" parquet:"key"`
	Size              *int64  `json:"size" parquet:"size,optional"`
	LastModified      *int64  `json:"last_modified" parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	ETag              *string `json:"etag" parquet:"etag,optional"`
	StorageClass      *string `json:"storage_class" parquet:"storage_class,optional"`
	VersionID         *string `json:"version_id" parquet:"version_id,optional"`
	IsLatest          *bool   `json:"is_latest" parquet:"is_latest,optional"`
	IsDeleteMarker    bool    `json:"is_delete_marker" parquet:"is_delete_marker"`
	OwnerID           *string `json:"owner_id" parquet:"owner_id,optional"`
	OwnerDisplayName  *string `json:"owner_display_name" parquet:"owner_display_name,optional"`
	ChecksumAlgorithm *string `json:"checksum_algorithm" parquet:"checksum_algorithm,optional"`
	ChecksumType      *string `json:"checksum_type" parquet:"checksum_type,optional"`
	RowType           string  `json:"row_type" parquet:"row_type"`
}

func canonicalSchema() *parquet.Schema {
	return parquet.NewSchema("swath_listing", canonicalRoot{parquet.SchemaOf(new(Row))})
}

// SchemaOf annotates Go int64 as INTEGER(64,true). The Java contract's size
// column is an unannotated physical INT64. This public Node/Field seam changes
// only that type, preserving declared order and the struct's value accessors.
type canonicalRoot struct{ parquet.Node }

func (root canonicalRoot) Fields() []parquet.Field {
	fields := append([]parquet.Field(nil), root.Node.Fields()...)
	for i, field := range fields {
		if field.Name() == "size" {
			fields[i] = physicalSizeField{field}
		}
	}
	return fields
}

type physicalSizeField struct{ parquet.Field }

func (field physicalSizeField) Type() parquet.Type {
	return unannotatedInt64{parquet.Int64Type}
}

type unannotatedInt64 struct{ parquet.Type }

func (typ unannotatedInt64) LogicalType() *format.LogicalType { return nil }

func loadRows(path string) ([]Row, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var rows []Row
	lines := bufio.NewScanner(file)
	lines.Buffer(make([]byte, 64<<10), 8<<20)
	for line := 1; lines.Scan(); line++ {
		data := lines.Bytes()
		if !utf8.Valid(data) {
			return nil, fmt.Errorf("line %d: invalid UTF-8 JSON", line)
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		var row Row
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("line %d: expected exactly one JSON value", line)
		}
		rows = append(rows, row)
	}
	if err := lines.Err(); err != nil {
		return nil, err
	}
	if err := validateRows(rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func validateRows(rows []Row) error {
	for i, row := range rows {
		if !utf8.ValidString(row.Key) || len(row.Key) > 1024 {
			return fmt.Errorf("row %d: key must be UTF-8 and at most 1024 bytes", i)
		}
		switch row.RowType {
		case "OBJECT", "COMMON_PREFIX", "DELETE_MARKER":
		default:
			return fmt.Errorf("row %d: invalid row_type %q", i, row.RowType)
		}
		if i > 0 && compareRows(rows[i-1], row) > 0 {
			return fmt.Errorf("row %d: rows violate %s", i, sortOrder)
		}
	}
	return nil
}

// Match ListEntryComparator's total preorder. Equal key/version/type tuples
// are retained in input order, as they are by the Java stable sort.
func compareRows(a, b Row) int {
	if order := strings.Compare(a.Key, b.Key); order != 0 {
		return order
	}
	versionA, versionB := a.VersionID, b.VersionID
	if a.RowType == "COMMON_PREFIX" {
		versionA = nil
	}
	if b.RowType == "COMMON_PREFIX" {
		versionB = nil
	}
	if versionA == nil && versionB != nil {
		return -1
	}
	if versionA != nil && versionB == nil {
		return 1
	}
	if versionA != nil {
		if order := strings.Compare(*versionA, *versionB); order != 0 {
			return order
		}
	}
	rank := func(rowType string) int {
		switch rowType {
		case "OBJECT":
			return 0
		case "COMMON_PREFIX":
			return 1
		default:
			return 2
		}
	}
	return rank(a.RowType) - rank(b.RowType)
}

// logicalBytes counts present primitive payload, excluding null-level and
// length framing. It is input accounting, not Java's row-group byte measure.
func logicalBytes(rows []Row) int64 {
	var size int64
	for _, row := range rows {
		size += int64(len(row.Key) + len(row.RowType) + 1)
		for _, value := range []*int64{row.Size, row.LastModified} {
			if value != nil {
				size += 8
			}
		}
		if row.IsLatest != nil {
			size++
		}
		for _, value := range []*string{row.ETag, row.StorageClass, row.VersionID, row.OwnerID, row.OwnerDisplayName, row.ChecksumAlgorithm, row.ChecksumType} {
			if value != nil {
				size += int64(len(*value))
			}
		}
	}
	return size
}
