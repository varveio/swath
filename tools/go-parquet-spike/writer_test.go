// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func probeRows(count int) []Row {
	rows := make([]Row, count)
	for i := range rows {
		row := Row{Key: fmt.Sprintf("%s%04d", strings.Repeat("界", 340), i), RowType: "OBJECT"}
		if i%3 != 0 {
			size, modified, latest, text := int64(i)<<33, int64(-123456+i), false, ""
			row.Size, row.LastModified, row.IsLatest = &size, &modified, &latest
			row.ETag, row.StorageClass, row.VersionID = &text, &text, &text
			row.OwnerID, row.OwnerDisplayName = &text, &text
			row.ChecksumAlgorithm, row.ChecksumType = &text, &text
		}
		rows[i] = row
	}
	return rows
}

func TestCanonicalSchemaNullsAndGeometry(t *testing.T) {
	rows := probeRows(113)
	for _, layout := range []string{"served", "direct"} {
		for _, encoding := range []string{"dict", "delta"} {
			t.Run(layout+"/"+encoding, func(t *testing.T) {
				config := writerConfig{Layout: layout, KeyEncoding: encoding, IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
				path := filepath.Join(t.TempDir(), "part.parquet")
				part, err := writePart(path, rows, config, diskOps())
				if err != nil {
					t.Fatal(err)
				}
				if part.Rows != 113 || *part.MinKey != rows[0].Key || *part.MaxKey != rows[112].Key || len(part.MD5) != 32 {
					t.Fatalf("unexpected successful metadata: %+v", part)
				}
				report, err := verifyPart(path, rows)
				if err != nil {
					t.Fatal(err)
				}
				if !report.VerifiedRows || report.MaxPageRows != 16 || len(report.RowGroups) != 3 {
					t.Fatalf("unexpected geometry: %+v", report)
				}
				if err := checkGeometry(report, config); err != nil {
					t.Fatal(err)
				}
				for group, expectedRows := range []int64{40, 40, 33} {
					if report.RowGroups[group].Rows != expectedRows || len(report.RowGroups[group].Columns) != 13 {
						t.Fatalf("group %d: %+v", group, report.RowGroups[group])
					}
					key := report.RowGroups[group].Columns[0]
					if *key.ExactKeyBounds != (layout == "served") {
						t.Fatalf("%s exact bounds = %v", layout, *key.ExactKeyBounds)
					}
					for _, bound := range key.KeyBounds {
						if !bound.CoversRows {
							t.Fatalf("unsafe bounds: %+v", bound)
						}
					}
					if key.Codec != "ZSTD" {
						t.Fatalf("codec = %s", key.Codec)
					}
					wantEncoding := "RLE_DICTIONARY"
					if encoding == "delta" {
						wantEncoding = "DELTA_BYTE_ARRAY"
					}
					found := false
					for _, physical := range key.Encodings {
						found = found || physical == wantEncoding
					}
					if !found {
						t.Fatalf("encoding %s missing from %v", wantEncoding, key.Encodings)
					}
				}
			})
		}
	}
}

func TestInspectChecksActualCountsAndCaps(t *testing.T) {
	rows := probeRows(113)
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	path := filepath.Join(t.TempDir(), "part.parquet")
	if _, err := writePart(path, rows, config, diskOps()); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectPart(path, nil); err == nil {
		t.Fatal("nonempty file passed against an empty corpus")
	}
	report, err := inspectPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	tooSmall := config
	tooSmall.PageRows = 15
	if err := checkGeometry(report, tooSmall); err == nil {
		t.Fatal("actual 16-row pages passed a 15-row cap")
	}
	wrongGroups := config
	wrongGroups.GroupRows = 41
	if err := checkGeometry(report, wrongGroups); err == nil {
		t.Fatal("actual 40-row full groups passed a 41-row target")
	}
	var output bytes.Buffer
	if _, err := encodeRows(&output, rows, config); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	file.OffsetIndexes()[0].PageLocations[0].FirstRowIndex = 1
	if _, err := inspectFile(file, rows); err == nil {
		t.Fatal("corrupt first-page row offset passed inspection")
	}
}

func TestSortedStampPreconditions(t *testing.T) {
	empty, earlier, later := "", "\ue000", "\U00010000"
	valid := []Row{
		{Key: "k", RowType: "OBJECT"},
		{Key: "k", RowType: "COMMON_PREFIX"},
		{Key: "k", RowType: "DELETE_MARKER"},
		{Key: "k", VersionID: &empty, RowType: "OBJECT"},
		{Key: "k", VersionID: &earlier, RowType: "OBJECT"},
		{Key: "k", VersionID: &later, RowType: "OBJECT"},
	}
	if err := validateRows(valid); err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]Row{
		{valid[3], valid[0]},
		{valid[5], valid[4]},
		{valid[2], valid[1]},
	} {
		if err := validateRows(rows); err == nil {
			t.Fatalf("unordered versions or row types accepted: %+v", rows)
		}
	}
}

func TestIntegerEncodingAndPageStatistics(t *testing.T) {
	rows := probeRows(113)
	for _, layout := range []string{"served", "direct"} {
		for _, integerEncoding := range []string{"dict", "delta"} {
			for _, pageStatistics := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/stats=%t", layout, integerEncoding, pageStatistics), func(t *testing.T) {
					config := writerConfig{
						Layout: layout, KeyEncoding: "delta", IntegerEncoding: integerEncoding,
						PageStatistics: pageStatistics, PageRows: 16, GroupRows: 40,
					}
					path := filepath.Join(t.TempDir(), "part.parquet")
					if _, err := writePart(path, rows, config, diskOps()); err != nil {
						t.Fatal(err)
					}
					report, err := verifyPart(path, rows)
					if err != nil {
						t.Fatal(err)
					}
					if err := checkGeometry(report, config); err != nil {
						t.Fatal(err)
					}
					tooSmall := config
					tooSmall.PageRows = 15
					if err := checkGeometry(report, tooSmall); err == nil {
						t.Fatal("actual pages passed a smaller row cap")
					}
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
					if err != nil {
						t.Fatal(err)
					}
					for _, column := range []int{0, 1, 2} {
						metadata := file.Metadata().RowGroups[0].Columns[column].MetaData
						if len(metadata.Statistics.MinValue) == 0 || len(metadata.Statistics.MaxValue) == 0 {
							t.Fatalf("column %d footer bounds disappeared with stats=%t", column, pageStatistics)
						}
						reader := bytes.NewReader(data[metadata.DataPageOffset:])
						var header format.PageHeader
						if err := thrift.NewDecoder((&thrift.CompactProtocol{}).NewReader(reader)).Decode(&header); err != nil {
							t.Fatal(err)
						}
						if !header.DataPageHeaderV2.Valid {
							t.Fatal("expected data page v2")
						}
						page := header.DataPageHeaderV2.V
						stats := page.Statistics
						boundBytes := len(stats.Min) + len(stats.Max) + len(stats.MinValue) + len(stats.MaxValue)
						if (boundBytes > 0) != pageStatistics {
							t.Fatalf("column %d header bound bytes=%d, stats=%t", column, boundBytes, pageStatistics)
						}
						if column != 0 {
							want := format.RLEDictionary
							if integerEncoding == "delta" {
								want = format.DeltaBinaryPacked
							}
							if page.Encoding != want {
								t.Fatalf("column %d integer encoding=%s, expected %s", column, page.Encoding, want)
							}
						}
					}
				})
			}
		}
	}
}

func TestRawCanonicalSchema(t *testing.T) {
	var output bytes.Buffer
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	if _, err := encodeRows(&output, probeRows(1), config); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	schema := file.Metadata().Schema
	if len(schema) != 14 || schema[0].Name != "swath_listing" {
		t.Fatalf("schema = %+v", schema)
	}
	names := []string{"key", "size", "last_modified", "etag", "storage_class", "version_id", "is_latest", "is_delete_marker", "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"}
	for index, name := range names {
		field := schema[index+1]
		if field.Name != name {
			t.Fatalf("column %d is %s, expected %s", index, field.Name, name)
		}
		repetition := format.Optional
		if index == 0 || index == 7 || index == 12 {
			repetition = format.Required
		}
		if !field.RepetitionType.Valid || field.RepetitionType.V != repetition {
			t.Fatalf("column %s repetition = %+v", name, field.RepetitionType)
		}
		physical := format.ByteArray
		switch index {
		case 1, 2:
			physical = format.Int64
		case 6, 7:
			physical = format.Boolean
		}
		if !field.Type.Valid || field.Type.V != physical {
			t.Fatalf("column %s physical type = %+v", name, field.Type)
		}
		if physical == format.ByteArray {
			if _, ok := field.LogicalType.Value.(*format.StringType); !ok {
				t.Fatalf("column %s lacks STRING annotation", name)
			}
		}
	}
	stamp, ok := schema[3].LogicalType.Value.(*format.TimestampType)
	if !ok || !stamp.IsAdjustedToUTC {
		t.Fatalf("timestamp annotation = %+v", schema[3].LogicalType)
	}
	if _, ok := stamp.Unit.Value.(*format.MicroSeconds); !ok {
		t.Fatalf("timestamp unit = %+v", stamp.Unit)
	}
	if schema[2].LogicalType.Value != nil || schema[2].ConvertedType.Valid {
		t.Fatalf("size must be unannotated INT64, got logical=%+v converted=%+v", schema[2].LogicalType, schema[2].ConvertedType)
	}
}

func TestSinglePageAndEmptyOutput(t *testing.T) {
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	for _, rows := range [][]Row{nil, probeRows(1)} {
		path := filepath.Join(t.TempDir(), "part.parquet")
		part, err := writePart(path, rows, config, diskOps())
		if err != nil {
			t.Fatal(err)
		}
		report, err := verifyPart(path, rows)
		if err != nil {
			t.Fatal(err)
		}
		if report.Rows != int64(len(rows)) {
			t.Fatalf("rows = %d", report.Rows)
		}
		if len(rows) == 0 {
			if part.MinKey != nil || part.MaxKey != nil || len(report.RowGroups) != 0 {
				t.Fatalf("empty file metadata = %+v", part)
			}
		} else if report.RowGroups[0].Columns[0].BoundsOrder != "UNORDERED" {
			t.Fatal("stock single-page boundary order changed; recheck Java compatibility")
		}
	}
}

func TestInvalidKeysFailBeforeOutput(t *testing.T) {
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	for _, rows := range [][]Row{
		{{Key: strings.Repeat("x", 1025), RowType: "OBJECT"}},
		{{Key: "\xff", RowType: "OBJECT"}},
		{{Key: "b", RowType: "OBJECT"}, {Key: "a", RowType: "OBJECT"}},
	} {
		var output bytes.Buffer
		result, err := encodeRows(&output, rows, config)
		if err == nil || !reflect.DeepEqual(result, encodeResult{}) || output.Len() != 0 {
			t.Fatalf("invalid rows accepted: %+v, %v, %d bytes", result, err, output.Len())
		}
	}
}
