// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

type fileReport struct {
	Rows                 int64             `json:"rows"`
	Schema               string            `json:"schema"`
	Stamps               map[string]string `json:"stamps"`
	RowGroups            []groupReport     `json:"row_groups"`
	MaxPageRows          int64             `json:"max_page_rows"`
	VerifiedRows         bool              `json:"verified_rows"`
	RowCountMatchesInput bool              `json:"row_count_matches_input"`
}

type groupReport struct {
	Rows              int64          `json:"rows"`
	CompressedBytes   int64          `json:"compressed_bytes"`
	UncompressedBytes int64          `json:"uncompressed_bytes"`
	Columns           []columnReport `json:"columns"`
}

type columnReport struct {
	Name              string           `json:"name"`
	Codec             string           `json:"codec"`
	Encodings         []string         `json:"encodings"`
	CompressedBytes   int64            `json:"compressed_bytes"`
	UncompressedBytes int64            `json:"uncompressed_bytes"`
	DictionaryExtent  int64            `json:"dictionary_extent_bytes"`
	PageRows          []int64          `json:"page_rows"`
	BoundsOrder       string           `json:"bounds_order"`
	MaxBoundBytes     int              `json:"max_bound_bytes"`
	ExactKeyBounds    *bool            `json:"exact_key_bounds,omitempty"`
	KeyBounds         []keyBoundReport `json:"key_bounds,omitempty"`
}

type keyBoundReport struct {
	Page       int  `json:"page"`
	MinBytes   int  `json:"min_bytes"`
	MaxBytes   int  `json:"max_bytes"`
	Exact      bool `json:"exact"`
	CoversRows bool `json:"covers_rows"`
}

// inspectPart reports metadata outside writer timings. Dictionary extent is
// compressed dictionary bytes including its header, not retained heap size.
func inspectPart(path string, expected []Row) (fileReport, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileReport{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return fileReport{}, err
	}
	parsed, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return fileReport{}, err
	}
	return inspectFile(parsed, expected)
}

func inspectFile(file *parquet.File, expected []Row) (fileReport, error) {
	if err := checkCanonicalSchema(file.Metadata().Schema); err != nil {
		return fileReport{}, err
	}
	if file.NumRows() != int64(len(expected)) {
		return fileReport{}, fmt.Errorf("footer has %d rows, expected %d", file.NumRows(), len(expected))
	}
	report := fileReport{
		Rows: file.NumRows(), Schema: canonicalSchema().String(), Stamps: make(map[string]string),
		RowCountMatchesInput: true, VerifiedRows: len(expected) == 0,
	}
	for _, key := range []string{"swath.sort.order", "swath.sort.mode", "swath.sort.format_version", "swath.sort.file_index", "swath.sort.file_final"} {
		if value, found := file.Lookup(key); found {
			report.Stamps[key] = value
		}
	}
	var rowBase int64
	for groupIndex, group := range file.Metadata().RowGroups {
		if group.NumRows < 0 || group.NumRows > int64(len(expected))-rowBase {
			return fileReport{}, errors.New("row-group count exceeds input row interval")
		}
		groupResult := groupReport{Rows: group.NumRows, CompressedBytes: group.TotalCompressedSize, UncompressedBytes: group.TotalByteSize}
		for columnIndex, column := range group.Columns {
			metadata := column.MetaData
			if len(metadata.PathInSchema) != 1 {
				return fileReport{}, errors.New("expected flat canonical columns")
			}
			result := columnReport{
				Name: metadata.PathInSchema[0], Codec: metadata.Codec.String(),
				CompressedBytes: metadata.TotalCompressedSize, UncompressedBytes: metadata.TotalUncompressedSize,
			}
			for _, encoding := range metadata.Encoding {
				result.Encodings = append(result.Encodings, encoding.String())
			}
			if metadata.DictionaryPageOffset > 0 {
				result.DictionaryExtent = metadata.DataPageOffset - metadata.DictionaryPageOffset
			}
			chunk := file.RowGroups()[groupIndex].ColumnChunks()[columnIndex]
			offsets, err := chunk.OffsetIndex()
			if err != nil {
				return fileReport{}, fmt.Errorf("group %d column %s offsets: %w", groupIndex, result.Name, err)
			}
			bounds, err := chunk.ColumnIndex()
			if err != nil {
				return fileReport{}, fmt.Errorf("group %d column %s bounds: %w", groupIndex, result.Name, err)
			}
			if offsets.NumPages() != bounds.NumPages() {
				return fileReport{}, errors.New("page index counts disagree")
			}
			if offsets.NumPages() > 0 && offsets.FirstRowIndex(0) != 0 {
				return fileReport{}, errors.New("first page does not start at row zero")
			}
			result.BoundsOrder = "UNORDERED"
			if bounds.IsAscending() {
				result.BoundsOrder = "ASCENDING"
			} else if bounds.IsDescending() {
				result.BoundsOrder = "DESCENDING"
			}
			if columnIndex == 0 {
				exact := true
				result.ExactKeyBounds = &exact
			}
			for page := 0; page < offsets.NumPages(); page++ {
				first := offsets.FirstRowIndex(page)
				end := group.NumRows
				if page+1 < offsets.NumPages() {
					end = offsets.FirstRowIndex(page + 1)
				}
				if first < 0 || end <= first || end > group.NumRows {
					return fileReport{}, errors.New("invalid page row interval")
				}
				result.PageRows = append(result.PageRows, end-first)
				report.MaxPageRows = max(report.MaxPageRows, end-first)
				if columnIndex == 0 {
					lo, hi := bounds.MinValue(page).ByteArray(), bounds.MaxValue(page).ByteArray()
					result.MaxBoundBytes = max(result.MaxBoundBytes, len(lo), len(hi))
					if string(lo) != expected[rowBase+first].Key || string(hi) != expected[rowBase+end-1].Key {
						*result.ExactKeyBounds = false
					}
					firstKey, lastKey := expected[rowBase+first].Key, expected[rowBase+end-1].Key
					covers := string(lo) <= firstKey && string(hi) >= lastKey
					result.KeyBounds = append(result.KeyBounds, keyBoundReport{Page: page, MinBytes: len(lo), MaxBytes: len(hi), Exact: string(lo) == firstKey && string(hi) == lastKey, CoversRows: covers})
					if !covers {
						return fileReport{}, fmt.Errorf("group %d key page %d bounds exclude source keys", groupIndex, page)
					}
				}
			}
			groupResult.Columns = append(groupResult.Columns, result)
		}
		report.RowGroups = append(report.RowGroups, groupResult)
		rowBase += group.NumRows
	}
	if rowBase != int64(len(expected)) {
		return fileReport{}, errors.New("row-group totals disagree with input")
	}
	return report, nil
}

func checkGeometry(report fileReport, config writerConfig) error {
	for group, geometry := range report.RowGroups {
		if geometry.Rows <= 0 || geometry.Rows > int64(config.GroupRows) || group+1 < len(report.RowGroups) && geometry.Rows != int64(config.GroupRows) {
			return fmt.Errorf("group %d violates configured row limit", group)
		}
		for _, column := range geometry.Columns {
			if len(column.PageRows) == 0 {
				return fmt.Errorf("group %d column %s has no pages", group, column.Name)
			}
			for _, rows := range column.PageRows {
				if rows <= 0 || rows > int64(config.PageRows) {
					return fmt.Errorf("group %d column %s violates page row cap", group, column.Name)
				}
			}
		}
	}
	return nil
}

// Check raw footer elements: parquet-go's reconstructed Node for an unannotated
// INT64 reports an INTEGER logical type, so Schema.String is not a raw-schema
// oracle. These are semantic schema checks, not serialized footer identity.
func checkCanonicalSchema(elements []format.SchemaElement) error {
	schema := canonicalSchema()
	fields := schema.Fields()
	if len(elements) != len(fields)+1 || elements[0].Name != schema.Name() || !elements[0].NumChildren.Valid || int(elements[0].NumChildren.V) != len(fields) {
		return errors.New("canonical schema root or column count differs")
	}
	for i, field := range fields {
		actual := elements[i+1]
		typ := field.Type()
		repetition := format.Required
		if field.Optional() {
			repetition = format.Optional
		}
		if actual.Name != field.Name() || !actual.Type.Valid || actual.Type.V != *typ.PhysicalType() || !actual.RepetitionType.Valid || actual.RepetitionType.V != repetition || actual.NumChildren.Valid {
			return fmt.Errorf("canonical column %s physical type, order or repetition differs", field.Name())
		}
		if actual.TypeLength.Valid && (typ.Length() == 0 || actual.TypeLength.V != int32(typ.Length())) {
			return fmt.Errorf("canonical column %s has a nonnatural type length", field.Name())
		}
		var logical any
		if expected := typ.LogicalType(); expected != nil {
			logical = expected.Value
		}
		if !reflect.DeepEqual(actual.LogicalType.Value, logical) {
			return fmt.Errorf("canonical column %s logical annotation differs", field.Name())
		}
		converted := typ.ConvertedType()
		if actual.ConvertedType.Valid != (converted != nil) || converted != nil && actual.ConvertedType.V != *converted {
			return fmt.Errorf("canonical column %s converted annotation differs", field.Name())
		}
	}
	return nil
}

// verifyPart compares the full canonical rows, including null/empty/zero
// distinctions, then reports indexes. This is separate from timed encoding.
func verifyPart(path string, expected []Row) (fileReport, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileReport{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return fileReport{}, err
	}
	parsed, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return fileReport{}, err
	}
	report, err := inspectFile(parsed, expected)
	if err != nil {
		return fileReport{}, err
	}
	reader := parquet.NewGenericReader[Row](parsed)
	defer reader.Close()
	buffer := make([]Row, 128)
	position := 0
	for {
		n, readErr := reader.Read(buffer)
		for _, row := range buffer[:n] {
			if position >= len(expected) || !reflect.DeepEqual(row, expected[position]) {
				return fileReport{}, fmt.Errorf("row %d differs from JSONL", position)
			}
			position++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fileReport{}, readErr
		}
		if n == 0 {
			return fileReport{}, io.ErrNoProgress
		}
	}
	if position != len(expected) {
		return fileReport{}, fmt.Errorf("read %d rows, expected %d", position, len(expected))
	}
	report.VerifiedRows = true
	return report, nil
}
