// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// Positions match the canonical Row declaration. This flat schema has no
// repetition levels; nullable columns use definition level one when present.
const (
	keyColumn = iota
	sizeColumn
	modifiedColumn
	etagColumn
	storageClassColumn
	versionColumn
	latestColumn
	deleteMarkerColumn
	ownerColumn
	ownerNameColumn
	checksumAlgorithmColumn
	checksumTypeColumn
	rowTypeColumn
	canonicalColumnCount
)

// rowValues owns one reusable slab and row views into it. ValueOf borrows
// immutable string bytes; input rows stay alive through the synchronous write.
// parquet-go copies values into its column buffers before WriteRows returns.
type rowValues struct {
	values []parquet.Value
	rows   []parquet.Row
}

func newRowValues(capacity int) *rowValues {
	batch := &rowValues{
		values: make([]parquet.Value, canonicalColumnCount*capacity),
		rows:   make([]parquet.Row, capacity),
	}
	for i := range batch.rows {
		start := i * canonicalColumnCount
		batch.rows[i] = batch.values[start : start+canonicalColumnCount : start+canonicalColumnCount]
	}
	return batch
}

func (batch *rowValues) fill(input []Row) []parquet.Row {
	for i := range input {
		fillCanonicalValues(batch.rows[i], &input[i], 1)
	}
	return batch.rows[:len(input)]
}

// columnValues uses the same mapping over a column-major slab, eliminating
// Row.Range and the library's row-to-column regrouping. Input stays immutable.
type columnValues struct {
	values   []parquet.Value
	columns  [][]parquet.Value
	capacity int
}

func newColumnValues(capacity int) *columnValues {
	batch := &columnValues{
		values:  make([]parquet.Value, canonicalColumnCount*capacity),
		columns: make([][]parquet.Value, canonicalColumnCount), capacity: capacity,
	}
	for column := range batch.columns {
		start := column * capacity
		batch.columns[column] = batch.values[start : start+capacity : start+capacity]
	}
	return batch
}

func (batch *columnValues) fill(input []Row) {
	for i := range input {
		fillCanonicalValues(batch.values[i:], &input[i], batch.capacity)
	}
}

func (batch *columnValues) write(columns []*parquet.ColumnWriter, rows int) (int, error) {
	if len(columns) != canonicalColumnCount {
		return 0, fmt.Errorf("canonical column count differs: %d", len(columns))
	}
	// v0.32 WriteRows groups at most 64 rows before checking page byte size.
	// Preserve that quantum so wide columns keep identical physical pages.
	const rowsPerWrite = 64
	written := 0
	for written < rows {
		end := written + min(rowsPerWrite, rows-written)
		for column, writer := range columns {
			n, err := writer.WriteRowValues(batch.columns[column][written:end])
			if err != nil {
				return written, err
			}
			if n != end-written {
				return written, io.ErrShortWrite
			}
		}
		written = end
	}
	return written, nil
}

// A stride of one fills a row view; the batch capacity fills a column view.
// Keep field/null/level policy in this single mapping for both public APIs.
func fillCanonicalValues(values []parquet.Value, row *Row, stride int) {
	values[keyColumn*stride] = parquet.ValueOf(row.Key).Level(0, 0, keyColumn)
	values[sizeColumn*stride] = optionalInt64(row.Size, sizeColumn)
	values[modifiedColumn*stride] = optionalInt64(row.LastModified, modifiedColumn)
	values[etagColumn*stride] = optionalString(row.ETag, etagColumn)
	values[storageClassColumn*stride] = optionalString(row.StorageClass, storageClassColumn)
	values[versionColumn*stride] = optionalString(row.VersionID, versionColumn)
	values[latestColumn*stride] = optionalBoolean(row.IsLatest, latestColumn)
	values[deleteMarkerColumn*stride] = parquet.BooleanValue(row.IsDeleteMarker).Level(0, 0, deleteMarkerColumn)
	values[ownerColumn*stride] = optionalString(row.OwnerID, ownerColumn)
	values[ownerNameColumn*stride] = optionalString(row.OwnerDisplayName, ownerNameColumn)
	values[checksumAlgorithmColumn*stride] = optionalString(row.ChecksumAlgorithm, checksumAlgorithmColumn)
	values[checksumTypeColumn*stride] = optionalString(row.ChecksumType, checksumTypeColumn)
	values[rowTypeColumn*stride] = parquet.ValueOf(row.RowType).Level(0, 0, rowTypeColumn)
}

func optionalInt64(value *int64, column int) parquet.Value {
	if value == nil {
		return parquet.NullValue().Level(0, 0, column)
	}
	return parquet.Int64Value(*value).Level(0, 1, column)
}

func optionalString(value *string, column int) parquet.Value {
	if value == nil {
		return parquet.NullValue().Level(0, 0, column)
	}
	return parquet.ValueOf(*value).Level(0, 1, column)
}

func optionalBoolean(value *bool, column int) parquet.Value {
	if value == nil {
		return parquet.NullValue().Level(0, 0, column)
	}
	return parquet.BooleanValue(*value).Level(0, 1, column)
}
