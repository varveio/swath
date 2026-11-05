// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import (
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// columnValues is a reusable column-major value slab (the Oct spike's public
// column path, which avoids parquet-go's reflective row conversion).
type columnValues struct {
	values   []parquet.Value
	columns  [][]parquet.Value
	capacity int
}

func newColumnValues(capacity int) *columnValues {
	b := &columnValues{values: make([]parquet.Value, columnCount*capacity), columns: make([][]parquet.Value, columnCount), capacity: capacity}
	for c := range b.columns {
		start := c * capacity
		b.columns[c] = b.values[start : start+capacity : start+capacity]
	}
	return b
}

// fillColumn writes column col of rows into dst (one value per row). It is
// the single row->column mapping; columns are independent, so different
// goroutines may fill different columns of the same page.
func fillColumn(col int, dst []parquet.Value, rows []record.Row) {
	switch col {
	case colKey:
		for i := range rows {
			dst[i] = parquet.ByteArrayValue(unsafeBytes(rows[i].Key)).Level(0, 0, colKey)
		}
	case colKind:
		for i := range rows {
			dst[i] = parquet.ByteArrayValue(unsafeBytes(rows[i].Kind.String())).Level(0, 0, colKind)
		}
	case colLink:
		for i := range rows {
			if rows[i].Has(record.HasLink) {
				dst[i] = parquet.ByteArrayValue(unsafeBytes(rows[i].LinkTarget)).Level(0, 1, colLink)
			} else {
				dst[i] = parquet.NullValue().Level(0, 0, colLink)
			}
		}
	case colMode, colUID, colGID, colNlink:
		bit, get := int32Column(col)
		for i := range rows {
			if rows[i].Has(bit) {
				dst[i] = parquet.Int32Value(int32(get(&rows[i]))).Level(0, 1, col)
			} else {
				dst[i] = parquet.NullValue().Level(0, 0, col)
			}
		}
	default:
		bit, get := int64Column(col)
		for i := range rows {
			if rows[i].Has(bit) {
				dst[i] = parquet.Int64Value(get(&rows[i])).Level(0, 1, col)
			} else {
				dst[i] = parquet.NullValue().Level(0, 0, col)
			}
		}
	}
}

func int32Column(col int) (record.Presence, func(*record.Row) uint32) {
	switch col {
	case colMode:
		return record.HasMode, func(r *record.Row) uint32 { return r.Mode }
	case colUID:
		return record.HasUID, func(r *record.Row) uint32 { return r.UID }
	case colGID:
		return record.HasGID, func(r *record.Row) uint32 { return r.GID }
	}
	return record.HasNlink, func(r *record.Row) uint32 { return r.Nlink }
}

func int64Column(col int) (record.Presence, func(*record.Row) int64) {
	switch col {
	case colSize:
		return record.HasSize, func(r *record.Row) int64 { return r.Size }
	case colMtime:
		return record.HasMtime, func(r *record.Row) int64 { return r.Mtime }
	case colInode:
		return record.HasInode, func(r *record.Row) int64 { return r.Inode }
	case colDev:
		return record.HasDev, func(r *record.Row) int64 { return r.Dev }
	case colBlocks:
		return record.HasBlocks, func(r *record.Row) int64 { return r.Blocks }
	case colCtime:
		return record.HasCtime, func(r *record.Row) int64 { return r.Ctime }
	case colAtime:
		return record.HasAtime, func(r *record.Row) int64 { return r.Atime }
	}
	return record.HasBtime, func(r *record.Row) int64 { return r.Btime }
}

// writeColumn writes rows values of one column in parquet-go v0.32's 64-row
// WriteRows quantum (so pages match what the row API would produce) and
// flushes the page.
func writeColumn(w *parquet.ColumnWriter, values []parquet.Value) error {
	const quantum = 64
	for written := 0; written < len(values); {
		end := written + min(quantum, len(values)-written)
		n, err := w.WriteRowValues(values[written:end])
		if err != nil {
			return err
		}
		if n != end-written {
			return io.ErrShortWrite
		}
		written = end
	}
	return w.Flush()
}
