// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"fmt"
	"os"

	pq "github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
)

// KeyPages reads the key column's page index of a part (1,024-row pages)
// in key order, never a data page. A SeekParquet part is read by the
// module (seekparquet.InspectKeyPages, which checks its footer and stamps
// first), its bounds exact; a part of a legacy format 2 dataset (an
// earlier build's, read by --seed-from) by its footer alone, its greatest
// key rounded up when longer than 1,024 bytes, above every key of its page
// (S3 and GCS keys fit within it).
func KeyPages(path string, legacy bool) ([]seekparquet.KeyPage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !legacy {
		return seekparquet.InspectKeyPages(f, fi.Size(), dataset.KeyColumn)
	}
	pf, err := pq.OpenFile(f, fi.Size(), pq.SkipPageIndex(true), pq.SkipBloomFilters(true))
	if err != nil {
		return nil, err
	}
	leaf, ok := pf.Schema().Lookup(dataset.KeyColumn)
	if !ok {
		return nil, fmt.Errorf("%s: no key column", path)
	}
	var pages []seekparquet.KeyPage
	var start int64
	for g, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[leaf.ColumnIndex]
		ci, err := cc.ColumnIndex()
		if err != nil {
			return nil, fmt.Errorf("%s: row group %d: column index: %w", path, g, err)
		}
		oi, err := cc.OffsetIndex()
		if err != nil {
			return nil, fmt.Errorf("%s: row group %d: offset index: %w", path, g, err)
		}
		n := ci.NumPages()
		if n == 0 || oi.NumPages() != n {
			return nil, fmt.Errorf("%s: row group %d: %d column index pages, %d offset index pages", path, g, n, oi.NumPages())
		}
		for i := range n {
			end := rg.NumRows()
			if i+1 < n {
				end = oi.FirstRowIndex(i + 1)
			}
			rows := end - oi.FirstRowIndex(i)
			if rows <= 0 {
				return nil, fmt.Errorf("%s: row group %d: page %d holds %d rows", path, g, i, rows)
			}
			pages = append(pages, seekparquet.KeyPage{RowGroup: g, FirstRow: start + oi.FirstRowIndex(i), Rows: rows,
				Min: ci.MinValue(i).ByteArray(), Max: ci.MaxValue(i).ByteArray()})
		}
		start += rg.NumRows()
	}
	return pages, nil
}
