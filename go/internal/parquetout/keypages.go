// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"fmt"
	"os"

	pq "github.com/parquet-go/parquet-go"
)

// KeyPage is one page of a part's key column: its greatest key and its rows.
type KeyPage struct {
	Max  string
	Rows int64
}

// KeyPages reads the key column's page index of a part (1,024-row pages):
// each page's greatest key from the column index and its row count from the
// offset index, in key order. Only the footer and the key column's two
// index sections are read, never a data page. A greatest key longer than
// the index bound (StampIndexBound, 1,024 bytes) is rounded up, above every
// key of its page; S3 and GCS keys fit within it.
func KeyPages(path string) ([]KeyPage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := pq.OpenFile(f, fi.Size(), pq.SkipPageIndex(true), pq.SkipBloomFilters(true))
	if err != nil {
		return nil, err
	}
	leaf, ok := pf.Schema().Lookup("key")
	if !ok {
		return nil, fmt.Errorf("%s: no key column", path)
	}
	var pages []KeyPage
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
			pages = append(pages, KeyPage{Max: string(ci.MaxValue(i).ByteArray()), Rows: rows})
		}
	}
	return pages, nil
}
