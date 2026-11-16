// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"bytes"
	"fmt"
	"os"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
)

// KeyPages reads the key column's page index of a SeekParquet part
// (1,024-row pages) in key order, never a data page, through the module
// (seekparquet.InspectKeyPages, which checks its footer and stamps first):
// its bounds are exact.
func KeyPages(path string) ([]seekparquet.KeyPage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return seekparquet.InspectKeyPages(f, fi.Size(), dataset.KeyColumn)
}

// PageEnds are the page ends of a part this package wrote, of rows rows,
// as its writer collected them, from its key page index (KeyPages): the end
// of each page of pageRows rows the writer filled (the module may end a key
// page sooner, at its page buffer, inside one), unless the page after it
// starts with its last key, and the part's last row. A page the writer cut
// at pageKeyBytes is not told apart: only a local listing's keys reach it,
// and a local listing does not resume, the one reader of retained parts.
func PageEnds(pages []seekparquet.KeyPage, rows int64) ([]dataset.PageEnd, error) {
	var ends []dataset.PageEnd
	var at int64
	for i, pg := range pages {
		if pg.FirstRow != at || pg.Rows <= 0 {
			return nil, fmt.Errorf("key page %d starts at row %d, after %d rows", i, pg.FirstRow, at)
		}
		at += pg.Rows
		if at%pageRows != 0 && at != rows {
			continue
		}
		if i+1 < len(pages) && bytes.Equal(pages[i+1].Min, pg.Max) {
			continue
		}
		ends = append(ends, dataset.PageEnd{Key: string(pg.Max), Rows: at})
	}
	if at != rows || len(ends) == 0 {
		return nil, fmt.Errorf("the key pages hold %d rows, the part %d", at, rows)
	}
	return ends, nil
}
