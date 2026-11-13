// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
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
