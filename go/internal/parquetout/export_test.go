// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/schema"
)

// FillPage is fillPage, for the external tests and benchmarks.
func FillPage[P any](s *schema.Schema[P], page []schema.Row[P], column int, values []pq.Value) {
	fillPage(s, page, column, values)
}
