// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/schema"
)

// FillPage is fillPage, for the external tests and benchmarks.
func FillPage[P any](s *schema.Schema[P], page []schema.Row[P], column int, values []pq.Value) {
	fillPage(s, page, column, values)
}

// discard is an artifact that keeps nothing.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
func (discard) Sync() error                 { return nil }
func (discard) Close() error                { return nil }
func (discard) Name() string                { return "discard" }
func (discard) Identity() string            { return "" }

// NewDiscardWriter is a writer of s (one column goroutine) into an
// artifact that keeps nothing, for the benchmarks: write appends rows,
// close seals the part. Without sized it counts no object bytes (the
// writer before the count).
func NewDiscardWriter[P any](s *schema.Schema[P], sized bool) (write func([]schema.Row[P]) error, close func() (*int64, error), err error) {
	c := newCodec(1)
	w, err := create[P](discard{}, s, c, func() int { return 1 }, "")
	if err != nil {
		return nil, nil, err
	}
	if !sized {
		w.size = nil
	}
	return w.write, func() (*int64, error) {
		defer c.Close()
		p, err := w.close()
		return p.ObjectBytes, err
	}, nil
}
