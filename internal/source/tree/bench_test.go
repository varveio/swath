// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"fmt"
	"testing"

	"github.com/varveio/swath/internal/schema"
)

type discard[D, S, P any] struct{}

func (discard[D, S, P]) Rows([]schema.Row[P]) error { return nil }
func (discard[D, S, P]) Child(Range[D, S]) error    { return nil }

// BenchmarkOutRows is the walker's per-row cost of splicing a directory's
// rows: the walk call and the counts, over 2,048 rows with one directory
// per 64 (the localfs chunk), with a payload of localfs's size.
func BenchmarkOutRows(b *testing.B) {
	type entry struct {
		Size int64
		_    [112]byte
	}
	rows := make([]schema.Row[entry], 2048)
	for i := range rows {
		rows[i].Key = fmt.Sprintf("p/%05d", i)
		if i%64 == 0 {
			rows[i].Key += "/"
		}
	}
	walk := func(r *schema.Row[entry]) (string, Walk) {
		if r.Key[len(r.Key)-1] == '/' {
			return r.Key, Descend
		}
		return "", Leaf
	}
	out := &Out[string, int, entry]{e: discard[string, int, entry]{}, size: func(e *entry) int64 { return e.Size }}
	b.ResetTimer()
	for range b.N {
		if err := out.Rows(rows, walk); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(rows)), "ns/row")
}
