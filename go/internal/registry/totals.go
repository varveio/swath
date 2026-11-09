// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"fmt"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/totals"
)

// totalsSink sums the counted rows per prefix (swath count --depth) into a
// tally, reading the payload through the schema's columns by name, as the
// filters and the versions counts do, so it serves every schema: the bytes
// column (size, or another the tally names), last_modified, and what is not
// an object (schema.Schema.Container, an S3 is_delete_marker). It is
// order.CountWithSink's sink: called concurrently, one batch per call, and
// it keeps nothing of the rows. next, if set, observes the rows after it.
type totalsSink[P any] struct {
	t      *totals.Tally
	bytes  func(*P) (int64, bool)
	mtime  func(*P) (int64, bool)
	marker func(*P) (bool, bool)
	box    func(*P) bool
	next   order.Sink[P]
}

// bindTotals binds t to schema s.
func bindTotals[P any](t *totals.Tally, s *schema.Schema[P], next order.Sink[P]) (order.Sink[P], error) {
	k := totalsSink[P]{t: t, box: s.Container, next: next}
	col := t.Options().BytesColumn
	for _, c := range s.Columns {
		switch c.Name {
		case col:
			k.bytes = c.Int
		case "last_modified":
			k.mtime = c.Int
		case "is_delete_marker":
			k.marker = c.Bool
		}
	}
	if k.bytes == nil {
		return nil, fmt.Errorf("per-prefix totals need the %s column; schema %s has none", col, s.ID)
	}
	return k, nil
}

func (k totalsSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	b := k.t.Batch()
	for _, seg := range segs {
		for i := range seg {
			p := &seg[i].Data
			r := totals.Row{Key: seg[i].Key, Object: true}
			if k.box != nil && k.box(p) {
				r.Object = false
			} else if k.marker != nil {
				if m, _ := k.marker(p); m {
					r.Object = false
				}
			}
			r.Bytes, r.HasBytes = k.bytes(p)
			if k.mtime != nil {
				r.Mtime, r.HasMtime = k.mtime(p)
			}
			b.Add(&r)
		}
	}
	if err := b.Flush(); err != nil {
		return err
	}
	if k.next != nil {
		return k.next.Write(ctx, segs)
	}
	return nil
}
