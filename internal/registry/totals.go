// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"fmt"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/totals"
)

// totalsSink sums the counted rows per prefix (swath du) into a
// tally, reading the payload through the schema's columns by name, as the
// filters do, so it serves every schema: size and allocated_bytes (where
// the schema has it), last_modified, and what is not an object
// (schema.Schema.Container, or a delete marker by the schema's version
// roles), and to count hard links once nlink, dev and inode (the local
// schema's). It is order.CountWithSink's sink: called concurrently, one
// batch per call, and it keeps nothing of the rows. next, if set, observes
// the rows after it.
type totalsSink[P any] struct {
	t      *totals.Tally
	size   func(*P) (int64, bool)
	alloc  func(*P) (int64, bool) // nil: the schema has no allocated bytes
	mtime  func(*P) (int64, bool)
	marker func(*P) bool
	box    func(*P) bool
	// strict: the allocated bytes are the primary figure (--allocated),
	// asked for by name, so an object without them fails the count rather
	// than count none.
	strict            bool
	nlink, dev, inode func(*P) (int64, bool)
	next              order.Sink[P]
}

// bindTotals binds t to schema s and its version roles (nil for none).
func bindTotals[P any](t *totals.Tally, s *schema.Schema[P], vers *schema.Versions[P], next order.Sink[P]) (order.Sink[P], error) {
	opt := t.Options()
	k := totalsSink[P]{t: t, box: s.Container, next: next, strict: opt.Primary == totals.AllocatedBytes}
	if vers != nil {
		k.marker = vers.DeleteMarker
	}
	for _, c := range s.Columns {
		switch c.Name {
		case totals.Size:
			k.size = c.Int
		case totals.AllocatedBytes:
			k.alloc = c.Int
		case "last_modified":
			k.mtime = c.Int
		case "nlink":
			k.nlink = c.Int
		case "dev":
			k.dev = c.Int
		case "inode":
			k.inode = c.Int
		}
	}
	switch {
	case k.size == nil:
		return nil, fmt.Errorf("per-prefix totals need the %s column; schema %s has none", totals.Size, s.ID)
	case k.strict && k.alloc == nil:
		return nil, fmt.Errorf("per-prefix totals by %s need that column; schema %s has none", totals.AllocatedBytes, s.ID)
	}
	if opt.LinksOnce && (k.nlink == nil || k.dev == nil || k.inode == nil) {
		return nil, fmt.Errorf("counting hard links once needs the nlink, dev and inode columns; schema %s lacks them", s.ID)
	}
	if !opt.LinksOnce {
		k.nlink = nil
	}
	if s.Container != nil {
		t.SetDirs()
	}
	if k.alloc != nil {
		t.SetAllocated()
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
				r.Object, r.Dir = false, true
			} else if k.marker != nil && k.marker(p) {
				r.Object = false
			}
			r.Size, r.HasSize = k.size(p)
			if k.alloc != nil {
				r.Allocated, r.HasAllocated = k.alloc(p)
			}
			if r.Object && k.strict && !r.HasAllocated {
				b.Discard()
				return fmt.Errorf("per-prefix totals: %q has no %s (a metadata tier or platform that does not read it)", seg[i].Key, totals.AllocatedBytes)
			}
			if k.mtime != nil {
				r.Mtime, r.HasMtime = k.mtime(p)
			}
			if r.Object && k.nlink != nil {
				n, ok1 := k.nlink(p)
				dev, ok2 := k.dev(p)
				ino, ok3 := k.inode(p)
				// A row without a link count, device or inode (a dataset
				// listed on Windows) counts under its name: nothing tells
				// its links apart. nlink is unsigned, its bits carried in
				// the int64.
				if ok1 && ok2 && ok3 {
					r.Link, r.Dev, r.Inode = uint32(n) > 1, dev, ino
				} else {
					r.Unidentified = true
				}
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
