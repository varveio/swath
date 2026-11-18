// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package bind

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
// batch per call, and it keeps nothing of the rows.
type totalsSink[P any] struct {
	t      *totals.Tally
	size   func(*P) (int64, bool)
	alloc  func(*P) (int64, bool) // nil: the schema has no allocated bytes
	mtime  func(*P) (int64, bool)
	marker func(*P) bool
	box    func(*P) bool
	// strict: the allocated bytes are the primary figure (--allocated, or
	// a local tree's default where its listing reads them), so an object or
	// a directory without them fails the count (totals.NoAllocatedError)
	// rather than count none.
	strict            bool
	nlink, dev, inode func(*P) (int64, bool)
}

// Totals binds t to schema s and its version roles (nil for none): a sink
// that sums the rows into t, then hands them to next (nil: none), which
// keeps its runs, their intervals and its subtrees' ends (observed). A
// tally with a view (totals.Options.View) takes the rows in runs
// (order.RangeSink): ordered, each run's rows ascend; otherwise they come
// in any order (order.Options.Unordered).
func Totals[P any](t *totals.Tally, s *schema.Schema[P], vers *schema.Versions[P], next order.Sink[P], ordered bool) (order.Sink[P], error) {
	opt := t.Options()
	k := totalsSink[P]{t: t, box: s.Container, strict: opt.Primary == totals.AllocatedBytes}
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
	if opt.View {
		return viewSink[P]{k, ordered, next}, nil
	}
	return observed(k.Write, next), nil
}

// viewSink is a totalsSink whose tally builds the view: it takes rows in
// runs, each a range's (order.RangeSink), and sums each row into the
// tally and its run, then hands them to next, in a run of next's own when
// next takes runs.
type viewSink[P any] struct {
	totalsSink[P]
	ordered bool
	next    order.Sink[P] // nil: none
}

func (k viewSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	if err := k.totalsSink.Write(ctx, segs); err != nil {
		return err
	}
	return handOn(ctx, k.next, segs)
}

func (k viewSink[P]) Run() order.RangeRun[P] {
	r := viewRun[P]{k: k.totalsSink, v: k.t.ViewRun(k.ordered), next: k.next}
	if runs, ok := k.next.(order.RangeSink[P]); ok {
		r.run = runs.Run()
		r.covered, _ = r.run.(order.CoveredRun)
	}
	return r
}

// SubtreeEnd completes a local directory once its listing ended
// (order.SubtreeSink), and tells next when it is told of subtrees.
func (k viewSink[P]) SubtreeEnd(prefix string) {
	k.t.ViewSubtreeEnd(prefix)
	if trees, ok := k.next.(order.SubtreeSink[P]); ok {
		trees.SubtreeEnd(prefix)
	}
}

type viewRun[P any] struct {
	k totalsSink[P]
	v *totals.ViewRun
	// next is the sink after, run its run when it takes runs and covered
	// that run when it is told its interval.
	next    order.Sink[P]
	run     order.RangeRun[P]
	covered order.CoveredRun
}

func (r viewRun[P]) Write(ctx context.Context, rows []schema.Row[P]) error {
	if err := r.k.write([][]schema.Row[P]{rows}, r.v); err != nil {
		return err
	}
	if r.run != nil {
		return r.run.Write(ctx, rows)
	}
	return handOn(ctx, r.next, [][]schema.Row[P]{rows})
}

func (r viewRun[P]) End() {
	r.v.End()
	if r.run != nil {
		r.run.End()
	}
}

// From and Through take the interval the run covers (order.CoveredRun).
func (r viewRun[P]) From(lo string) {
	r.v.From(lo)
	if r.covered != nil {
		r.covered.From(lo)
	}
}

func (r viewRun[P]) Through(hi string, top bool) {
	r.v.Through(hi, top)
	if r.covered != nil {
		r.covered.Through(hi, top)
	}
}

func (k totalsSink[P]) Write(_ context.Context, segs [][]schema.Row[P]) error {
	return k.write(segs, nil)
}

// write sums segs into the tally, and into run v unless nil.
func (k totalsSink[P]) write(segs [][]schema.Row[P], v *totals.ViewRun) error {
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
			if (r.Object || r.Dir) && k.strict && !r.HasAllocated { // a directory's own blocks count too
				b.Discard()
				return &totals.NoAllocatedError{Key: seg[i].Key}
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
			if v != nil {
				v.Add(&r)
			}
		}
	}
	if v != nil {
		v.Flush() // the rows are the provider's once Write returns
	}
	return b.Flush()
}
