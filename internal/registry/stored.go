// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// DatasetRead is what DatasetTotals read.
type DatasetRead struct {
	Rows     int64    // the rows under the prefix
	Filtered int64    // of them, the rows the filters dropped
	Columns  []string // the columns read besides the key
	// Plan and Scan are the reads of planning and of the scan
	// (SeekParquet's counters: parts opened, pages decoded, bytes read).
	Plan, Scan seekparquet.ReadStats
}

// DatasetTotals sums the rows of the committed dataset d whose keys start
// with prefix into t, through the filters f (nil: none), as a listing's
// rows are summed; relative sums them as a listing of the directory prefix
// names would have keyed them, prefix trimmed and its own row left out, or,
// when prefix without its '/' is a row (a file), that row alone, keyed by
// its name, as a listing of the file: the same binding to the schema (bindTotals,
// filter.Compile, the version roles and the containers the producer
// declares, SchemaInfo) over a payload that reads the dataset's columns.
// It seeks to prefix and reads the key and only the columns the totals and
// the filters read, with workers concurrent scans (Plan.ScanParallel; a
// tally with a view takes each shard's rows as one run, scanRuns). A
// totals or filter option the schema cannot answer, and a dataset whose
// parts break its schema (reader.Dataset.CheckSchema: every part's footer
// and columns), are refused before any row is read.
func DatasetTotals(ctx context.Context, d *reader.Dataset, prefix string, relative bool, f *filter.Spec, t *totals.Tally, workers int) (DatasetRead, error) {
	var read DatasetRead
	id := d.Metadata().SchemaID
	spec, ok := reader.Schema(id)
	info, known := SchemaInfo(string(id))
	if !ok || !known {
		return read, fmt.Errorf("no provider of this build writes schema %s", id)
	}
	opt := t.Options()
	need := []string{totals.Size, totals.AllocatedBytes, "last_modified", info.Containers.Column, spec.Versions.DeleteMarker}
	if opt.LinksOnce {
		need = append(need, "nlink", "dev", "inode")
	}
	if size, mtime, class := f.Reads(); size || mtime || class {
		need = append(need, "size", "last_modified", info.StorageClass)
	}
	for _, c := range spec.Columns {
		if slices.Contains(need, c.Name) {
			read.Columns = append(read.Columns, c.Name)
		}
	}
	s := storedSchema(info, spec, read.Columns)
	keep, err := filter.Compile(f, &s)
	if err != nil {
		return read, err
	}
	vers, err := schema.BindVersions(&s)
	if err != nil {
		return read, err
	}
	sink, err := bindTotals(t, &s, vers, nil, true)
	if err != nil {
		return read, err
	}
	// The accessors read an absent column as null, which the schema allows
	// only where a part may lack it: every part's columns are checked
	// against the spec first, so a dataset that breaks it is refused, never
	// totalled (a part without kind would count its directories as files).
	if err := d.CheckSchema(ctx); err != nil {
		return read, fmt.Errorf("the dataset does not hold schema %s as swath writes it: %w", id, err)
	}
	span, trim := seekparquet.PrefixSpan([]byte(prefix)), len(prefix)
	var probe seekparquet.ReadStats
	if file := strings.TrimSuffix(prefix, "/"); relative && file != prefix {
		rows, err := d.SeekParquet().Get(ctx, []byte(file), &probe)
		if err != nil {
			return read, err
		}
		if len(rows) > 0 {
			span, trim = seekparquet.KeySpan([]byte(file)), strings.LastIndexByte(file, '/')+1
		}
	}
	plan, err := d.SeekParquet().Plan(ctx, []seekparquet.Selection{{Span: span}}, read.Columns, &read.Plan)
	read.Plan.Add(probe)
	if err != nil {
		return read, err
	}
	var rows, filtered atomic.Int64
	scan := func(b *seekparquet.Batch, write func([]schema.Row[stored]) error) error {
		views := make([]seekparquet.ColumnView, len(read.Columns))
		for j := range views {
			views[j] = b.Column(j)
		}
		seg := make([]schema.Row[stored], 0, b.Len())
		dropped := 0
		for i := range b.Len() {
			// The key views the batch, which outlives the sink's Write: the
			// tally copies a prefix it keeps.
			k := b.Key(i)
			if relative {
				if k = k[trim:]; len(k) == 0 {
					continue // the directory itself, which its listing does not hold
				}
			}
			r := schema.Row[stored]{Key: unsafe.String(unsafe.SliceData(k), len(k)), Data: stored{views, i}}
			if keep != nil && !keep(&r) {
				dropped++
				continue
			}
			seg = append(seg, r)
		}
		rows.Add(int64(b.Len()))
		filtered.Add(int64(dropped))
		return write(seg)
	}
	if runs, ok := sink.(order.RangeSink[stored]); ok {
		err = scanRuns(ctx, plan, workers, &read.Scan, runs, scan)
	} else {
		err = plan.ScanParallel(ctx, workers, &read.Scan, func(b *seekparquet.Batch) error {
			return scan(b, func(seg []schema.Row[stored]) error { return sink.Write(ctx, [][]schema.Row[stored]{seg}) })
		})
	}
	read.Rows, read.Filtered = rows.Load(), filtered.Load()
	return read, err
}

// scanRuns scans plan as Plan.ScanParallel does, workers shards at a time
// (Plan.Split, the same cut), each in dataset order through one run of
// runs, and checks the keys across the shards' boundaries, as
// ScanParallel checks its own.
func scanRuns(ctx context.Context, plan *seekparquet.Plan, workers int, st *seekparquet.ReadStats, runs order.RangeSink[stored],
	scan func(*seekparquet.Batch, func([]schema.Row[stored]) error) error) error {
	workers = max(workers, 1)
	shards, err := plan.Split(ctx, workers*8, st)
	if err != nil {
		return err
	}
	type shard struct {
		first, last []byte
		seen        bool
		st          seekparquet.ReadStats
		err         error
	}
	done := make([]shard, len(shards))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(workers, len(shards)) {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(shards) || ctx.Err() != nil {
					return
				}
				s := &done[i]
				run := runs.Run()
				s.err = shards[i].Scan(ctx, &s.st, func(b *seekparquet.Batch) error {
					if n := b.Len(); n > 0 {
						if !s.seen {
							s.first, s.seen = bytes.Clone(b.Key(0)), true
						}
						s.last = append(s.last[:0], b.Key(n-1)...)
					}
					return scan(b, func(seg []schema.Row[stored]) error { return run.Write(ctx, seg) })
				})
				run.End()
				if s.err != nil {
					cancel()
					return
				}
			}
		})
	}
	wg.Wait()
	var first error
	for i := range done {
		st.Add(done[i].st)
		if e := done[i].err; e != nil && (first == nil || errors.Is(first, context.Canceled)) {
			first = e
		}
	}
	if first != nil {
		return first
	}
	var last []byte
	for i := range done {
		if !done[i].seen {
			continue
		}
		if last != nil && bytes.Compare(done[i].first, last) <= 0 {
			return fmt.Errorf("the dataset's keys are out of order: %q follows %q", done[i].first, last)
		}
		last = done[i].last
	}
	return nil
}

// stored is a dataset row as a payload: its batch's projected columns and
// its index there, valid while the scan's callback runs.
type stored struct {
	cols []seekparquet.ColumnView
	i    int
}

// storedSchema is the schema of spec's rows read as stored payloads, with
// the producer's declarations info: each column in projected reads its
// slot; any other is never read (DatasetTotals projects what its consumers
// bind), and reading one is a bug that panics.
func storedSchema(info schema.Info, spec reader.SchemaSpec, projected []string) schema.Schema[stored] {
	cols := make([]schema.Column[stored], len(spec.Columns))
	for n, c := range spec.Columns {
		j := slices.Index(projected, c.Name)
		if j < 0 {
			missing := func() { panic("registry: column " + c.Name + " of a dataset is read but not projected") }
			switch c.Type {
			case schema.String, schema.Binary:
				cols[n].Str = func(*stored) (string, bool) { missing(); return "", false }
			case schema.Bool:
				cols[n].Bool = func(*stored) (bool, bool) { missing(); return false, false }
			default:
				cols[n].Int = func(*stored) (int64, bool) { missing(); return 0, false }
			}
			continue
		}
		null := func(p *stored) bool { v := &p.cols[j]; return !v.Present() || v.IsNull(p.i) }
		switch c.Type {
		case schema.String, schema.Binary:
			cols[n].Str = func(p *stored) (string, bool) {
				if null(p) {
					return "", false
				}
				b := p.cols[j].Bytes(p.i)
				return unsafe.String(unsafe.SliceData(b), len(b)), true
			}
		case schema.Bool:
			cols[n].Bool = func(p *stored) (bool, bool) {
				if null(p) {
					return false, false
				}
				return p.cols[j].Boolean(p.i), true
			}
		case schema.Int32, schema.Uint32:
			// An unsigned value's bits, as the producers carry them.
			cols[n].Int = func(p *stored) (int64, bool) {
				if null(p) {
					return 0, false
				}
				return int64(p.cols[j].Int32(p.i)), true
			}
		default:
			cols[n].Int = func(p *stored) (int64, bool) {
				if null(p) {
					return 0, false
				}
				return p.cols[j].Int64(p.i), true
			}
		}
	}
	return schema.Bind(spec.ID, schema.Schema[stored]{Info: info, Columns: cols})
}
