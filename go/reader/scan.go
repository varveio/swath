// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"sync"

	seekparquet "github.com/varveio/seekparquet/go"
)

// ScanRequest is a parallel scan of every current object under a prefix.
type ScanRequest struct {
	// Prefix scopes the scan; empty scans every key.
	Prefix []byte
	// Workers is the most shards scanned at once, one goroutine each; 0
	// or less means 1.
	Workers int
	// Stats, when not nil, receives the scan's read statistics: reset when
	// the request starts, and set whether it succeeds or fails, with the
	// work of every shard up to where it stopped. A plan or a split that
	// fails returns no statistics, so its own reads are missing.
	Stats *ReadStats
}

// Scan hands fn, in batches, every current object whose key begins with
// req.Prefix, read by req.Workers goroutines at once. It plans the
// prefix's rows (seekparquet.Dataset.PlanRanges, projecting the columns
// Object holds), which checks every part holding them, divides the plan
// into at most Workers shards of consecutive rows (seekparquet.RangePlan.
// Split) and scans each in its own goroutine, decoding and checking each
// object as Stream does. Every object is handed to fn once.
//
// fn is called from several goroutines at once and must be safe for
// concurrent use. A shard's batches arrive in ascending key order, one
// after another; the order across shards is unspecified. A batch holds at
// most 1,024 objects, and its slice is reused once fn returns: fn must not
// keep it, while an Object copied out of it owns its memory.
//
// Versions schemas and filesystem entries are refused as Stream refuses
// them, before any read. The first error stops every shard and is
// returned: fn's, unchanged; a row that breaks Stream's checks (a
// SemanticError of KindRow); the module's and the Source's errors,
// unchanged; or the context's, checked before planning, before each batch
// and after the shards end, so a scan whose context ends is never a
// success. Objects already handed to fn stay handed: a failed scan is no
// result. Scan does not add to StreamStats.
func (d *Dataset) Scan(ctx context.Context, req ScanRequest, fn func(batch []Object) error) error {
	if req.Stats != nil {
		*req.Stats = ReadStats{}
	}
	columns, err := d.currentColumns()
	if err != nil {
		return err
	}
	return d.scan(ctx, req, columns, func(ctx context.Context, shard *seekparquet.RangePlan, st *ReadStats) error {
		return d.scanShard(ctx, shard, st, fn)
	})
}

// scan plans and runs parallel shards for either scan representation.
func (d *Dataset) scan(ctx context.Context, req ScanRequest, columns []string, scanShard func(context.Context, *seekparquet.RangePlan, *ReadStats) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := d.sp.PlanRanges(ctx, []seekparquet.Range{prefixSpan(req.Prefix).rng()}, columns)
	if err != nil {
		return err
	}
	addStats(req.Stats, plan.Stats())
	shards, err := plan.Split(ctx, max(req.Workers, 1))
	if err != nil {
		return err
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var first error
	var once sync.Once
	stats := make([]ReadStats, len(shards))
	var wg sync.WaitGroup
	for i, shard := range shards {
		addStats(req.Stats, shard.Stats())
		wg.Go(func() {
			if err := scanShard(ctx, shard, &stats[i]); err != nil {
				once.Do(func() { first = err; cancel() })
			}
		})
	}
	wg.Wait()
	for _, st := range stats {
		addStats(req.Stats, st)
	}
	if first == nil {
		return parent.Err()
	}
	return first
}

// scanBatchObjects is the most objects one Scan batch holds.
const scanBatchObjects = 1024

// scanShard scans one shard, handing fn its objects in batches.
func (d *Dataset) scanShard(ctx context.Context, shard *seekparquet.RangePlan, st *ReadStats, fn func([]Object) error) error {
	batch := make([]Object, 0, scanBatchObjects)
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(batch)
		batch = batch[:0]
		return err
	}
	err := shard.Scan(ctx, st, func(r seekparquet.Row) error {
		o, err := decodeObject(d.spec.ID, r)
		if err != nil {
			return err
		}
		if batch = append(batch, o); len(batch) == cap(batch) {
			return flush()
		}
		return nil
	})
	if err == nil && len(batch) > 0 {
		err = flush()
	}
	return err
}
