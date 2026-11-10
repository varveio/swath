// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	seekparquet "github.com/varveio/seekparquet/go"
)

// ScanRequest is a parallel scan of every current object under a prefix.
type ScanRequest struct {
	// Prefix scopes the scan; empty scans every key.
	Prefix []byte
	// Workers is the number of goroutines scanning at once, and so the
	// most calls of fn at once; 0 or less means 1.
	Workers int
	// Stats, when not nil, receives the scan's read statistics: reset when
	// the request starts, and set whether it succeeds or fails, with the
	// work of every shard up to where it stopped. A plan or a split that
	// fails returns no statistics, so its own reads are missing.
	Stats *ReadStats
}

// shardsPerWorker is how many shards a scan of more than one worker asks
// Split for per worker. Shards of equal bytes can take unequal time (keys
// differ in length and shape, and fn's work per object varies); a worker
// that finishes a shard takes the next one instead of waiting, so the
// scan's tail is bounded by the slowest shard still running, not by a
// fixed share of the work: Split cuts only at row groups, and one shard
// can cost more than others. A shard costs a split boundary, a part
// schema check in ScanBatches and its own SeekParquet scan buffers,
// released when it ends (go/docs/internals/measurements.md). A plan with
// fewer row groups has fewer shards. A var so that tests can vary it.
var shardsPerWorker = 8

// scanShards is the most shards a scan of workers workers asks Split for.
func scanShards(workers int) int {
	if workers == 1 {
		return 1
	}
	return workers * min(shardsPerWorker, math.MaxInt/workers)
}

// Scan hands fn, in batches, every current object whose key begins with
// req.Prefix, read by req.Workers goroutines at once. It plans the
// prefix's rows (seekparquet.Dataset.PlanRanges, projecting the columns
// Object holds), which checks every part holding them, divides the plan
// into shards of consecutive rows (seekparquet.RangePlan.Split): one for
// one worker, otherwise up to shardsPerWorker per worker. Each worker
// takes the next shard not yet taken until none is left, decoding and
// checking each object as Stream does. Every object is handed to fn once.
//
// fn is called from up to Workers goroutines at once and must be safe for
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
	return d.scan(ctx, req, columns, func() shardScan {
		batch := make([]Object, 0, scanBatchObjects)
		return func(ctx context.Context, shard *seekparquet.RangePlan, st *ReadStats) error {
			return d.scanShard(ctx, shard, st, batch, fn)
		}
	})
}

// shardScan scans one shard into st. A worker's shardScan is called for
// one shard at a time, in ascending shard order.
type shardScan func(ctx context.Context, shard *seekparquet.RangePlan, st *ReadStats) error

// scan plans and splits the scan, then runs req.Workers goroutines that
// take its shards in order, each scanning them with its own shardScan from
// worker, so that what a shardScan keeps between shards is per worker.
func (d *Dataset) scan(ctx context.Context, req ScanRequest, columns []string, worker func() shardScan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := d.sp.PlanRanges(ctx, []seekparquet.Range{prefixSpan(req.Prefix).rng()}, columns)
	if err != nil {
		return err
	}
	addStats(req.Stats, plan.Stats())
	workers := max(req.Workers, 1)
	shards, err := plan.Split(ctx, scanShards(workers))
	if err != nil {
		return err
	}
	for _, shard := range shards {
		addStats(req.Stats, shard.Stats())
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var first error
	var once sync.Once
	stats := make([]ReadStats, len(shards))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(workers, len(shards)) {
		wg.Go(func() {
			scanShard := worker()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(shards) || ctx.Err() != nil {
					return
				}
				if err := scanShard(ctx, shards[i], &stats[i]); err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
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

// scanShard scans one shard, handing fn its objects in batches built in
// batch, an empty slice reused from shard to shard.
func (d *Dataset) scanShard(ctx context.Context, shard *seekparquet.RangePlan, st *ReadStats, batch []Object, fn func([]Object) error) error {
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
