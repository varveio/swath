// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
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
	// work of the plan, the split and every shard up to where it stopped,
	// a failed plan's or split's included.
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
// prefix's rows (seekparquet.Dataset.Plan, projecting the columns
// Object holds), which checks every part holding them, divides the plan
// into shards of consecutive rows (seekparquet.Plan.Split): one for
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
// returned: fn's, unchanged (seekparquet.Stop included: no error of fn
// ends a scan successfully); a row that breaks Stream's checks (a
// SemanticError of KindRow); the module's and the Source's errors,
// unchanged; or the context's, checked before planning, before each batch
// and after the shards end, so a scan whose context ends is never a
// success. Each shard's scan checks the order of its own keys; once every
// shard has succeeded, each shard's first key is checked to be above the
// last key of the shard before it, which a key out of order or repeated
// where a row group begins can break: that is the module's key_order
// refusal (a *seekparquet.RefusalError naming the key, at no part or row
// group), returned after the shards' objects were handed on. Objects
// already handed to fn stay handed: a failed scan is no result. Scan does
// not add to StreamStats.
//
// This is a bulk read. With a Source that caches bytes, callers that want
// to avoid filling the cache mark ctx with their storage's bypass policy
// (for example, objfile.WithCacheMode(ctx, objfile.CacheBypass) for swath's
// objfile). The reader passes that policy through to SeekParquet.
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
		values := make([]seekparquet.Value, len(columns))
		return func(ctx context.Context, shard *seekparquet.Plan, st *ReadStats, keys *shardKeys) error {
			return d.scanShard(ctx, shard, st, keys, columns, values, batch, fn)
		}
	})
}

// shardScan scans one shard into st and records in keys the first and
// last key it hands on. A worker's shardScan is called for one shard at a
// time, in ascending shard order.
type shardScan func(ctx context.Context, shard *seekparquet.Plan, st *ReadStats, keys *shardKeys) error

// scan plans req.Prefix's rows and scans them as scanSelections does.
func (d *Dataset) scan(ctx context.Context, req ScanRequest, columns []string, worker func() shardScan) error {
	return d.scanSelections(ctx, []seekparquet.Selection{prefixSpan(req.Prefix).selection()}, req.Workers, req.Stats, columns, worker)
}

// scanSelections plans the rows of selections projecting columns and splits the
// plan, then runs workers goroutines (0 or less: 1) that take its shards
// in order, each scanning them with its own shardScan from worker, so that
// what a shardScan keeps between shards is per worker. It adds the plan's,
// the split's (a failed one's too) and every shard's statistics to stats
// when it is not nil. The first error cancels the other shards and is
// returned; without one, the context's error is, and then the order of
// the keys across shards is checked (shardOrder).
func (d *Dataset) scanSelections(ctx context.Context, selections []seekparquet.Selection, workers int, stats *ReadStats, columns []string, worker func() shardScan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var st ReadStats
	plan, err := d.sp.Plan(ctx, selections, columns, &st)
	addStats(stats, st)
	if err != nil {
		return err
	}
	workers = max(workers, 1)
	shards, err := plan.Split(ctx, scanShards(workers), &st)
	addStats(stats, st)
	if err != nil {
		return err
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var first error
	var once sync.Once
	shardStats := make([]ReadStats, len(shards))
	keys := make([]shardKeys, len(shards))
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
				if err := scanShard(ctx, shards[i], &shardStats[i], &keys[i]); err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		})
	}
	wg.Wait()
	for _, st := range shardStats {
		addStats(stats, st)
	}
	if first != nil {
		return first
	}
	if err := parent.Err(); err != nil {
		return err
	}
	return shardOrder(keys)
}

// shardKeys is the first and last key a shard handed on, owned copies.
type shardKeys struct {
	first, last []byte
	seen        bool
}

// note records first as the shard's first key, unless it has one, and
// last as its last. It copies both: a borrowed key is valid only during
// its batch, and an owning one may be changed by the callback.
func (k *shardKeys) note(first, last []byte) {
	if !k.seen {
		k.first, k.seen = append(k.first[:0], first...), true
	}
	k.last = append(k.last[:0], last...)
}

// noteBatch notes the keys of a batch of b.Len() > 0 rows.
func (k *shardKeys) noteBatch(b *seekparquet.Batch) { k.note(b.Key(0), b.Key(b.Len()-1)) }

// shardOrder checks the keys across the boundaries of shards that each
// succeeded, in plan order. Each shard's scan checks the order of the key
// pages it decodes against each other, and the module checks each row
// group's first key against its neighbours' first keys, but nothing
// checks a shard's first key against the last of the shard before it,
// which the one scan of an unsplit plan does. Shards begin at row groups,
// and with keys that may repeat Split keeps a group of equal keys inside
// a part in one shard, so the first key of a shard must be above the last
// of the one before it; an equal key is a duplicate of a unique key or a
// group crossing parts, both of which an unsplit scan refuses. The
// refusal is the module's key_order, its Key the shard's first key, at no
// part or row group: the reader does not see where a shard begins.
func shardOrder(keys []shardKeys) error {
	var last []byte
	for i := range keys {
		k := &keys[i]
		if !k.seen {
			continue
		}
		if last != nil && bytes.Compare(last, k.first) >= 0 {
			return &RefusalError{Code: seekparquet.CodeKeyOrder, RowGroup: -1, Row: -1, Key: bytes.Clone(k.first),
				Detail: "a shard's first key breaks the declared key order with the last key of the shard before it"}
		}
		last = k.last
	}
	return nil
}

// scanErr is the error of a module scan, err, whose batch callback kept
// the error of swath's caller's fn in failed. The module ends a scan whose
// callback returns seekparquet.Stop with no error; swath's callbacks have
// no such convention, so fn's error, Stop included, is returned as fn
// returned it (and so stops the other shards).
func scanErr(err, failed error) error {
	if failed != nil {
		return failed
	}
	return err
}

// scanBatchObjects is the most objects one Scan batch holds.
const scanBatchObjects = 1024

// scanShard scans one shard, handing fn its objects in batches built in
// batch, an empty slice reused from shard to shard; values is the scratch
// of each row's decoding (batchObject).
func (d *Dataset) scanShard(ctx context.Context, shard *seekparquet.Plan, st *ReadStats, keys *shardKeys, columns []string, values []seekparquet.Value, batch []Object, fn func([]Object) error) error {
	var failed error // fn's (scanErr)
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys.note(batch[0].Key, batch[len(batch)-1].Key)
		failed = fn(batch)
		batch = batch[:0]
		return failed
	}
	err := shard.Scan(ctx, st, func(b *seekparquet.Batch) error {
		for i := range b.Len() {
			o, err := d.batchObject(b, i, columns, values)
			if err != nil {
				return err
			}
			if batch = append(batch, o); len(batch) == cap(batch) {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err = scanErr(err, failed); err == nil && len(batch) > 0 {
		err = flush()
	}
	return err
}
