// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/tools/swath-local-spike/internal/dataset"
	"github.com/varveio/swath/tools/swath-local-spike/internal/schema"
)

// initialRatio is the compression ratio (encoded / logical bytes) assumed
// for part 0, before any part has been measured. Listings of the realistic
// benchmark tree measure about 0.11.
const initialRatio = 0.1

// partQueue bounds the batches (one per order batch, about 4096 rows)
// queued to one part's encoder.
const partQueue = 16

// Options configure a Router.
type Options struct {
	Out      string         // dataset directory; parts go to out/data/*.tmp
	Schema   *schema.Schema //
	PartSize int64          // target encoded bytes per part
	Encoders int            // parts encoded concurrently (>= 1)
}

// Router is the order stage's sink. It owns part boundaries: a row's logical
// size is its uncompressed field bytes; part 0 is cut at PartSize/0.1
// logical bytes, its measured ratio is then frozen, and every later part is
// cut at PartSize/ratio. Boundaries therefore depend only on the rows, never
// on timing or the encoder count. Ordinals are dense and the router marks
// the last part final.
//
// Each part is encoded by its own goroutine with its own writer and codec,
// fed through a bounded queue while the part is still open; at most
// Encoders parts are in flight. When fewer parts than encoders are active, a
// part encodes its page columns in parallel (byte-identical), so a single
// part still uses every core.
type Router struct {
	opt     Options
	slots   chan struct{}
	active  atomic.Int32
	cur     *part
	parts   []*part
	limit   int64 // logical bytes per part; 0 until part 0 is measured
	rows    int64
	full    bool
	errMu   sync.Mutex
	err     error // first encoder error
	aborted atomic.Bool
	closed  bool
}

type part struct {
	ordinal int
	segs    chan [][]schema.Row
	done    chan struct{}
	logical int64
	final   bool
	info    dataset.Part
	err     error
}

// NewRouter returns a router writing into opt.Out.
func NewRouter(opt Options) (*Router, error) {
	if opt.PartSize <= 0 || opt.Encoders < 1 || opt.Schema == nil {
		return nil, errors.New("parquet router: part size, encoders and schema are required")
	}
	return &Router{opt: opt, slots: make(chan struct{}, opt.Encoders)}, nil
}

// Write routes ascending segments into parts. Each part receives the
// segments of one Write call as a single batch.
func (r *Router) Write(ctx context.Context, segs [][]schema.Row) error {
	var batch [][]schema.Row
	for _, seg := range segs {
		start := 0
		for i := range seg {
			if r.full {
				if i > start {
					batch = append(batch, seg[start:i])
				}
				if err := r.send(ctx, batch); err != nil {
					return err
				}
				batch, start = nil, i
				if err := r.cut(ctx, false); err != nil {
					return err
				}
			}
			if r.cur == nil {
				if err := r.open(ctx); err != nil {
					return err
				}
			}
			r.rows++
			r.cur.logical += logicalBytes(&seg[i])
			target := r.limit
			if target == 0 {
				target = int64(float64(r.opt.PartSize) / initialRatio)
			}
			if r.cur.logical >= target {
				r.full = true // cut before the next row
			}
		}
		if start < len(seg) {
			batch = append(batch, seg[start:])
		}
	}
	return r.send(ctx, batch)
}

func (r *Router) send(ctx context.Context, batch [][]schema.Row) error {
	if len(batch) == 0 {
		return nil
	}
	select {
	case r.cur.segs <- batch:
		return nil
	case <-r.cur.done: // the encoder stopped early
		return r.cur.err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// open starts the next part's encoder, waiting for a free slot.
func (r *Router) open(ctx context.Context) error {
	if err := r.firstErr(); err != nil {
		return err
	}
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	p := &part{ordinal: len(r.parts), segs: make(chan [][]schema.Row, partQueue), done: make(chan struct{})}
	r.parts = append(r.parts, p)
	r.cur = p
	r.active.Add(1)
	go r.encode(p)
	return nil
}

func (r *Router) encode(p *part) {
	defer func() {
		r.active.Add(-1)
		<-r.slots
		close(p.done)
	}()
	columns := func() int { return max(1, r.opt.Encoders/max(1, int(r.active.Load()))) }
	w, err := create(dataset.TempPath(r.opt.Out, p.ordinal), r.opt.Schema, p.ordinal, r.opt.Encoders, columns)
	if err != nil {
		p.err = err
		r.fail(p.err)
		for range p.segs { // drain so the router never blocks
		}
		return
	}
	for batch := range p.segs {
		if p.err != nil || r.aborted.Load() {
			continue
		}
	write:
		for _, seg := range batch {
			for i := range seg {
				if err := w.write(&seg[i]); err != nil {
					p.err = fmt.Errorf("part %d: %w", p.ordinal, err)
					r.fail(p.err)
					break write
				}
			}
		}
	}
	if p.err != nil || r.aborted.Load() {
		w.abort()
		return
	}
	p.info, p.err = w.close(p.final)
	if p.err != nil {
		r.fail(p.err)
	}
}

// cut closes the current part. Part 0 is waited for, to freeze the ratio.
func (r *Router) cut(ctx context.Context, final bool) error {
	p := r.cur
	p.final = final
	close(p.segs)
	r.cur, r.full = nil, false
	if p.ordinal == 0 && !final {
		select {
		case <-p.done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		if p.err != nil {
			return p.err
		}
		ratio := float64(p.info.Bytes) / float64(max(1, p.logical))
		r.limit = max(1, int64(float64(r.opt.PartSize)/ratio))
	}
	return nil
}

func (r *Router) fail(err error) {
	r.errMu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.errMu.Unlock()
}

func (r *Router) firstErr() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

// Close marks the last part final, waits for every encoder and returns the
// parts in ordinal order after the publication gate.
func (r *Router) Close() ([]dataset.Part, error) {
	if r.closed {
		return nil, errors.New("router already closed")
	}
	if r.cur != nil {
		if err := r.cut(context.Background(), true); err != nil {
			r.Abort()
			return nil, err
		}
	}
	r.closed = true
	out := make([]dataset.Part, len(r.parts))
	var errs []error
	for i, p := range r.parts {
		<-p.done
		if p.err != nil {
			errs = append(errs, p.err)
		}
		out[i] = p.info
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if err := dataset.Check(out, r.rows); err != nil {
		return nil, fmt.Errorf("publication gate: %w", err)
	}
	return out, nil
}

// Abort stops every encoder; it is safe after Close. Unpublished files are
// removed by dataset.Cleanup.
func (r *Router) Abort() {
	if r.closed {
		return
	}
	r.closed = true
	r.aborted.Store(true)
	if r.cur != nil {
		close(r.cur.segs)
		r.cur = nil
	}
	for _, p := range r.parts {
		<-p.done
	}
}

// logicalBytes is the deterministic uncompressed size of a row.
func logicalBytes(r *schema.Row) int64 {
	n := int64(len(r.Key)+len(r.LinkTarget)+len(r.Kind.String())) + 1
	for _, p := range [...]schema.Presence{schema.HasSize, schema.HasMtime, schema.HasInode, schema.HasDev, schema.HasBlocks, schema.HasCtime, schema.HasAtime, schema.HasBtime} {
		if r.Has(p) {
			n += 8
		}
	}
	for _, p := range [...]schema.Presence{schema.HasMode, schema.HasUID, schema.HasGID, schema.HasNlink} {
		if r.Has(p) {
			n += 4
		}
	}
	if o := r.Object; o != nil {
		n += int64(len(o.ETag) + len(o.StorageClass) + len(o.OwnerID) + len(o.OwnerDisplayName) + len(o.ChecksumAlgorithm) + len(o.ChecksumType))
	}
	return n
}
