// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import (
	"github.com/varveio/swath/go/internal/faults"

	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
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

	// Publish, if set, publishes each part as soon as it is closed, in
	// ordinal order (part i only after part i-1): the journaled run makes
	// the part durable and renames it. limit is the frozen logical bytes
	// per part (for resume), rows the total rows in parts 0..ordinal. It
	// returns the part with its final path.
	Publish func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error)
	// Resume continues a run whose first parts are already published.
	Resume *Resume
	// Fail, if set, is called once with the first asynchronous failure (an
	// encoder, the disk, the journal), so the whole pipeline stops at once.
	Fail func(error)
	// Account, if set, is told the bytes (schema.RowsBytes) queued to the
	// encoders (+) and written (-): they share the order stage's budget.
	Account func(int64)
}

// Resume is the router state recovered from a journal.
type Resume struct {
	Parts []dataset.Part // published parts 0..n-1, final paths
	Limit int64          // frozen logical bytes per part
	Rows  int64          // rows in Parts
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
	done    []dataset.Part // resumed parts, already published
}

type part struct {
	ordinal   int
	prev      *part // the previous part of this run, nil for the first
	segs      chan batch
	done      chan struct{}
	published chan struct{} // closed once published (or failed), with Publish set
	logical   int64
	final     bool
	limit     int64 // logical bytes per part in effect after this part
	rows      int64 // rows in parts 0..ordinal
	info      dataset.Part
	err       error
}

// NewRouter returns a router writing into opt.Out.
func NewRouter(opt Options) (*Router, error) {
	if opt.PartSize <= 0 || opt.Encoders < 1 || opt.Schema == nil {
		return nil, errors.New("parquet router: part size, encoders and schema are required")
	}
	r := &Router{opt: opt, slots: make(chan struct{}, opt.Encoders)}
	if res := opt.Resume; res != nil {
		r.done, r.limit, r.rows = res.Parts, res.Limit, res.Rows
	}
	return r, nil
}

// Account sets the byte accounting hook (see Options.Account); call before
// the first Write.
func (r *Router) Account(f func(int64)) { r.opt.Account = f }

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

// batch is one order-stage batch queued to a part's encoder.
type batch struct {
	segs  [][]schema.Row
	bytes int64
}

func (r *Router) send(ctx context.Context, segs [][]schema.Row) error {
	if len(segs) == 0 {
		return nil
	}
	b := batch{segs: segs}
	if r.opt.Account != nil {
		for _, s := range segs {
			b.bytes += schema.RowsBytes(s)
		}
		r.opt.Account(b.bytes)
	}
	select {
	case r.cur.segs <- b:
		return nil
	case <-r.cur.done: // the encoder stopped early
		r.done1(b)
		return r.cur.err
	case <-ctx.Done():
		r.done1(b)
		return context.Cause(ctx)
	}
}

// done1 releases a batch's bytes once it is written or dropped.
func (r *Router) done1(b batch) {
	if r.opt.Account != nil && b.bytes != 0 {
		r.opt.Account(-b.bytes)
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
	p := &part{ordinal: len(r.done) + len(r.parts), segs: make(chan batch, partQueue), done: make(chan struct{}),
		published: make(chan struct{}), limit: r.limit}
	if n := len(r.parts); n > 0 {
		p.prev = r.parts[n-1]
	}
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
	defer close(p.published)
	columns := func() int { return max(1, r.opt.Encoders/max(1, int(r.active.Load()))) }
	w, err := create(dataset.TempPath(r.opt.Out, p.ordinal), r.opt.Schema, p.ordinal, r.opt.Encoders, columns)
	if err != nil {
		p.err = err
		r.fail(p.err)
		for b := range p.segs { // drain so the router never blocks
			r.done1(b)
		}
		return
	}
	for b := range p.segs {
		if p.err != nil || r.aborted.Load() {
			r.done1(b)
			continue
		}
		if err := faults.At("encoding"); err != nil {
			p.err = err
			r.fail(err)
			r.done1(b)
			continue
		}
	write:
		for _, seg := range b.segs {
			for i := range seg {
				if err := w.write(&seg[i]); err != nil {
					p.err = fmt.Errorf("part %d: %w", p.ordinal, err)
					r.fail(p.err)
					break write
				}
			}
		}
		r.done1(b)
	}
	if p.err != nil || r.aborted.Load() {
		w.abort()
		return
	}
	p.info, p.err = w.close(p.final)
	if p.err != nil {
		r.fail(p.err)
		return
	}
	if p.ordinal == 0 && !p.final {
		ratio := float64(p.info.Bytes) / float64(max(1, p.logical))
		p.limit = max(1, int64(float64(r.opt.PartSize)/ratio))
	}
	p.rows = p.info.Rows
	base := int64(0)
	if p.prev != nil {
		<-p.prev.published // publication goes in ordinal order
		if p.prev.err != nil {
			p.err = fmt.Errorf("part %d: not published after part %d failed", p.ordinal, p.prev.ordinal)
			return
		}
		base = p.prev.rows
	} else if len(r.done) > 0 {
		base = r.opt.Resume.Rows
	}
	p.rows += base
	if r.opt.Publish != nil && !r.aborted.Load() {
		if p.info, p.err = r.opt.Publish(p.ordinal, p.info, p.limit, p.rows, p.final); p.err != nil {
			r.fail(p.err)
		}
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
		r.limit = p.limit
	}
	return nil
}

func (r *Router) fail(err error) {
	r.errMu.Lock()
	first := r.err == nil
	if first {
		r.err = err
	}
	r.errMu.Unlock()
	if first && r.opt.Fail != nil {
		r.opt.Fail(err)
	}
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
	out := append([]dataset.Part(nil), r.done...)
	var errs []error
	for _, p := range r.parts {
		<-p.done
		if p.err != nil {
			errs = append(errs, p.err)
		}
		out = append(out, p.info)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if len(out) == 0 && r.rows == 0 {
		return out, nil
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
