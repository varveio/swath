// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import (
	"github.com/varveio/swath/go/internal/faults"
	"os"
	"path/filepath"

	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	Out      string // dataset directory; parts go to .swath/parts/
	PartSize int64  // target encoded bytes per part
	Encoders int    // parts encoded concurrently (>= 1)

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
type Router[P any] struct {
	schema  *schema.Schema[P]
	opt     Options
	slots   chan struct{}
	active  atomic.Int32
	cur     *part[P]
	parts   []*part[P]
	limit   int64 // logical bytes per part; 0 until part 0 is measured
	rows    int64
	full    bool
	errMu   sync.Mutex
	err     error // first encoder error
	aborted atomic.Bool
	waitNs  atomic.Int64 // time Write waited for an encoder (queue or slot)
	closed  bool
	done    []dataset.Part // resumed parts, already published
}

type part[P any] struct {
	ordinal   int
	prev      *part[P] // the previous part of this run, nil for the first
	segs      chan batch[P]
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
func NewRouter[P any](opt Options, s *schema.Schema[P]) (*Router[P], error) {
	if opt.PartSize <= 0 || opt.Encoders < 1 || s == nil {
		return nil, errors.New("parquet router: part size, encoders and schema are required")
	}
	if err := os.MkdirAll(filepath.Join(opt.Out, dataset.Workspace, dataset.PartsDir), 0o755); err != nil {
		return nil, fmt.Errorf("parquet router: %w", err)
	}
	r := &Router[P]{schema: s, opt: opt, slots: make(chan struct{}, opt.Encoders)}
	if res := opt.Resume; res != nil {
		r.done, r.limit, r.rows = res.Parts, res.Limit, res.Rows
	}
	return r, nil
}

// EncoderWait is the time Write waited for an encoder: a full part queue,
// or every encoder busy when a part was opened.
func (r *Router[P]) EncoderWait() time.Duration { return time.Duration(r.waitNs.Load()) }

// Write routes ascending segments into parts. Each part receives the
// segments of one Write call as a single batch.
func (r *Router[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	var batch [][]schema.Row[P]
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
			r.cur.logical += r.schema.LogicalBytes(&seg[i])
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
type batch[P any] struct {
	segs  [][]schema.Row[P]
	bytes int64
}

func (r *Router[P]) send(ctx context.Context, segs [][]schema.Row[P]) error {
	if len(segs) == 0 {
		return nil
	}
	b := batch[P]{segs: segs}
	if r.opt.Account != nil {
		for _, s := range segs {
			b.bytes += r.schema.RowsBytes(s)
		}
		r.opt.Account(b.bytes)
	}
	select {
	case r.cur.segs <- b:
		return nil
	default: // the part's queue is full: the encoder is the bottleneck
	}
	t := time.Now()
	defer func() { r.waitNs.Add(int64(time.Since(t))) }()
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
func (r *Router[P]) done1(b batch[P]) {
	if r.opt.Account != nil && b.bytes != 0 {
		r.opt.Account(-b.bytes)
	}
}

// open starts the next part's encoder, waiting for a free slot.
func (r *Router[P]) open(ctx context.Context) error {
	if err := r.firstErr(); err != nil {
		return err
	}
	select {
	case r.slots <- struct{}{}:
	default:
		t := time.Now()
		select {
		case r.slots <- struct{}{}:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		r.waitNs.Add(int64(time.Since(t)))
	}
	p := &part[P]{ordinal: len(r.done) + len(r.parts), segs: make(chan batch[P], partQueue), done: make(chan struct{}),
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

func (r *Router[P]) encode(p *part[P]) {
	defer func() {
		r.active.Add(-1)
		<-r.slots
		close(p.done)
	}()
	defer close(p.published)
	columns := func() int { return max(1, r.opt.Encoders/max(1, int(r.active.Load()))) }
	w, err := create(dataset.TempPath(r.opt.Out, p.ordinal), r.schema, p.ordinal, r.opt.Encoders, columns)
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
	p.info, p.err = w.close()
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
func (r *Router[P]) cut(ctx context.Context, final bool) error {
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

func (r *Router[P]) fail(err error) {
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

func (r *Router[P]) firstErr() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

// Close marks the last part final, waits for every encoder and returns the
// parts in ordinal order after the publication gate.
func (r *Router[P]) Close() ([]dataset.Part, error) {
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
func (r *Router[P]) Abort() {
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

// Encoder is a router seen without its row type: what the pipeline does
// with it after listing.
type Encoder interface {
	Close() ([]dataset.Part, error)
	Abort()
	EncoderWait() time.Duration
}
