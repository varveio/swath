// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

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
// until a part has been measured. Listings of the realistic
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
	// MaxEncoders, if above Encoders, is how many Admit raises them to
	// (the pipeline: once listing has ended); 0 is Encoders. Writers are
	// created for that many column goroutines, so a part opened before
	// the raise uses them too.
	MaxEncoders int

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
	// Encoded, if set, counts rows the encoders have written (progress).
	Encoded *atomic.Int64
	// Account, if set, is told the bytes (schema.RowsBytes) queued to the
	// encoders (+) and written (-): they share the order stage's budget.
	Account func(int64)
	// Interval, if positive, bounds the age of the open part: once it has
	// been open that long with rows, it is closed (and, journaled,
	// published) although not full, so durable progress advances on a
	// slow frontier. Boundaries then depend on timing whenever such a cut
	// happens; 0 cuts by size only.
	Interval time.Duration
	// Verify, if set, is called before each part file is created: the
	// output's pathname must still name the directory the run owns.
	Verify func() error
	// Open, if set, creates a part file (write-only, truncated) at its
	// path; nil opens it by pathname. A caller that owns the output passes
	// one relative to the directory it locked, so a pathname replaced
	// after Verify cannot receive the part.
	Open func(path string) (*os.File, error)
	// MaxRows, if positive, also closes a part once it holds that many rows
	// (Java's --part-rotation-max-rows). Such a cut depends only on the
	// rows, so boundaries stay deterministic (it is part of the resume
	// identity); a part it closes does not calibrate the size ratio, as a
	// time cut does not: only a part closed by size does.
	MaxRows int64
	// Cut, if set, fixes one part as it was written before: the part with
	// that ordinal is closed after exactly that many rows, neither by size
	// nor by the interval, and written with the claimed part's columns and
	// created_by. A resume sets it to reproduce a claimed part whose record
	// was never written, whatever cut it (size, or a timer that fires
	// differently now) and whichever build wrote it. A pinned part that
	// reaches the size target at its boundary was cut by size, and
	// calibrates as it did.
	Cut *Cut
}

// Cut is a part recovered from a claimed part's footer: its ordinal, its
// row count and, when read, its columns (the key first) and created_by.
// A build that adds an optional column writes the claimed part without it
// and the parts after it with it, so the claimed part is reproduced byte
// for byte; created_by names the build that wrote it, so a later build of
// the same recipe reproduces it too. Columns the schema does not hold in
// that order, or none, leave the schema whole, and CreatedBy empty leaves
// this build's: the byte comparison then decides.
type Cut struct {
	Ordinal   int
	Rows      int64
	Columns   []string
	CreatedBy string
}

// claimSchema is s restricted to the named columns, the key first, when
// they are s's columns in s's order; otherwise s.
func claimSchema[P any](s *schema.Schema[P], names []string) *schema.Schema[P] {
	if len(names) == 0 || names[0] != "key" {
		return s
	}
	names = names[1:]
	if len(names) == len(s.Columns) {
		return s
	}
	cols := make([]schema.Column[P], 0, len(names))
	for _, c := range s.Columns {
		if len(cols) < len(names) && c.Name == names[len(cols)] {
			cols = append(cols, c)
		}
	}
	if len(cols) != len(names) {
		return s
	}
	sub := *s
	sub.Columns = cols
	return &sub
}

// Resume is the router state recovered from a journal.
type Resume struct {
	Parts []dataset.Part // published parts 0..n-1, final paths
	Limit int64          // frozen logical bytes per part
	Rows  int64          // rows in Parts
}

// Router is the order stage's sink. It owns part boundaries: a row's logical
// size is its uncompressed field bytes; parts are cut at PartSize/0.1
// logical bytes until the calibrating part (the first closed by size: part
// 0 when the interval does not cut) has been measured; its ratio is then frozen and every later
// part is cut at PartSize/ratio. Without interval cuts, boundaries depend
// only on the rows, never on timing or the encoder count. Ordinals are dense and the router marks
// the last part final.
//
// Each part is encoded by its own goroutine with its own writer (the codec
// is the router's, shared), fed through a bounded queue while the part is still open; at most
// Encoders parts (MaxEncoders after Admit) are in flight. When fewer parts
// than encoders are active, a part encodes its page columns in parallel
// (byte-identical), so a single part still uses every core.
type Router[P any] struct {
	schema   *schema.Schema[P]
	opt      Options
	codec    *codec        // every writer's (closed once every part has ended)
	slots    chan struct{} // MaxEncoders; the reserved part held until Admit
	reserved atomic.Int32  // slots held for Admit
	encoders atomic.Int32  // encoders admitted
	active   atomic.Int32
	cur      *part[P]
	parts    []*part[P]
	limit    int64 // logical bytes per part; 0 until part 0 is measured
	rows     int64
	full     bool
	errMu    sync.Mutex
	err      error // first encoder error
	aborted  atomic.Bool
	waitNs   atomic.Int64 // time Write waited for an encoder (queue or slot)
	closed   bool
	done     []dataset.Part // resumed parts, already published

	// mu serializes Write, Close and Abort with the interval cut.
	mu       sync.Mutex
	stopTick chan struct{} // closed by Close and Abort (nil without Interval)
	tickCtx  context.Context
	tickStop context.CancelFunc // ends a part-0 cut the ticker waits on
	ticked   sync.WaitGroup
	timeCuts atomic.Int64 // parts closed by the interval
	rowCuts  atomic.Int64 // parts closed by MaxRows

	// segmented: the rest of the stream goes to segments (segments.go),
	// not Write; segRows counts the rows they routed.
	segmented bool
	segRows   atomic.Int64
}

type part[P any] struct {
	ordinal   int
	opened    time.Time
	prev      *part[P] // the previous part of this run, nil for the first
	segs      chan batch[P]
	done      chan struct{}
	published chan struct{} // closed once published (or failed), with Publish set
	drained   chan struct{} // closed once the encoder has taken every batch of its closed queue
	logical   int64
	n         int64 // rows routed into this part
	pinned    bool  // its boundary is Options.Cut
	bySize    bool  // closed because it reached the size target
	final     bool
	segment   bool // written as a segment (OpenSegment)
	// tokens bound the batches a segment queues to the part it continues
	// (its queue is partQueue deep) to segmentQueue, as a new segment's.
	tokens chan struct{}
	closed bool  // its queue is closed (r.mu)
	limit  int64 // logical bytes per part in effect after this part
	rows   int64 // rows in parts 0..ordinal
	info   dataset.Part
	err    error
}

// NewRouter returns a router writing into opt.Out.
func NewRouter[P any](opt Options, s *schema.Schema[P]) (*Router[P], error) {
	if opt.PartSize <= 0 || opt.Encoders < 1 || s == nil {
		return nil, errors.New("parquet router: part size, encoders and schema are required")
	}
	if opt.Verify != nil {
		if err := opt.Verify(); err != nil {
			return nil, fmt.Errorf("parquet router: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Join(opt.Out, dataset.Workspace, dataset.PartsDir), 0o755); err != nil {
		return nil, fmt.Errorf("parquet router: %w", err)
	}
	opt.MaxEncoders = max(opt.Encoders, opt.MaxEncoders)
	// One codec for every writer: its zstd states (each grows to the
	// largest page it compressed and keeps it) are at most one per encoder
	// admitted, the most column goroutines at once across the parts
	// (columns), however many parts are open; Admit raises them with the
	// encoders. A codec per writer held a pool of states each.
	r := &Router[P]{schema: s, opt: opt, slots: make(chan struct{}, opt.MaxEncoders), codec: newCodec(opt.Encoders)}
	for range opt.MaxEncoders - opt.Encoders {
		r.slots <- struct{}{}
	}
	r.reserved.Store(int32(opt.MaxEncoders - opt.Encoders))
	r.encoders.Store(int32(opt.Encoders))
	if res := opt.Resume; res != nil {
		r.done, r.limit, r.rows = res.Parts, res.Limit, res.Rows
	}
	r.startTick()
	return r, nil
}

// startTick starts the interval cut, if the router has one (mu held, or
// before the router is shared).
func (r *Router[P]) startTick() {
	if r.opt.Interval <= 0 || r.stopTick != nil {
		return
	}
	r.stopTick = make(chan struct{})
	r.tickCtx, r.tickStop = context.WithCancel(context.Background())
	r.ticked.Add(1)
	go r.tick(r.stopTick)
}

// tick closes the open part once it is Interval old, until stopped.
func (r *Router[P]) tick(stop <-chan struct{}) {
	defer r.ticked.Done()
	t := time.NewTicker(max(time.Millisecond, r.opt.Interval/8))
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		r.mu.Lock()
		if !r.closed && r.cur != nil && !r.cur.pinned && time.Since(r.cur.opened) >= r.opt.Interval && r.firstErr() == nil {
			r.timeCuts.Add(1)
			_ = r.cut(r.tickCtx, false) // a failure is the encoder's, and reported by it
		}
		r.mu.Unlock()
	}
}

// stop ends the interval cuts (mu held: the ticker may be waiting for it,
// so it is released while the ticker exits).
func (r *Router[P]) stop() {
	if r.stopTick == nil {
		return
	}
	close(r.stopTick)
	r.tickStop()
	r.stopTick = nil
	r.mu.Unlock()
	r.ticked.Wait()
	r.mu.Lock()
}

// Admit raises the encoders to MaxEncoders, once, and returns how many are
// admitted. Safe at any time, from any goroutine.
func (r *Router[P]) Admit() int {
	if n := r.reserved.Swap(0); n > 0 {
		for range n {
			<-r.slots // a slot token, held since NewRouter: never blocks
		}
		r.encoders.Add(n)
		r.codec.setLimit(r.Encoders())
	}
	return r.Encoders()
}

// Encoders is the encoders admitted now.
func (r *Router[P]) Encoders() int { return int(r.encoders.Load()) }

// CodecStates is the zstd states the router's codec has made so far: at
// most the encoders admitted (tests: every admitted state warmed).
func (r *Router[P]) CodecStates() int { return r.codec.states() }

// RowCuts is the number of parts MaxRows closed (before their size target).
func (r *Router[P]) RowCuts() int64 { return r.rowCuts.Load() }

// TimeCuts is the number of parts the interval closed.
func (r *Router[P]) TimeCuts() int64 { return r.timeCuts.Load() }

// EncoderWait is the time Write waited for an encoder: a full part queue,
// or every encoder busy when a part was opened.
func (r *Router[P]) EncoderWait() time.Duration { return time.Duration(r.waitNs.Load()) }

// Write routes ascending segments into parts. Each part receives the
// segments of one Write call as a single batch.
func (r *Router[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.segmented {
		return errors.New("parquet router: written after its segments began")
	}
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
			r.cur.n++
			r.cur.logical += r.schema.LogicalBytes(&seg[i])
			if r.cur.pinned {
				if r.cur.n >= r.opt.Cut.Rows {
					// The claimed boundary, whatever cut it. Whether
					// it reached the size target is recomputed from the
					// same rows: the cold run's part was cut by size
					// exactly when it was, and so calibrates here too,
					// before its successor is routed.
					r.full = true
					r.cur.bySize = r.cur.logical >= r.target()
				}
				continue
			}
			if r.cur.logical >= r.target() {
				r.full = true // cut before the next row
				r.cur.bySize = true
			} else if r.opt.MaxRows > 0 && r.cur.n >= r.opt.MaxRows {
				r.full = true
				r.rowCuts.Add(1)
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
	// tokens: the batch holds one of its part's tokens (Segment.Write on
	// the continued part), returned once it is written or dropped.
	tokens chan struct{}
}

func (r *Router[P]) send(ctx context.Context, segs [][]schema.Row[P]) error {
	return r.sendTo(ctx, r.cur, segs, nil)
}

// sendTo queues segs to part p's encoder (the open part, or a segment's).
func (r *Router[P]) sendTo(ctx context.Context, p *part[P], segs [][]schema.Row[P], tokens chan struct{}) error {
	if len(segs) == 0 {
		return nil
	}
	b := batch[P]{segs: segs, tokens: tokens}
	if r.opt.Account != nil {
		for _, s := range segs {
			b.bytes += r.schema.RowsBytes(s)
		}
		r.opt.Account(b.bytes)
	}
	select {
	case p.segs <- b:
		return nil
	default: // the part's queue is full: the encoder is the bottleneck
	}
	t := time.Now()
	defer func() { r.waitNs.Add(int64(time.Since(t))) }()
	select {
	case p.segs <- b:
		return nil
	case <-p.done: // the encoder stopped early
		r.done1(b)
		return p.err
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
	if b.tokens != nil {
		<-b.tokens
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
	if o := len(r.done) + len(r.parts); o >= dataset.MaxParts {
		// Every earlier part is published (or failed) before the refusal,
		// so the durable prefix ends at the last ordinal a dataset holds.
		if n := len(r.parts); n > 0 {
			<-r.parts[n-1].published
		}
		<-r.slots
		return fmt.Errorf("%w: part %d cannot be opened (%d rows so far; part size %d bytes, row cap %d)",
			dataset.ErrPartLimit, o, r.rows, r.opt.PartSize, r.opt.MaxRows)
	}
	p := &part[P]{ordinal: len(r.done) + len(r.parts), opened: time.Now(), segs: make(chan batch[P], partQueue), done: make(chan struct{}), drained: make(chan struct{}),
		published: make(chan struct{}), limit: r.limit}
	p.pinned = r.opt.Cut != nil && r.opt.Cut.Ordinal == p.ordinal
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
	columns := func() int { return max(1, int(r.encoders.Load())/max(1, int(r.active.Load()))) }
	var err error
	if r.opt.Verify != nil {
		err = r.opt.Verify()
	}
	if err == nil {
		err = faults.At("part-create") // tests: the output replaced after the check
	}
	s, createdBy := r.schema, ""
	if p.pinned {
		s, createdBy = claimSchema(r.schema, r.opt.Cut.Columns), r.opt.Cut.CreatedBy
	}
	var w *writer[P]
	if err == nil {
		w, err = create(r.opt.Open, dataset.TempPath(r.opt.Out, p.ordinal), s, r.codec, columns, createdBy)
	}
	if err != nil {
		p.err = err
		r.fail(p.err)
		for b := range p.segs { // drain so the router never blocks
			r.done1(b)
		}
		close(p.drained)
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
			if r.opt.Encoded != nil {
				r.opt.Encoded.Add(int64(len(seg)))
			}
		}
		r.done1(b)
	}
	close(p.drained)
	if p.err != nil || r.aborted.Load() {
		w.abort()
		return
	}
	p.info, p.err = w.close()
	if p.err != nil {
		r.fail(p.err)
		return
	}
	if p.limit == 0 && !p.final && p.bySize {
		// The calibrating part: the first closed by size. A part the
		// interval closed is smaller, its encoded size more footer, and
		// its ratio would freeze too small a limit for the whole run.
		ratio := float64(p.info.Bytes) / float64(max(1, p.logical))
		p.limit = max(1, int64(float64(r.opt.PartSize)/ratio))
	}
	p.rows = p.info.Rows
	base := int64(0)
	var before *dataset.Part // the part this one follows: published, or resumed
	if p.prev != nil {
		<-p.prev.published // publication goes in ordinal order
		if p.prev.err != nil {
			p.err = fmt.Errorf("part %d: not published after part %d failed", p.ordinal, p.prev.ordinal)
			return
		}
		base, before = p.prev.rows, &p.prev.info
	} else if len(r.done) > 0 {
		base, before = r.opt.Resume.Rows, &r.done[len(r.done)-1]
	}
	p.rows += base
	// Adjacency before publication: a part is checked against the part it
	// follows (the resumed seam included) before Publish records or renames
	// it, so overlapping parts are never journaled; Close's gate repeats
	// the check over the whole set.
	if p.err = adjacent(before, p.ordinal, p.info); p.err != nil {
		r.fail(p.err)
		return
	}
	if r.opt.Publish != nil && !r.aborted.Load() {
		if p.info, p.err = r.opt.Publish(p.ordinal, p.info, p.limit, p.rows, p.final); p.err != nil {
			r.fail(p.err)
		} else if p.segment {
			if p.err = faults.At("segment-published"); p.err != nil { // tests: killed with later segments open
				r.fail(p.err)
			}
		}
	}
}

// adjacent checks part ordinal (info) against the part before it (nil for
// the first part of a dataset): nonempty, and strictly after it.
func adjacent(before *dataset.Part, ordinal int, info dataset.Part) error {
	if info.Rows == 0 || info.MinKey > info.MaxKey {
		return &dataset.InvariantError{Reason: fmt.Sprintf("part %d: empty or inverted key range", ordinal)}
	}
	if before != nil && !(before.MaxKey < info.MinKey) {
		return &dataset.InvariantError{Reason: fmt.Sprintf("part %d overlaps part %d: %q !< %q", ordinal, ordinal-1, before.MaxKey, info.MinKey)}
	}
	return nil
}

// Routed is the rows routed into parts by this run (not the resumed ones).
func (r *Router[P]) Routed() int64 {
	if res := r.opt.Resume; res != nil {
		return r.rows + r.segRows.Load() - res.Rows
	}
	return r.rows + r.segRows.Load()
}

// target is the logical bytes that close a part by size: the frozen limit,
// or before calibration PartSize/initialRatio.
func (r *Router[P]) target() int64 {
	if r.limit != 0 {
		return r.limit
	}
	return int64(float64(r.opt.PartSize) / initialRatio)
}

// cut closes the current part. Until the ratio is frozen, the part is
// waited for: it may be the calibrating part.
func (r *Router[P]) cut(ctx context.Context, final bool) error {
	p := r.cur
	p.final = final
	p.closed = true
	close(p.segs)
	r.cur, r.full = nil, false
	if r.limit == 0 && !final {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stop()
	if r.closed {
		return nil, errors.New("router already closed")
	}
	if r.cur != nil {
		if err := r.cut(context.Background(), true); err != nil {
			r.abort()
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
	_ = r.codec.Close()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	rows := r.rows + r.segRows.Load()
	if len(out) == 0 && rows == 0 {
		return out, nil
	}
	if err := dataset.Check(out, rows); err != nil {
		return nil, fmt.Errorf("publication gate: %w", err)
	}
	return out, nil
}

// Abort stops every encoder; it is safe after Close. Unpublished files are
// removed by dataset.Cleanup.
func (r *Router[P]) Abort() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.abort()
}

func (r *Router[P]) abort() {
	r.stop()
	if r.closed {
		return
	}
	r.closed = true
	r.aborted.Store(true)
	for _, p := range r.parts { // the open part, and every segment's
		if !p.closed {
			p.closed = true
			close(p.segs)
		}
	}
	r.cur = nil
	for _, p := range r.parts {
		<-p.done
	}
	_ = r.codec.Close()
}

// Encoder is a router seen without its row type: what the pipeline does
// with it after listing.
type Encoder interface {
	Close() ([]dataset.Part, error)
	Abort()
	EncoderWait() time.Duration
	// Admit raises the encoders to Options.MaxEncoders and returns how
	// many are admitted; Encoders is that count now.
	Admit() int
	Encoders() int
	TimeCuts() int64
	RowCuts() int64
	// Routed is the rows this run routed into parts: after listing it
	// must equal the rows the order stage handed on.
	Routed() int64
}
