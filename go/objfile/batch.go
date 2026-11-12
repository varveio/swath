// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
)

// Range is one destination of a batched read: Buf is filled with the bytes
// of the object from Off.
type Range struct {
	Off int64
	Buf []byte
}

// RangeReader is an Object that reads a batch of ranges together. Every
// Object the store packages open is one, as are the Objects of Limit, Batch
// and Pinned over them; ReadRanges reads any Object.
type RangeReader interface {
	// ReadRanges fills every range's Buf with the object's bytes from its
	// Off. The ranges may come in any order and may overlap or touch in
	// the object; their Bufs must not share memory, with each other or with
	// another call's (this is not checked), and the caller must not use them
	// until ReadRanges returns. Each range must lie within the object
	// (Off >= 0, Off+len(Buf) <= Size; an empty range at Size is valid and
	// needs no request), and a batch is at most MaxBatchRanges ranges of at
	// most MaxBatchBytes together: a batch that is not fails whole, with
	// an error wrapping fs.ErrInvalid, before any request, as does one whose
	// ctx is already done, with its error. Nothing of the call goes on after it returns.
	//
	// It returns nil when every range was filled. Otherwise a *BatchError
	// names the error of each range that failed, and every range it gives
	// no error was filled; any other error failed every range. A failed
	// range's Buf holds unspecified bytes. When ctx is cancelled, the
	// requests in flight are cancelled and ReadRanges returns once they
	// have stopped.
	ReadRanges(ctx context.Context, ranges []Range) error
}

// BatchError is the error of a batch in which some ranges failed: Errs[i]
// is the error of the batch's ranges[i], nil for a range that was filled.
// errors.Is and errors.As see each range's error.
type BatchError struct {
	Errs []error
}

func (e *BatchError) Error() string {
	failed, first := 0, -1
	for i, err := range e.Errs {
		if err != nil {
			failed++
			if first < 0 {
				first = i
			}
		}
	}
	if first < 0 {
		return "objfile: batch: no range failed"
	}
	return fmt.Sprintf("objfile: %d of %d ranges failed; range %d: %v", failed, len(e.Errs), first, e.Errs[first])
}

// Unwrap returns the errors of the failed ranges, one for each (an error a
// request gave appears once for each range it failed).
func (e *BatchError) Unwrap() []error {
	var out []error
	for _, err := range e.Errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// BatchPolicy is how a batch's ranges become requests. The ranges' bytes
// are merged where they overlap or touch, and a gap between them is read
// and discarded, joining two requests into one, when it is at most MaxGap
// bytes, the joined request is at most MaxRequest bytes, and the request's
// discarded bytes stay at most MaxOverRead times its requested bytes. A
// run of requested bytes longer than MaxRequest is split into requests of
// near-equal length. The plan is greedy, in object order: runs are split
// first, and each piece joins the request before it when the bounds allow,
// which need not give the fewest requests. At most MaxConcurrent requests of
// a batch are in flight at once, holding at most MaxInFlightBytes between
// them, strictly: no request is larger than MaxInFlightBytes either.
//
// A field left 0 takes DefaultBatchPolicy's value, so the zero BatchPolicy
// is DefaultBatchPolicy(). A negative field, or a MaxOverRead that is not a
// number, is invalid (Validate).
type BatchPolicy struct {
	MaxGap           int64
	MaxRequest       int64
	MaxOverRead      float64
	MaxConcurrent    int
	MaxInFlightBytes int64
}

// DefaultBatchPolicy is the policy of an Object's ReadRanges unless Batch
// sets another: gaps of up to 64 KiB bridged while at most a quarter of a
// request is discarded, requests of up to 4 MiB, 16 requests and 64 MiB in
// flight a batch. A ranged GET costs about one round trip whether it
// returns 4 KiB or 4 MiB, so bytes up to that size are nearly free and a
// round trip saved is worth a bounded over-read.
func DefaultBatchPolicy() BatchPolicy {
	return BatchPolicy{
		MaxGap:           64 << 10,
		MaxRequest:       4 << 20,
		MaxOverRead:      0.25,
		MaxConcurrent:    16,
		MaxInFlightBytes: 64 << 20,
	}
}

// The bounds of one batch (one ReadRanges call), checked before any
// allocation or request: at most MaxBatchRanges ranges of at most
// MaxBatchBytes bytes together. A larger batch fails whole with an error
// wrapping fs.ErrInvalid.
const (
	MaxBatchRanges       = 4096
	MaxBatchBytes  int64 = 128 << 20
)

// maxBatchPieces bounds the bookkeeping of one batch: the pieces its ranges
// make when cut at the request size, summed over the ranges, so that a
// policy of small requests over many overlapping ranges cannot make it
// quadratic. A batch over it fails with an error wrapping fs.ErrInvalid.
const maxBatchPieces = 1 << 20

// Validate reports whether p is a usable policy: no field negative and
// MaxOverRead a number. A zero field is valid (it takes the default).
func (p BatchPolicy) Validate() error {
	if p.MaxGap < 0 || p.MaxRequest < 0 || p.MaxConcurrent < 0 || p.MaxInFlightBytes < 0 || !(p.MaxOverRead >= 0) {
		return fmt.Errorf("objfile: batch policy %+v: %w: a bound is negative or not a number", p, fs.ErrInvalid)
	}
	return nil
}

// normal is p with DefaultBatchPolicy's value for each field left 0, and
// MaxRequest no larger than MaxInFlightBytes, so that every request fits
// the in-flight allowance. p is valid.
func (p BatchPolicy) normal() BatchPolicy {
	d := DefaultBatchPolicy()
	if p.MaxGap == 0 {
		p.MaxGap = d.MaxGap
	}
	if p.MaxOverRead == 0 {
		p.MaxOverRead = d.MaxOverRead
	}
	if p.MaxRequest == 0 {
		p.MaxRequest = d.MaxRequest
	}
	if p.MaxConcurrent == 0 {
		p.MaxConcurrent = d.MaxConcurrent
	}
	if p.MaxInFlightBytes == 0 {
		p.MaxInFlightBytes = d.MaxInFlightBytes
	}
	p.MaxRequest = min(p.MaxRequest, p.MaxInFlightBytes)
	return p
}

// ReadRanges reads ranges of f as RangeReader says: by f's own ReadRanges
// when it has one, else as ReadRangesWith does under DefaultBatchPolicy.
func ReadRanges(ctx context.Context, f Object, ranges []Range) error {
	if rr, ok := f.(RangeReader); ok {
		return rr.ReadRanges(ctx, ranges)
	}
	return ReadRangesWith(ctx, f, ranges, DefaultBatchPolicy())
}

// ReadRangesWith reads ranges of f as RangeReader says, under p: each
// request is one ReadAt of f bounded by ctx, so the requests of an Object
// of Limit each hold a slot. An invalid p, a batch over MaxBatchRanges or
// MaxBatchBytes, and a range outside the object fail the whole batch, with
// an error wrapping fs.ErrInvalid, before any request.
//
// While f has no Identity (an OpenSized Object before a read pinned it),
// the batch's requests are sent one at a time until one has pinned it (or
// one succeeds, for an Object without identities); when one ends with an
// error wrapping ErrObjectChanged, fs.ErrNotExist or a *SizeMismatchError
// before that, the ranges not yet read fail with that error, unsent.
func ReadRangesWith(ctx context.Context, f Object, ranges []Range, p BatchPolicy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if len(ranges) > MaxBatchRanges {
		return fmt.Errorf("objfile: a batch of %d ranges, at most %d: %w", len(ranges), MaxBatchRanges, fs.ErrInvalid)
	}
	p = p.normal()
	size := f.Size()
	var total, pieces int64
	for i, r := range ranges {
		n := int64(len(r.Buf))
		if r.Off < 0 || r.Off > size || n > size-r.Off {
			return fmt.Errorf("objfile: range %d (%d bytes at %d) is not within the object's %d bytes: %w",
				i, n, r.Off, size, fs.ErrInvalid)
		}
		if total += n; total > MaxBatchBytes { // no overflow: each step adds at most MaxBatchBytes+1
			return fmt.Errorf("objfile: a batch of more than %d bytes: %w", MaxBatchBytes, fs.ErrInvalid)
		}
		if pieces += n/p.MaxRequest + 2; pieces > maxBatchPieces {
			return fmt.Errorf("objfile: a batch of more than %d pieces of at most %d bytes: %w", maxBatchPieces, p.MaxRequest, fs.ErrInvalid)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b := plan(ranges, p)
	if len(b.spans) == 0 {
		return nil
	}
	b.run(ctx, f, p)
	errs := b.rangeErrors(len(ranges))
	if errs == nil {
		return nil
	}
	return &BatchError{Errs: errs}
}

// batch is a planned batch: the requests (spans) that cover its ranges'
// bytes, in object order and disjoint, and the pieces of the ranges each
// fills.
type batch struct {
	ranges []Range
	spans  []span
}

// span is one request: the bytes [off, end), read into the Buf of ranges
// direct when that range is exactly the span, else into a buffer of its
// own; its pieces are copied out to the ranges it covers.
type span struct {
	off, end int64
	direct   int // the index of the range read in place, or -1
	covers   []int
	err      error
}

// plan merges and splits ranges into spans under p.
func plan(ranges []Range, p BatchPolicy) *batch {
	b := &batch{ranges: ranges}
	order := make([]int, 0, len(ranges))
	for i, r := range ranges {
		if len(r.Buf) > 0 {
			order = append(order, i)
		}
	}
	if len(order) == 0 {
		return b
	}
	slices.SortFunc(order, func(i, j int) int { return cmp.Compare(ranges[i].Off, ranges[j].Off) })

	// The runs of requested bytes: ranges joined where they overlap or touch.
	type run struct{ off, end int64 }
	var runs []run
	for _, i := range order {
		off, end := ranges[i].Off, ranges[i].Off+int64(len(ranges[i].Buf))
		if n := len(runs); n > 0 && off <= runs[n-1].end {
			runs[n-1].end = max(runs[n-1].end, end)
			continue
		}
		runs = append(runs, run{off, end})
	}

	// Each run cut into near-equal pieces of at most MaxRequest bytes, and
	// a piece joined to the span before it across a gap the policy allows.
	var useful int64 // requested bytes of the last span
	for _, r := range runs {
		// parts pieces of n/parts bytes, the first n%parts one byte more,
		// computed without overflow whatever MaxRequest is.
		n := r.end - r.off
		parts := n / p.MaxRequest
		if n%p.MaxRequest != 0 {
			parts++
		}
		piece, longer := n/parts, n%parts
		end := r.off
		for k := range parts {
			off := end
			end = off + piece
			if k < longer {
				end++
			}
			if last := len(b.spans) - 1; last >= 0 {
				s := &b.spans[last]
				gap := off - s.end
				joined := end - s.off
				if gap > 0 && gap <= p.MaxGap && joined <= p.MaxRequest &&
					float64(joined-useful-(end-off)) <= p.MaxOverRead*float64(useful+end-off) {
					s.end = end
					useful += end - off
					continue
				}
			}
			b.spans = append(b.spans, span{off: off, end: end, direct: -1})
			useful = end - off
		}
	}

	// The ranges each span covers; a range exactly a span's is read in place.
	for _, i := range order {
		off, end := ranges[i].Off, ranges[i].Off+int64(len(ranges[i].Buf))
		k, _ := slices.BinarySearchFunc(b.spans, off, func(s span, off int64) int {
			if s.end <= off {
				return -1
			}
			return 1
		})
		for ; k < len(b.spans) && b.spans[k].off < end; k++ {
			s := &b.spans[k]
			s.covers = append(s.covers, i)
			if s.direct < 0 && s.off == off && s.end == end {
				s.direct = i
			}
		}
	}
	return b
}

// run sends the spans' requests under p, as ReadRangesWith says, and
// returns once every request it sent has ended. A span not sent fails with
// the error that stopped the batch: the context's, or the first request's
// terminal error.
func (b *batch) run(ctx context.Context, f Object, p BatchPolicy) {
	type result struct {
		k   int
		err error
	}
	done := make(chan result, min(p.MaxConcurrent, len(b.spans)))
	r := f.WithContext(ctx)
	var (
		next, inFlight int
		inBytes        int64
		stop           error // set: send nothing more; the unsent spans fail with it
		cancelled      = ctx.Done()
		alone          = f.Identity() == "" // one at a time until the Object is pinned
	)
	for next < len(b.spans) || inFlight > 0 {
		if stop == nil && ctx.Err() != nil {
			stop = context.Cause(ctx) // seen with a completion: send nothing more
		}
		if next < len(b.spans) && stop == nil {
			n := b.spans[next].end - b.spans[next].off
			if (inFlight == 0 || !alone && inFlight < p.MaxConcurrent) && inBytes+n <= p.MaxInFlightBytes {
				k := next
				next++
				inFlight++
				inBytes += n
				go func() { done <- result{k, b.fetch(r, k)} }()
				continue
			}
		}
		if inFlight == 0 {
			break
		}
		select {
		case res := <-done:
			b.spans[res.k].err = res.err
			inFlight--
			inBytes -= b.spans[res.k].end - b.spans[res.k].off
			if alone {
				switch {
				case res.err == nil || f.Identity() != "":
					alone = false // pinned (or never will be): the rest at once
				case terminal(res.err):
					stop = res.err // the object is not the one expected: no more requests
				}
			}
		case <-cancelled:
			// Send nothing more; wait for the requests in flight.
			cancelled = nil
			if stop == nil {
				stop = context.Cause(ctx)
			}
		}
	}
	for ; next < len(b.spans); next++ {
		b.spans[next].err = stop
	}
}

// terminal reports whether err says the object is not the one expected
// (changed, missing, or of another size), so that no other request of the
// batch can succeed.
func terminal(err error) bool {
	var sm *SizeMismatchError
	return errors.Is(err, ErrObjectChanged) || errors.Is(err, fs.ErrNotExist) || errors.As(err, &sm)
}

// fetch reads span k with one ReadAt and copies it out to the ranges it
// covers.
func (b *batch) fetch(r io.ReaderAt, k int) error {
	s := &b.spans[k]
	var buf []byte
	if s.direct >= 0 {
		buf = b.ranges[s.direct].Buf
	} else {
		buf = make([]byte, s.end-s.off)
	}
	n, err := r.ReadAt(buf, s.off)
	if n == len(buf) && err == io.EOF {
		err = nil
	}
	if err == nil && n < len(buf) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return err
	}
	for _, i := range s.covers {
		if i == s.direct {
			continue
		}
		rg := b.ranges[i]
		lo, hi := max(s.off, rg.Off), min(s.end, rg.Off+int64(len(rg.Buf)))
		copy(rg.Buf[lo-rg.Off:hi-rg.Off], buf[lo-s.off:hi-s.off])
	}
	return nil
}

// rangeErrors returns each range's error, the first of the spans covering
// it, or nil when no range failed.
func (b *batch) rangeErrors(n int) []error {
	var errs []error
	for _, s := range b.spans {
		if s.err == nil {
			continue
		}
		if errs == nil {
			errs = make([]error, n)
		}
		for _, i := range s.covers {
			if errs[i] == nil {
				errs[i] = s.err
			}
		}
	}
	return errs
}

// Batch makes the ReadRanges of the Objects o opens follow p, whether
// they come from o directly or through Limit, Root or Pin around it. An
// invalid p (Validate) is refused.
func Batch(o Opener, p BatchPolicy) (Opener, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return batched{o: o, p: p}, nil
}

type batched struct {
	o Opener
	p BatchPolicy
}

func (b batched) Open(ctx context.Context, name string) (Object, error) {
	f, err := b.o.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return batchedObject{Object: f, p: b.p}, nil
}

func (b batched) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	f, err := b.o.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return batchedObject{Object: f, p: b.p}, nil
}

func (b batched) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return ReadObject(ctx, b.o, name, limit)
}

type batchedObject struct {
	Object
	p BatchPolicy
}

func (f batchedObject) ReadRanges(ctx context.Context, ranges []Range) error {
	return ReadRangesWith(ctx, f.Object, ranges, f.p)
}

func (f batchedObject) batchPolicy() BatchPolicy { return f.p }

// policyOf returns the BatchPolicy f's ReadRanges follows: the one Batch
// set beneath f, or DefaultBatchPolicy.
func policyOf(f Object) BatchPolicy {
	if p, ok := f.(interface{ batchPolicy() BatchPolicy }); ok {
		return p.batchPolicy()
	}
	return DefaultBatchPolicy()
}
