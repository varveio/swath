// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package ranged is the Object the store packages share: io.ReaderAt semantics,
// the pinned version, the closed state and the context binding over one
// ranged-read function.
package ranged

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/go/objfile"
)

// Pin names one stored version of an object: Version is the store's version
// id or generation, ETag its entity tag; either may be empty.
type Pin struct {
	Version string
	ETag    string
}

// IsZero reports whether p names no version.
func (p Pin) IsZero() bool { return p == Pin{} }

// Response is a ranged read's body and what its headers said: the version
// served, the offset of the body's first byte, the object's total size and
// the body's length (each -1 when not given). A response that states no
// start serves the object from its first byte.
type Response struct {
	Body   io.ReadCloser
	Pin    Pin
	Start  int64
	Total  int64
	Length int64
}

// Getter reads the bytes [off, off+n) of the object; n is at least 1 and
// off+n is at most the size, or n is -1 and off 0 for the whole object,
// requested without a range. With a non-zero pin it reads only that version
// and fails with an error wrapping objfile.ErrObjectChanged when the version
// is gone; with a zero pin it reads the live version and fails with an error
// wrapping fs.ErrNotExist when there is none.
type Getter func(ctx context.Context, off, n int64, pin Pin) (Response, error)

// Stat is the store's metadata request for the live version of an object:
// its size and pin. It fails with an error wrapping fs.ErrNotExist when
// there is none.
type Stat func(ctx context.Context) (size int64, pin Pin, err error)

// BodyRetries is how many times a read requests the rest of its range again
// after a response's body fails part way. The SDK retries the request itself;
// this covers only a body cut short, which the SDKs leave to the caller, and
// is safe because every request is pinned to one version.
const BodyRetries = 2

// New returns the Object of the object name of size bytes, read through get,
// pinned to pin (from a metadata request that also gave size).
func New(name string, size int64, pin Pin, get Getter) objfile.Object {
	return &file{name: name, size: size, pin: pin, get: get}
}

// NewSized returns the Object of the object name, expected to be size bytes,
// read through get, as objfile.Opener's OpenSized says: the first response
// that passes its checks pins the Object, once it states a total size equal
// to size, or stat, called when it states none, gives size and the same
// version.
func NewSized(name string, size int64, get Getter, stat Stat) objfile.Object {
	return &file{name: name, size: size, get: get, stat: stat}
}

type file struct {
	name   string
	size   int64
	get    Getter
	stat   Stat
	closed atomic.Bool

	// establish is held by the response that pins an unpinned file, so
	// that of concurrent first responses one pins and the rest are
	// checked against it.
	establish sync.Mutex

	mu  sync.Mutex
	pin Pin
}

func (f *file) Size() int64 { return f.size }

// Identity is the pinned version's (see identity), "" before the file is
// pinned.
func (f *file) Identity() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return identity(f.pin)
}

// identity is the version id of p, or the ETag when there is none or it is
// S3's "null": S3 keeps the version id "null" when it overwrites the object
// of an unversioned or versioning-suspended key, so "null" does not tell two
// contents apart and the ETag does.
func identity(p Pin) string {
	if v := p.Version; v != "" && v != "null" {
		return v
	}
	return p.ETag
}

func (f *file) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(context.Background(), p, off)
}

func (f *file) WithContext(ctx context.Context) io.ReaderAt { return bound{f: f, ctx: ctx} }

type bound struct {
	f   *file
	ctx context.Context
}

func (b bound) ReadAt(p []byte, off int64) (int, error) { return b.f.readAt(b.ctx, p, off) }

// ReadRanges reads ranges as objfile.ReadRangesWith does under
// objfile.DefaultBatchPolicy: one ReadAt, with its body retries, a request.
func (f *file) ReadRanges(ctx context.Context, ranges []objfile.Range) error {
	return objfile.ReadRangesWith(ctx, f, ranges, objfile.DefaultBatchPolicy())
}

func (f *file) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, f.fail(fs.ErrClosed)
	}
	if off < 0 {
		return 0, f.fail(fmt.Errorf("%w: negative offset", fs.ErrInvalid))
	}
	if off >= f.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), f.size-off))
	got, retries := 0, 0
	for got < n {
		if err := ctx.Err(); err != nil {
			return got, f.fail(err)
		}
		body, err := f.open(ctx, off+int64(got), int64(n-got))
		if err != nil {
			return got, f.fail(err)
		}
		m, err := io.ReadFull(body, p[got:n])
		body.Close()
		got += m
		if err == nil {
			break
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return got, f.fail(ctxErr)
		}
		if retries == BodyRetries {
			return got, f.fail(err)
		}
		retries++
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// open requests a range and checks the response against the pin, taking the
// pin from it when the file has none yet, and against the size: the total
// first, so that a range clipped by a shorter object is ErrObjectChanged.
func (f *file) open(ctx context.Context, off, n int64) (io.ReadCloser, error) {
	f.mu.Lock()
	pin := f.pin
	f.mu.Unlock()
	res, err := f.get(ctx, off, n, pin)
	if err != nil {
		return nil, err
	}
	if err := f.check(ctx, res, off, n); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res.Body, nil
}

func (f *file) check(ctx context.Context, res Response, off, n int64) error {
	if res.Total >= 0 && res.Total != f.size {
		return fmt.Errorf("%w: size %d, opened at %d", objfile.ErrObjectChanged, res.Total, f.size)
	}
	if start := max(res.Start, 0); start != off {
		return fmt.Errorf("the store served bytes from %d for a range from %d", start, off)
	}
	if res.Length >= 0 && res.Length != n {
		return fmt.Errorf("the store returned %d bytes for a range of %d", res.Length, n)
	}
	f.mu.Lock()
	pin := f.pin
	f.mu.Unlock()
	if pin.IsZero() {
		return f.pinTo(ctx, res)
	}
	return matches(res.Pin, pin)
}

// pinTo pins the file to the version res was served, once res or a
// metadata request has shown that version is the file's size; when another
// response pinned the file first, res is checked against it.
func (f *file) pinTo(ctx context.Context, res Response) error {
	f.establish.Lock()
	defer f.establish.Unlock()
	f.mu.Lock()
	pin := f.pin
	f.mu.Unlock()
	if !pin.IsZero() {
		return matches(res.Pin, pin)
	}
	if res.Pin.IsZero() {
		return fmt.Errorf("the response names no version to pin")
	}
	if res.Total < 0 {
		// The size was given, not learnt: a response that does not state
		// the total cannot prove it, so the store is asked.
		if f.stat == nil {
			return fmt.Errorf("the response states no total size to check the size %d against", f.size)
		}
		size, live, err := f.stat(ctx)
		if err != nil {
			return err
		}
		if size != f.size {
			return fmt.Errorf("%w: size %d, opened at %d", objfile.ErrObjectChanged, size, f.size)
		}
		if err := matches(res.Pin, live); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.pin = res.Pin
	f.mu.Unlock()
	return nil
}

// matches fails with ErrObjectChanged when served names another version
// than pinned.
func matches(served, pinned Pin) error {
	if (served.Version != "" && served.Version != pinned.Version) || (served.ETag != "" && served.ETag != pinned.ETag) {
		return fmt.Errorf("%w: served %+v, pinned %+v", objfile.ErrObjectChanged, served, pinned)
	}
	return nil
}

// ReadObject reads the object name whole with one request through get,
// unless its body fails part way, when the rest is read as a ranged read of
// the version served; it returns the bytes and that version's Identity, as
// objfile.ObjectReader says.
func ReadObject(ctx context.Context, name string, limit int64, get Getter) ([]byte, string, error) {
	fail := func(err error) error { return &fs.PathError{Op: "read", Path: name, Err: err} }
	if err := ctx.Err(); err != nil {
		return nil, "", fail(err)
	}
	res, err := get(ctx, 0, -1, Pin{})
	if err != nil {
		return nil, "", fail(err)
	}
	defer res.Body.Close()
	if res.Pin.IsZero() {
		return nil, "", fail(errors.New("the response names no version to pin"))
	}
	if res.Start > 0 {
		return nil, "", fail(fmt.Errorf("the store served bytes from %d for the whole object", res.Start))
	}
	size := res.Length
	if res.Total >= 0 {
		if size >= 0 && size != res.Total {
			return nil, "", fail(fmt.Errorf("the store returned %d bytes of an object of %d", size, res.Total))
		}
		size = res.Total
	}
	id := identity(res.Pin)
	limit = max(limit, 0)
	if size > limit {
		return nil, id, fail(fmt.Errorf("%w: %d bytes, limit %d", objfile.ErrTooLarge, size, limit))
	}
	if size < 0 {
		// No length stated: the body, up to one byte past the limit, is
		// the object, and a failure part way cannot be resumed.
		b, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
		if err != nil {
			return nil, "", fail(err)
		}
		if int64(len(b)) > limit {
			return nil, id, fail(fmt.Errorf("%w: more than %d bytes", objfile.ErrTooLarge, limit))
		}
		return b, id, nil
	}
	b := make([]byte, size)
	m, err := io.ReadFull(res.Body, b)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, "", fail(ctxErr)
		}
		f := &file{name: name, size: size, pin: res.Pin, get: get}
		if _, err := f.readAt(ctx, b[m:], int64(m)); err != nil {
			return nil, "", err
		}
	}
	return b, id, nil
}

func (f *file) fail(err error) error {
	return &fs.PathError{Op: "read", Path: f.name, Err: err}
}

// ContentRange parses a Content-Range header value, "bytes first-last/total".
func ContentRange(v string) (first, last, total int64, ok bool) {
	rng, ok := strings.CutPrefix(v, "bytes ")
	if !ok {
		return 0, 0, 0, false
	}
	rng, size, ok := strings.Cut(rng, "/")
	a, b, ok2 := strings.Cut(rng, "-")
	if !ok || !ok2 {
		return 0, 0, 0, false
	}
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	total, err3 := strconv.ParseInt(size, 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || first < 0 || last < first || total < 0 {
		return 0, 0, 0, false
	}
	return first, last, total, true
}

// Stated returns the first byte and the total size a Content-Range header
// value states, as a Response carries them: each -1 when v does not parse,
// and the total -1 when it is "*" (unknown).
func Stated(v string) (start, total int64) {
	if rng, ok := strings.CutSuffix(v, "/*"); ok {
		if first, _, _, ok := ContentRange(rng + "/0"); ok {
			return first, -1
		}
		return -1, -1
	}
	first, _, total, ok := ContentRange(v)
	if !ok {
		return -1, -1
	}
	return first, total
}
