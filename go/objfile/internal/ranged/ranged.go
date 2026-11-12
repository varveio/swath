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
	"math"
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
	return newFile(name, size, pin, get, nil)
}

func newFile(name string, size int64, pin Pin, get Getter, stat Stat) *file {
	return &file{name: name, size: size, pin: pin, get: get, stat: stat, establish: make(chan struct{}, 1)}
}

// NewSized returns the Object of the object name, expected to be size bytes,
// read through get, as objfile.Opener's OpenSized says: the first response
// that passes its checks pins the Object, once it states a total size equal
// to size, or stat, called when it states none, gives size and the same
// version.
func NewSized(name string, size int64, get Getter, stat Stat) objfile.Object {
	if size < 0 {
		panic("ranged: NewSized of a negative size; CheckSize first")
	}
	return newFile(name, size, Pin{}, get, stat)
}

// CheckSize is OpenSized's check of the size it is given: a negative size
// is invalid.
func CheckSize(name string, size int64) error {
	if size < 0 {
		return &fs.PathError{Op: "open", Path: name, Err: fmt.Errorf("%w: negative size %d", fs.ErrInvalid, size)}
	}
	return nil
}

type file struct {
	name   string
	size   int64
	get    Getter
	stat   Stat
	closed atomic.Bool

	// establish is a lock (a token in a channel of one) held while an
	// unpinned file is pinned, so that of concurrent first responses one
	// pins and the rest are checked against it; a waiter for it gives up
	// when its own context ends.
	establish chan struct{}

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

// open requests a range and checks the response against the pin. An
// unpinned file is pinned first (establish): its requests go one at a time,
// under the establish lock, until one has pinned it.
func (f *file) open(ctx context.Context, off, n int64) (io.ReadCloser, error) {
	f.mu.Lock()
	pin := f.pin
	f.mu.Unlock()
	if pin.IsZero() {
		body, pinned, err := f.establish1(ctx, off, n)
		if !pinned {
			return body, err
		}
		f.mu.Lock()
		pin = f.pin
		f.mu.Unlock()
	}
	res, err := f.get(ctx, off, n, pin)
	if err != nil {
		if st := (*StatedTotal)(nil); errors.As(err, &st) && st.Total != f.size {
			return nil, fmt.Errorf("%w: size %d, opened at %d: %w", objfile.ErrObjectChanged, st.Total, f.size, err)
		}
		return nil, err
	}
	if err := f.check(res, off, n, pin); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res.Body, nil
}

// check checks a response to a request of the pinned file: a stated total
// of another size, or another version, is ErrObjectChanged.
func (f *file) check(res Response, off, n int64, pin Pin) error {
	if res.Total >= 0 && res.Total != f.size {
		return fmt.Errorf("%w: size %d, opened at %d", objfile.ErrObjectChanged, res.Total, f.size)
	}
	if err := rangeServed(res, off, n); err != nil {
		return err
	}
	return matches(res.Pin, pin)
}

// rangeServed checks that a response is the range [off, off+n), as far as
// it states its start and length.
func rangeServed(res Response, off, n int64) error {
	if start := max(res.Start, 0); start != off {
		return fmt.Errorf("the store served bytes from %d for a range from %d", start, off)
	}
	if res.Length >= 0 && res.Length != n {
		return fmt.Errorf("the store returned %d bytes for a range of %d", res.Length, n)
	}
	return nil
}

// mismatch is the error of a first piece of evidence that the object is
// actual bytes, not the size the file was given.
func (f *file) mismatch(actual int64) error {
	return &objfile.SizeMismatchError{Name: f.name, Expected: f.size, Actual: actual}
}

// lock takes the establish lock, or fails when ctx ends first.
func (f *file) lock(ctx context.Context) error {
	select {
	case f.establish <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *file) unlock() { <-f.establish }

// establish1 makes the request for [off, off+n) of an unpinned file while
// holding the establish lock, so that no other unpinned request, nor a
// response waiting for the lock, holds a connection meanwhile: the
// metadata request it may need can always be sent. It returns the checked
// body of a response that pinned the file, or the error that left it
// unpinned; pinned without a body says that the file is pinned now (by
// another read, or by a metadata request after a response that stated no
// total) and the range is to be requested of that pin.
func (f *file) establish1(ctx context.Context, off, n int64) (body io.ReadCloser, pinned bool, err error) {
	if err := f.lock(ctx); err != nil {
		return nil, false, err
	}
	defer f.unlock()
	f.mu.Lock()
	already := !f.pin.IsZero()
	f.mu.Unlock()
	if already {
		return nil, true, nil
	}
	res, err := f.get(ctx, off, n, Pin{})
	if err != nil {
		var st *StatedTotal
		switch {
		case errors.As(err, &st) && st.Total != f.size:
			// The transport refused a range clipped by a shorter object;
			// its stated total is the evidence.
			return nil, false, f.mismatch(st.Total)
		case errors.Is(err, ErrUnsatisfiable) && f.stat != nil:
			// A range the store cannot serve: the size given may be
			// wrong (the object is shorter), which a metadata request
			// tells.
			if size, _, serr := f.stat(ctx); serr != nil {
				return nil, false, serr
			} else if size != f.size {
				return nil, false, f.mismatch(size)
			}
		}
		return nil, false, err
	}
	if res.Total >= 0 && res.Total != f.size {
		res.Body.Close()
		return nil, false, f.mismatch(res.Total)
	}
	if err := rangeServed(res, off, n); err != nil {
		res.Body.Close()
		return nil, false, err
	}
	if res.Pin.IsZero() {
		res.Body.Close()
		return nil, false, errors.New("the response names no version to pin")
	}
	if res.Total >= 0 {
		f.mu.Lock()
		f.pin = res.Pin
		f.mu.Unlock()
		return res.Body, false, nil
	}
	// The size was given, not learnt: a response that does not state the
	// total cannot prove it. It is let go (it holds a connection the
	// metadata request may need), the store is asked, and the range is
	// requested again of the version proved.
	res.Body.Close()
	if f.stat == nil {
		return nil, false, fmt.Errorf("the response states no total size to check the size %d against", f.size)
	}
	size, live, err := f.stat(ctx)
	switch {
	case err != nil:
		return nil, false, err
	case size != f.size:
		return nil, false, f.mismatch(size)
	}
	if err := matches(res.Pin, live); err != nil {
		return nil, false, err
	}
	f.mu.Lock()
	f.pin = live
	f.mu.Unlock()
	return nil, true, nil
}

// StatedTotal is a transport's error for a response it refused that states
// the object's total size: a range clipped by a shorter object. The shared
// reader classifies it (a *SizeMismatchError before a pin).
type StatedTotal struct {
	Total int64
	Err   error
}

func (e *StatedTotal) Error() string { return e.Err.Error() }
func (e *StatedTotal) Unwrap() error { return e.Err }

// ErrUnsatisfiable is a Getter's error (wrapped) for a range the store
// answered 416 Range Not Satisfiable.
var ErrUnsatisfiable = errors.New("the store cannot serve the range (416)")

// matches fails with ErrObjectChanged when served names another version
// than pinned.
func matches(served, pinned Pin) error {
	if (served.Version != "" && served.Version != pinned.Version) || (served.ETag != "" && served.ETag != pinned.ETag) {
		return fmt.Errorf("%w: served %+v, pinned %+v", objfile.ErrObjectChanged, served, pinned)
	}
	return nil
}

// wholeRead is the context key of a whole-object request's bodyFailure.
type wholeRead struct{}

// bodyFailure is where Transport keeps the failure of a whole-object
// response's body, which it ends instead (so that the SDK does not request
// the rest itself): ReadObject sees it, and does not take the body's end
// for the object's.
type bodyFailure struct {
	mu  sync.Mutex
	err error
}

func (b *bodyFailure) set(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err == nil {
		b.err = err
	}
}

func (b *bodyFailure) get() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// wholeReadContext marks ctx as the context of a request for a whole
// object, with the bodyFailure Transport records into.
func wholeReadContext(ctx context.Context) (context.Context, *bodyFailure) {
	b := &bodyFailure{}
	return context.WithValue(ctx, wholeRead{}, b), b
}

func wholeReadOf(ctx context.Context) *bodyFailure {
	b, _ := ctx.Value(wholeRead{}).(*bodyFailure)
	return b
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
	wctx, failure := wholeReadContext(ctx)
	res, err := get(wctx, 0, -1, Pin{})
	if err != nil {
		return nil, "", fail(err)
	}
	body := res.Body
	defer func() {
		if body != nil {
			body.Close()
		}
	}()
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
		probe := limit
		if probe < math.MaxInt64 {
			probe++ // one byte past the limit shows an object over it
		}
		b, err := io.ReadAll(io.LimitReader(res.Body, probe))
		if err == nil {
			err = failure.get() // a body Transport ended where it failed
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return nil, "", fail(err)
		}
		if int64(len(b)) > limit {
			return nil, id, fail(fmt.Errorf("%w: more than %d bytes", objfile.ErrTooLarge, limit))
		}
		return b, id, nil
	}
	b := make([]byte, size)
	m, err := readToEnd(res.Body, b)
	if err == nil && m == len(b) {
		// Transport ends a whole read's failed body as if it ended (so
		// the SDK does not resume it): a failure it kept, or the context's,
		// is not a clean end.
		if err = failure.get(); err == nil {
			err = ctx.Err()
		}
	}
	if err != nil && m == len(b) {
		// Every byte arrived, but the body did not end cleanly: an error
		// with its last bytes or at its end (the GCS reader's CRC32C check
		// fails only then), or bytes past the length stated. The bytes
		// are not trusted, and not read again: a ranged read checks no
		// checksum.
		return nil, "", fail(err)
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, "", fail(ctxErr)
		}
		// The rest is a ranged read of the version served, sent once this
		// response has let go of its connection.
		body.Close()
		body = nil
		if n, err := newFile(name, size, res.Pin, get, nil).readAt(ctx, b[m:], int64(m)); err != nil || n != len(b)-m {
			if err == nil {
				err = fail(io.ErrUnexpectedEOF)
			}
			return nil, "", err
		}
	}
	return b, id, nil
}

// readToEnd reads len(b) bytes of body into b and then reads on to the
// body's end: it returns how many bytes it read into b and the first error
// the body gave, kept even when it came with the last bytes (io.ReadFull
// drops one then), or io.EOF before len(b) bytes; a byte past len(b) is an
// error, and a clean end after len(b) bytes is nil. A body that gives no
// byte and no error a hundred times in a row fails with io.ErrNoProgress.
func readToEnd(body io.Reader, b []byte) (int, error) {
	m, idle := 0, 0
	var one [1]byte
	for {
		p := b[m:]
		if len(p) == 0 {
			p = one[:] // past the length: only the end is expected
		}
		k, err := body.Read(p)
		switch {
		case len(b[m:]) == 0 && k > 0:
			return m, fmt.Errorf("the store returned more than the %d bytes stated", len(b))
		case k > 0:
			m += k
			idle = 0
		case err == nil:
			if idle++; idle == 100 {
				return m, io.ErrNoProgress
			}
		}
		if err == io.EOF {
			if m < len(b) {
				return m, io.ErrUnexpectedEOF
			}
			return m, nil
		}
		if err != nil {
			return m, err
		}
	}
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
