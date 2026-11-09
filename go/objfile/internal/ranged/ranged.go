// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package ranged is the Object the store packages share: io.ReaderAt semantics,
// the pinned version, the closed state and the context binding over one
// ranged-read function.
package ranged

import (
	"context"
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
// served, the object's total size and the body's length (each -1 when not
// given).
type Response struct {
	Body   io.ReadCloser
	Pin    Pin
	Total  int64
	Length int64
}

// Getter reads the bytes [off, off+n) of the object; n is at least 1 and
// off+n is at most the size. With a non-zero pin it reads only that version
// and fails with an error wrapping objfile.ErrObjectChanged when the version
// is gone; with a zero pin it reads the live version and fails with an error
// wrapping fs.ErrNotExist when there is none.
type Getter func(ctx context.Context, off, n int64, pin Pin) (Response, error)

// BodyRetries is how many times a read requests the rest of its range again
// after a response's body fails part way. The SDK retries the request itself;
// this covers only a body cut short, which the SDKs leave to the caller, and
// is safe because every request is pinned to one version.
const BodyRetries = 2

// New returns the Object of the object name of size bytes, read through get.
// A zero pin is taken from the first read's response.
func New(name string, size int64, pin Pin, get Getter) objfile.Object {
	return &file{name: name, size: size, pin: pin, get: get}
}

type file struct {
	name   string
	size   int64
	get    Getter
	closed atomic.Bool

	mu  sync.Mutex
	pin Pin
}

func (f *file) Size() int64 { return f.size }

// Identity is the pinned version id, or the ETag when there is none or it is
// S3's "null": S3 keeps the version id "null" when it overwrites the object
// of an unversioned or versioning-suspended key, so "null" does not tell two
// contents apart and the ETag does.
func (f *file) Identity() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v := f.pin.Version; v != "" && v != "null" {
		return v
	}
	return f.pin.ETag
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
	if err := f.check(res, n); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res.Body, nil
}

func (f *file) check(res Response, n int64) error {
	if res.Total >= 0 && res.Total != f.size {
		return fmt.Errorf("%w: size %d, opened at %d", objfile.ErrObjectChanged, res.Total, f.size)
	}
	if res.Length >= 0 && res.Length != n {
		return fmt.Errorf("the store returned %d bytes for a range of %d", res.Length, n)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pin.IsZero() {
		if res.Pin.IsZero() {
			return fmt.Errorf("the response names no version to pin")
		}
		f.pin = res.Pin
		return nil
	}
	if (res.Pin.Version != "" && res.Pin.Version != f.pin.Version) || (res.Pin.ETag != "" && res.Pin.ETag != f.pin.ETag) {
		return fmt.Errorf("%w: served %+v, pinned %+v", objfile.ErrObjectChanged, res.Pin, f.pin)
	}
	return nil
}

func (f *file) fail(err error) error {
	return &fs.PathError{Op: "read", Path: f.name, Err: err}
}

// Total returns the total size a Content-Range header value states, or -1.
func Total(contentRange string) int64 {
	_, total, ok := strings.Cut(contentRange, "/")
	if !ok {
		return -1
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1
	}
	return n
}
