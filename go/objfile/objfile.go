// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objfile provides pinned byte-range reads, create-only uploads and
// conditional deletes for objects in S3, Google Cloud Storage and Azure Blob
// Storage. It does not list objects, plan reads or cache bytes.
//
// The store packages provide an Opener and Writer over the caller's SDK client;
// objfile/remote constructs clients from store URIs. Reads check the
// pinned version or ETag and size, failing with ErrObjectChanged on a mismatch.
// An S3 ETag alone cannot distinguish separate creations with the same ETag.
// An Object supplies io.ReaderAt and a known size, as used by SeekParquet and
// parquet-go. Provider package comments describe their request and retry limits.
package objfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"strings"
)

// Object is an opened object. It is safe for concurrent use. ReadAt follows
// io.ReaderAt: a read shortened by the end of the object returns io.EOF with
// the bytes it read; a full read ending exactly there succeeds. After Close,
// reads fail with an error wrapping fs.ErrClosed.
//
// Reads validate the response's range offset, total size and length. A wrong
// range fails with an error; a changed identity or size fails with
// ErrObjectChanged. A short response body is requested again from where it
// stopped, at most twice, in addition to the SDK's request retries. The GCS
// package documents the transport required for this validation with its SDK.
type Object interface {
	io.ReaderAt
	// Size returns the object's length in bytes, as it was when opened. An
	// Object from OpenSized returns the size it was given, which its first
	// request to the store checks; Size alone proves nothing.
	Size() int64
	// Identity names the pinned version: the S3 version id, or the ETag
	// when the object has none or has the version id "null" (which S3
	// keeps across an overwrite), the GCS generation, or the Azure ETag.
	// An Object from OpenSized pins on its first read and returns "" before
	// it. An S3 ETag is not a unique creation identifier: a replacement can
	// have the same ETag. A cache can key bytes by name and Identity within
	// these provider identity limits.
	Identity() string
	Close() error
	// WithContext returns a view of the file whose reads are bounded by
	// ctx; ReadAt on the Object itself uses context.Background.
	WithContext(ctx context.Context) io.ReaderAt
}

// Opener opens objects by name, honoring ctx. A missing object fails with
// an error wrapping fs.ErrNotExist.
type Opener interface {
	// Open asks the store for the object's size and version (one metadata
	// request) and pins that version.
	Open(ctx context.Context, name string) (Object, error)
	// OpenSized opens an object whose size the caller already knows, such
	// as a part a manifest lists, without a request: the Object pins the
	// version its first request is served, once that response has stated
	// the object's total size and it is size (a response that states none
	// is followed by one metadata request for it); no byte of the object is
	// returned before. A read that finds another size fails with
	// ErrObjectChanged, and a missing object fails with an error wrapping
	// fs.ErrNotExist. An Object of size 0, or one never read, makes no
	// request and so proves nothing.
	OpenSized(ctx context.Context, name string, size int64) (Object, error)
}

// ObjectReader is an Opener that reads a small object whole in one request,
// with no metadata request before it.
type ObjectReader interface {
	// ReadObject returns the bytes of the object name and the Identity of
	// the version served. An object of more than limit bytes fails with an
	// error wrapping ErrTooLarge, still returning its Identity, without
	// reading its body; a missing one fails with an error wrapping
	// fs.ErrNotExist.
	ReadObject(ctx context.Context, name string, limit int64) (data []byte, identity string, err error)
}

// ErrTooLarge reports an object larger than ReadObject's limit.
var ErrTooLarge = errors.New("objfile: object larger than the read's limit")

// ReadObject reads the object name of o whole, as ObjectReader says: in one
// request when o is an ObjectReader, else by Open and one ReadAt.
func ReadObject(ctx context.Context, o Opener, name string, limit int64) ([]byte, string, error) {
	if r, ok := o.(ObjectReader); ok {
		return r.ReadObject(ctx, name, limit)
	}
	f, err := o.Open(ctx, name)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	if f.Size() > limit {
		return nil, f.Identity(), &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, f.Size(), limit)}
	}
	b := make([]byte, f.Size())
	if _, err := f.WithContext(ctx).ReadAt(b, 0); err != nil && err != io.EOF {
		return nil, "", err
	}
	return b, f.Identity(), nil
}

// ErrObjectChanged reports a missing or mismatched pinned identity or size.
// A read that fails with it returns no bytes from the mismatched response.
var ErrObjectChanged = errors.New("objfile: object changed since it was opened")

// CheckPath reports whether name is a path a Root may open: segments
// separated by "/", each non-empty and neither "." nor "..", without "\" or
// a control character, and no leading "/" (SeekParquet's PART-1). Paths are
// literal: nothing is decoded or normalised.
func CheckPath(name string) error {
	invalid := func(why string) error {
		return &fs.PathError{Op: "open", Path: name, Err: fmt.Errorf("%w: %s", fs.ErrInvalid, why)}
	}
	if name == "" {
		return invalid("empty path")
	}
	if strings.HasPrefix(name, "/") {
		return invalid("begins with /")
	}
	for _, r := range name {
		if r == '\\' || r < 0x20 || r == 0x7f {
			return invalid(`contains \ or a control character`)
		}
	}
	for seg := range strings.SplitSeq(name, "/") {
		switch seg {
		case "":
			return invalid("has an empty segment")
		case ".", "..":
			return invalid(fmt.Sprintf("has a %q segment", seg))
		}
	}
	return nil
}

// Root scopes o to the objects under prefix, a dataset root: Open checks name
// with CheckPath and opens prefix + "/" + name, as does OpenSized. An empty prefix is the whole
// namespace of o; a trailing "/" on prefix is not doubled.
func Root(o Opener, prefix string) Opener {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return root{o: o, prefix: prefix}
}

type root struct {
	o      Opener
	prefix string
}

func (r root) Open(ctx context.Context, name string) (Object, error) {
	if err := CheckPath(name); err != nil {
		return nil, err
	}
	return r.o.Open(ctx, r.prefix+name)
}

func (r root) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	if err := CheckPath(name); err != nil {
		return nil, err
	}
	return r.o.OpenSized(ctx, r.prefix+name, size)
}

func (r root) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	if err := CheckPath(name); err != nil {
		return nil, "", err
	}
	return ReadObject(ctx, r.o, r.prefix+name, limit)
}

// Limit bounds the requests in flight through o to n: each Open, each
// ReadObject, each ReadAt of an Object it opened and each request of such an
// Object's ReadRanges holds one of n slots for its duration, and waits for a
// slot only as long as its context allows. n below 1 is 1. A batch's
// requests are sent as ReadRangesWith sends them, under the BatchPolicy of
// the Object Limit wraps (Batch sets one), each through ReadAt.
func Limit(o Opener, n int) Opener {
	return limited{o: o, slots: make(chan struct{}, max(n, 1))}
}

type limited struct {
	o     Opener
	slots chan struct{}
}

func (l limited) acquire(ctx context.Context) error {
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l limited) release() { <-l.slots }

func (l limited) Open(ctx context.Context, name string) (Object, error) {
	if err := l.acquire(ctx); err != nil {
		return nil, err
	}
	defer l.release()
	f, err := l.o.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return limitedObject{Object: f, l: l}, nil
}

func (l limited) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	if err := l.acquire(ctx); err != nil {
		return nil, "", err
	}
	defer l.release()
	return ReadObject(ctx, l.o, name, limit)
}

func (l limited) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	f, err := l.o.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return limitedObject{Object: f, l: l}, nil
}

// KnownSizes opens the names sizes lists with the size it gives: Open of
// such a name is OpenSized, which makes no request. Other names, and
// OpenSized, ReadObject, open as o does. It is for a reader that knows its
// files' sizes, from a manifest it has read, but opens them through an
// interface that does not carry one (SeekParquet's Source.Open). sizes is
// copied.
func KnownSizes(o Opener, sizes map[string]int64) Opener {
	return knownSizes{o: o, sizes: maps.Clone(sizes)}
}

type knownSizes struct {
	o     Opener
	sizes map[string]int64
}

func (k knownSizes) Open(ctx context.Context, name string) (Object, error) {
	if size, ok := k.sizes[name]; ok {
		return k.o.OpenSized(ctx, name, size)
	}
	return k.o.Open(ctx, name)
}

func (k knownSizes) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	return k.o.OpenSized(ctx, name, size)
}

func (k knownSizes) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return ReadObject(ctx, k.o, name, limit)
}

type limitedObject struct {
	Object
	l limited
}

func (f limitedObject) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(context.Background(), p, off)
}

func (f limitedObject) WithContext(ctx context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) { return f.readAt(ctx, p, off) })
}

func (f limitedObject) ReadRanges(ctx context.Context, ranges []Range) error {
	return ReadRangesWith(ctx, f, ranges, policyOf(f.Object))
}

func (f limitedObject) batchPolicy() BatchPolicy { return policyOf(f.Object) }

func (f limitedObject) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := f.l.acquire(ctx); err != nil {
		return 0, err
	}
	defer f.l.release()
	return f.Object.WithContext(ctx).ReadAt(p, off)
}

type readerFunc func(p []byte, off int64) (int, error)

func (r readerFunc) ReadAt(p []byte, off int64) (int, error) { return r(p, off) }
