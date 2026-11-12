// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objfile provides pinned byte-range reads, create-only uploads and
// conditional deletes for objects in S3, Google Cloud Storage and Azure Blob
// Storage. It does not list objects, and does not decide which bytes a
// reader needs; given a batch of ranges (ReadRanges), it plans the
// requests that fetch them, merged, split and run concurrently under a
// BatchPolicy. Every read of an Object returns bytes of the version it
// pinned, or fails with ErrObjectChanged and returns none of another
// version's bytes.
//
// The store packages provide an Opener and Writer over the caller's SDK client;
// objfile/remote constructs clients from store URIs. Reads check the
// pinned version or ETag and size, failing with ErrObjectChanged on a mismatch.
// An S3 ETag alone cannot distinguish separate creations with the same ETag.
// An Object supplies io.ReaderAt and a known size, as used by SeekParquet and
// parquet-go. Provider package comments describe their request and retry limits.
//
// An Observer on a context (WithObserver) is told of every physical request
// made under it, through every wrapper; IOReporter says whether that
// observation includes the requests an SDK retries itself. Cache keeps
// fetched bytes of pinned versions in a ByteCache, off unless used, and
// CacheMode lets an operation bypass it.
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
// Every request a read makes asks for one byte range: a ReadAt is one
// request, and a batch of ReadRanges one for each span it merges. Reads
// validate each response's range offset, and its total size and length when
// the response states them; an Object from OpenSized is not pinned until a
// response, or a metadata request after one that states none, has shown its
// total size, and until then its requests are made one at a time. A wrong range fails with an error; a changed identity or size
// fails with ErrObjectChanged. A short response body is requested again from
// where it stopped, at most twice for each request, in addition to the SDK's
// request retries. The GCS package documents the transport required for this
// validation with its SDK.
//
// An Object the store packages open is also a RangeReader.
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
	// request) and pins that version; the Object's reads come after.
	Open(ctx context.Context, name string) (Object, error)
	// OpenSized opens an object whose size the caller already knows, such
	// as a part a manifest lists, without a request (except for size 0,
	// below): the Object pins the
	// version its first request is served, once that response has stated
	// the object's total size and it is size (a response that states none
	// is let go, a metadata request proves the size and version, and the
	// range is requested again of that version); no byte of the object is
	// returned before. A read that finds another size fails with a
	// *SizeMismatchError (a range the store cannot serve, 416, is followed
	// by a metadata request to tell a shorter object from a failure), and a
	// missing object fails with an error wrapping fs.ErrNotExist. An
	// Object never read makes no request and so proves nothing. Size 0 is
	// the exception: no read of an empty object reaches the store, so
	// OpenSized(..., 0) proves it at once by a metadata request (OpenEmpty).
	OpenSized(ctx context.Context, name string, size int64) (Object, error)
}

// ObjectReader is an Opener that reads a small object whole in one request,
// with no metadata request before it.
type ObjectReader interface {
	// ReadObject returns the bytes of the object name and the Identity of
	// the version served. An object of more than limit bytes fails with an
	// error wrapping ErrTooLarge, still returning its Identity, without
	// reading its body when the response states its length (one that does
	// not is read up to one byte past limit); a missing one fails with an
	// error wrapping fs.ErrNotExist.
	ReadObject(ctx context.Context, name string, limit int64) (data []byte, identity string, err error)
}

// OpenEmpty is OpenSized(ctx, name, 0) of a store, the one rule for an
// expected size of zero, which every wrapper follows by passing
// OpenSized(..., 0) inwards to the store's:
//
// No range read proves an empty object (no read of it reaches the store),
// so it is proved by a metadata request to storage: metadata, the store's
// own Open (never a wrapper's open, whose size may be a hint such as
// KnownSizes'). That request is a physical request like any other: Limit
// holds a slot for an OpenSized(..., 0) passing through it, and Pin judges
// its outcome as first evidence (a missing object, or another size, is
// ErrObjectChanged against a pin recorded when it fails). The size it finds
// is compared with 0 before the Object is returned, and before Pin records
// anything: another size closes it and fails with a *SizeMismatchError of
// the size found. A size a wrapper was given is never evidence of the size
// stored.
func OpenEmpty(ctx context.Context, name string, metadata func(ctx context.Context, name string) (Object, error)) (Object, error) {
	f, err := metadata(ctx, name)
	if err != nil {
		return nil, err
	}
	if f.Size() != 0 {
		f.Close()
		return nil, &fs.PathError{Op: "open", Path: name, Err: &SizeMismatchError{Name: name, Expected: 0, Actual: f.Size()}}
	}
	return f, nil
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
	if f.Size() < 0 {
		return nil, "", &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("the object states a negative size %d", f.Size())}
	}
	if f.Size() > limit {
		return nil, f.Identity(), &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, f.Size(), limit)}
	}
	b := make([]byte, f.Size())
	n, err := f.WithContext(ctx).ReadAt(b, 0)
	if n < len(b) {
		if err == nil || err == io.EOF {
			err = &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("%w: %d of %d bytes", io.ErrUnexpectedEOF, n, len(b))}
		}
		return nil, "", err
	}
	if err != nil && err != io.EOF {
		return nil, "", err
	}
	return b, f.Identity(), nil
}

// SizeMismatchError reports that an object opened at a size it was given
// (OpenSized) is another size: its first evidence, a response's stated
// total or a metadata request, showed Actual bytes, an observed total, where
// Expected were given. It is not ErrObjectChanged, which reports a change
// after the version was pinned: a reader can tell a wrong size it was given
// (a manifest that does not describe the object) from an object replaced
// under it.
type SizeMismatchError struct {
	Name     string
	Expected int64
	Actual   int64
}

func (e *SizeMismatchError) Error() string {
	return fmt.Sprintf("objfile: %s is %d bytes, not the %d it was opened at", e.Name, e.Actual, e.Expected)
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
// with CheckPath and opens prefix + "/" + name, as do OpenSized and
// ReadObject. An empty prefix is the whole namespace of o; a trailing "/" on
// prefix is not doubled. The Objects are o's own, batches included.
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

func (r root) ReportsIO() bool { return reportsIO(r.o) }

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
// OpenSized(..., 0) (a metadata request, OpenEmpty), each ReadObject, each
// ReadAt of an Object it opened and each request of such an Object's
// ReadRanges holds one of n slots for its duration, and waits for a
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

type admissionWaitKey struct{} // test handshake at request admission

func (l limited) acquire(ctx context.Context) error {
	if wait, _ := ctx.Value(admissionWaitKey{}).(func()); wait != nil {
		wait()
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l limited) release() { <-l.slots }

func (l limited) ReportsIO() bool { return reportsIO(l.o) }

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
	if size == 0 { // a metadata request (OpenEmpty): it holds a slot
		if err := l.acquire(ctx); err != nil {
			return nil, err
		}
		defer l.release()
	}
	f, err := l.o.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return limitedObject{Object: f, l: l}, nil
}

// KnownSizes opens the names sizes lists with the size it gives: Open of
// such a name is OpenSized, which makes no request for a positive size (one
// of 0 makes the metadata request OpenEmpty requires). Other names, OpenSized
// and ReadObject go to o unchanged. It is for a reader that knows its
// files' sizes, from a manifest it has read, but opens them through an
// interface that does not carry one (SeekParquet's Source.Open). sizes is
// copied. OpenSized checks each size at the Object's first read, or, for
// size 0, at once (OpenEmpty). The map is only the default for Open: an
// explicit OpenSized of a name the map lists opens at the size it is given.
// Compose it either inside or outside Pin: Pin checks the Objects it
// returns at their first read.
func KnownSizes(o Opener, sizes map[string]int64) Opener {
	return knownSizes{o: o, sizes: maps.Clone(sizes)}
}

type knownSizes struct {
	o     Opener
	sizes map[string]int64
}

func (k knownSizes) ReportsIO() bool { return reportsIO(k.o) }

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

func (f limitedObject) ReportsIO() bool { return reportsIO(f.Object) }

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
