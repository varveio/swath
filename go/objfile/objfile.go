// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objfile opens an object in an object store as a file read by byte
// range, with one stored version of the object pinned for the file's
// lifetime: every read of an Object returns bytes of the object as it was when
// the Object was opened, or fails with ErrObjectChanged. Nothing else is offered: no
// listing, caching, read planning or writing.
//
// The store packages (objfile/s3, objfile/gcs, objfile/azblob) each provide an
// Opener over the caller's own SDK client. An Object satisfies the file
// interfaces of the SeekParquet module (SourceFile, ContextBinder) and anything
// else that reads an io.ReaderAt of a known size, such as parquet-go's
// OpenFile.
package objfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
)

// Object is an opened object. It is safe for concurrent use. ReadAt follows
// io.ReaderAt: a read that reaches the end of the object returns io.EOF with
// the bytes it read. After Close, reads fail with an error wrapping
// fs.ErrClosed.
type Object interface {
	io.ReaderAt
	// Size returns the object's length in bytes, as it was when opened.
	Size() int64
	// Identity names the pinned version: the S3 version id, or the ETag
	// when the object has none or has the version id "null" (which S3
	// keeps across an overwrite), the GCS generation, or the Azure ETag.
	// An Object from OpenSized
	// pins on its first read and returns "" before it. A cache can key
	// bytes by name and Identity.
	Identity() string
	Close() error
	// WithContext returns a view of the file whose reads are bounded by
	// ctx; ReadAt on the Object itself uses context.Background.
	WithContext(ctx context.Context) io.ReaderAt
}

// Opener opens objects by name, honouring ctx. A missing object fails with
// an error wrapping fs.ErrNotExist.
type Opener interface {
	// Open asks the store for the object's size and version (one metadata
	// request) and pins that version.
	Open(ctx context.Context, name string) (Object, error)
	// OpenSized opens an object whose size the caller already knows, such
	// as a part a manifest lists, without a request: the Object pins the
	// version its first read is served, and a read that finds another size
	// fails with ErrObjectChanged. A missing object fails that first read.
	OpenSized(ctx context.Context, name string, size int64) (Object, error)
}

// ErrObjectChanged reports that an object was replaced or deleted after its Object
// was opened. A read that fails with it returned no bytes of another version.
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

// Limit bounds the requests in flight through o to n: each Open and each
// ReadAt of an Object it opened holds one of n slots for its duration, and waits
// for a slot only as long as its context allows. n below 1 is 1.
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

func (l limited) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	f, err := l.o.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return limitedObject{Object: f, l: l}, nil
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

func (f limitedObject) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := f.l.acquire(ctx); err != nil {
		return 0, err
	}
	defer f.l.release()
	return f.Object.WithContext(ctx).ReadAt(p, off)
}

type readerFunc func(p []byte, off int64) (int, error)

func (r readerFunc) ReadAt(p []byte, off int64) (int, error) { return r(p, off) }
