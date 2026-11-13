// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package spsource is a SeekParquet Source over an objfile.Opener. SeekParquet
// decides what to read; objfile decides how. Each of the Source's optional
// capabilities maps onto one objfile call:
//
//	seekparquet.Source.Open          objfile.Opener.Open
//	seekparquet.SizedSource          objfile.Opener.OpenSized
//	seekparquet.ManifestReader       objfile.ReadObject("manifest.json", limit)
//	seekparquet.RangeReader          objfile.ReadRanges
//	seekparquet.ContextBinder        objfile.Object.WithContext
//
// Errors are translated where objfile names what SeekParquet also names:
// objfile.ErrObjectChanged also matches seekparquet.ErrObjectChanged, on
// every read, scalar or batched, context-bound or not; an
// *objfile.SizeMismatchError also matches a *seekparquet.SizeMismatchError;
// objfile.ErrTooLarge also matches seekparquet.ErrObjectTooLarge; a batch
// objfile refuses before any request (fs.ErrInvalid) also matches
// seekparquet.ErrInvalidRequest; and an *objfile.BatchError becomes a
// *seekparquet.BatchError, member by member. The original error stays in
// the chain, so errors.Is and errors.As find objfile's errors and
// fs.ErrNotExist too.
//
// An operation's context reaches objfile unchanged, including the caller's
// objfile.WithCacheMode and objfile.WithObserver values. Cache policy and
// physical request observation belong to objfile.
package spsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/objfile"
)

// ManifestName is the path ReadManifest reads, the dataset's manifest.
const ManifestName = "manifest.json"

// Source is a SeekParquet Source whose files are the Objects of an Opener,
// usually objfile.Pin over objfile.Root (or a remote.Store) and
// objfile.Limit. It is a seekparquet.SizedSource and a
// seekparquet.ManifestReader; its files are seekparquet.ContextBinders and
// seekparquet.RangeReaders. It is safe for concurrent use.
type Source struct {
	o objfile.Opener
}

// New returns the Source of the objects of o.
func New(o objfile.Opener) *Source { return &Source{o: o} }

var (
	_ seekparquet.Source         = (*Source)(nil)
	_ seekparquet.SizedSource    = (*Source)(nil)
	_ seekparquet.ManifestReader = (*Source)(nil)
	_ seekparquet.SourceFile     = file{}
	_ seekparquet.ContextBinder  = file{}
	_ seekparquet.RangeReader    = file{}
)

// Open opens path by the Opener's Open, which learns its size.
func (s *Source) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := s.o.Open(ctx, path)
	if err != nil {
		return nil, mapError(err)
	}
	return file{f}, nil
}

// OpenSized opens path at the size its manifest entry gives, by the
// Opener's OpenSized: the first read proves the size.
func (s *Source) OpenSized(ctx context.Context, path string, expectedSize int64) (seekparquet.SourceFile, error) {
	f, err := s.o.OpenSized(ctx, path, expectedSize)
	if err != nil {
		return nil, mapError(err)
	}
	return file{f}, nil
}

// ReadManifest reads manifest.json whole, at most maxBytes of it, by
// objfile.ReadObject: one request when the Opener is an
// objfile.ObjectReader.
func (s *Source) ReadManifest(ctx context.Context, maxBytes int64) ([]byte, string, error) {
	b, id, err := objfile.ReadObject(ctx, s.o, ManifestName, maxBytes)
	if err != nil {
		return nil, "", mapError(err)
	}
	return b, id, nil
}

// file is an opened Object.
type file struct{ f objfile.Object }

func (f file) Size() int64  { return f.f.Size() }
func (f file) Close() error { return f.f.Close() }

func (f file) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.f.ReadAt(p, off)
	return n, mapError(err)
}

func (f file) WithContext(ctx context.Context) io.ReaderAt {
	return reader{f.f.WithContext(ctx)}
}

// ReadRanges reads ranges by objfile.ReadRanges. The descriptors are
// copied; the buffers are the caller's, filled in place.
func (f file) ReadRanges(ctx context.Context, ranges []seekparquet.ReadRange) error {
	rs := make([]objfile.Range, len(ranges))
	for i, r := range ranges {
		rs[i] = objfile.Range{Off: r.Off, Buf: r.Buf}
	}
	err := objfile.ReadRanges(ctx, f.f, rs)
	var be *objfile.BatchError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &be) && len(be.Errs) == len(ranges):
		errs := make([]error, len(be.Errs))
		for i, e := range be.Errs {
			errs[i] = mapError(e)
		}
		return &seekparquet.BatchError{Errs: errs}
	case errors.Is(err, fs.ErrInvalid):
		return fmt.Errorf("%w: %w", seekparquet.ErrInvalidRequest, err)
	}
	return mapError(err)
}

// reader is a context-bound view of an Object.
type reader struct{ r io.ReaderAt }

func (r reader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	return n, mapError(err)
}

// mapError gives err the SeekParquet error objfile's says the same of,
// keeping err in the chain. io.EOF, which ReadAt returns with a read cut
// short by the object's end, is returned as it is.
func mapError(err error) error {
	var mismatch *objfile.SizeMismatchError
	switch {
	case err == nil || err == io.EOF:
		return err
	case errors.Is(err, objfile.ErrObjectChanged):
		return fmt.Errorf("%w: %w", seekparquet.ErrObjectChanged, err)
	case errors.As(err, &mismatch):
		return fmt.Errorf("%w: %w", &seekparquet.SizeMismatchError{Path: mismatch.Name, Expected: mismatch.Expected, Actual: mismatch.Actual}, err)
	case errors.Is(err, objfile.ErrTooLarge):
		return fmt.Errorf("%w: %w", seekparquet.ErrObjectTooLarge, err)
	}
	return err
}
