// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcs reads and creates objects in a Google Cloud Storage bucket with
// cloud.google.com/go/storage. New supplies an objfile.Opener; NewWriter supplies
// an objfile.Writer. Open learns the object's size and
// generation from its attributes; OpenSized takes the generation from its
// first range reader (of size 0, from the attributes: objfile.OpenEmpty), and ReadObject from its one reader of the whole
// object, with no Range. Every request after that is a range reader of one
// byte range of that generation, of the stored bytes (no decompressive
// transcoding): one for each ReadAt, and one for each merged span of a
// ReadRanges batch, sent concurrently. A read of a generation that no longer
// exists, or one finding another size, fails with objfile.ErrObjectChanged
// (before a pin, another size is an objfile.SizeMismatchError); one served
// from another offset fails.
//
// The SDK's reader requests the rest of a range by itself when a body fails
// part way, checks only that answer's status, and for one labelled
// Content-Encoding: gzip skips as many bytes of it as it had read. The client
// objfile/remote builds validates every response to a request for one byte
// range (a batch never asks for more than one range a request): its status,
// Content-Range, length and lack of transport decompression, then ends a
// failed body with EOF so objfile requests and checks the rest. A
// whole-object read (ReadObject) is marked so that the transport ends its
// failed body the same way, and the rest is a checked ranged read; a
// request for the rest of an object, "bytes=N-", is validated alike, as the
// range to the object's end. A caller-supplied client needs
// the same transport behavior; otherwise a failed-body retry may return
// wrong bytes without an error.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/ranged"
)

// New returns an Opener of the objects of bucket through client, which
// carries the caller's credentials, endpoint, transport and retry settings.
func New(client *storage.Client, bucket string) objfile.Opener {
	return opener{bucket: client.Bucket(bucket)}
}

type opener struct {
	bucket *storage.BucketHandle
}

func (o opener) Open(ctx context.Context, name string) (objfile.Object, error) {
	size, pin, err := o.stat(ctx, name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return ranged.New(name, size, pin, o.getter(name)), nil
}

func (o opener) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	if err := ranged.CheckSize(name, size); err != nil {
		return nil, err
	}
	if size == 0 {
		return objfile.OpenEmpty(ctx, name, o.Open)
	}
	return ranged.NewSized(name, size, o.getter(name), func(ctx context.Context) (int64, ranged.Pin, error) {
		return o.stat(ctx, name)
	}), nil
}

// ReadObject reads name whole with one media request, with no Range.
func (o opener) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return ranged.ReadObject(ctx, name, limit, o.getter(name))
}

// stat is the object's attributes: its size and generation.
func (o opener) stat(ctx context.Context, name string) (int64, ranged.Pin, error) {
	attrs, err := o.bucket.Object(name).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
		}
		return 0, ranged.Pin{}, err
	}
	return attrs.Size, ranged.Pin{Version: strconv.FormatInt(attrs.Generation, 10)}, nil
}

func (o opener) getter(name string) ranged.Getter {
	return func(ctx context.Context, off, n int64, pin ranged.Pin) (ranged.Response, error) {
		obj := o.bucket.Object(name).ReadCompressed(true)
		if pin.Version != "" {
			gen, err := strconv.ParseInt(pin.Version, 10, 64)
			if err != nil {
				return ranged.Response{}, err
			}
			obj = obj.Generation(gen)
		}
		r, err := obj.NewRangeReader(ctx, off, n)
		if err != nil {
			var gerr *googleapi.Error
			switch {
			case errors.Is(err, storage.ErrObjectNotExist) && pin.IsZero():
				err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
			case errors.Is(err, storage.ErrObjectNotExist):
				err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
			case errors.As(err, &gerr) && gerr.Code == http.StatusRequestedRangeNotSatisfiable:
				err = fmt.Errorf("%w: %w", ranged.ErrUnsatisfiable, err)
			}
			return ranged.Response{}, err
		}
		res := ranged.Response{Body: r, Start: r.Attrs.StartOffset, Total: r.Attrs.Size, Length: r.Remain()}
		if r.Attrs.Size < 0 {
			// No length stated (a 200 without Content-Length): the
			// reader's Remain is then 0, not unknown.
			res.Total, res.Length = -1, -1
		}
		if r.Attrs.Generation > 0 {
			res.Pin.Version = strconv.FormatInt(r.Attrs.Generation, 10)
		}
		return res, nil
	}
}
