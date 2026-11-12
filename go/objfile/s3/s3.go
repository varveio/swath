// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3 reads and creates objects in an S3 bucket with the AWS SDK for Go v2.
// New supplies an objfile.Opener; NewWriter supplies an objfile.Writer.
// Open learns the object's size, ETag and, in a versioned bucket, its version
// id with HeadObject; OpenSized takes them from its first ranged GetObject (of
// size 0, from HeadObject: objfile.OpenEmpty), and ReadObject from its one
// GetObject of the whole object, with no Range. Every request after that is a
// GetObject of one byte range, with If-Match on that ETag and, when there is
// one, that version id: one for each ReadAt, and one for each merged span of
// a ReadRanges batch, sent concurrently. Once pinned, a 412, a 404, or a
// response naming another ETag, version or size fails the read with
// objfile.ErrObjectChanged. Before a pin (a first sized read, a whole-object
// read), a 404 is an error wrapping fs.ErrNotExist and a 412 (no condition
// was sent) is the store's error; a first sized read (OpenSized) that finds
// another size is an objfile.SizeMismatchError. A whole-object read has no
// size to expect: one over its limit is objfile.ErrTooLarge.
// Without a non-null version id, separate creations with the same ETag and
// size cannot be distinguished. The service must honor If-Match on reads
// and conditional deletes, and If-None-Match on creation.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/ranged"
)

// New returns an Opener of the objects of bucket through client, which
// carries the caller's credentials, endpoint, transport and retry settings.
func New(client *s3.Client, bucket string) objfile.Opener {
	return opener{client: client, bucket: bucket}
}

type opener struct {
	client *s3.Client
	bucket string
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

// ReadObject reads name whole with one GetObject, with no Range.
func (o opener) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return ranged.ReadObject(ctx, name, limit, o.getter(name))
}

// stat is HeadObject: the object's size and pin.
func (o opener) stat(ctx context.Context, name string) (int64, ranged.Pin, error) {
	head, err := o.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &o.bucket, Key: &name})
	if err != nil {
		if status(err) == http.StatusNotFound {
			err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
		}
		return 0, ranged.Pin{}, err
	}
	pin := pinOf(head.ETag, head.VersionId)
	if pin.ETag == "" {
		return 0, ranged.Pin{}, errors.New("HeadObject returned no ETag to pin")
	}
	return aws.ToInt64(head.ContentLength), pin, nil
}

func (o opener) getter(name string) ranged.Getter {
	return func(ctx context.Context, off, n int64, pin ranged.Pin) (ranged.Response, error) {
		in := &s3.GetObjectInput{Bucket: &o.bucket, Key: &name}
		if n >= 0 {
			in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1))
		}
		if pin.ETag != "" {
			in.IfMatch = aws.String(pin.ETag)
		}
		if pin.Version != "" {
			in.VersionId = aws.String(pin.Version)
		}
		out, err := o.client.GetObject(ctx, in)
		if err != nil {
			switch s := status(err); {
			case s == http.StatusNotFound && pin.IsZero():
				err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
			case !pin.IsZero() && (s == http.StatusNotFound || s == http.StatusPreconditionFailed):
				// The pinned version is gone, or If-Match failed on it. A
				// 412 of a request with no pin, which sent no condition,
				// is the store's error.
				err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
			case s == http.StatusRequestedRangeNotSatisfiable:
				err = fmt.Errorf("%w: %w", ranged.ErrUnsatisfiable, err)
			}
			return ranged.Response{}, err
		}
		res := ranged.Response{Body: out.Body, Pin: pinOf(out.ETag, out.VersionId), Length: lengthOf(out.ContentLength)}
		res.Start, res.Total = ranged.Stated(aws.ToString(out.ContentRange))
		return res, nil
	}
}

// pinOf pins an ETag and version id. A version id of "null" (an object
// written before versioning was enabled, or while it is suspended) is kept:
// S3 reads that version by it. It is not the Object's Identity, which is the
// ETag then, since an overwrite keeps the id "null".
func pinOf(etag, versionID *string) ranged.Pin {
	return ranged.Pin{Version: aws.ToString(versionID), ETag: aws.ToString(etag)}
}

func lengthOf(n *int64) int64 {
	if n == nil {
		return -1
	}
	return *n
}

// status returns the HTTP status of an SDK error, or 0.
func status(err error) int {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}
