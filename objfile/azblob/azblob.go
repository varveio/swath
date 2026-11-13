// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblob reads and creates blobs in an Azure Blob Storage container with
// the azblob SDK. New supplies an objfile.Opener; NewWriter supplies an
// objfile.Writer. Open learns the blob's size and ETag from its
// properties; OpenSized takes the ETag from its first ranged download (of
// size 0, from the properties: objfile.OpenEmpty), and
// ReadObject from its one download of the whole blob, with no range. Every
// request after that is a download of one byte range with If-Match on that
// ETag: one for each ReadAt, and one for each merged span of a ReadRanges
// batch, sent concurrently. Once pinned, a 412, a 404, or a response naming
// another ETag or size fails the read with objfile.ErrObjectChanged. Before
// a pin (a first sized read, a whole-object read), a 404 is an error wrapping
// fs.ErrNotExist and a 412 (no condition was sent) is the store's error; a
// first sized read (OpenSized) that finds another size is an
// objfile.SizeMismatchError. A whole-object read has no size to expect: one
// over its limit is objfile.ErrTooLarge.
//
// Reads use the current blob's ETag; Azure snapshots and version-id reads are
// not exposed. The service must honor If-Match and If-None-Match conditions.
package azblob

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/internal/ranged"
)

// New returns an Opener of the blobs of containerName through client, which
// carries the caller's credentials, endpoint, transport and retry settings.
func New(client *azsdk.Client, containerName string) objfile.Opener {
	return opener{container: client.ServiceClient().NewContainerClient(containerName), observed: ranged.Observing(client)}
}

type opener struct {
	container *container.Client
	observed  bool // the client's transport reports every attempt
}

// ReportsIO reports whether every request's attempts are observed
// (objfile.IOReporter): only for a client objfile/remote built, whose
// transport sees the SDK's retries.
func (o opener) ReportsIO() bool { return o.observed }

func (o opener) Open(ctx context.Context, name string) (objfile.Object, error) {
	b := o.container.NewBlobClient(name)
	size, pin, err := ranged.Metadata(ctx, name, o.observed, func(ctx context.Context) (int64, ranged.Pin, error) { return stat(ctx, b) })
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return ranged.New(name, size, pin, getter(b), o.observed), nil
}

func (o opener) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	if err := ranged.CheckSize(name, size); err != nil {
		return nil, err
	}
	if size == 0 {
		return objfile.OpenEmpty(ctx, name, o.Open)
	}
	b := o.container.NewBlobClient(name)
	return ranged.NewSized(name, size, getter(b), func(ctx context.Context) (int64, ranged.Pin, error) {
		return stat(ctx, b)
	}, o.observed), nil
}

// ReadObject reads name whole with one download, with no range.
func (o opener) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return ranged.ReadObject(ctx, name, limit, o.observed, getter(o.container.NewBlobClient(name)))
}

// stat is the blob's properties: its size and ETag.
func stat(ctx context.Context, b *blob.Client) (int64, ranged.Pin, error) {
	props, err := b.GetProperties(ctx, nil)
	if err != nil {
		if status(err) == http.StatusNotFound {
			err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
		}
		return 0, ranged.Pin{}, err
	}
	pin := ranged.Pin{ETag: string(deref(props.ETag))}
	if pin.ETag == "" {
		return 0, ranged.Pin{}, errors.New("GetProperties returned no ETag to pin")
	}
	return deref(props.ContentLength), pin, nil
}

func getter(b *blob.Client) ranged.Getter {
	return func(ctx context.Context, off, n int64, pin ranged.Pin) (ranged.Response, error) {
		opt := &blob.DownloadStreamOptions{}
		if n >= 0 {
			opt.Range = blob.HTTPRange{Offset: off, Count: n}
		} else {
			// The stored bytes: without this Go's transport asks for gzip on
			// a request with no Range and decompresses a blob stored with
			// Content-Encoding: gzip, which the SDK does not see.
			ctx = policy.WithHTTPHeader(ctx, http.Header{"Accept-Encoding": {"identity"}})
		}
		if pin.ETag != "" {
			etag := azcore.ETag(pin.ETag)
			opt.AccessConditions = &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &etag},
			}
		}
		out, err := b.DownloadStream(ctx, opt)
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
		res := ranged.Response{Body: out.Body, Pin: ranged.Pin{ETag: string(deref(out.ETag))}, Length: -1}
		res.Start, res.Total = ranged.Stated(deref(out.ContentRange))
		if out.ContentLength != nil {
			res.Length = *out.ContentLength
		}
		return res, nil
	}
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// status returns the HTTP status of an SDK error, or 0.
func status(err error) int {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return re.StatusCode
	}
	return 0
}
