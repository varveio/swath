// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblob reads and creates blobs in an Azure Blob Storage container with
// the azblob SDK. New supplies an objfile.Opener; NewWriter supplies an
// objfile.Writer. Open learns the blob's size and ETag from its
// properties; OpenSized takes the ETag from its first ranged download, and
// ReadObject from its one download of the whole blob, with no range. Every
// request after that is a download of one byte range with If-Match on that
// ETag: one for each ReadAt, and one for each merged span of a ReadRanges
// batch, sent concurrently. A 412, a 404, or a
// response naming another ETag or size fails the read with
// objfile.ErrObjectChanged.
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
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/ranged"
)

// New returns an Opener of the blobs of containerName through client, which
// carries the caller's credentials, endpoint, transport and retry settings.
func New(client *azsdk.Client, containerName string) objfile.Opener {
	return opener{container: client.ServiceClient().NewContainerClient(containerName)}
}

type opener struct {
	container *container.Client
}

func (o opener) Open(ctx context.Context, name string) (objfile.Object, error) {
	b := o.container.NewBlobClient(name)
	size, pin, err := stat(ctx, b)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return ranged.New(name, size, pin, getter(b)), nil
}

func (o opener) OpenSized(_ context.Context, name string, size int64) (objfile.Object, error) {
	b := o.container.NewBlobClient(name)
	return ranged.NewSized(name, size, getter(b), func(ctx context.Context) (int64, ranged.Pin, error) {
		return stat(ctx, b)
	}), nil
}

// ReadObject reads name whole with one download, with no range.
func (o opener) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return ranged.ReadObject(ctx, name, limit, getter(o.container.NewBlobClient(name)))
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
			case s == http.StatusNotFound || s == http.StatusPreconditionFailed:
				err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
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
