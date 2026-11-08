// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblob opens blobs of an Azure Blob Storage container as objfile
// Objects with the azblob SDK. Open learns the blob's size and ETag from its
// properties; OpenSized takes the ETag from the first read. Every later read
// is a ranged download with If-Match on that ETag. A 412, a 404, or a
// response naming another ETag or size fails the read with
// objfile.ErrObjectChanged.
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
	props, err := b.GetProperties(ctx, nil)
	if err != nil {
		if status(err) == http.StatusNotFound {
			err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
		}
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	pin := ranged.Pin{ETag: string(deref(props.ETag))}
	if pin.ETag == "" {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.New("GetProperties returned no ETag to pin")}
	}
	return ranged.New(name, deref(props.ContentLength), pin, getter(b)), nil
}

func (o opener) OpenSized(_ context.Context, name string, size int64) (objfile.Object, error) {
	return ranged.New(name, size, ranged.Pin{}, getter(o.container.NewBlobClient(name))), nil
}

func getter(b *blob.Client) ranged.Getter {
	return func(ctx context.Context, off, n int64, pin ranged.Pin) (ranged.Response, error) {
		opt := &blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset: off, Count: n}}
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
		res := ranged.Response{
			Body:   out.Body,
			Pin:    ranged.Pin{ETag: string(deref(out.ETag))},
			Total:  ranged.Total(deref(out.ContentRange)),
			Length: -1,
		}
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
