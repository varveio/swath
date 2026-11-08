// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3 opens objects of an S3 bucket as objfile Objects with the AWS SDK
// for Go v2. Open learns the object's size, ETag and, in a versioned bucket,
// its version id with HeadObject; OpenSized takes them from the first read.
// Every later read is a ranged GetObject with If-Match on that ETag and, when
// there is one, that version id. A 412, a 404, or a response naming another
// ETag, version or size fails the read with objfile.ErrObjectChanged.
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
	head, err := o.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &o.bucket, Key: &name})
	if err != nil {
		if status(err) == http.StatusNotFound {
			err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
		}
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	pin := pinOf(head.ETag, head.VersionId)
	if pin.ETag == "" {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.New("HeadObject returned no ETag to pin")}
	}
	return ranged.New(name, aws.ToInt64(head.ContentLength), pin, o.getter(name)), nil
}

func (o opener) OpenSized(_ context.Context, name string, size int64) (objfile.Object, error) {
	return ranged.New(name, size, ranged.Pin{}, o.getter(name)), nil
}

func (o opener) getter(name string) ranged.Getter {
	return func(ctx context.Context, off, n int64, pin ranged.Pin) (ranged.Response, error) {
		in := &s3.GetObjectInput{
			Bucket: &o.bucket,
			Key:    &name,
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1)),
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
			case s == http.StatusNotFound || s == http.StatusPreconditionFailed:
				err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
			}
			return ranged.Response{}, err
		}
		if out.ContentLength != nil && *out.ContentLength != n {
			out.Body.Close()
			return ranged.Response{}, fmt.Errorf("GetObject returned %d bytes for a range of %d", *out.ContentLength, n)
		}
		return ranged.Response{
			Body:  out.Body,
			Pin:   pinOf(out.ETag, out.VersionId),
			Total: ranged.Total(aws.ToString(out.ContentRange)),
		}, nil
	}
}

// pinOf pins an ETag and version id; "null" is the version id of an object
// written before versioning was enabled, which S3 reads without one.
func pinOf(etag, versionID *string) ranged.Pin {
	v := aws.ToString(versionID)
	if v == "null" {
		v = ""
	}
	return ranged.Pin{Version: v, ETag: aws.ToString(etag)}
}

// status returns the HTTP status of an SDK error, or 0.
func status(err error) int {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}
