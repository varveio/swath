// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"

	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/objfile"
)

// objectFields and bucketFields are the partial-response selectors of
// objects.get and buckets.get on Google's endpoint: what Get reads of an
// object, and only the kind and name of a bucket, plus the kind every 200
// must name.
const (
	objectFields = "kind,name,size,updated,timeCreated,etag,generation,metageneration,storageClass,md5Hash,crc32c,contentType"
	bucketFields = "kind,name"
)

// Get sends one objects.get request per attempt for name's metadata, not
// its data, on List's path: the same credentials, admission, retries and
// classification. Each attempt is reported to ctx's objfile.Observer as a
// metadata request of name. A missing object and a missing bucket are both
// an *Error of status 404, which Bucket tells apart.
func (c *Client) Get(ctx context.Context, bucket, name string) (*Object, error) {
	r := &objfile.Request{Kind: objfile.MetadataRequest, Name: name}
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (*Object, error) {
		return exchange(c, ctx, r, func(svc *storage.Service, actx context.Context) caller[*storage.Object] {
			call := svc.Objects.Get(bucket, name).Projection("noAcl").Context(actx)
			if c.endpoint == DefaultEndpoint {
				call.Fields(objectFields)
			}
			if p := c.cfg.UserProject; p != "" {
				call.UserProject(p)
			}
			return call
		}, resource)
	})
}

// Bucket sends one buckets.get request per attempt on List's path. It
// succeeds when the bucket exists and the credentials may read its
// metadata (storage.buckets.get); a missing bucket is an *Error of status
// 404. Each attempt is reported to ctx's objfile.Observer as a metadata
// request with an empty name.
func (c *Client) Bucket(ctx context.Context, bucket string) error {
	r := &objfile.Request{Kind: objfile.MetadataRequest}
	_, err := objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (struct{}, error) {
		return exchange(c, ctx, r, func(svc *storage.Service, actx context.Context) caller[*storage.Bucket] {
			call := svc.Buckets.Get(bucket).Projection("noAcl").Context(actx)
			if c.endpoint == DefaultEndpoint {
				call.Fields(bucketFields)
			}
			if p := c.cfg.UserProject; p != "" {
				call.UserProject(p)
			}
			return call
		}, bucketResource)
	})
	return err
}

// CloseIdleConnections closes the idle connections of every pooled
// transport. The client stays usable: a later request opens a new one.
func (c *Client) CloseIdleConnections() {
	for _, hc := range c.pool {
		hc.CloseIdleConnections()
	}
}

// resource converts a decoded objects.get 200 strictly: an object resource
// (kind), as a listed item is converted.
func resource(o *storage.Object) (*Object, error) {
	switch {
	case o == nil:
		return nil, fmt.Errorf("%w: a 200 whose body is null", objhttp.ErrSyntax)
	case o.Kind != "storage#object":
		return nil, fmt.Errorf("%w: a 200 that is not an object resource (kind %q)", objhttp.ErrSyntax, o.Kind)
	}
	x, err := item(o)
	if err != nil {
		return nil, err
	}
	return &x, nil
}

// bucketResource checks a decoded buckets.get 200: a bucket resource
// (kind).
func bucketResource(b *storage.Bucket) (struct{}, error) {
	if b == nil || b.Kind != "storage#bucket" {
		return struct{}{}, fmt.Errorf("%w: a 200 that is not a bucket resource", objhttp.ErrSyntax)
	}
	return struct{}{}, nil
}
