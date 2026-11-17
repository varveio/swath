// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/gcs/client"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/reader/listing"
)

// maxListResults is the most entries, objects and prefixes together, one
// objects.list answer holds.
const maxListResults = 1000

// Live is a bucket opened for one-level listings and lookups (OpenLive):
// the built client, with its credentials and admission, and no range
// engine. It is a live.Store, safe for concurrent use.
type Live struct {
	bucket   string
	client   *client.Client
	location string
}

var _ live.Store = (*Live)(nil)

// OpenLive builds opt.Bucket's client as New does, which may load
// credentials, and lists nothing. It does not use opt.Prefix, the range
// engine's policy or a supplied Client. Its errors match a class of
// package listing (objhttp.ListingClass), or are ctx's own.
func OpenLive(ctx context.Context, opt Options) (*Live, error) {
	opt.Client = nil
	p, _, err := connect[Object](ctx, opt)
	if err != nil {
		return nil, failure(ctx, err)
	}

	// The client was built from this endpoint, so it parses.
	ep, _ := client.Endpoint(opt.Endpoint)
	return &Live{bucket: opt.Bucket, client: p.client.(*client.Client), location: location(opt.Bucket, ep)}, nil
}

// location names the bucket the client reaches by what decides which
// bucket of a name that is, as Identity does: the bucket and the JSON API
// endpoint in its canonical form. The billed project decides who pays, not
// which bucket is listed.
func location(bucket, endpoint string) string {
	return strings.Join([]string{"gcs", bucket, objhttp.CanonicalRoute(endpoint)}, "\x00")
}

// Location identifies the bucket (live.Store).
func (l *Live) Location() string { return l.location }

// List is one objects.list with the delimiter "/", the client's retries
// included (live.Store). The answer's items are the objects and its
// prefixes the common prefixes; a next page token, which GCS may send
// with neither, says the level goes on.
func (l *Live) List(ctx context.Context, prefix []byte, token string, max int) (*live.Level, error) {
	in := &client.ListInput{Bucket: l.bucket, Prefix: string(prefix), Delimiter: "/", PageToken: token, MaxResults: min(max, maxListResults)}
	out, err := l.client.List(ctx, in)
	if err != nil {
		return nil, failure(ctx, err)
	}

	lvl := &live.Level{
		Objects:   make([]listing.Entry, len(out.Items)),
		Prefixes:  make([][]byte, len(out.Prefixes)),
		Truncated: out.NextPageToken != "",
		Next:      out.NextPageToken,
	}
	for i := range out.Items {
		lvl.Objects[i] = listed(&out.Items[i])
	}
	for i, p := range out.Prefixes {
		lvl.Prefixes[i] = []byte(p)
	}
	return lvl, nil
}

// listed is an object's entry, listed or looked up. A zero-length object
// whose name ends in "/" is an object like any other.
func listed(o *client.Object) listing.Entry {
	size := o.Size
	e := listing.Entry{Key: []byte(o.Name), Kind: listing.KindObject, Size: &size}
	if o.Updated != 0 {
		t := time.UnixMicro(o.Updated).UTC()
		e.LastModified = &t
	}
	if o.StorageClass != "" {
		class := o.StorageClass
		e.StorageClass = &class
	}
	return e
}

// Lookup is objects.get of key (live.Store): present with what the
// object's metadata says. GCS answers a missing object and a missing
// bucket with the same 404, so a 404 is absent only once buckets.get shows
// the bucket is there; a missing bucket and a failed probe leave the key
// unknown, with the probe's failure class. Any other failure, a 404
// without the reason notFound among them, leaves it unknown too, with its
// class: a 403 is ErrAuth.
func (l *Live) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	o, err := l.client.Get(ctx, l.bucket, string(key))
	switch {
	case err == nil:
		e := listed(o)
		e.Key = bytes.Clone(key)
		return listing.LookupResult{State: listing.LookupPresent, Entry: &e}, nil
	case ctx.Err() != nil:
		return listing.LookupResult{}, ctx.Err()
	case !notFound(err):
		return unknown(objhttp.Classed(err)), nil
	}

	err = l.client.Bucket(ctx, l.bucket)
	switch {
	case err == nil:
		return listing.LookupResult{State: listing.LookupAbsent}, nil
	case ctx.Err() != nil:
		return listing.LookupResult{}, ctx.Err()
	}
	return unknown(objhttp.Classed(err)), nil
}

// notFound: GCS answered 404 with its reason notFound. A 404 without that
// reason (a proxy's or a wrong path's) says nothing about the object.
func notFound(err error) bool {
	var e *client.Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound && e.Reason == "notFound"
}

func unknown(reason error) listing.LookupResult {
	return listing.LookupResult{State: listing.LookupUnknown, Reason: reason}
}

// failure is a failed call's error: ctx's own once it has ended, else err
// in its class.
func failure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return objhttp.Classed(err)
}

// Close closes the client's idle connections (live.Store).
func (l *Live) Close() error {
	l.client.CloseIdleConnections()
	return nil
}
