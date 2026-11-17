// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/reader/listing"
)

// maxListKeys is the most keys one ListObjectsV2 answer holds.
const maxListKeys = 1000

// Live is a bucket opened for one-level listings, lookups and walks
// (OpenLive): the built client, with its credentials and admission, and
// no range engine, which each walk builds for itself (Walk). It is a
// live.Store and a live.Walker, safe for concurrent use.
type Live struct {
	bucket   string
	client   *client.Client
	location string
}

var (
	_ live.Store  = (*Live)(nil)
	_ live.Walker = (*Live)(nil)
)

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
	return &Live{bucket: opt.Bucket, client: p.client.(*client.Client), location: location(opt, p.region)}, nil
}

// location names the bucket the client reaches by what decides which
// bucket of a name that is, as Identity does: the bucket, the endpoint in
// its canonical form, the resolved signing region and the addressing
// flags.
func location(opt Options, region string) string {
	return strings.Join([]string{"s3", opt.Bucket, endpointCanonical(opt.Endpoint, opt.PathStyle), region,
		strconv.FormatBool(opt.PathStyle), strconv.FormatBool(opt.FIPS), strconv.FormatBool(opt.DualStack)}, "\x00")
}

// Location identifies the bucket (live.Store).
func (l *Live) Location() string { return l.location }

// List is one ListObjectsV2 with the delimiter "/", the client's retries
// included (live.Store).
func (l *Live) List(ctx context.Context, prefix []byte, token string, max int) (*live.Level, error) {
	in := &client.ListInput{Bucket: l.bucket, Prefix: string(prefix), Delimiter: "/", ContinuationToken: token, MaxKeys: min(max, maxListKeys)}
	out, err := l.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, failure(ctx, err)
	}

	lvl := &live.Level{
		Objects:   make([]listing.Entry, len(out.Contents)),
		Prefixes:  make([][]byte, len(out.CommonPrefixes)),
		Truncated: out.IsTruncated,
		Next:      out.NextContinuationToken,
	}
	for i := range out.Contents {
		lvl.Objects[i] = listed(&out.Contents[i])
	}
	for i, p := range out.CommonPrefixes {
		lvl.Prefixes[i] = []byte(p)
	}
	return lvl, nil
}

// listed is a listed object's entry.
func listed(o *client.Object) listing.Entry {
	size := o.Size
	e := listing.Entry{Key: []byte(o.Key), Kind: listing.KindObject, Size: &size}
	if o.Mtime != 0 {
		t := time.UnixMicro(o.Mtime).UTC()
		e.LastModified = &t
	}
	if o.StorageClass != "" {
		class := o.StorageClass
		e.StorageClass = &class
	}
	return e
}

// Lookup is HeadObject of key (live.Store): present with what the answer
// says of the object. S3 answers a missing key and a missing bucket with
// the same 404, so a 404 is absent only once HeadBucket shows the bucket
// is there; a missing bucket and a failed probe leave the key unknown. Any
// other failure leaves it unknown too, with its class: a 403 is ErrAuth,
// since S3 answers a missing key so to credentials that may not list.
func (l *Live) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	h, err := l.client.HeadObject(ctx, l.bucket, string(key))
	switch {
	case err == nil:
		return listing.LookupResult{State: listing.LookupPresent, Entry: headed(key, h)}, nil
	case ctx.Err() != nil:
		return listing.LookupResult{}, ctx.Err()
	case !notFound(err):
		return unknown(classed(err)), nil
	}

	err = l.client.HeadBucket(ctx, l.bucket)
	switch {
	case err == nil:
		return listing.LookupResult{State: listing.LookupAbsent}, nil
	case ctx.Err() != nil:
		return listing.LookupResult{}, ctx.Err()
	case notFound(err):
		return unknown(fmt.Errorf("%w: bucket %q: %w", listing.ErrNotFound, l.bucket, err)), nil
	}
	return unknown(classed(err)), nil
}

// headed is the entry a HeadObject answer describes. S3 names the storage
// class in every answer but one of class STANDARD.
func headed(key []byte, h *client.Head) *listing.Entry {
	class := h.StorageClass
	if class == "" {
		class = "STANDARD"
	}
	return &listing.Entry{Key: bytes.Clone(key), Kind: listing.KindObject, Size: h.Size, LastModified: h.LastModified, StorageClass: &class}
}

// notFound: S3 answered 404. A HEAD answer has no body to name a code.
func notFound(err error) bool {
	var e *client.Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
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
	return classed(err)
}

// classed is err in its class (objhttp.Classed), told as a library caller
// reads it (library).
func classed(err error) error {
	return objhttp.Classed(library(err))
}

// library is err with the message a library caller reads: a wrong-region
// answer names the bucket's region but not the command's flag, which a
// library caller does not have (it sets s3.Options.Region). The answer
// stays in the chain, for errors.As and the classes.
func library(err error) error {
	var e *client.Error
	if !errors.As(err, &e) || e.Region == "" {
		return err
	}
	return libraryError{err: err, msg: strings.Replace(err.Error(), e.Error(), e.Library(), 1)}
}

type libraryError struct {
	err error
	msg string
}

func (e libraryError) Error() string { return e.msg }
func (e libraryError) Unwrap() error { return e.err }

// Close closes the client's idle connections (live.Store).
func (l *Live) Close() error {
	l.client.CloseIdleConnections()
	return nil
}
