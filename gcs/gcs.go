// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcs opens a Google Cloud Storage bucket, through the JSON API
// of Google's service or of a compatible endpoint, as a live swath.Source.
//
// The API is experimental: it may change in any release.
package gcs

import (
	"context"
	"fmt"
	"strings"

	"github.com/varveio/swath"
	"github.com/varveio/swath/internal/live"
	gcssource "github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Options configure a GCS source. The zero value reaches Google's own
// service with Application Default Credentials, as Google's client
// libraries find them.
type Options struct {
	// Endpoint is a JSON API base URL,
	// http(s)://host[:port]/storage/v1/, without user information, a
	// query or a fragment: an emulator's or a proxy's. Empty is Google's
	// own, https://storage.googleapis.com/storage/v1/.
	Endpoint string
	// CredentialsFile is a service-account or authorized-user JSON file.
	// Empty takes Application Default Credentials:
	// GOOGLE_APPLICATION_CREDENTIALS, gcloud's file, or the metadata
	// server on Google Cloud.
	CredentialsFile string
	// Anonymous sends every request without credentials, whatever
	// CredentialsFile or the environment holds: a public bucket's.
	Anonymous bool
	// UserProject is the project billed for the requests, which a
	// requester-pays bucket needs.
	UserProject string
	// MaxRequests bounds the requests in flight at once across every call
	// on the source, through an adaptive limiter that lowers the bound
	// while the service throttles and raises it back. Zero sets no bound.
	MaxRequests int
	// RequestRate caps the requests the source sends per second, retries
	// included. Zero sets no cap.
	RequestRate float64
}

// Open opens bucket as a live source. It builds the source's client,
// which may load credentials, and lists nothing: a bucket that is not
// there, or that the credentials may not read, fails the first call, not
// Open. Credentials that cannot be found are listing.ErrAuth; an endpoint
// of the wrong form is listing.ErrUnsupported. The caller closes the
// source.
//
// Page needs storage.objects.list on the bucket. Lookup needs
// storage.objects.get on the object, and storage.buckets.get on the bucket
// to tell an absent key: without it a missing key is unknown.
func Open(ctx context.Context, bucket string, opts Options) (*swath.Source, error) {
	switch {
	case bucket == "" || strings.ContainsAny(bucket, "/\x00"):
		return nil, objfile.RedactError(fmt.Errorf("%w: bucket %q: a bucket's name, with no slash", listing.ErrInvalidRequest, bucket))
	case opts.MaxRequests < 0 || opts.RequestRate < 0:
		return nil, fmt.Errorf("%w: a negative MaxRequests or RequestRate", listing.ErrInvalidRequest)
	}

	st, err := gcssource.OpenLive(ctx, gcssource.Options{
		Bucket:          bucket,
		Endpoint:        opts.Endpoint,
		CredentialsFile: opts.CredentialsFile,
		Anonymous:       opts.Anonymous,
		UserProject:     opts.UserProject,
		Workers:         opts.MaxRequests,
		Admission:       objhttp.Admission{Limiter: opts.MaxRequests > 0, RequestRate: opts.RequestRate},
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, objfile.RedactError(err)
	}
	return (*swath.Source)(live.NewSource(st)), nil
}
