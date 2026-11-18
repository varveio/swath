// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3 opens an Amazon S3 bucket, or a bucket of an S3-compatible
// store, as a live source.Source.
//
// The API is experimental: it may change in any release.
package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/objhttp"
	s3source "github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/source"
)

// Options configure an S3 source. The zero value reaches AWS's own
// service with the credentials, profile and region the environment
// resolves, as the AWS CLI does. With an IAM Identity Center (SSO)
// profile the AWS SDK may refresh its token cache on disk
// (~/.aws/sso/cache), as the AWS CLI does.
type Options struct {
	// Region is the bucket's region, which requests are signed for. Empty
	// takes AWS_REGION, AWS_DEFAULT_REGION or the profile's, else
	// us-east-1. A bucket in another region fails a call with
	// listing.ErrNotFound, its message naming the bucket's region.
	Region string
	// Endpoint is an S3-compatible store's URL, http(s)://host[:port][/path],
	// without user information, a query or a fragment. Empty is AWS's own.
	Endpoint string
	// PathStyle addresses the bucket in the URL's path
	// (https://host/bucket) instead of its host name
	// (https://bucket.host), as many S3-compatible stores need. It is
	// never inferred from the endpoint.
	PathStyle bool
	// Profile is the shared configuration's profile. Empty takes
	// AWS_PROFILE, else the default profile.
	Profile string
	// CredentialsFile replaces the shared credentials file
	// (AWS_SHARED_CREDENTIALS_FILE, else ~/.aws/credentials).
	CredentialsFile string
	// Anonymous sends every request unsigned, whatever credentials the
	// environment holds: a public bucket's.
	Anonymous bool
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
// Open. An endpoint of the wrong form and an S3 directory bucket, which
// lists in no byte order, are listing.ErrUnsupported. The caller closes the
// source.
//
// Page and Walk need s3:ListBucket on the bucket. Lookup needs
// s3:GetObject on the key, and s3:ListBucket on the bucket to tell an
// absent key: without it S3 answers a missing key with 403, which leaves
// the key unknown.
func Open(ctx context.Context, bucket string, opts Options) (*source.Source, error) {
	switch {
	case bucket == "" || strings.ContainsAny(bucket, "/\x00"):
		return nil, objfile.RedactError(fmt.Errorf("%w: bucket %q: a bucket's name, with no slash", listing.ErrInvalidRequest, bucket))
	case opts.MaxRequests < 0 || opts.RequestRate < 0:
		return nil, fmt.Errorf("%w: a negative MaxRequests or RequestRate", listing.ErrInvalidRequest)
	}

	st, err := s3source.OpenLive(ctx, s3source.Options{
		Bucket:          bucket,
		Endpoint:        opts.Endpoint,
		Region:          opts.Region,
		PathStyle:       opts.PathStyle,
		Profile:         opts.Profile,
		CredentialsFile: opts.CredentialsFile,
		Anonymous:       opts.Anonymous,
		Workers:         opts.MaxRequests,
		Admission:       objhttp.Admission{Limiter: opts.MaxRequests > 0, RequestRate: opts.RequestRate},
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, objfile.RedactError(err)
	}
	return (*source.Source)(live.NewSource(st, false)), nil
}
