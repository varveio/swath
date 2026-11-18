// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Options configure Open. Each field names the providers that read it; a
// provider ignores a field another provider owns. Each provider package's
// own Options says what its fields mean in full, and the zero value
// reaches each provider's own service with the credentials the
// environment holds.
type Options struct {
	// Region is an S3 bucket's region, which requests are signed for.
	// Read by s3.
	Region string
	// Endpoint replaces the provider's own service URL: an S3-compatible
	// store's, a GCS JSON API base URL, or an Azure endpoint such as
	// Azurite's. Read by s3, gcs and azblob.
	Endpoint string
	// PathStyle addresses the bucket in the URL's path instead of its
	// host name. Read by s3.
	PathStyle bool
	// Profile is the AWS shared configuration's profile. Read by s3.
	Profile string
	// CredentialsFile replaces the AWS shared credentials file (s3), or
	// is a service-account or authorized-user JSON file (gcs). Read by s3
	// and gcs.
	CredentialsFile string
	// Anonymous sends every request without credentials: a public
	// bucket's or container's. Read by s3, gcs and azblob.
	Anonymous bool
	// UserProject is the project billed for the requests, which a
	// requester-pays bucket needs. Read by gcs.
	UserProject string
	// MaxRequests bounds the requests in flight at once, through an
	// adaptive limiter. Zero sets no bound. Read by s3, gcs and azblob.
	MaxRequests int
	// RequestRate caps the requests sent per second, retries included.
	// Zero sets no cap. Read by s3, gcs and azblob.
	RequestRate float64
}

// addressable are the address schemes swath knows: the package under
// this one that provides each, and how many path segments its location
// has.
var addressable = map[string]struct {
	pkg      string
	segments int
}{
	"s3": {"s3", 1},
	"gs": {"gcs", 1},
	"az": {"azblob", 2},
}

// Open opens the location address names as a live source: s3://BUCKET,
// gs://BUCKET or az://ACCOUNT/CONTAINER, with at most one trailing slash.
// An address names a bucket or container only: a key prefix goes in each
// request, and an address with one is listing.ErrInvalidRequest, as is an
// address of a scheme swath does not know, with an empty name, or with a
// query or fragment.
//
// Open opens the providers compiled into the program: a program links
// one by importing its package, such as
//
//	import _ "github.com/varveio/swath/source/s3"
//
// or every one by importing github.com/varveio/swath/source/all. An
// address whose provider is not linked is listing.ErrUnsupported, its
// message naming the import to add. Otherwise Open is the provider's own
// Open, with its errors.
func Open(ctx context.Context, address string, opts Options) (*Source, error) {
	scheme, location, ok := strings.Cut(address, "://")
	p, known := addressable[scheme]
	location = strings.TrimSuffix(location, "/")
	parts := strings.Split(location, "/")
	if !ok || !known || len(parts) != p.segments || slices.Contains(parts, "") || strings.ContainsAny(location, "?#") {
		return nil, objfile.RedactError(fmt.Errorf("%w: address %q: not s3://BUCKET, gs://BUCKET or az://ACCOUNT/CONTAINER (a key prefix goes in each request)", listing.ErrInvalidRequest, address))
	}

	open, ok := live.Lookup(scheme)
	if !ok {
		linked := "none"
		if s := live.Schemes(); len(s) > 0 {
			linked = strings.Join(s, ", ")
		}
		return nil, fmt.Errorf("%w: address scheme %s:// is not compiled in (compiled in: %s); add import _ %q",
			listing.ErrUnsupported, scheme, linked, "github.com/varveio/swath/source/"+p.pkg)
	}

	s, err := open(ctx, location, live.Options(opts))
	if err != nil {
		return nil, err
	}
	return (*Source)(s), nil
}
