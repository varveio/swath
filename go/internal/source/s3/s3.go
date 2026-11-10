// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3 is the S3 listing provider. ListObjectsV2 returns keys in UTF-8
// binary (unsigned byte) order, so every page can be emitted as it arrives.
//
// By default ranges are key intervals (lo, hi] listed in parallel: the root
// is (⊥, ⊤] or, with Seed, delimiter listings of the folders that tile the scope; a range
// whose owner sees idle workers splits itself at a page boundary into
// (cursor, pivot] and (pivot, hi] (see parallel.go). With Sequential the
// whole scope is one continuation-token chain: the reference oracle.
package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// API is the one S3 call the provider makes. *client.Client implements
// it; tests substitute fakes and the AWS SDK's stock client (the oracle).
type API interface {
	ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error)
}

// Routed is implemented by a supplied client (Options.Client) that names
// where a bucket's requests go: its resume identity, in place of the
// endpoint inputs the built client is identified by. A supplied client that
// does not is listed, but the run declines resume: nothing would tell where
// it lists from another's.
type Routed interface{ Route(bucket string) string }

// counted is implemented by clients that count their requests.
type counted interface{ Counters() client.Counters }

// Options configure a Provider.
type Options struct {
	Bucket    string
	Prefix    string
	Endpoint  string // custom endpoint URL (S3-compatible stores, tests)
	Region    string // else AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1
	PathStyle bool
	FIPS      bool   // FIPS endpoint
	DualStack bool   // dual-stack (IPv6) endpoint
	Profile   string // shared-file profile
	Anonymous bool   // never sign, whatever the environment holds
	// BearerCommand, if set, is a shell command printing a bearer token,
	// sent instead of a SigV4 signature (client.BearerToken), fetched again
	// every BearerRefresh (0: 45 minutes). Never recorded.
	BearerCommand string
	BearerRefresh time.Duration
	// RequestPayer accepts the request charges of a requester-pays bucket.
	RequestPayer bool
	Timeout      time.Duration // per request; default 30 s
	Warn         func(string)  // one-time warnings for the user; may be nil
	FetchOwner   bool
	// AllVersions lists every version and delete marker
	// (ListObjectVersions, NewVersions) instead of the current objects.
	AllVersions bool
	Workers     int     // parallel: the order stage's worker count (split demand)
	Client      API     // optional; built from the options above when nil
	Resume      *Resume // continue after a journaled run's last published key
	// Policy is the range engine's (the shared engine flags).
	keyrange.Policy
	// Admission is the built-in client's (the shared engine flags); a
	// supplied Client has its own.
	Admission objhttp.Admission
}

// Resume restarts a listing after the last published key.
type Resume struct {
	After string // the last published key: every key <= After is in the dataset
}

// ParseURL splits s3://bucket/prefix.
func ParseURL(u string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(u, "s3://")
	if !ok || rest == "" {
		return "", "", fmt.Errorf("not an s3://bucket[/prefix] URL: %q", u)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("no bucket in %q", u)
	}
	return bucket, prefix, nil
}

// Stats is what a listing did: the counter snapshot (Counters, the one
// representation of every count, with the run's feature states) and the
// few values that are not counts.
type Stats struct {
	Anonymous   bool   `json:"anonymous"`
	Credentials string `json:"credentials,omitempty"` // where they came from
	Region      string `json:"region,omitempty"`
	// MeanActive is the time-weighted mean of ranges in Expand.
	MeanActive float64           `json:"mean_active"`
	Counters   counters.Snapshot `json:"counters"`
}

// Get is counter id's value.
func (s *Stats) Get(id counters.ID) int64 { return s.Counters.Get(id) }

// Provider lists one bucket (or prefix) on the range engine. It implements
// source.Provider[Range, Object].
type Provider = provider[Object]

// VersionsProvider is the versions listing (Options.AllVersions):
// ListObjectVersions, rows of schema swath/s3-versions/v1.
type VersionsProvider = provider[Version]

// provider is an S3 listing with payload P: the client and the range
// engine over one of its list calls.
type provider[P any] struct {
	*keyrange.Engine[P]
	objhttp.Activity // the client's, observed once it is chosen
	opt              Options
	client           API
	anonymous        bool
	creds            string
	region           string // the resolved signing region ("" with a supplied Client)
	route            string // a supplied Routed client's Route ("" otherwise)
}

// Range is the engine's unit of listing work.
type Range = keyrange.Range

var (
	_ source.Provider[Range, Object]  = (*Provider)(nil)
	_ source.Provider[Range, Version] = (*VersionsProvider)(nil)
)

// VersionsAPI is the client of a versions listing.
type VersionsAPI interface {
	ListObjectVersions(ctx context.Context, in *client.VersionsInput) (*client.VersionPage, error)
}

// New builds the client (see package client for credentials and retries)
// unless Options.Client supplies one, and lists the current objects.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.AllVersions {
		return nil, errors.New("s3: Options.AllVersions lists versions: use NewVersions")
	}
	p, own, err := connect[Object](ctx, opt)
	if err != nil {
		return nil, err
	}
	e, err := keyrange.New(p.engineOptions(own), keyrange.Description[Object]{
		Target:      "s3://" + opt.Bucket + "/" + opt.Prefix,
		Listing:     keyrange.Listing,
		AcceptBound: AcceptBound,
		Requeue:     requeue,
		InvalidPage: client.ErrInvalidPage,
		Size:        func(o *Object) int64 { return o.Size },
		Counters:    clientCounters,
	}, p.fetch)
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// NewVersions is New for a versions listing (ListObjectVersions): every
// version and delete marker, a key's entries consecutive, newest first.
// The client must implement VersionsAPI.
func NewVersions(ctx context.Context, opt Options) (*VersionsProvider, error) {
	opt.AllVersions = true
	p, own, err := connect[Version](ctx, opt)
	if err != nil {
		return nil, err
	}
	api, ok := p.client.(VersionsAPI)
	if !ok {
		return nil, fmt.Errorf("s3: the client (%T) cannot list versions", p.client)
	}
	e, err := keyrange.New(p.engineOptions(own), keyrange.Description[Version]{
		Target:      "s3://" + opt.Bucket + "/" + opt.Prefix,
		Listing:     keyrange.Listing,
		AcceptBound: AcceptBound,
		Requeue:     requeue,
		InvalidPage: client.ErrInvalidPage,
		Groups:      VersionsSchema.Groups,
		Detach:      VersionsSchema.Detach,
		Size:        func(v *Version) int64 { return v.Size },
		Counters:    clientCounters,
	}, func(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Version], error) {
		return p.fetchVersions(ctx, api, q)
	})
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// connect builds the client unless Options.Client supplies one (own).
func connect[P any](ctx context.Context, opt Options) (*provider[P], bool, error) {
	if opt.Bucket == "" {
		return nil, false, errors.New("no bucket")
	}
	if strings.HasSuffix(opt.Bucket, "--x-s3") {
		// Refused before any request, as Java: a directory bucket lists in
		// no lexicographic order, which the range engine relies on.
		return nil, false, source.Unsupported(fmt.Errorf("S3 directory bucket %q (Express One Zone) is not supported: swath requires the "+
			"lexicographic listing order of a general-purpose S3 bucket; use a general-purpose bucket", opt.Bucket))
	}
	p := &provider[P]{opt: opt, client: opt.Client}
	own := p.client == nil
	if own {
		lim, rate := opt.Admission.New(opt.Workers)
		cc := client.Config{Limiter: lim, Rate: rate,
			Region: opt.Region, Endpoint: opt.Endpoint, PathStyle: opt.PathStyle, FIPS: opt.FIPS, DualStack: opt.DualStack,
			Profile: opt.Profile, Anonymous: opt.Anonymous, RequestTimeout: opt.Timeout, RequestPayer: opt.RequestPayer,
		}
		if opt.BearerCommand != "" {
			cc.Bearer = client.NewBearerToken(opt.BearerCommand, opt.BearerRefresh)
		}
		c, err := client.New(ctx, cc)
		if err != nil {
			return nil, false, fmt.Errorf("S3 client: %w", err)
		}
		if n := c.Note(); n != "" && opt.Warn != nil {
			opt.Warn(n)
		}
		p.client, p.region = c, c.Region()
		p.creds, p.anonymous = c.Credentials()
	} else if r, ok := p.client.(Routed); ok {
		p.route = r.Route(opt.Bucket)
	}
	p.Activity = objhttp.Observe(p.client)
	return p, own, nil
}

// engineOptions is the range engine's options for p.
func (p *provider[P]) engineOptions(own bool) keyrange.Options {
	opt := p.opt
	var resume *keyrange.Resume
	if opt.Resume != nil {
		resume = &keyrange.Resume{After: opt.Resume.After}
	}
	eo := opt.Policy.Options()
	eo.Prefix, eo.Workers, eo.Resume = opt.Prefix, opt.Workers, resume
	eo.ClientOff = opt.Admission.Off(own)
	return eo
}

// AcceptBound is S3's bound policy: the pivot alphabet, and '%' where a
// real key holds it (keyrange.SafeKey). S3 refuses control characters in
// start-after, so a key holding one is never sent as a start bound. '%' is
// out of the pivot alphabet for an SDK that decodes a verbatim-echo
// endpoint's StartAfter (I12); this client percent-encodes the query and
// parses no echo, and synthesis never adds '%', so a bound holds '%' only
// copied from a key the service returned (a cursor, a folder, a pivot's
// shared prefix).
func AcceptBound(s string) bool { return keyrange.SafeKey(s) }

// requeue: a transient failure whose client retries were exhausted is
// asked again (keyrange.Description.Requeue).
var requeue = objhttp.Requeue

// Restart continues after a journaled run's last published key, from
// the normal start-up: one range (after, ⊤], split as on a fresh run
// (source.Resumable). It records the resume in the provider's options and
// the engine's.
func (p *provider[P]) Restart(after string) error {
	p.opt.Resume = &Resume{After: after}
	p.Engine.Restart(after)
	return nil
}

// Stats returns the counters so far.
func (p *provider[P]) Stats() Stats {
	if c, ok := p.client.(counted); ok {
		c.Counters().Store(p.Set(), p.ID(0))
	}
	e := p.Engine.Stats()
	return Stats{Anonymous: p.anonymous, Credentials: p.creds, Region: p.region, MeanActive: e.MeanActive, Counters: e.Counters}
}

// fetch is the engine's page fetch: one ListObjectsV2. Start-after is
// exclusive and there is no end bound, so the engine trims at hi. A
// continuation token carries the chain's start, so start-after is sent on
// the first request only.
func (p *provider[P]) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Object], error) {
	in := &client.ListInput{Bucket: p.opt.Bucket, Prefix: q.Prefix, Delimiter: q.Delimiter,
		ContinuationToken: q.Token, MaxKeys: q.Max, FetchOwner: p.opt.FetchOwner && !q.Probe}
	if q.Token == "" {
		in.StartAfter = q.After
	}
	out, err := p.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, err
	}
	return &keyrange.Page[Object]{Rows: p.rows(out), Prefixes: out.CommonPrefixes, Truncated: out.IsTruncated,
		Token: out.NextContinuationToken}, nil
}

// rows converts a page's objects to rows (Swath's mapping). Rows share
// the page's strings.
func (p *provider[P]) rows(out *client.Page) []Row {
	rows := make([]Row, len(out.Contents))
	for i := range out.Contents {
		o, r := &out.Contents[i], &rows[i]
		m := &r.Data
		r.Key = o.Key
		m.ETag = stripQuotes(o.ETag)
		m.StorageClass = o.StorageClass
		if p.opt.FetchOwner {
			m.OwnerID, m.OwnerDisplayName = o.OwnerID, o.OwnerDisplayName
		}
		m.ChecksumAlgorithm, m.ChecksumType = o.ChecksumAlgorithm, o.ChecksumType
		m.Size, m.Mtime = o.Size, o.Mtime
	}
	return rows
}

// stripQuotes removes one pair of surrounding quotes, as Swath does; the
// multipart form "hex-N" is kept as is.
func stripQuotes(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}
