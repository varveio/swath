// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3 is the S3 listing provider. ListObjectsV2 returns keys in UTF-8
// binary (unsigned byte) order, so every page can be emitted as it arrives.
//
// By default ranges are key intervals (lo, hi] listed in parallel: the root
// is (⊥, ⊤] or, with Seed, one delimiter page that tiles the scope; a range
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
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// API is the one S3 call the provider makes. *client.Client implements
// it; tests substitute fakes and the AWS SDK (the oracle).
type API interface {
	ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error)
}

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
	// RequestPayer accepts the request charges of a requester-pays bucket.
	RequestPayer bool
	Timeout      time.Duration // per request; default 30 s
	Warn         func(string)  // one-time warnings for the user; may be nil
	FetchOwner   bool
	Sequential   bool    // one continuation-token chain, no ranges (the oracle)
	Seed         bool    // parallel: tile the scope from delimiter probes first
	SeedDescent  bool    // the seed walks the folder hierarchy (seedplan.go); else one delimiter page
	Structure    bool    // pivot on folder boundaries from structure probes sent alongside pages
	Workers      int     // parallel: the order stage's worker count (split demand)
	MaxKeys      int64   // stop after this many objects (sequential only; 0 = all)
	Client       API     // optional; built from the options above when nil
	Resume       *Resume // continue after a journaled run's last published key
	Probe        bool    // check each split's upper child with a 1-key request first (off by default)
	Floors       bool    // the gate-chain floors: no split of a range worth a few pages, nor into a tail child below two pages (off by default)

	// Test hooks: splitCoin replaces the demand gate; trace receives every
	// interval an Expand covered, (lo, hi] or (lo, ⊤].
	splitCoin func() bool
	trace     func(lo, hi string, open bool)
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
type Provider struct {
	*keyrange.Engine[Object]
	opt       Options
	client    API
	anonymous bool
	creds     string
	region    string
}

// Range is the engine's unit of listing work.
type Range = keyrange.Range

var _ source.Provider[Range, Object] = (*Provider)(nil)

// New builds the client (see package client for credentials and retries)
// unless Options.Client supplies one.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.Bucket == "" {
		return nil, errors.New("no bucket")
	}
	p := &Provider{opt: opt, client: opt.Client}
	if p.client == nil {
		c, err := client.New(client.Config{
			Region: opt.Region, Endpoint: opt.Endpoint, PathStyle: opt.PathStyle, FIPS: opt.FIPS, DualStack: opt.DualStack,
			Profile: opt.Profile, Anonymous: opt.Anonymous, RequestTimeout: opt.Timeout, RequestPayer: opt.RequestPayer,
		})
		if err != nil {
			return nil, fmt.Errorf("S3 client: %w", err)
		}
		if n := c.Note(); n != "" && opt.Warn != nil {
			opt.Warn(n)
		}
		p.client, p.region = c, c.Region()
		p.creds, p.anonymous = c.Credentials()
	}
	var resume *keyrange.Resume
	if opt.Resume != nil {
		resume = &keyrange.Resume{After: opt.Resume.After}
	}
	e, err := keyrange.New(keyrange.Options{
		Prefix: opt.Prefix, Sequential: opt.Sequential, Seed: opt.Seed, SeedDescent: opt.SeedDescent,
		Structure: opt.Structure, Workers: opt.Workers, MaxKeys: opt.MaxKeys, Resume: resume, Probe: opt.Probe,
		Floors: opt.Floors, SplitCoin: opt.splitCoin, Trace: opt.trace,
	}, keyrange.Description[Object]{
		Target:      "s3://" + opt.Bucket + "/" + opt.Prefix,
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

// requeue: a transient failure whose client retries were exhausted is
// asked again (keyrange.Description.Requeue).
func requeue(err error) bool {
	var ex *client.ExhaustedError
	return errors.As(err, &ex) && client.Classify(err).Transient()
}

// Restart continues after the last published key.
func (p *Provider) Restart(after string) {
	p.opt.Resume = &Resume{After: after}
	p.Engine.Restart(after)
}

// Stats returns the counters so far.
func (p *Provider) Stats() Stats {
	if c, ok := p.client.(counted); ok {
		n, set := c.Counters(), p.Set()
		set.Set(p.ID(cAttempts), n.Attempts)
		set.Set(p.ID(cRetries), n.Retries)
		set.Set(p.ID(cThrottles), n.Throttles)
		set.Set(p.ID(cTimeouts), n.Timeouts)
		set.Set(p.ID(cParseFailures), n.ParseFailures)
	}
	e := p.Engine.Stats()
	return Stats{Anonymous: p.anonymous, Credentials: p.creds, Region: p.region, MeanActive: e.MeanActive, Counters: e.Counters}
}

// fetch is the engine's page fetch: one ListObjectsV2. Start-after is
// exclusive and there is no end bound, so the engine trims at hi.
func (p *Provider) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Object], error) {
	in := &client.ListInput{Bucket: p.opt.Bucket, Prefix: q.Prefix, Delimiter: q.Delimiter, StartAfter: q.After,
		ContinuationToken: q.Token, MaxKeys: q.Max, FetchOwner: p.opt.FetchOwner && !q.Probe}
	out, err := p.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, err
	}
	return &keyrange.Page[Object]{Rows: p.rows(out), Prefixes: out.CommonPrefixes, Truncated: out.IsTruncated,
		Token: out.NextContinuationToken}, nil
}

// rows converts a page's objects to rows (Swath's mapping). Rows share
// the page's strings.
func (p *Provider) rows(out *client.Page) []Row {
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
