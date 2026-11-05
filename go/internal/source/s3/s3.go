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
	"github.com/varveio/swath/go/internal/counters"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/source"
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
	Bucket     string
	Prefix     string
	Endpoint   string // custom endpoint URL (S3-compatible stores, tests)
	Region     string // else AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1
	PathStyle  bool
	FIPS       bool          // FIPS endpoint
	DualStack  bool          // dual-stack (IPv6) endpoint
	Profile    string        // shared-file profile
	Anonymous  bool          // never sign, whatever the environment holds
	Timeout    time.Duration // per request; default 30 s
	Warn       func(string)  // one-time warnings for the user; may be nil
	FetchOwner bool
	Sequential bool    // one continuation-token chain, no ranges (the oracle)
	Seed       bool    // parallel: tile the scope from one delimiter page first
	Workers    int     // parallel: the order stage's worker count (split demand)
	MaxKeys    int64   // stop after this many objects (sequential only; 0 = all)
	Client     API     // optional; built from the options above when nil
	Resume     *Resume // continue after a journaled run's last published key
	Probe      bool    // check each split's upper child with a 1-key request first (off by default)

	// Test hooks: splitCoin replaces the demand gate; trace receives every
	// interval an Expand covered, (lo, hi] or (lo, ⊤].
	splitCoin func() bool
	trace     func(lo, hi string, open bool)
}

// Resume restarts a listing after the last published key.
type Resume struct {
	After string // the last published key: every key <= After is in the dataset
}

// pageKeys is the ListObjectsV2 page size.
const pageKeys = 1000

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

// Provider lists one bucket (or prefix). It implements source.Provider[Range, Object].
type Provider struct {
	opt       Options
	client    API
	anonymous bool
	creds     string
	region    string
	objects   atomic.Int64
	ids       atomic.Uint64
	start     time.Time

	c       *counters.Set // the tally (Counters)
	mu      sync.Mutex
	running map[uint64]*running // ranges in Expand
	queued  int
	area    float64 // ∫ active dt, for the mean
	areaAt  time.Time
}

var _ source.Provider[Range, Object] = (*Provider)(nil)

// New builds the client (see package client for credentials and retries)
// unless Options.Client supplies one.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.Bucket == "" {
		return nil, errors.New("no bucket")
	}
	if !opt.Sequential && opt.MaxKeys > 0 {
		return nil, errors.New("a key limit needs the sequential listing")
	}
	if opt.Workers < 1 {
		opt.Workers = 1
	}
	now := time.Now()
	p := &Provider{opt: opt, client: opt.Client, running: map[uint64]*running{}, start: now, areaAt: now}
	p.c = counters.New(Counters)
	if p.client != nil {
		return p, nil
	}
	c, err := client.New(client.Config{
		Region: opt.Region, Endpoint: opt.Endpoint, PathStyle: opt.PathStyle, FIPS: opt.FIPS, DualStack: opt.DualStack,
		Profile: opt.Profile, Anonymous: opt.Anonymous, RequestTimeout: opt.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("S3 client: %w", err)
	}
	if n := c.Note(); n != "" && opt.Warn != nil {
		opt.Warn(n)
	}
	p.client, p.region = c, c.Region()
	p.creds, p.anonymous = c.Credentials()
	return p, nil
}

// Stats returns the counters so far.
func (p *Provider) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	if c, ok := p.client.(counted); ok {
		n := c.Counters()
		p.c.Set(cRetries, n.Retries)
		p.c.Set(cThrottles, n.Throttles)
		p.c.Set(cTimeouts, n.Timeouts)
		p.c.Set(cAttempts, n.Attempts)
		p.c.Set(cParseFailures, n.ParseFailures)
	}
	s := Stats{Anonymous: p.anonymous, Credentials: p.creds, Region: p.region, Counters: p.c.Snapshot(p.disabled()...)}
	if el := time.Since(p.start).Seconds(); el > 0 {
		s.MeanActive = p.area / el
	}
	return s
}

func (p *Provider) list(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	p.mu.Lock()
	p.c.Inc(cRequests)
	p.mu.Unlock()
	out, err := p.client.ListObjectsV2(ctx, in)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("list s3://%s/%s: %w", p.opt.Bucket, p.opt.Prefix, err)
	}
	return out, nil
}

// expandSequential lists the whole scope with one continuation-token chain.
func (p *Provider) expandSequential(ctx context.Context, e source.Emitter[Range, Object]) error {
	in := p.input()
	last := ""
	skip := ""
	if p.opt.Resume != nil {
		in.StartAfter, skip = safePrefix(p.opt.Resume.After), p.opt.Resume.After
	}
	for {
		out, err := p.list(ctx, in)
		if err != nil {
			return err
		}
		for skip != "" && len(out.Contents) > 0 && out.Contents[0].Key <= skip {
			out.Contents = out.Contents[1:] // resume: already published
		}
		n := len(out.Contents)
		stop := false
		if p.opt.MaxKeys > 0 {
			left := p.opt.MaxKeys - p.objects.Add(int64(n)) + int64(n)
			if left <= int64(n) {
				n, stop = int(max(0, left)), true
			}
		}
		rows := p.rows(out, n)
		for i := range rows {
			if last != "" && rows[i].Key <= last {
				return fmt.Errorf("listing out of order: %q after %q", rows[i].Key, last)
			}
			last = rows[i].Key
		}
		if err := p.emit(rows, e); err != nil {
			return err
		}
		p.mu.Lock()
		if stop {
			p.c.Set(cTruncated, 1)
		}
		p.mu.Unlock()
		if stop || !out.IsTruncated {
			return nil
		}
		if out.NextContinuationToken == "" {
			return errors.New("truncated page without a continuation token")
		}
		in.ContinuationToken = out.NextContinuationToken
	}
}

// emit counts and hands one page of rows to e.
func (p *Provider) emit(rows []Row, e source.Emitter[Range, Object]) error {
	var bytes int64
	for i := range rows {
		bytes += rows[i].Data.Size
	}
	t := time.Now()
	err := e.Rows(rows)
	p.mu.Lock()
	p.c.Add(cSinkWaitNs, int64(time.Since(t)))
	p.c.Inc(cPages)
	p.c.Add(cObjects, int64(len(rows)))
	p.c.Add(cBytes, bytes)
	p.mu.Unlock()
	return err
}

// input is the request for the provider's scope.
func (p *Provider) input() *client.ListInput {
	return &client.ListInput{Bucket: p.opt.Bucket, Prefix: p.opt.Prefix, MaxKeys: pageKeys, FetchOwner: p.opt.FetchOwner}
}

// rows converts the first n objects of a page to rows (Swath's mapping).
// Rows share the page's strings.
func (p *Provider) rows(out *client.Page, n int) []Row {
	rows := make([]Row, n)
	for i := range out.Contents[:n] {
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
