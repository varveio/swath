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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// API is the subset of the S3 client the provider uses.
type API interface {
	ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, opts ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

// Options configure a Provider.
type Options struct {
	Bucket     string
	Prefix     string
	Endpoint   string // custom endpoint URL (S3-compatible stores, tests)
	Region     string // default us-east-1
	PathStyle  bool
	FetchOwner bool
	Sequential bool  // one continuation-token chain, no ranges (the oracle)
	Seed       bool  // parallel: tile the scope from one delimiter page first
	Workers    int   // parallel: the order stage's worker count (split demand)
	MaxKeys    int64 // stop after this many objects (sequential only; 0 = all)
	Client     API   // optional; built from the options above when nil

	// Test hooks: splitCoin replaces the demand gate; trace receives every
	// interval an Expand covered, (lo, hi] or (lo, ⊤].
	splitCoin func() bool
	trace     func(lo, hi string, open bool)
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

// Stats count what a listing did. Split and seed counters say which
// algorithm paths engaged and what constrained them.
type Stats struct {
	Objects       int64      `json:"objects"`
	Bytes         int64      `json:"bytes"`
	Pages         int64      `json:"pages"`
	Requests      int64      `json:"requests"`
	Retries       int64      `json:"retries"`
	OverfetchKeys int64      `json:"overfetch_keys"` // listed past a range's hi and trimmed
	Anonymous     bool       `json:"anonymous"`
	Truncated     bool       `json:"truncated_at_max_keys"`
	Seed          SeedStats  `json:"seed"`
	Split         SplitStats `json:"split"`
	ChildEmpty    struct {
		Lower int64 `json:"lower"`
		Upper int64 `json:"upper"`
	} `json:"child_empty"` // split children that listed no key (the cost of not probing)
	MaxActive  int     `json:"max_active"`
	MaxQueued  int     `json:"max_queued"`
	MeanActive float64 `json:"mean_active"` // time-weighted
	BusyNs     int64   `json:"busy_ns"`     // sum of Expand wall time
	SinkWaitNs int64   `json:"sink_wait_ns"`
}

// SeedStats describe the one-page seed.
type SeedStats struct {
	Prefixes   int64            `json:"prefixes"` // common prefixes returned
	Cuts       int64            `json:"cuts"`     // cut points used
	Unsafe     int64            `json:"unsafe"`   // prefixes dropped as unusable start-after values
	Subsampled bool             `json:"subsampled"`
	Truncated  bool             `json:"truncated"`
	Sentinel   bool             `json:"sentinel"`
	Declined   map[string]int64 `json:"sentinel_declined,omitempty"`
	Skipped    bool             `json:"skipped"`
}

// SplitStats count split decisions; every accepted split is a steal for an
// idle worker.
type SplitStats struct {
	Considered       int64 `json:"considered"`
	Extrapolated     int64 `json:"pivot_extrapolated"` // pivots from page density (else midpoint/reflection)
	AcceptedBounded  int64 `json:"accepted_bounded"`
	AcceptedFrontier int64 `json:"accepted_frontier"`
	Rejected         struct {
		Cursor       int64 `json:"cursor"`
		Bound        int64 `json:"bound"`
		Done         int64 `json:"done"`
		NoPivot      int64 `json:"no_pivot"`
		UnsafeCursor int64 `json:"unsafe_cursor"`
	} `json:"rejected"`
	Skipped struct {
		NoDemand      int64 `json:"no_demand"`
		Outranked     int64 `json:"outranked"`
		NotContinuing int64 `json:"not_continuing"`
	} `json:"skipped"`
}

// Provider lists one bucket (or prefix). It implements source.Provider[Range].
type Provider struct {
	opt       Options
	client    API
	retries   *atomic.Int64
	anonymous bool
	objects   atomic.Int64
	ids       atomic.Uint64
	start     time.Time

	mu      sync.Mutex
	stats   Stats
	running map[uint64]*running // ranges in Expand
	queued  int
	area    float64 // ∫ active dt, for the mean
	areaAt  time.Time
}

var _ source.Provider[Range] = (*Provider)(nil)

// New builds the client: the default credential chain, or anonymous access
// when it finds none; the SDK's standard retryer (9 attempts, backoff up to
// 5 s), counted.
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
	p := &Provider{opt: opt, client: opt.Client, retries: new(atomic.Int64), running: map[uint64]*running{}, start: now, areaAt: now}
	p.stats.Seed.Declined = map[string]int64{}
	if p.client != nil {
		return p, nil
	}
	region := opt.Region
	if region == "" {
		region = "us-east-1"
	}
	retryer := func() aws.Retryer {
		return counting{retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts, o.MaxBackoff = 9, 5*time.Second
		}), p.retries}
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithRetryer(retryer))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := cfg.Credentials.Retrieve(probe); err != nil {
		cfg.Credentials = aws.AnonymousCredentials{}
		p.anonymous = true
	}
	p.client = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if opt.Endpoint != "" {
			o.BaseEndpoint = aws.String(opt.Endpoint)
		}
		o.UsePathStyle = opt.PathStyle
	})
	return p, nil
}

// counting is the standard retryer, counting the retries it allows.
type counting struct {
	aws.RetryerV2
	n *atomic.Int64
}

func (c counting) RetryDelay(attempt int, err error) (time.Duration, error) {
	c.n.Add(1)
	return c.RetryerV2.RetryDelay(attempt, err)
}

// Stats returns the counters so far.
func (p *Provider) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	s := p.stats
	if el := time.Since(p.start).Seconds(); el > 0 {
		s.MeanActive = p.area / el
	}
	s.Retries = p.retries.Load()
	s.Anonymous = p.anonymous
	return s
}

func (p *Provider) list(ctx context.Context, in *awss3.ListObjectsV2Input) (*awss3.ListObjectsV2Output, error) {
	p.mu.Lock()
	p.stats.Requests++
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
func (p *Provider) expandSequential(ctx context.Context, e source.Emitter[Range]) error {
	in := &awss3.ListObjectsV2Input{
		Bucket: aws.String(p.opt.Bucket), Prefix: aws.String(p.opt.Prefix),
		MaxKeys: aws.Int32(pageKeys), FetchOwner: aws.Bool(p.opt.FetchOwner),
	}
	last := ""
	for {
		out, err := p.list(ctx, in)
		if err != nil {
			return err
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
		p.stats.Truncated = p.stats.Truncated || stop
		p.mu.Unlock()
		if stop || !aws.ToBool(out.IsTruncated) {
			return nil
		}
		if out.NextContinuationToken == nil {
			return errors.New("truncated page without a continuation token")
		}
		in.ContinuationToken = out.NextContinuationToken
	}
}

// emit counts and hands one page of rows to e.
func (p *Provider) emit(rows []schema.Row, e source.Emitter[Range]) error {
	var bytes int64
	for i := range rows {
		bytes += rows[i].Size
	}
	t := time.Now()
	err := e.Rows(rows)
	p.mu.Lock()
	p.stats.SinkWaitNs += int64(time.Since(t))
	p.stats.Pages++
	p.stats.Objects += int64(len(rows))
	p.stats.Bytes += bytes
	p.mu.Unlock()
	return err
}

// rows converts the first n objects of a page to rows (Swath's mapping).
func (p *Provider) rows(out *awss3.ListObjectsV2Output, n int) []schema.Row {
	rows := make([]schema.Row, n)
	meta := make([]schema.ObjectMeta, n)
	for i, o := range out.Contents[:n] {
		m := &meta[i]
		m.ETag = stripQuotes(aws.ToString(o.ETag))
		m.StorageClass = string(o.StorageClass)
		if p.opt.FetchOwner && o.Owner != nil {
			m.OwnerID, m.OwnerDisplayName = aws.ToString(o.Owner.ID), aws.ToString(o.Owner.DisplayName)
		}
		if len(o.ChecksumAlgorithm) > 0 {
			m.ChecksumAlgorithm = string(o.ChecksumAlgorithm[0])
		}
		m.ChecksumType = string(o.ChecksumType)
		r := &rows[i]
		r.Key, r.Kind, r.Object = aws.ToString(o.Key), schema.KindFile, m
		r.Size = aws.ToInt64(o.Size)
		if o.LastModified != nil {
			r.Mtime = o.LastModified.UnixMicro()
		}
		r.Present = schema.HasSize | schema.HasMtime
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
