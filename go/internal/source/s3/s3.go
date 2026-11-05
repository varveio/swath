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
	After      string   // the last published key: every key <= After is in the dataset
	Boundaries []string // recorded range boundaries; those above After tile the rest
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
	Throttles     int64      `json:"throttles"`
	Timeouts      int64      `json:"timeouts"`
	Attempts      int64      `json:"attempts"`       // HTTP requests, retries included
	ParseFailures int64      `json:"parse_failures"` // 200 bodies that did not parse, retried
	OverfetchKeys int64      `json:"overfetch_keys"` // listed past a range's hi and trimmed
	Anonymous     bool       `json:"anonymous"`
	Credentials   string     `json:"credentials,omitempty"` // where they came from
	Region        string     `json:"region,omitempty"`
	Truncated     bool       `json:"truncated_at_max_keys"`
	Seed          SeedStats  `json:"seed"`
	Split         SplitStats `json:"split"`
	ChildEmpty    struct {
		Lower int64 `json:"lower"`
		Upper int64 `json:"upper"`
	} `json:"child_empty"` // split children that listed no key (the cost of not probing)
	ResumedRanges int64   `json:"resumed_ranges,omitempty"` // tiles of (after, ⊤] on resume
	MaxActive     int     `json:"max_active"`
	MaxQueued     int     `json:"max_queued"`
	MeanActive    float64 `json:"mean_active"` // time-weighted
	BusyNs        int64   `json:"busy_ns"`     // sum of Expand wall time
	SinkWaitNs    int64   `json:"sink_wait_ns"`

	tally map[string]any // the counter table, rendered for MarshalJSON
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
	bounds  []string            // range boundaries created since the last TakeBoundaries
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

// TakeBoundaries returns the range boundaries (split cursors and pivots,
// seed cuts) created since the previous call. The journal records them so a
// resume starts with the same tiling.
func (p *Provider) TakeBoundaries() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.bounds
	p.bounds = nil
	return b
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
	c := p.c.Load
	var s Stats
	s.Objects, s.Bytes, s.Pages, s.Requests = c(cObjects), c(cBytes), c(cPages), c(cRequests)
	s.Attempts, s.Retries, s.Throttles, s.Timeouts, s.ParseFailures = c(cAttempts), c(cRetries), c(cThrottles), c(cTimeouts), c(cParseFailures)
	s.OverfetchKeys, s.Truncated = c(cOverfetchKeys), c(cTruncated) != 0
	s.Seed = SeedStats{Prefixes: c(cSeedPrefixes), Cuts: c(cSeedCuts), Unsafe: c(cSeedUnsafe), Subsampled: c(cSeedSubsampled) != 0,
		Truncated: c(cSeedTruncated) != 0, Sentinel: c(cSeedSentinel) != 0,
		Declined: map[string]int64{}}
	for id, k := range map[int]string{cSentinelDeclinedTruncated: "truncated", cSentinelDeclinedObjectLast: "object_last",
		cSentinelDeclinedNoCeiling: "no_ceiling", cSentinelDeclinedUnsafe: "unsafe"} {
		if v := c(id); v > 0 {
			s.Seed.Declined[k] = v
		}
	}
	sp := &s.Split
	sp.Considered, sp.Extrapolated, sp.AcceptedBounded, sp.AcceptedFrontier = c(cSplitConsidered), c(cSplitExtrapolated), c(cSplitAcceptedBounded), c(cSplitAcceptedFrontier)
	sp.Rejected.Cursor, sp.Rejected.Bound, sp.Rejected.Done = c(cSplitRejectedCursor), c(cSplitRejectedBound), c(cSplitRejectedDone)
	sp.Rejected.NoPivot, sp.Rejected.UnsafeCursor = c(cSplitRejectedNoPivot), c(cSplitRejectedUnsafeCursor)
	sp.Skipped.NoDemand, sp.Skipped.Outranked, sp.Skipped.NotContinuing = c(cSplitSkippedNoDemand), c(cSplitSkippedOutranked), c(cSplitSkippedNotContinuing)
	s.ChildEmpty.Lower, s.ChildEmpty.Upper, s.ResumedRanges = c(cChildEmptyLower), c(cChildEmptyUpper), c(cResumedRanges)
	s.MaxActive, s.MaxQueued, s.BusyNs, s.SinkWaitNs = int(c(cMaxActive)), int(c(cMaxQueued)), c(cBusyNs), c(cSinkWaitNs)
	if el := time.Since(p.start).Seconds(); el > 0 {
		s.MeanActive = p.area / el
	}
	s.Anonymous, s.Credentials, s.Region = p.anonymous, p.creds, p.region
	s.tally = p.c.Map(p.disabled()...)
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
