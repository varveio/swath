// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3 is the S3 listing provider. A range is a key prefix listed with
// ListObjectsV2, whose pages arrive in UTF-8 binary (unsigned byte) key
// order, so each page is emitted as it arrives. The root range may instead
// be a delimiter seed: one delimited listing of the root whose objects are
// rows and whose common prefixes become child ranges, listed in parallel by
// the order stage.
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

	"github.com/varveio/swath/tools/swath-local-spike/internal/schema"
	"github.com/varveio/swath/tools/swath-local-spike/internal/source"
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
	Seed       bool  // list the root with delimiter "/" and its prefixes in parallel
	MaxKeys    int64 // stop after this many objects (sequential listing only; 0 = all)
	Client     API   // optional; built from the options above when nil
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

// Stats count what a listing did.
type Stats struct {
	Objects   int64 `json:"objects"`
	Bytes     int64 `json:"bytes"`
	Pages     int64 `json:"pages"`
	Prefixes  int64 `json:"seed_prefixes"`
	Retries   int64 `json:"retries"`
	Anonymous bool  `json:"anonymous"`
	Truncated bool  `json:"truncated_at_max_keys"`
}

// Range is a key prefix; seed marks the delimited root listing.
type Range struct {
	prefix string
	seed   bool
}

// Provider lists one bucket (or prefix). It implements source.Provider[Range].
type Provider struct {
	opt       Options
	client    API
	retries   *atomic.Int64
	anonymous bool
	objects   atomic.Int64
	mu        sync.Mutex
	stats     Stats
}

var _ source.Provider[Range] = (*Provider)(nil)

// New builds the client: the default credential chain, or anonymous access
// when it finds no credentials; the SDK's standard retryer, counted.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.Bucket == "" {
		return nil, errors.New("no bucket")
	}
	if opt.Seed && opt.MaxKeys > 0 {
		return nil, errors.New("a key limit needs the sequential listing (no seed)")
	}
	p := &Provider{opt: opt, client: opt.Client, retries: new(atomic.Int64)}
	if p.client != nil {
		return p, nil
	}
	region := opt.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region),
		config.WithRetryer(func() aws.Retryer { return counting{retry.NewStandard(), p.retries} }))
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

// Root is the bucket or prefix range.
func (p *Provider) Root() Range { return Range{prefix: p.opt.Prefix, seed: p.opt.Seed} }

// Stats returns the counters so far.
func (p *Provider) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.Retries = p.retries.Load()
	s.Anonymous = p.anonymous
	return s
}

// Expand lists one range page by page, emitting each page as it arrives.
func (p *Provider) Expand(ctx context.Context, r Range, e source.Emitter[Range]) error {
	in := &awss3.ListObjectsV2Input{
		Bucket: aws.String(p.opt.Bucket), Prefix: aws.String(r.prefix),
		MaxKeys: aws.Int32(pageKeys), FetchOwner: aws.Bool(p.opt.FetchOwner),
	}
	if r.seed {
		in.Delimiter = aws.String("/")
	}
	last := ""
	for {
		out, err := p.client.ListObjectsV2(ctx, in)
		if err != nil {
			return fmt.Errorf("list s3://%s/%s: %w", p.opt.Bucket, r.prefix, err)
		}
		rows, prefixes, stop := p.page(out)
		if r.seed {
			rows = ownLevel(rows, r.prefix)
		}
		if err := emitPage(rows, prefixes, &last, e); err != nil {
			return err
		}
		p.mu.Lock()
		p.stats.Pages++
		p.stats.Objects += int64(len(rows))
		p.stats.Prefixes += int64(len(prefixes))
		for i := range rows {
			p.stats.Bytes += rows[i].Size
		}
		p.stats.Truncated = p.stats.Truncated || stop
		p.mu.Unlock()
		if stop || out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			return nil
		}
		in.ContinuationToken = out.NextContinuationToken
	}
}

// page converts one response; stop reports that the key limit was reached.
func (p *Provider) page(out *awss3.ListObjectsV2Output) (rows []schema.Row, prefixes []string, stop bool) {
	n := len(out.Contents)
	if p.opt.MaxKeys > 0 {
		left := p.opt.MaxKeys - p.objects.Add(int64(n)) + int64(n)
		if left <= int64(n) {
			n, stop = int(max(0, left)), true
		}
	}
	rows = make([]schema.Row, n)
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
	for _, cp := range out.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(cp.Prefix))
	}
	return rows, prefixes, stop
}

// emitPage hands a page to e in key order: objects as rows and, for a seed
// listing, each common prefix as a child range at its position ("a/" sorts
// as the string "a/", like a directory key).
func emitPage(rows []schema.Row, prefixes []string, last *string, e source.Emitter[Range]) error {
	check := func(k string) error {
		if *last != "" && k <= *last {
			return fmt.Errorf("listing out of order: %q after %q", k, *last)
		}
		*last = k
		return nil
	}
	start, j := 0, 0
	for i := range rows {
		for j < len(prefixes) && prefixes[j] < rows[i].Key {
			if err := check(prefixes[j]); err != nil {
				return err
			}
			if err := e.Rows(rows[start:i]); err != nil {
				return err
			}
			start = i
			if err := e.Child(Range{prefix: prefixes[j]}); err != nil {
				return err
			}
			j++
		}
		if err := check(rows[i].Key); err != nil {
			return err
		}
	}
	if err := e.Rows(rows[start:]); err != nil {
		return err
	}
	for ; j < len(prefixes); j++ {
		if err := check(prefixes[j]); err != nil {
			return err
		}
		if err := e.Child(Range{prefix: prefixes[j]}); err != nil {
			return err
		}
	}
	return nil
}

// ownLevel drops objects of a delimited listing whose key holds the
// delimiter after the prefix: they belong to a common prefix and are listed
// by its child range. S3 never returns them as objects; some S3-compatible
// stores (gofakes3) return a key equal to a common prefix ("a/") as both.
func ownLevel(rows []schema.Row, prefix string) []schema.Row {
	out := rows[:0]
	for _, r := range rows {
		if !strings.Contains(r.Key[len(prefix):], "/") {
			out = append(out, r)
		}
	}
	return out
}

// stripQuotes removes one pair of surrounding quotes, as Swath does; the
// multipart form "hex-N" is kept as is.
func stripQuotes(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}
