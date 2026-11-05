// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
)

func init() {
	registry.Register(registry.Factory{Scheme: "s3", Flags: flags, Defaults: func() any { return &Options{} }, Open: open})
}

// flags registers the S3 flag group; the returned *Options is filled when
// the flag set is parsed. The comparison modes and policy toggles are
// diagnostic (swath diag list).
func flags(f *flag.FlagSet, diag bool) any {
	o := &Options{}
	f.StringVar(&o.Endpoint, "endpoint", "", "S3: endpoint `URL` of an S3-compatible store")
	f.StringVar(&o.Region, "region", "", "S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)")
	f.StringVar(&o.Profile, "profile", "", "S3: shared config `profile` (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)")
	f.BoolVar(&o.Anonymous, "anonymous", false, "S3: send unsigned requests, ignoring any credentials")
	f.DurationVar(&o.Timeout, "request-timeout", 30*time.Second, "S3: per-request timeout; a request that exceeds it is retried")
	f.BoolVar(&o.PathStyle, "path-style", false, "S3: path-style addressing")
	f.BoolVar(&o.FIPS, "fips", false, "S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)")
	f.BoolVar(&o.DualStack, "dualstack", false, "S3: use the dual-stack (IPv6) endpoint")
	f.BoolVar(&o.FetchOwner, "fetch-owner", false, "S3: fill owner_id and owner_display_name")
	f.BoolVar(&o.Seed, "seed", false, "S3: tile the keyspace from one delimiter page before splitting")
	if diag {
		f.BoolVar(&o.Sequential, "sequential", false, "S3: list with one continuation-token chain (no ranges; the reference)")
		f.BoolVar(&o.Probe, "probe", false, "S3: before each split, a 1-key request checks the upper child is not empty (off: it costs more than it saves, REPORT.md Step 5a)")
		f.Int64Var(&o.MaxKeys, "max-keys", 0, "S3: with -sequential, stop after this many objects (experiments; the dataset is marked incomplete)")
	}
	return o
}

func open(ctx context.Context, req registry.Request) (registry.Source, error) {
	bucket, prefix, err := ParseURL(req.Root)
	if err != nil {
		return nil, err
	}
	o, ok := req.Options.(*Options)
	if !ok {
		return nil, fmt.Errorf("s3: options are %T, not *s3.Options", req.Options)
	}
	opt := *o
	opt.Bucket, opt.Prefix, opt.Workers, opt.Warn = bucket, prefix, req.Workers, req.Warn
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &opened{Bound: registry.Bind[Range, Object](p, &Schema), p: p, root: req.Root}, nil
}

// opened is the S3 provider as the pipeline sees it.
type opened struct {
	registry.Bound[Object]
	p    *Provider
	root string
}

func (o *opened) Locator() source.Locator {
	return source.Locator{Kind: "s3", Root: o.root, Container: o.p.opt.Bucket}
}

func (o *opened) Listed() int64 { return o.p.c.Load(cObjects) }

func (o *opened) target() string {
	t := "s3://" + o.p.opt.Bucket
	if o.p.opt.Prefix != "" {
		t += "/" + o.p.opt.Prefix
	}
	return t
}

// Identity: the bucket and prefix, the endpoint (a different endpoint can
// serve a different bucket of the same name; equivalent spellings are
// allowed), the requested region and the owner columns.
func (o *opened) Identity() source.Identity {
	opt := o.p.opt
	return source.Identity{Provider: "s3", Fields: []source.IdentityField{
		{Name: "source", Value: "s3://" + opt.Bucket + "/" + opt.Prefix},
		{Name: "endpoint", Value: opt.Endpoint, Canonical: NormalizeEndpoint(opt.Endpoint)},
		{Name: "region", Value: opt.Region},
		{Name: "fetch owner", Value: strconv.FormatBool(opt.FetchOwner)},
	}}
}

// NormalizeEndpoint is the allowlist of equivalent endpoint spellings: the
// scheme and host are case-insensitive and a trailing slash is ignored.
// Nothing else is assumed equivalent.
func NormalizeEndpoint(e string) string {
	e = strings.TrimSuffix(e, "/")
	if i := strings.Index(e, "://"); i >= 0 {
		rest := e[i+3:]
		host, path, _ := strings.Cut(rest, "/")
		e = strings.ToLower(e[:i]) + "://" + strings.ToLower(host)
		if path != "" {
			e += "/" + path
		}
	}
	return e
}

func (o *opened) Capabilities() source.Capabilities { return source.Capabilities{Resume: true} }

// Restart continues after the last published key, from the normal
// start-up: one range (after, ⊤], split as on a fresh run.
func (o *opened) Restart(after string) error {
	o.p.opt.Resume = &Resume{After: after}
	return nil
}

func (o *opened) Report() source.Report {
	st := o.p.Stats()
	opt := o.p.opt
	truncated := st.Get(cTruncated) != 0
	r := source.Report{Name: "s3", Stats: &st,
		Brief: fmt.Sprintf("objects=%d bytes=%d pages=%d requests=%d retries=%d splits=%d child_empty=%d max_active=%d truncated=%v",
			st.Get(cObjects), st.Get(cBytes), st.Get(cPages), st.Get(cRequests), st.Get(cRetries), st.Get(cSplitAcceptedBounded)+st.Get(cSplitAcceptedFrontier),
			st.Get(cChildEmptyLower)+st.Get(cChildEmptyUpper), st.Get(cMaxActive), truncated),
		Coverage: source.Coverage{Target: o.target(), Prefix: opt.Prefix, Complete: !truncated, StopReason: "completed"},
	}
	if truncated {
		r.Coverage.StopReason = "max_keys"
	}
	return r
}

var (
	_ source.Identified = (*opened)(nil)
	_ source.Resumable  = (*opened)(nil)
	_ source.Capable    = (*opened)(nil)
	_ source.Reporter   = (*opened)(nil)
)
