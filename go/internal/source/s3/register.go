// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

func init() {
	source.Register(source.Factory{Scheme: "s3", Flags: flags, Defaults: func() any { return &Options{} }, Open: open})
}

// flags registers the S3 flag group; the returned *Options is filled when
// the flag set is parsed.
func flags(f *flag.FlagSet) any {
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
	f.BoolVar(&o.Sequential, "sequential", false, "S3: list with one continuation-token chain (no ranges; the reference)")
	f.BoolVar(&o.NoProbe, "no-probe", false, "S3: split without the 1-key probe that checks the upper child is not empty")
	f.Int64Var(&o.MaxKeys, "max-keys", 0, "S3: with -sequential, stop after this many objects (experiments; the dataset is marked incomplete)")
	return o
}

func open(ctx context.Context, req source.Request) (source.Source, error) {
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
	return &opened{Provider: source.Erase[Range](p), p: p, root: req.Root}, nil
}

// opened is the S3 provider as the pipeline sees it.
type opened struct {
	source.Provider[any]
	p    *Provider
	root string
}

func (o *opened) Schema() *schema.Schema { return &schema.S3 }

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

// Checkpoint descriptors are version 1: the version byte, then the range
// boundaries (split cursors and pivots, seed cuts) created since the
// previous checkpoint, each a uvarint length and the key, at most
// maxCheckpointBounds of them (the most recent), so the journal grows with
// parts, not splits. Boundaries only shape the restart's tiling.
const (
	checkpointV1        = 1
	maxCheckpointBounds = 4096
)

func (o *opened) Checkpoint() []byte {
	b := o.p.TakeBoundaries()
	if len(b) > maxCheckpointBounds {
		b = b[len(b)-maxCheckpointBounds:]
	}
	if len(b) == 0 {
		return nil
	}
	buf := []byte{checkpointV1}
	for _, k := range b {
		buf = binary.AppendUvarint(buf, uint64(len(k)))
		buf = append(buf, k...)
	}
	return buf
}

var errCheckpoint = errors.New("s3: unreadable checkpoint descriptor")

func (o *opened) Restart(after string, checkpoints [][]byte) error {
	var bounds []string
	for _, c := range checkpoints {
		if len(c) == 0 {
			continue
		}
		if c[0] != checkpointV1 {
			return fmt.Errorf("s3: checkpoint version %d is not supported (this tool writes %d)", c[0], checkpointV1)
		}
		for c = c[1:]; len(c) > 0; {
			n, k := binary.Uvarint(c)
			if k <= 0 || uint64(len(c)-k) < n {
				return errCheckpoint
			}
			bounds = append(bounds, string(c[k:k+int(n)]))
			c = c[k+int(n):]
		}
	}
	o.p.opt.Resume = &Resume{After: after, Boundaries: bounds}
	return nil
}

func (o *opened) Report() source.Report {
	st := o.p.Stats()
	opt := o.p.opt
	r := source.Report{Name: "s3", Stats: &st,
		Brief: fmt.Sprintf("objects=%d bytes=%d pages=%d requests=%d retries=%d splits=%d child_empty=%d max_active=%d truncated=%v",
			st.Objects, st.Bytes, st.Pages, st.Requests, st.Retries, st.Split.AcceptedBounded+st.Split.AcceptedFrontier,
			st.ChildEmpty.Lower+st.ChildEmpty.Upper, st.MaxActive, st.Truncated),
		Coverage: source.Coverage{Target: o.target(), Prefix: opt.Prefix, Complete: !st.Truncated, StopReason: "completed"},
		Activity: &source.Activity{Items: st.Objects, Requests: st.Requests, Pages: st.Pages, PeakInFlight: st.MaxActive, AvgInFlight: st.MeanActive,
			Splits: st.Split.AcceptedBounded + st.Split.AcceptedFrontier, Region: st.Region, Unsigned: st.Anonymous,
			Flags:           map[string]bool{"owner_split": !opt.Sequential, "density_extrapolation": !opt.Sequential, "seed": opt.Seed, "sequential": opt.Sequential},
			RecoveredErrors: map[string]int64{"retries": st.Retries, "throttle_events": st.Throttles, "attempt_timeout_events": st.Timeouts, "parse_failures": st.ParseFailures},
			SeedMode:        "none"},
	}
	if st.Truncated {
		r.Coverage.StopReason = "max_keys"
	}
	if opt.Seed {
		r.Activity.SeedMode, r.Activity.SeedCuts, r.Activity.SeedRanges = "shallow", st.Seed.Cuts, st.Seed.Cuts+1
		if st.Seed.Sentinel {
			r.Activity.SeedRanges++
		}
	}
	return r
}

var (
	_ source.Identified = (*opened)(nil)
	_ source.Resumable  = (*opened)(nil)
	_ source.Capable    = (*opened)(nil)
	_ source.Reporter   = (*opened)(nil)
)
