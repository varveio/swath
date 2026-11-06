// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

func init() {
	registry.Register(registry.Factory{Classify: classify, Scheme: "gs", Flags: flags, Defaults: defaults, Open: open})
}

// flagged is the GCS flag group's value: the GCS options and the range
// engine's policy, whose flags every engine provider shares.
type flagged struct {
	Options
	policy *keyrange.Policy
}

func defaults() any { return &flagged{Options: Options{Policy: keyrange.Policy{SeedDescent: true}}} }

// flags registers the GCS flag group and the shared engine flags. The GCS
// flags are prefixed: one flag set serves every provider.
func flags(f *flag.FlagSet, diag bool) any {
	o := &flagged{policy: keyrange.Flags(f, diag)}
	f.StringVar(&o.Endpoint, "gcs-endpoint", "", "GCS: JSON API base `URL` (default "+client.DefaultEndpoint+")")
	f.BoolVar(&o.Anonymous, "gcs-anonymous", false, "GCS: send no credentials, ignoring any in the environment")
	f.StringVar(&o.CredentialsFile, "gcs-credentials", "", "GCS: service-account or authorized-user JSON `file` (default: Application Default Credentials)")
	f.StringVar(&o.UserProject, "gcs-user-project", "", "GCS: bill a requester-pays bucket's requests to this `project`")
	f.DurationVar(&o.Timeout, "gcs-request-timeout", 30*time.Second, "GCS: per-request timeout; a request that exceeds it is retried")
	return o
}

func open(ctx context.Context, req registry.Request) (registry.Source, error) {
	bucket, prefix, err := ParseURL(req.Root)
	if err != nil {
		return nil, err
	}
	var opt Options
	switch o := req.Options.(type) {
	case *flagged: // the command line
		opt = o.Options
		if o.policy != nil {
			opt.Policy = *o.policy
		}
	case *Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("gcs: options are %T, not *gcs.Options", req.Options)
	}
	opt.Bucket, opt.Prefix, opt.Workers = bucket, prefix, req.Workers
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &opened{Bound: registry.Bind[Range, Object](p, &Schema), p: p, root: req.Root}, nil
}

type opened struct {
	registry.Bound[Object]
	p    *Provider
	root string
}

func (o *opened) Locator() source.Locator {
	return source.Locator{Kind: "gcs", Root: o.root, Container: o.p.opt.Bucket}
}

func (o *opened) Listed() int64 { return o.p.Listed() }

func (o *opened) target() string {
	t := "gs://" + o.p.opt.Bucket
	if o.p.opt.Prefix != "" {
		t += "/" + o.p.opt.Prefix
	}
	return t
}

// Identity freezes what a resume must list the same: the source, the
// route requests go to (the JSON API base and the bucket), and the billed
// project. A supplied client must name its route (Routed), or the run
// declines resume.
func (o *opened) Identity() source.Identity {
	opt := o.p.opt
	return source.Identity{Provider: "gcs", Fields: []source.IdentityField{
		{Name: "source", Value: "gs://" + opt.Bucket + "/" + opt.Prefix},
		{Name: "route", Value: o.p.route, Canonical: canonicalRoute(o.p.route)},
		{Name: "user project", Value: opt.UserProject},
	}}
}

// Capabilities: names list in byte order and startOffset takes any name,
// so the range engine applies; --sequential lists one token chain. A run
// with an object-count cap, or through a supplied client that does not
// name its route, cannot resume (as for S3).
func (o *opened) Capabilities() source.Capabilities {
	c := source.Capabilities{Resume: o.p.opt.MaxKeys == 0 && o.p.route != "", Seek: source.SeekFabricable, Order: source.OrderBytes}
	if o.p.opt.Sequential {
		c.Seek = source.SeekTokenOnly
	}
	return c
}

func (o *opened) Restart(after string) error {
	o.p.Restart(after)
	return nil
}

func (o *opened) Report() source.Report {
	st := o.p.Stats()
	v := st.Counters.Value
	truncated := v("truncated_at_max_keys") != 0
	r := source.Report{Name: "gcs", Stats: &st,
		Brief: fmt.Sprintf("objects=%d bytes=%d pages=%d requests=%d retries=%d splits=%d child_empty=%d max_active=%d truncated=%v",
			v("objects"), v("bytes"), v("pages"), v("requests"), v("retries"), v("split.accepted_bounded")+v("split.accepted_frontier"),
			v("child_empty.lower")+v("child_empty.upper"), v("max_active"), truncated),
		Coverage: source.Coverage{Target: o.target(), Prefix: o.p.opt.Prefix, Complete: !truncated, StopReason: "completed"},
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

// classify names a failure's class for the outcome (error.json cause).
func classify(err error) string {
	var ce *client.CredentialError
	if errors.As(err, &ce) {
		return "auth"
	}
	var e *client.Error
	if !errors.As(err, &e) {
		return ""
	}
	switch {
	case e.Throttle():
		return "throttle"
	case e.Status == 401:
		return "auth"
	case e.Status == 403:
		return "access_denied"
	case e.Status == 404:
		return "no_such_bucket"
	case e.Status >= 500:
		return "server"
	}
	return ""
}

// canonicalRoute: the scheme and host are case-insensitive; the path is
// kept byte for byte (the client already added the base's trailing slash).
func canonicalRoute(r string) string {
	i := strings.Index(r, "://")
	if i < 0 {
		return r
	}
	host, path, cut := strings.Cut(r[i+3:], "/")
	r = strings.ToLower(r[:i]) + "://" + strings.ToLower(host)
	if cut {
		r += "/" + path
	}
	return r
}
