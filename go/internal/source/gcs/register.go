// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&Schema), Classify: classify, Scheme: "gs", KeyPrefix: keyPrefix, Flags: flags, Shared: keyrange.FlagGroup, SharedFlags: sharedFlags, Defaults: defaults, Open: open, Usage: "gs://bucket[/prefix]"})
}

// flagged is the GCS flag group's value: the GCS options and the range
// engine's policy and HTTP admission, whose flags every engine provider shares.
type flagged struct {
	Options
	common *keyrange.Common
}

// sharedFlags registers the range engine's flag group (keyrange.Flags).
func sharedFlags(f *flag.FlagSet, diag bool) any { return keyrange.Flags(f, diag) }

func defaults() any { return &flagged{} }

// flags registers the GCS flag group and the shared engine flags. The GCS
// flags are prefixed: one flag set serves every provider.
func flags(f *flag.FlagSet, diag bool, shared any) any {
	common, _ := shared.(*keyrange.Common)
	o := &flagged{common: common}
	f.StringVar(&o.Endpoint, "gcs-endpoint", "", "GCS: JSON API base `URL` (default "+client.DefaultEndpoint+")")
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
		if o.common != nil {
			opt.Policy, opt.Admission, opt.AllVersions, opt.Anonymous = o.common.Policy, o.common.Admission, o.common.AllVersions, o.common.Anonymous
		}
	case *Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("gcs: options are %T, not *gcs.Options", req.Options)
	}
	opt.Bucket, opt.Prefix, opt.Workers = bucket, prefix, req.Workers
	if opt.AllVersions {
		p, err := NewGenerations(ctx, opt)
		if err != nil {
			return nil, err
		}
		return &opened[Generation]{Bound: registry.Bind[Range, Generation](p, &GenerationSchema), p: p, root: req.Root}, nil
	}
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &opened[Object]{Bound: registry.Bind[Range, Object](p, &Schema), p: p, root: req.Root}, nil
}

type opened[P any] struct {
	registry.Bound[P]
	p    *provider[P]
	root string
}

func (o *opened[P]) Locator() source.Locator {
	return source.Locator{Kind: "gcs", Root: o.root, Container: o.p.opt.Bucket}
}

func (o *opened[P]) Listed() int64 { return o.p.Listed() }

func (o *opened[P]) Answered() int64 { return o.p.Answered() }

// Frontier is the engine's key-space progress (source.Advancing).
func (o *opened[P]) Frontier() (advanced, emptyChained int64) { return o.p.Frontier() }

// Requests is the live request account (source.Requested).
func (o *opened[P]) Requests() source.Requests { return o.p.Requests() }

// SeedTime is the engine's seed time (source.SeedTimed).
func (o *opened[P]) SeedTime() (time.Duration, bool) { return o.p.SeedTime() }

// Tariff is the reference list rate the report labels its estimate with
// (source.Priced): only on the provider's own service; a custom endpoint
// has no known tariff, and the report then gives no figure.
func (o *opened[P]) Tariff() (usdPer1k float64, basis string) {
	if (o.p.opt.Endpoint == "" || o.p.opt.Endpoint == client.DefaultEndpoint) && o.p.opt.Client == nil {
		return 0.005, "gcs-class-a-reference-rate"
	}
	return 0, ""
}

func (o *opened[P]) Faults() (throttles, timeouts int64) { return o.p.Faults() }

// Contention is the engine's decision-mutex wait and longest hold
// (source.Contended).
func (o *opened[P]) Contention() (wait, maxHold time.Duration) { return o.p.Contention() }

func (o *opened[P]) target() string {
	t := "gs://" + o.p.opt.Bucket
	if o.p.opt.Prefix != "" {
		t += "/" + o.p.opt.Prefix
	}
	return t
}

// Identity freezes what a resume must list the same, from the recorded
// inputs: the source (bucket and prefix), the JSON API endpoint as the
// built client sends to it (client.Endpoint: the default when none is
// given, a literal trailing slash added, escapes kept; scheme and host
// case are the only other equivalence) and the billed project.
// Credentials may change.
func (o *opened[P]) Identity() source.Identity {
	opt := o.p.opt
	ep, err := client.Endpoint(opt.Endpoint)
	if err != nil {
		ep = opt.Endpoint // unreachable: the client was built from it
	}
	return source.Identity{Provider: "gcs", Fields: []source.IdentityField{
		{Name: "source", Value: "gs://" + opt.Bucket + "/" + opt.Prefix},
		{Name: "endpoint", Value: ep, Canonical: objhttp.CanonicalRoute(ep)},
		{Name: "user project", Value: opt.UserProject},
	}}
}

// Capabilities: names list in byte order and startOffset takes any name,
// so the range engine applies; --sequential lists one token chain. A run
// with an object-count cap, or through a supplied client (whose endpoint
// and billed project are not the recorded inputs), cannot resume.
func (o *opened[P]) Capabilities() source.Capabilities {
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && o.p.opt.Client == nil)
}

func (o *opened[P]) Restart(after string) error {
	o.p.Restart(after)
	return nil
}

// SeedFrom starts the listing from a prior's cuts instead of the seed
// (source.Seeded, --seed-from).
func (o *opened[P]) SeedFrom(pr *source.Prior) error { return o.p.SeedFrom(pr) }

// Reuse lists around a resume's reused coverage certificates
// (source.Reusing): the engine replays them, lists the gaps.
func (o *opened[P]) Reuse(spans []source.Span) { o.p.Reuse(spans) }

// Fences reports the engine's coverage fences (source.Fencing).
func (o *opened[P]) Fences() bool { return o.p.Fences() }

func (o *opened[P]) Report() source.Report {
	st := o.p.Stats()
	return keyrange.Report("gcs", o.target(), o.p.opt.Prefix, &st, st.Counters)
}

var (
	_ source.Identified = (*opened[Object])(nil)
	_ source.Resumable  = (*opened[Object])(nil)
	_ source.Seeded     = (*opened[Object])(nil)
	_ source.Reusing    = (*opened[Object])(nil)
	_ source.Capable    = (*opened[Object])(nil)
	_ source.Reporter   = (*opened[Object])(nil)
)

// classify names the class of a GCS failure (client.Classify) for the
// run's outcome.
func classify(err error) string {
	switch c := client.Classify(err); c {
	case objhttp.ClassOther, objhttp.ClassCanceled:
		return ""
	default:
		return string(c)
	}
}

// keyPrefix is the key prefix a root names (registry.Factory.KeyPrefix).
func keyPrefix(root string) (string, error) {
	_, prefix, err := ParseURL(root)
	return prefix, err
}
