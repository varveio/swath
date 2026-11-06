// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

func init() {
	registry.Register(registry.Factory{Classify: classify, Scheme: "s3", Flags: flags, Shared: keyrange.FlagGroup, SharedFlags: sharedFlags, Defaults: defaults, Open: open})
}

// flagged is the S3 flag group's value: the S3 options and the range
// engine's policy and HTTP admission, whose flags every engine provider shares.
type flagged struct {
	Options
	common       *keyrange.Common
	pathStyleSet bool // --path-style given (else on with a custom endpoint, as Java's)
	refreshSet   bool // --bearer-token-refresh-interval given
}

// boolFunc is a boolean flag that records that it was given.
type boolFunc struct {
	v   *bool
	set *bool
}

func (b boolFunc) IsBoolFlag() bool { return true }
func (b boolFunc) String() string {
	if b.v == nil {
		return "false"
	}
	return strconv.FormatBool(*b.v)
}
func (b boolFunc) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*b.v, *b.set = v, true
	return nil
}

// sharedFlags registers the range engine's flag group (keyrange.Flags).
func sharedFlags(f *flag.FlagSet, diag bool) any { return keyrange.Flags(f, diag) }

func defaults() any { return &flagged{Options: Options{Policy: keyrange.Policy{SeedDescent: true}}} }

// flags registers the S3 flag group and the shared engine flags; the
// returned value is filled when the flag set is parsed.
func flags(f *flag.FlagSet, diag bool, shared any) any {
	common, _ := shared.(*keyrange.Common)
	o := &flagged{common: common}
	f.StringVar(&o.Endpoint, "endpoint", "", "S3: endpoint `URL` of an S3-compatible store (path-style addressing unless --path-style=false)")
	f.StringVar(&o.Endpoint, "endpoint-url", "", "S3: Java swath's name for --endpoint")
	f.StringVar(&o.Region, "region", "", "S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)")
	f.StringVar(&o.Profile, "profile", "", "S3: shared config `profile` (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)")
	f.BoolVar(&o.Anonymous, "anonymous", false, "S3: send unsigned requests, ignoring any credentials")
	f.BoolVar(&o.RequestPayer, "request-payer", false, "S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)")
	f.DurationVar(&o.Timeout, "request-timeout", 30*time.Second, "S3: per-request timeout; a request that exceeds it is retried")
	f.Var(boolFunc{&o.PathStyle, &o.pathStyleSet}, "path-style", "S3: path-style addressing (default: on with --endpoint, as Java's --force-path-style)")
	f.StringVar(&o.BearerCommand, "bearer-token-command", "", "S3: shell `command` whose stdout is a bearer token, sent instead of SigV4 signatures (GCS's XML API: 'gcloud auth print-access-token'); never recorded, so swath resume needs it again")
	f.Func("bearer-token-refresh-interval", "S3: run --bearer-token-command again after this `duration` (default 45m)", func(v string) error {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("want a positive duration")
		}
		o.BearerRefresh, o.refreshSet = d, true
		return nil
	})
	f.BoolVar(&o.FIPS, "fips", false, "S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)")
	f.BoolVar(&o.DualStack, "dualstack", false, "S3: use the dual-stack (IPv6) endpoint")
	f.BoolVar(&o.FetchOwner, "fetch-owner", false, "S3: fill owner_id and owner_display_name")
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
			opt.Policy, opt.Admission = o.common.Policy, o.common.Admission
		}
		if !o.pathStyleSet && opt.Endpoint != "" {
			opt.PathStyle = true // Java's default for a custom endpoint
		}
		switch {
		case o.refreshSet && opt.BearerCommand == "":
			return nil, source.Unsupported(errors.New("--bearer-token-refresh-interval requires --bearer-token-command"))
		case opt.BearerCommand != "" && (opt.Anonymous || opt.Profile != ""):
			return nil, source.Unsupported(errors.New("--bearer-token-command excludes --anonymous and --profile: the token is the credential"))
		}
	case *Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("s3: options are %T, not *s3.Options", req.Options)
	}
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

func (o *opened) Listed() int64 { return o.p.Listed() }

func (o *opened) Answered() int64 { return o.p.Answered() }

// Requests is the live request account (source.Requested).
func (o *opened) Requests() source.Requests { return o.p.Requests() }

// Tariff is the reference list rate the report labels its estimate with
// (source.Priced): only on the provider's own service; a custom endpoint
// has no known tariff, and the report then gives no figure.
func (o *opened) Tariff() (usdPer1k float64, basis string) {
	if o.p.opt.Endpoint == "" && o.p.opt.Client == nil {
		return 0.005, "aws-list-reference-rate"
	}
	return 0, ""
}

func (o *opened) Faults() (throttles, timeouts int64) { return o.p.Faults() }

// Contention is the engine's decision-mutex wait and longest hold
// (source.Contended).
func (o *opened) Contention() (wait, maxHold time.Duration) { return o.p.Contention() }

func (o *opened) target() string {
	t := "s3://" + o.p.opt.Bucket
	if o.p.opt.Prefix != "" {
		t += "/" + o.p.opt.Prefix
	}
	return t
}

// Identity: the bucket and prefix, the effective route and the owner
// columns. The route is what the client builds, not what was asked: the URL
// the bucket's requests go to (endpoint, its path bytes, path or
// virtual-host addressing; a different endpoint, another AWS partition or
// the other addressing style can serve a different bucket of the same
// name) and the signing region. With neither --endpoint nor --region
// given, both come from the environment or the profile, so a resume in an
// environment that resolves them differently is refused. A supplied client
// must name its route (Routed), or the run declines resume.
func (o *opened) Identity() source.Identity {
	opt := o.p.opt
	return source.Identity{Provider: "s3", Fields: []source.IdentityField{
		{Name: "source", Value: "s3://" + opt.Bucket + "/" + opt.Prefix},
		{Name: "route", Value: o.p.route, Canonical: objhttp.CanonicalRoute(o.p.route)},
		{Name: "region", Value: o.p.region},
		{Name: "fetch owner", Value: strconv.FormatBool(opt.FetchOwner)},
	}}
}

// Capabilities: S3 has no rank oracle and start-after is a fabricable
// seek, so the range engine applies; -sequential lists one token chain. A
// run with an object-count cap (-max-keys) cannot resume: the cap counts
// the objects one process listed, and a resume could neither honour it
// nor tell a capped final part from a complete one. Such a run keeps no
// journal, and a resume of one is refused; so does a run through a
// supplied client that does not name its route (Routed).
func (o *opened) Capabilities() source.Capabilities {
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && o.p.route != "")
}

// Restart continues after the last published key, from the normal
// start-up: one range (after, ⊤], split as on a fresh run.
func (o *opened) Restart(after string) error {
	o.p.Restart(after)
	return nil
}

func (o *opened) Report() source.Report {
	st := o.p.Stats()
	return keyrange.Report("s3", o.target(), o.p.opt.Prefix, &st, st.Counters)
}

var (
	_ source.Identified = (*opened)(nil)
	_ source.Resumable  = (*opened)(nil)
	_ source.Capable    = (*opened)(nil)
	_ source.Reporter   = (*opened)(nil)
)

// classify names the class of an S3 failure (client.Classify) for the
// run's outcome.
func classify(err error) string {
	switch c := client.Classify(err); c {
	case objhttp.ClassOther, objhttp.ClassCanceled:
		return ""
	default:
		return string(c)
	}
}
