// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&Schema), Classify: objhttp.Label, Scheme: "s3", KeyPrefix: keyPrefix, WithPrefix: withPrefix,
		Flags: flags, Shared: []*source.FlagGroup{source.StoreGroup, keyrange.Group}, Defaults: defaults, Open: open,
		Secrets:     []registry.Secret{{Flag: "bearer-token-command"}},
		Operational: []string{"profile", "bearer-token-refresh-interval"},
		Hints: []source.Hint{{Flag: "region"}, {Flag: "path-style"}, {Flag: "profile"}, {Flag: "request-payer"},
			{Flag: "bearer-token-command"}, {Flag: "bearer-token-refresh-interval"}, {Flag: "fips"}, {Flag: "dualstack"}, {Flag: "fetch-owner"}},
		Usage: "s3://bucket[/prefix]"})
}

// flagged is the S3 flag group's value: the S3 options, the store group's
// values and the range engine's policy and HTTP admission, whose flags
// every engine provider shares.
type flagged struct {
	Options
	store        *source.Store
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

func defaults() any { return &flagged{} }

// flags registers the S3 flag group beside the shared groups' values; the
// returned value is filled when the flag set is parsed.
func flags(f *flag.FlagSet, diag bool, shared source.SharedValues) any {
	o := &flagged{store: source.Shared[source.Store](shared, source.StoreGroup), common: source.Shared[keyrange.Common](shared, keyrange.Group)}
	f.StringVar(&o.Region, "region", "", "S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)")
	f.StringVar(&o.Profile, "profile", "", "S3: shared config `profile` (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)")
	f.BoolVar(&o.RequestPayer, "request-payer", false, "S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)")
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
		if o.store != nil {
			opt.Endpoint, opt.Timeout, opt.Anonymous = o.store.Endpoint, o.store.Timeout, o.store.Anonymous
		}
		if o.common != nil {
			opt.Policy, opt.Admission, opt.AllVersions = o.common.Policy, o.common.Admission, o.common.AllVersions
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
	if opt.AllVersions {
		p, err := NewVersions(ctx, opt)
		if err != nil {
			return nil, err
		}
		return &opened[Version]{Bound: registry.Bind[Range, Version](p, &VersionsSchema), Registered: p, p: p, root: req.Root}, nil
	}
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &opened[Object]{Bound: registry.Bind[Range, Object](p, &Schema), Registered: p, p: p, root: req.Root}, nil
}

// opened is the S3 provider as the pipeline sees it.
type opened[P any] struct {
	registry.Bound[P]
	keyrange.Registered
	p    *provider[P]
	root string
}

func (o *opened[P]) Locator() source.Locator {
	return source.Locator{Kind: "s3", Root: o.root, Container: o.p.opt.Bucket}
}

// Tariff is the reference list rate the report labels its estimate with
// (source.Priced): only on the provider's own service; a custom endpoint
// has no known tariff, and the report then gives no figure.
func (o *opened[P]) Tariff() (usdPer1k float64, basis string) {
	if o.p.opt.Endpoint == "" && o.p.opt.Client == nil {
		return 0.005, "aws-list-reference-rate"
	}
	return 0, ""
}

func (o *opened[P]) target() string {
	t := "s3://" + o.p.opt.Bucket
	if o.p.opt.Prefix != "" {
		t += "/" + o.p.opt.Prefix
	}
	return t
}

// Identity: the inputs the listing was asked with, as given. The bucket
// and prefix; the endpoint (--endpoint, "" for AWS's own), the signing
// region and the addressing flags (path-style, FIPS, dual-stack), which
// together decide which service, partition and bucket of a name the SDK
// lists (another endpoint, partition or addressing style can serve a
// different bucket of the same name); and the owner columns. The region is
// --region, else what the environment or the profile resolves, so a resume
// in an environment that resolves another region is refused even when the
// flag was not given. The inputs are compared as given: nothing resolves
// them to a URL first (a FIPS region alias is its own value). The
// endpoint's canonical form folds the scheme and host case
// (objhttp.CanonicalRoute) and only a trailing slash the built client
// sends the same requests without (endpointCanonical). A supplied client
// is identified by the Route it names (Routed) in place of the endpoint,
// its path byte for byte, or the run declines resume.
func (o *opened[P]) Identity() source.Identity {
	opt := o.p.opt
	endpoint, canonical := opt.Endpoint, endpointCanonical(opt.Endpoint, opt.PathStyle)
	if opt.Client != nil {
		endpoint = o.p.route
		canonical = objhttp.CanonicalRoute(endpoint) // a route promises no slash equivalence
	}
	return source.Identity{Provider: "s3", Fields: []source.IdentityField{
		{Name: "source", Value: "s3://" + opt.Bucket + "/" + opt.Prefix},
		{Name: "endpoint", Value: endpoint, Canonical: canonical},
		{Name: "region", Value: o.p.region},
		{Name: "path style", Value: strconv.FormatBool(opt.PathStyle)},
		{Name: "fips", Value: strconv.FormatBool(opt.FIPS)},
		{Name: "dual-stack", Value: strconv.FormatBool(opt.DualStack)},
		{Name: "fetch owner", Value: strconv.FormatBool(opt.FetchOwner)},
	}}
}

// endpointCanonical is the built client's endpoint as identity: the scheme
// and host case folded, and one trailing slash folded only where the SDK
// sends the same requests with or without it. Path-style addressing
// appends "/bucket" to the endpoint's path after trimming one slash
// (/base and /base/ both send /base/bkt), and an endpoint with no path
// sends "/" either way; virtual-host addressing keeps any other path as
// given (bkt.store/base and bkt.store/base/ are two request paths, and
// may be two namespaces), so it is kept byte for byte (TestRequestURLs).
func endpointCanonical(endpoint string, pathStyle bool) string {
	if u, err := url.Parse(endpoint); err == nil && (pathStyle || u.Path == "/" && u.RawQuery == "") {
		endpoint = strings.TrimSuffix(endpoint, "/")
	}
	return objhttp.CanonicalRoute(endpoint)
}

// Capabilities: S3 has no rank oracle and start-after is a fabricable
// seek, so the range engine applies; -sequential lists one token chain. A
// run with an object-count cap (-max-keys) cannot resume: the cap counts
// the objects one process listed, and a resume could neither honour it
// nor tell a capped final part from a complete one. Such a run keeps no
// journal, and a resume of one is refused; so does a run through a
// supplied client that does not name its route (Routed).
func (o *opened[P]) Capabilities() source.Capabilities {
	identified := o.p.opt.Client == nil || o.p.route != ""
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && identified)
}

func (o *opened[P]) Report() source.Report {
	st := o.p.Stats()
	return keyrange.Report("s3", o.target(), o.p.opt.Prefix, &st, st.Counters)
}

// The registered source's whole method set (keyrange.Registered: the ten
// optional interfaces a range-engine provider gives the pipeline); a
// capability whose method went missing would silently be off.
var (
	_ registry.Source     = (*opened[Object])(nil)
	_ keyrange.Registered = (*opened[Object])(nil)
	_ source.Identified   = (*opened[Object])(nil)
	_ source.Capable      = (*opened[Object])(nil)
	_ source.Reporter     = (*opened[Object])(nil)
	_ source.Priced       = (*opened[Object])(nil)
	_ registry.Source     = (*opened[Version])(nil)
	_ keyrange.Registered = (*opened[Version])(nil)
	_ source.Identified   = (*opened[Version])(nil)
	_ source.Capable      = (*opened[Version])(nil)
	_ source.Reporter     = (*opened[Version])(nil)
	_ source.Priced       = (*opened[Version])(nil)
)

// keyPrefix is the key prefix a root names (registry.Factory.KeyPrefix).
func keyPrefix(root string) (string, error) {
	_, prefix, err := ParseURL(root)
	return prefix, err
}

// withPrefix is root naming prefix instead (registry.Factory.WithPrefix).
func withPrefix(root, prefix string) string {
	bucket, _, err := ParseURL(root)
	if err != nil {
		return root
	}
	return "s3://" + bucket + "/" + prefix
}
