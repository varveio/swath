// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

func init() {
	registry.Register(registry.Factory{
		EncoderBytes: parquetout.EncoderBytes(&Schema), Classify: objhttp.Label,
		Scheme: "s3", KeyPrefix: keyPrefix, WithPrefix: withPrefix,
		Flags: flags, Shared: []*source.FlagGroup{source.StoreGroup, keyrange.Group}, Defaults: defaults, Open: open,
		Secrets:     []registry.Secret{{Flag: "s3-bearer-token-command"}},
		Operational: []string{"s3-profile", "s3-bearer-token-refresh-interval"},
		Title:       "S3", Example: "s3://bucket/logs/", Usage: "s3://bucket[/prefix]"})
}

// flagged is the S3 flag group's value: the S3 options, the store group's
// values and the range engine's policy and HTTP admission, whose flags
// every engine provider shares.
type flagged struct {
	Options
	store        *source.Store
	common       *keyrange.Common
	pathStyleSet bool // --s3-path-style given (else on with a custom endpoint, as Java's)
	refreshSet   bool // --s3-bearer-token-refresh-interval given
}

func defaults() any { return &flagged{} }

// flags registers the S3 flag group beside the shared groups' values; the
// returned value is filled when the flag set is parsed.
func flags(f *pflag.FlagSet, diag bool, shared source.SharedValues) any {
	o := &flagged{store: source.Shared[source.Store](shared, source.StoreGroup), common: source.Shared[keyrange.Common](shared, keyrange.Group)}
	source.Define(f, source.Flag{Name: "s3-region", Familiar: []string{"region"}, Value: source.Text(&o.Region), Metavar: "NAME", Usage: "the bucket's region",
		Long: "Default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, else us-east-1."})
	source.Define(f, source.Flag{Name: "s3-profile", Familiar: []string{"profile"}, Value: source.Text(&o.Profile), Metavar: "NAME", Usage: "the shared config profile to take credentials and region from",
		Long: "Default: the AWS SDK chain (environment, AWS_PROFILE or default, SSO, web identity, IMDS)."})
	source.Define(f, source.Flag{Name: "s3-requester-pays", Familiar: []string{"requester-pays", "request-payer"}, Value: source.Switch(&o.RequestPayer), Usage: "accept the request charges of a requester-pays bucket"})
	pathStyle := func(on bool) func(string) error {
		return func(s string) error {
			v, err := strconv.ParseBool(s)
			if err != nil {
				return errors.New("want true or false")
			}
			o.PathStyle, o.pathStyleSet = v == on, true
			return nil
		}
	}
	source.Define(f, source.Flag{Name: "s3-path-style", Value: source.Func(pathStyle(true)), Usage: "address the bucket in the path (default with --endpoint-url)"})
	source.Define(f, source.Flag{Name: "no-s3-path-style", Value: source.Func(pathStyle(false)), Usage: "address the bucket in the host name, also with --endpoint-url"})
	source.Define(f, source.Flag{Name: "s3-bearer-token-command", Value: source.Text(&o.BearerCommand), Metavar: "COMMAND",
		Usage: "a shell command printing a bearer token, sent instead of SigV4",
		Long:  "For GCS's XML API: 'gcloud auth print-access-token'. Never recorded: swath resume needs it again."})
	source.Define(f, source.Flag{Name: "s3-bearer-token-refresh-interval", Metavar: "DURATION", Default: source.FormatDuration(tokensource.DefaultRefresh),
		Usage: "run --s3-bearer-token-command again after this long",
		Value: source.Func(func(v string) error {
			d, err := source.ParseDuration(v)
			if err != nil || d <= 0 {
				return fmt.Errorf("want a positive duration such as 90s, 1h30m")
			}
			o.BearerRefresh, o.refreshSet = d, true
			return nil
		})})
	source.Define(f, source.Flag{Name: "s3-fips", Value: source.Switch(&o.FIPS), Usage: "use the FIPS endpoint (s3-fips.REGION.amazonaws.com)"})
	source.Define(f, source.Flag{Name: "s3-dualstack", Value: source.Switch(&o.DualStack), Usage: "use the dual-stack (IPv6) endpoint"})
	source.Define(f, source.Flag{Name: "s3-fetch-owner", Value: source.Switch(&o.FetchOwner), Usage: "fill owner_id and owner_display_name"})
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
			return nil, source.Unsupported(errors.New("--s3-bearer-token-refresh-interval requires --s3-bearer-token-command"))
		case opt.BearerCommand != "" && (opt.Anonymous || opt.Profile != ""):
			return nil, source.Unsupported(errors.New("--s3-bearer-token-command excludes --anonymous and --s3-profile: the token is the credential"))
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
// and prefix; the endpoint (--endpoint-url, "" for AWS's own), the signing
// region and the addressing flags (path-style, FIPS, dual-stack), which
// together decide which service, partition and bucket of a name the SDK
// lists (another endpoint, partition or addressing style can serve a
// different bucket of the same name); and the owner columns. The region is
// --s3-region, else what the environment or the profile resolves, so a resume
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
