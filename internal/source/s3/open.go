// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

// Opened is an S3 listing opened by Open or OpenVersions: the provider
// bound to its row schema (Bound), and what a run asks of an opened source
// besides its rows: the range engine's optional interfaces
// (keyrange.Registered, set to the provider, so nothing else of it is
// promoted), the locator, the resume identity, the capabilities, the report
// and the tariff. It chooses no writer; the command joins it with one
// (internal/cli).
type Opened[P any] struct {
	keyrange.Registered
	bound bind.Bound[P]
	p     *provider[P]
	root  string
}

// Open opens root, s3://bucket[/prefix], for its current objects: opt with
// the bucket and prefix root names (ParseURL). opt.AllVersions is refused
// (New): OpenVersions lists versions.
func Open(ctx context.Context, root string, opt Options) (*Opened[Object], error) {
	if err := opt.setRoot(root); err != nil {
		return nil, err
	}
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return opened(p, &Schema, root)
}

// OpenVersions is Open for every version and delete marker of each key
// (NewVersions), its rows of schema swath/s3-versions/v1.
func OpenVersions(ctx context.Context, root string, opt Options) (*Opened[Version], error) {
	if err := opt.setRoot(root); err != nil {
		return nil, err
	}
	p, err := NewVersions(ctx, opt)
	if err != nil {
		return nil, err
	}
	return opened(p, &VersionsSchema, root)
}

// setRoot sets the bucket and prefix root names.
func (o *Options) setRoot(root string) error {
	bucket, prefix, err := ParseURL(root)
	if err != nil {
		return err
	}
	o.Bucket, o.Prefix = bucket, prefix
	return nil
}

// opened binds p to its row schema s (bind.Provider).
func opened[P any](p *provider[P], s *schema.Schema[P], root string) (*Opened[P], error) {
	b, err := bind.Provider[Range, P](p, s)
	if err != nil {
		return nil, err
	}
	return &Opened[P]{Registered: p, bound: b, p: p, root: root}, nil
}

// Bound is the provider bound to its row schema.
func (o *Opened[P]) Bound() bind.Bound[P] { return o.bound }

// Locator records the root as given.
func (o *Opened[P]) Locator() source.Locator {
	return source.Locator{Kind: "s3", Root: o.root, Container: o.p.opt.Bucket}
}

// Tariff is the reference list rate the report labels its estimate with
// (source.Priced): only on the provider's own service; a custom endpoint
// has no known tariff, and the report then gives no figure.
func (o *Opened[P]) Tariff() (usdPer1k float64, basis string) {
	if o.p.opt.Endpoint == "" && o.p.opt.Client == nil {
		return 0.005, "aws-list-reference-rate"
	}
	return 0, ""
}

func (o *Opened[P]) target() string {
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
func (o *Opened[P]) Identity() source.Identity {
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
func (o *Opened[P]) Capabilities() source.Capabilities {
	identified := o.p.opt.Client == nil || o.p.route != ""
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && identified)
}

// Report is the listing's statistics, naming the owner columns when they
// were fetched.
func (o *Opened[P]) Report() source.Report {
	st := o.p.Stats()
	r := keyrange.Report("s3", o.target(), o.p.opt.Prefix, &st, st.Counters)
	if o.p.opt.FetchOwner {
		r.Collected = []string{"owner_id", "owner_display_name"}
	}
	return r
}

// The opened view's whole method set (keyrange.Registered: the eleven
// optional interfaces a range-engine provider gives the pipeline, and the
// provider's own); a capability whose method went missing would silently
// be off.
var (
	_ keyrange.Registered = (*Opened[Object])(nil)
	_ source.Identified   = (*Opened[Object])(nil)
	_ source.Capable      = (*Opened[Object])(nil)
	_ source.Reporter     = (*Opened[Object])(nil)
	_ source.Priced       = (*Opened[Object])(nil)
	_ keyrange.Registered = (*Opened[Version])(nil)
	_ source.Identified   = (*Opened[Version])(nil)
	_ source.Capable      = (*Opened[Version])(nil)
	_ source.Reporter     = (*Opened[Version])(nil)
	_ source.Priced       = (*Opened[Version])(nil)
)

// KeyPrefix is the key prefix root names: s3://b/logs/ names "logs/".
func KeyPrefix(root string) (string, error) {
	_, prefix, err := ParseURL(root)
	return prefix, err
}

// WithPrefix is root naming prefix instead of its own, in the same bucket;
// root itself when it does not parse.
func WithPrefix(root, prefix string) string {
	bucket, _, err := ParseURL(root)
	if err != nil {
		return root
	}
	return "s3://" + bucket + "/" + prefix
}
