// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/gcs/client"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

// Opened is a GCS listing opened by Open or OpenGenerations: the provider
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

// Open opens root, gs://bucket[/prefix], for its live objects: opt with
// the bucket and prefix root names (ParseURL). opt.AllVersions is refused
// (New): OpenGenerations lists every generation.
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

// OpenGenerations is Open for every generation, live and noncurrent
// (NewGenerations), its rows of schema swath/gcs-generations/v1.
func OpenGenerations(ctx context.Context, root string, opt Options) (*Opened[Generation], error) {
	if err := opt.setRoot(root); err != nil {
		return nil, err
	}
	p, err := NewGenerations(ctx, opt)
	if err != nil {
		return nil, err
	}
	return opened(p, &GenerationSchema, root)
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
	return source.Locator{Kind: "gcs", Root: o.root, Container: o.p.opt.Bucket}
}

// Tariff is the reference list rate the report labels its estimate with
// (source.Priced): only on the provider's own service, however its
// endpoint is spelled (compared canonically, as the identity is); a
// custom endpoint has no known tariff, and the report then gives no
// figure.
func (o *Opened[P]) Tariff() (usdPer1k float64, basis string) {
	ep, err := client.Endpoint(o.p.opt.Endpoint)
	if err == nil && objhttp.CanonicalRoute(ep) == objhttp.CanonicalRoute(client.DefaultEndpoint) && o.p.opt.Client == nil {
		return 0.005, "gcs-class-a-reference-rate"
	}
	return 0, ""
}

func (o *Opened[P]) target() string {
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
func (o *Opened[P]) Identity() source.Identity {
	opt := o.p.opt
	ep, err := client.Endpoint(opt.Endpoint)
	if err != nil {
		// Only a supplied client, which never resumes, gets here with an
		// endpoint it does not use; the value is not recorded (it may hold
		// a password).
		ep = ""
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
func (o *Opened[P]) Capabilities() source.Capabilities {
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && o.p.opt.Client == nil)
}

// Report is the listing's statistics.
func (o *Opened[P]) Report() source.Report {
	st := o.p.Stats()
	return keyrange.Report("gcs", o.target(), o.p.opt.Prefix, &st, st.Counters)
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
	_ keyrange.Registered = (*Opened[Generation])(nil)
	_ source.Identified   = (*Opened[Generation])(nil)
	_ source.Capable      = (*Opened[Generation])(nil)
	_ source.Reporter     = (*Opened[Generation])(nil)
	_ source.Priced       = (*Opened[Generation])(nil)
)

// KeyPrefix is the key prefix root names: gs://b/logs/ names "logs/".
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
	return "gs://" + bucket + "/" + prefix
}
