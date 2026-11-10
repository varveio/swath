// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcs lists a Cloud Storage bucket (gs://bucket[/prefix]) on the
// shared range engine (internal/source/keyrange) through the JSON API's
// objects.list (internal/source/gcs/client).
package gcs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// API is the one call the provider makes.
type API interface {
	List(ctx context.Context, in *client.ListInput) (*client.Page, error)
}

// pooled is implemented by clients that spread requests over a transport
// pool (client.Client).
type pooled interface{ Transports() int }

// Options configure a listing.
type Options struct {
	Bucket          string
	Prefix          string
	Endpoint        string // JSON API base supplied by --endpoint; default client.DefaultEndpoint
	Anonymous       bool   // send no credentials, whatever the environment holds
	CredentialsFile string // service-account or authorized-user JSON; else Application Default Credentials
	UserProject     string // billed project for a requester-pays bucket
	Timeout         time.Duration
	Workers         int
	Client          API // optional; built from the options above when nil
	// AllVersions lists every generation (versions=true, NewGenerations)
	// instead of the live objects.
	AllVersions bool
	// Policy is the range engine's (the shared engine flags).
	keyrange.Policy
	// Admission is the built-in client's (the shared engine flags); a
	// supplied Client has its own.
	Admission objhttp.Admission
}

// ParseURL splits gs://bucket[/prefix].
func ParseURL(u string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(u, "gs://")
	if !ok || rest == "" {
		return "", "", fmt.Errorf("not a gs://bucket[/prefix] URL: %q", u)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("no bucket in %q", u)
	}
	return bucket, prefix, nil
}

// Stats are the provider's part of the run report.
type Stats struct {
	Anonymous   bool              `json:"anonymous"`
	Credentials string            `json:"credentials,omitempty"`
	MeanActive  float64           `json:"mean_active"`
	Counters    counters.Snapshot `json:"counters"`
}

// Provider is the range engine bound to objects.list.
type Provider = provider[Object]

// GenerationsProvider is the generations listing (Options.AllVersions):
// objects.list with versions=true, rows of swath/gcs-generations/v1.
type GenerationsProvider = provider[Generation]

// provider is a GCS listing with payload P.
type provider[P any] struct {
	*keyrange.Engine[P]
	objhttp.Activity // the client's, observed once it is chosen
	opt              Options
	client           API
	anonymous        bool
	creds            string
}

// Range is the engine's range.
type Range = keyrange.Range

var (
	_ source.Provider[Range, Object]     = (*Provider)(nil)
	_ source.Provider[Range, Generation] = (*GenerationsProvider)(nil)
)

// New builds a provider of the current objects.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.AllVersions {
		return nil, errors.New("gcs: Options.AllVersions lists generations: use NewGenerations")
	}
	p, own, err := connect[Object](ctx, opt)
	if err != nil {
		return nil, err
	}
	e, err := keyrange.New(p.engineOptions(own), keyrange.Description[Object]{
		Target:  "gs://" + opt.Bucket + "/" + opt.Prefix,
		Listing: keyrange.Listing, // names list in unsigned UTF-8 byte order; startOffset is any name
		// startOffset lists names >= the offset; endOffset lists names
		// below it.
		StartInclusive: true,
		AcceptBound:    AcceptBound,
		Requeue:        objhttp.Requeue,
		InvalidPage:    objhttp.ErrInvalidPage,
		Size:           func(o *Object) int64 { return o.Size },
		Counters:       clientCounters,
	}, p.fetch)
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// NewGenerations builds a provider of every generation (versions=true):
// live and noncurrent, a name's generations consecutive and ascending. A
// physical seek starts at the exclusive successor of its bound
// (ExclusiveStart), so no row of a complete name is listed again; where
// the bound policy refuses the successor, it starts at the name and the
// engine drops the name's rows.
func NewGenerations(ctx context.Context, opt Options) (*GenerationsProvider, error) {
	opt.AllVersions = true
	p, own, err := connect[Generation](ctx, opt)
	if err != nil {
		return nil, err
	}
	e, err := keyrange.New(p.engineOptions(own), keyrange.Description[Generation]{
		Target:         "gs://" + opt.Bucket + "/" + opt.Prefix,
		Listing:        keyrange.Listing,
		InclusiveStart: func(after string) bool { _, ok := ExclusiveStart(after); return !ok },
		AcceptBound:    AcceptBound,
		Requeue:        objhttp.Requeue,
		InvalidPage:    objhttp.ErrInvalidPage,
		Groups:         GenerationSchema.Groups,
		GroupOrdinal:   func(g *Generation) int64 { return g.Generation },
		Detach:         GenerationSchema.Detach,
		Size:           func(g *Generation) int64 { return g.Size },
		Counters:       clientCounters,
	}, p.fetchGenerations)
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// connect builds the client unless Options.Client supplies one (own).
func connect[P any](ctx context.Context, opt Options) (*provider[P], bool, error) {
	if opt.Bucket == "" {
		return nil, false, errors.New("no bucket")
	}
	p := &provider[P]{opt: opt, client: opt.Client}
	own := p.client == nil
	if own {
		lim, rate := opt.Admission.New(opt.Workers)
		c, err := client.New(ctx, client.Config{Retry: objhttp.Retry{Limiter: lim, Rate: rate}, Endpoint: opt.Endpoint, Anonymous: opt.Anonymous,
			CredentialsFile: opt.CredentialsFile, UserProject: opt.UserProject, RequestTimeout: opt.Timeout, Workers: opt.Workers})
		if err != nil {
			return nil, false, fmt.Errorf("GCS client: %w", err)
		}
		p.client = c
		p.creds, p.anonymous = c.Credentials()
	}
	p.Activity = objhttp.Observe(p.client)
	return p, own, nil
}

// engineOptions is the range engine's options for p.
func (p *provider[P]) engineOptions(own bool) keyrange.Options {
	opt := p.opt
	eo := opt.Policy.Options()
	eo.Prefix, eo.Workers, eo.ClientOff = opt.Prefix, opt.Workers, opt.Admission.Off(own)
	return eo
}

// AcceptBound is GCS's bound policy: an object name is valid UTF-8 of 1 to
// 1,024 bytes without CR or LF, and a bound travels query-escaped, so '%',
// controls and non-ASCII are sent as they are.
func AcceptBound(s string) bool {
	return len(s) <= maxName && !strings.ContainsAny(s, "\r\n")
}

// ExclusiveStart is the startOffset that lists names strictly after name,
// and whether the provider sends it: name + NUL, the immediate successor
// in byte order (the bound ExclusiveEnd builds for endOffset). startOffset
// is inclusive, so a generations listing that resumes after a complete
// name sends this bound and lists none of that name's generations again
// (checked live on 2026-11-08: TestExclusiveStart). When AcceptBound
// refuses it (name is 1,024 bytes, so the bound is 1,025; the live API
// answered 200 for that too, but the cap keeps start and end bounds to one
// rule that emulators and the replay server also meet), the provider sends
// startOffset = name and the engine's StartInclusive drop removes the
// name's rows, every generation of it, as for any inclusive start.
func ExclusiveStart(name string) (string, bool) {
	return keyrange.ExclusiveEnd(name, AcceptBound)
}

// maxName is the longest object name, in bytes.
const maxName = 1024

// Stats snapshots the counters.
func (p *provider[P]) Stats() Stats {
	p.Activity.Store(p.Set(), p.ID(0))
	if c, ok := p.client.(pooled); ok {
		p.Set().Set(p.ID(cPoolSize), int64(c.Transports()))
	}
	e := p.Engine.Stats()
	return Stats{Anonymous: p.anonymous, Credentials: p.creds, MeanActive: e.MeanActive, Counters: e.Counters}
}

func (p *provider[P]) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Object], error) {
	in := p.input(q, false)
	out, err := p.client.List(ctx, in)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, len(out.Items))
	for i := range out.Items {
		o, r := &out.Items[i], &rows[i]
		r.Key, r.Data = o.Name, object(o)
	}
	return &keyrange.Page[Object]{Rows: rows, Prefixes: out.Prefixes, Truncated: out.NextPageToken != "",
		Token: out.NextPageToken}, nil
}

// input translates an engine query to objects.list, with exclusive starts
// for complete generation groups and an exclusive upper name bound.
func (p *provider[P]) input(q *keyrange.Query, versions bool) *client.ListInput {
	in := &client.ListInput{Bucket: p.opt.Bucket, Prefix: q.Prefix, Delimiter: q.Delimiter,
		StartOffset: q.After, PageToken: q.Token, MaxResults: q.Max, Versions: versions}
	if versions && q.After != "" {
		if start, ok := ExclusiveStart(q.After); ok {
			in.StartOffset = start
		}
	}
	if q.Bounded {
		// endOffset is a name bound too; through+NUL is accepted unless
		// it exceeds the name length. The engine trims when it is omitted.
		if end, ok := keyrange.ExclusiveEnd(q.Through, AcceptBound); ok {
			in.EndOffset = end
		} else {
			p.Set().Inc(p.ID(cEndOmitted)) // the engine trims at Through
		}
	}
	return in
}

func object(o *client.Object) Object {
	return Object{Size: o.Size, Mtime: o.Updated, TimeCreated: o.TimeCreated, ETag: o.ETag, Generation: o.Generation,
		Metageneration: o.Metageneration, StorageClass: o.StorageClass, MD5: o.MD5, CRC32C: o.CRC32C, ContentType: o.ContentType}
}
