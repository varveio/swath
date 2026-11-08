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

type counted interface{ Counters() client.Counters }

// pooled is implemented by clients that spread requests over a transport
// pool (client.Client).
type pooled interface{ Transports() int }

// live is implemented by clients that count their attempts live
// (source.Requested).
type live interface{ Live() objhttp.Live }

// answering is implemented by clients that count the requests their
// service answered (source.Live).
type answering interface{ Answered() int64 }

// Routed is implemented by clients that can name what their requests
// carry that resume identity freezes: the route a bucket's requests go to
// and the billed project (client.Client). A supplied client that cannot is
// listed, but the run declines resume.
type Routed interface {
	Route(bucket string) string
	UserProject() string
}

// Options configure a listing.
type Options struct {
	Bucket          string
	Prefix          string
	Endpoint        string // JSON API base (replay, tests); default client.DefaultEndpoint
	Anonymous       bool   // send no credentials, whatever the environment holds
	CredentialsFile string // service-account or authorized-user JSON; else Application Default Credentials
	UserProject     string // billed project for a requester-pays bucket
	Timeout         time.Duration
	Workers         int
	Client          API     // optional; built from the options above when nil
	Resume          *Resume // continue after a journaled run's last published key
	// Policy is the range engine's (the shared engine flags).
	keyrange.Policy
	// Admission is the built-in client's (the shared engine flags); a
	// supplied Client has its own.
	Admission objhttp.Admission
}

// Resume continues after the last published key.
type Resume struct{ After string }

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

// Faults are the throttle answers and attempt timeouts so far
// (source.Faulted); 0 for a supplied client that does not count them.
func (p *Provider) Faults() (throttles, timeouts int64) {
	if c, ok := p.client.(counted); ok {
		n := c.Counters()
		return n.Throttles, n.Timeouts
	}
	return 0, 0
}

// Requests is the client's live request account (source.Requested); zero
// for a supplied client that does not count it.
func (p *Provider) Requests() source.Requests {
	if c, ok := p.client.(live); ok {
		l := c.Live()
		return source.Requests{Attempts: l.Attempts, Retries: l.Retries, Throttles: l.Throttles, Inflight: l.Inflight,
			InflightMax: l.InflightMax, InflightNs: l.InflightNs, Limit: int64(l.Limit)}
	}
	return source.Requests{}
}

// Answered is the requests the service answered usably (source.Live); 0
// for a supplied client that does not count them.
func (p *Provider) Answered() int64 {
	if c, ok := p.client.(answering); ok {
		return c.Answered()
	}
	return 0
}

// Stats are the provider's part of the run report.
type Stats struct {
	Anonymous   bool              `json:"anonymous"`
	Credentials string            `json:"credentials,omitempty"`
	MeanActive  float64           `json:"mean_active"`
	Counters    counters.Snapshot `json:"counters"`
}

// Provider is the range engine bound to objects.list.
type Provider struct {
	*keyrange.Engine[Object]
	opt       Options
	client    API
	anonymous bool
	creds     string
	route     string // the URL the bucket's requests go to ("" with a supplied Client that is not Routed)
	project   string // the billed project the client sends (userProject)
}

// Range is the engine's range.
type Range = keyrange.Range

var _ source.Provider[Range, Object] = (*Provider)(nil)

// New builds a provider.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.Bucket == "" {
		return nil, errors.New("no bucket")
	}
	p := &Provider{opt: opt, client: opt.Client}
	own := p.client == nil
	if own {
		lim, rate := opt.Admission.New(opt.Workers)
		c, err := client.New(ctx, client.Config{Limiter: lim, Rate: rate, Endpoint: opt.Endpoint, Anonymous: opt.Anonymous,
			CredentialsFile: opt.CredentialsFile, UserProject: opt.UserProject, RequestTimeout: opt.Timeout, Workers: opt.Workers})
		if err != nil {
			return nil, fmt.Errorf("GCS client: %w", err)
		}
		p.client = c
		p.creds, p.anonymous = c.Credentials()
	}
	if r, ok := p.client.(Routed); ok {
		p.route, p.project = r.Route(opt.Bucket), r.UserProject()
	}
	var resume *keyrange.Resume
	if opt.Resume != nil {
		resume = &keyrange.Resume{After: opt.Resume.After}
	}
	eo := opt.Policy.Options()
	eo.Prefix, eo.Workers, eo.Resume, eo.ClientOff = opt.Prefix, opt.Workers, resume, opt.Admission.Off(own)
	e, err := keyrange.New(eo, keyrange.Description[Object]{
		Target:  "gs://" + opt.Bucket + "/" + opt.Prefix,
		Listing: keyrange.Listing, // names list in unsigned UTF-8 byte order; startOffset is any name
		// startOffset lists names >= the offset; endOffset lists names
		// below it.
		StartInclusive: true,
		AcceptBound:    AcceptBound,
		Requeue:        requeue,
		InvalidPage:    client.ErrInvalidPage,
		Size:           func(o *Object) int64 { return o.Size },
		Counters:       clientCounters,
	}, p.fetch)
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// AcceptBound is GCS's bound policy: an object name is valid UTF-8 of 1 to
// 1,024 bytes without CR or LF, and a bound travels query-escaped, so '%',
// controls and non-ASCII are sent as they are.
func AcceptBound(s string) bool {
	return len(s) <= maxName && !strings.ContainsAny(s, "\r\n")
}

// acceptEnd: an endOffset is a name bound too; NUL is sent query-escaped,
// so through+NUL is accepted unless it passes the name length.
func acceptEnd(s string) bool { return AcceptBound(s) }

// maxName is the longest object name, in bytes.
const maxName = 1024

var requeue = objhttp.Requeue(client.Classify)

// Restart continues after a journaled run's last published key.
func (p *Provider) Restart(after string) {
	p.opt.Resume = &Resume{After: after}
	p.Engine.Restart(after)
}

// Stats snapshots the counters.
func (p *Provider) Stats() Stats {
	if c, ok := p.client.(counted); ok {
		c.Counters().Store(p.Set(), p.ID(0))
	}
	if c, ok := p.client.(pooled); ok {
		p.Set().Set(p.ID(cPoolSize), int64(c.Transports()))
	}
	e := p.Engine.Stats()
	return Stats{Anonymous: p.anonymous, Credentials: p.creds, MeanActive: e.MeanActive, Counters: e.Counters}
}

func (p *Provider) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Object], error) {
	in := &client.ListInput{Bucket: p.opt.Bucket, Prefix: q.Prefix, Delimiter: q.Delimiter,
		StartOffset: q.After, PageToken: q.Token, MaxResults: q.Max}
	if q.Bounded {
		if end, ok := keyrange.ExclusiveEnd(q.Through, acceptEnd); ok {
			in.EndOffset = end
		} else {
			p.Set().Inc(p.ID(cEndOmitted)) // the engine trims at Through
		}
	}
	out, err := p.client.List(ctx, in)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, len(out.Items))
	for i := range out.Items {
		o, r := &out.Items[i], &rows[i]
		r.Key = o.Name
		r.Data = Object{Size: o.Size, Mtime: o.Updated, TimeCreated: o.TimeCreated, ETag: o.ETag, Generation: o.Generation,
			Metageneration: o.Metageneration, StorageClass: o.StorageClass, MD5: o.MD5, CRC32C: o.CRC32C,
			ContentType: o.ContentType}
	}
	return &keyrange.Page[Object]{Rows: rows, Prefixes: out.Prefixes, Truncated: out.NextPageToken != "",
		Token: out.NextPageToken}, nil
}
