// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblob lists an Azure Blob Storage container
// (az://ACCOUNT/CONTAINER[/PREFIX]) on the shared range engine
// (internal/source/keyrange) through List Blobs
// (internal/source/azblob/client).
//
// Azure lists in UTF-16 code-unit order, which is byte order except
// between names whose first differing scalars are one in U+E000..U+FFFF
// and one at U+10000 and above. Until the provider reorders such pages, it
// refuses any page, cursor or bound that carries a byte in EE..F4 (the
// lead bytes of those scalars) before the engine sees it, so every page
// the engine takes is byte-ordered; and it lists only accounts whose
// namespace is positively flat.
package azblob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// API is the calls the provider makes: Get Account Information once, at
// open, then List Blobs.
type API interface {
	AccountInfo(ctx context.Context, container string) (hns bool, err error)
	List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error)
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

// Routed is implemented by clients that can name the route a container's
// requests go to, which resume identity freezes (client.Client). A
// supplied client that cannot is listed, but the run declines resume.
type Routed interface {
	Route(container string) string
}

// The environment variables of the Azure CLI that carry a credential: a
// SAS (when neither --azblob-sas nor the root gives one) and an account
// key (Shared Key).
const (
	EnvSAS = "AZURE_STORAGE_SAS_TOKEN"
	EnvKey = "AZURE_STORAGE_KEY"
)

// Options configure a listing.
type Options struct {
	Account   string
	Container string
	Prefix    string
	// Endpoint replaces https://ACCOUNT.blob.core.windows.net (the replay
	// server, Azurite); the account is then its first path segment.
	Endpoint  string
	Anonymous bool   // send no credentials, whatever the environment holds
	SAS       string // shared access signature; else the root's, else EnvSAS
	Timeout   time.Duration
	Workers   int
	Warn      func(string) // one-time warnings for the user; may be nil
	Client    API          // optional; built from the options above when nil
	Resume    *Resume      // continue after a journaled run's last published key
	// AllVersions is refused: versions, snapshots and deleted blobs are a
	// later lane.
	AllVersions bool
	// Policy is the range engine's (the shared engine flags).
	keyrange.Policy
	// Admission is the built-in client's (the shared engine flags); a
	// supplied Client has its own.
	Admission objhttp.Admission
}

// Resume continues after the last published key.
type Resume struct{ After string }

// RedactedSAS stands for a root's SAS in a recorded command line: a resume
// of a run started with one must be given it again.
const RedactedSAS = "sig=[redacted]"

// ParseURL splits az://ACCOUNT/CONTAINER[/PREFIX][?SAS]. A query is the
// root's SAS when it carries a signature (sig); otherwise '?' belongs to
// the prefix. Errors never quote the SAS.
func ParseURL(u string) (account, container, prefix, sas string, err error) {
	rest, ok := strings.CutPrefix(u, "az://")
	if !ok || rest == "" {
		return "", "", "", "", fmt.Errorf("not an az://ACCOUNT/CONTAINER[/PREFIX] URL: %q", stripSAS(u))
	}
	if path, q, found := strings.Cut(rest, "?"); found && isSAS(q) {
		rest, sas = path, q
	}
	account, rest, _ = strings.Cut(rest, "/")
	container, prefix, _ = strings.Cut(rest, "/")
	if account == "" || container == "" {
		return "", "", "", "", fmt.Errorf("no account or container in %q: want az://ACCOUNT/CONTAINER[/PREFIX]", stripSAS(u))
	}
	return account, container, prefix, sas, nil
}

// isSAS: q is a query string with a signature.
func isSAS(q string) bool {
	v, err := url.ParseQuery(q)
	return err == nil && v.Has("sig")
}

// stripSAS is u without a SAS query.
func stripSAS(u string) string {
	if path, q, found := strings.Cut(u, "?"); found && isSAS(q) {
		return path
	}
	return u
}

// RedactRoot replaces an az:// root's SAS with RedactedSAS, for a recorded
// command line; any other argument is returned as it is.
func RedactRoot(arg string) string {
	if !strings.HasPrefix(arg, "az://") {
		return arg
	}
	if path, q, found := strings.Cut(arg, "?"); found && isSAS(q) {
		return path + "?" + RedactedSAS
	}
	return arg
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
		return source.Requests{Attempts: l.Attempts, Retries: l.Retries, Throttles: l.Throttles, Timeouts: l.Timeouts, Inflight: l.Inflight,
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

// Provider is the range engine bound to List Blobs.
type Provider struct {
	*keyrange.Engine[Object]
	opt       Options
	client    API
	anonymous bool
	creds     string
	route     string // the URL the container's requests go to ("" with a supplied Client that is not Routed)
}

// Range is the engine's range.
type Range = keyrange.Range

var _ source.Provider[Range, Object] = (*Provider)(nil)

// New builds a provider and admits the account: Get Account Information
// must answer that its namespace is flat (x-ms-is-hns-enabled: false).
// Anything else, a hierarchical namespace, a refused call, a missing
// header or a failure, refuses the run (source.Unsupported) before any
// listing request: on a hierarchical-namespace account '/' sorts lowest,
// and after a byte cursor "a.z" the key "a/child" is still in range, but a
// seek that sorts '/' lowest skips it while every page still ascends, so
// nothing downstream could notice.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.AllVersions {
		return nil, source.Unsupported(errors.New("azblob: --all-versions is not supported for az:// roots (versions, snapshots and deleted blobs are not listed yet)"))
	}
	p, own, err := connect(ctx, opt)
	if err != nil {
		return nil, err
	}
	if err := p.admit(ctx); err != nil {
		return nil, err
	}
	e, err := keyrange.New(p.engineOptions(own), keyrange.Description[Object]{
		Target: p.target(),
		// With the staged refusal every page the engine takes ascends in
		// byte order, and startFrom takes any name.
		Listing: keyrange.Listing,
		// startFrom lists names >= it; there is no end bound, so the
		// engine trims at Through.
		StartInclusive: true,
		AcceptBound:    AcceptBound,
		Requeue:        requeue,
		InvalidPage:    client.ErrInvalidPage,
		Size:           func(o *Object) int64 { return o.Size },
		Counters:       client.CounterDefs,
	}, p.fetch)
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// admitHNS is the namespace admission (tests replace it).
var admitHNS = func(ctx context.Context, c API, container string) (bool, error) { return c.AccountInfo(ctx, container) }

// admit refuses the run unless the account's namespace is positively flat.
func (p *Provider) admit(ctx context.Context) error {
	hns, err := admitHNS(ctx, p.client, p.opt.Container)
	switch {
	case err != nil && ctx.Err() != nil:
		return context.Cause(ctx)
	case err != nil:
		return source.Unsupported(fmt.Errorf("azblob: account %s: cannot establish that its namespace is flat (%v); only flat accounts are listed", p.opt.Account, err))
	case hns:
		return source.Unsupported(fmt.Errorf("azblob: account %s has a hierarchical namespace; only flat accounts are listed", p.opt.Account))
	}
	return nil
}

// connect builds the client unless Options.Client supplies one (own).
func connect(ctx context.Context, opt Options) (*Provider, bool, error) {
	if opt.Account == "" || opt.Container == "" {
		return nil, false, errors.New("azblob: no account or container")
	}
	p := &Provider{opt: opt, client: opt.Client}
	own := p.client == nil
	if own {
		lim, rate := opt.Admission.New(opt.Workers)
		cfg := client.Config{Limiter: lim, Rate: rate, Endpoint: opt.Endpoint, Account: opt.Account, Anonymous: opt.Anonymous,
			SAS: opt.SAS, RequestTimeout: opt.Timeout, Workers: opt.Workers}
		if cfg.SAS == "" {
			cfg.SAS = os.Getenv(EnvSAS)
		}
		cfg.SharedKey = os.Getenv(EnvKey)
		c, err := client.New(ctx, cfg)
		if err != nil {
			return nil, false, fmt.Errorf("Azure client: %w", err)
		}
		if n := c.Note(); n != "" && opt.Warn != nil {
			opt.Warn(n)
		}
		p.client = c
		p.creds, p.anonymous = c.Credentials()
	}
	if r, ok := p.client.(Routed); ok {
		p.route = r.Route(opt.Container)
	}
	return p, own, nil
}

// engineOptions is the range engine's options for p: pages of
// client.MaxPageSize unless the policy asks for smaller ones.
func (p *Provider) engineOptions(own bool) keyrange.Options {
	opt := p.opt
	var resume *keyrange.Resume
	if opt.Resume != nil {
		resume = &keyrange.Resume{After: opt.Resume.After}
	}
	eo := opt.Policy.Options()
	eo.Prefix, eo.Workers, eo.Resume, eo.ClientOff = opt.Prefix, opt.Workers, resume, opt.Admission.Off(own)
	if eo.PageSize <= 0 {
		eo.PageSize = client.MaxPageSize
	}
	return eo
}

// target is the listed root, without credentials.
func (p *Provider) target() string {
	t := "az://" + p.opt.Account + "/" + p.opt.Container
	if p.opt.Prefix != "" {
		t += "/" + p.opt.Prefix
	}
	return t
}

// maxNameUnits is the longest blob name, in UTF-16 code units.
const maxNameUnits = 1024

// AcceptBound is Azure's bound policy: a startFrom is a blob name, valid
// UTF-8 of at most 1,024 UTF-16 code units, without NUL or any other
// control character, not ending in '.'.
func AcceptBound(s string) bool {
	if !utf8.ValidString(s) || strings.HasSuffix(s, ".") {
		return false
	}
	units := 0
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
		units += utf16.RuneLen(r)
	}
	return units <= maxNameUnits
}

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
		p.Set().Set(p.ID(client.CPoolSize), int64(c.Transports()))
	}
	e := p.Engine.Stats()
	return Stats{Anonymous: p.anonymous, Credentials: p.creds, MeanActive: e.MeanActive, Counters: e.Counters}
}

// highByte reports whether s holds a byte in EE..F4: a lead byte of a
// scalar at U+E000 and above, where Azure's UTF-16 order and byte order
// can disagree.
func highByte(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0xEE && s[i] <= 0xF4 {
			return true
		}
	}
	return false
}

// unsupportedName is the staged refusal: a name at U+E000 and above met
// in a page or a bound. It is a refusal (source.Unsupported, class
// unsupported), not a protocol violation: Azure's order is valid, the
// engine does not requeue it, and the journal stays resumable for a build
// that reorders such pages.
func (p *Provider) unsupportedName(name string) error {
	return source.Unsupported(fmt.Errorf("azblob: names at U+E000 and above are not supported yet (container %s, first name %q)", p.opt.Container, name))
}

func (p *Provider) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Object], error) {
	// The bounds first, before the request is sent: a cursor or a bound
	// past such a name would compare in the wrong order.
	if highByte(q.After) {
		return nil, p.unsupportedName(q.After)
	}
	if q.Bounded && highByte(q.Through) {
		return nil, p.unsupportedName(q.Through)
	}
	out, err := p.client.List(ctx, &client.ListInput{Container: p.opt.Container, Prefix: q.Prefix, Delimiter: q.Delimiter,
		StartFrom: q.After, Marker: q.Token, MaxResults: q.Max})
	if err != nil {
		return nil, err
	}
	// Then every raw item, blob or prefix, before the engine trims any.
	nblobs := 0
	for i := range out.Items {
		if highByte(out.Items[i].Name) {
			return nil, p.unsupportedName(out.Items[i].Name)
		}
		if !out.Items[i].Prefix {
			nblobs++
		}
	}
	page := &keyrange.Page[Object]{Rows: make([]Row, 0, nblobs), Truncated: out.NextMarker != "", Token: out.NextMarker}
	for i := range out.Items {
		it := &out.Items[i]
		if it.Prefix {
			page.Prefixes = append(page.Prefixes, it.Name)
			continue
		}
		page.Rows = append(page.Rows, Row{Key: it.Name, Data: Object{Size: it.Size, Mtime: it.LastModified, CreationTime: it.CreationTime,
			ETag: it.ETag, ContentMD5: it.ContentMD5, ContentType: it.ContentType, BlobType: it.BlobType, AccessTier: it.AccessTier}})
	}
	return page, nil
}
