// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblob lists an Azure Blob Storage container
// (az://ACCOUNT/CONTAINER[/PREFIX]) on the shared range engine
// (internal/source/keyrange) through List Blobs
// (internal/source/azblob/client).
//
// Azure lists in UTF-16 code-unit order, which is byte order except
// between names whose first differing scalars are one in U+E000..U+FFFF
// and one at U+10000 and above. The engine's pages come from the
// byte-order adapter (internal/source/azblob/order), which reads Azure's
// order and serves each query in unsigned byte order; and the provider
// lists only accounts whose namespace is positively flat.
package azblob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/azblob/order"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// API is the calls the provider makes: Get Account Information once, at
// open, then List Blobs.
type API interface {
	AccountInfo(ctx context.Context, container string) (hns bool, err error)
	List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error)
}

// namespaced is implemented by clients that can also ask the account's
// Data Lake host (client.Client.NamespaceInfo), admission's fallback.
type namespaced interface {
	NamespaceInfo(ctx context.Context, container string) (hns bool, err error)
}

// pooled is implemented by clients that spread requests over a transport
// pool (client.Client).
type pooled interface{ Transports() int }

// capped is implemented by clients that count the waits they cut
// (client.Client: retry_after_capped).
type capped interface{ RetryAfterCapped() int64 }

// formatted is implemented by clients that count the format of each
// listing page (client.Client: list_format.*).
type formatted interface{ Formats() client.Formats }

var _ capped = (*client.Client)(nil)
var _ pooled = (*client.Client)(nil)
var _ formatted = (*client.Client)(nil)

// Routed is implemented by clients that can name the route a container's
// requests go to, which resume identity freezes (client.Client). A
// supplied client that cannot is listed, but the run declines resume.
type Routed interface {
	Route(container string) string
}

// The environment variables of the Azure CLI that carry a credential: a
// SAS (when neither --azure-sas-token nor the root gives one) and an account
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
	// Flat asserts a flat namespace where admission cannot establish it
	// (--azure-assume-flat); an answered hierarchical namespace still refuses.
	// It is part of the resume identity and the report.
	Flat    bool
	Timeout time.Duration
	Trust   source.Trust // how the service's certificate is verified
	Workers int
	Warn    func(string) // one-time warnings for the user; may be nil
	Client  API          // optional; built from the options above when nil
	// AllVersions is refused: versions, snapshots and deleted blobs are
	// not supported.
	AllVersions bool
	// Policy is the range engine's (the shared engine flags).
	keyrange.Policy
	// Admission is the built-in client's (the shared engine flags); a
	// supplied Client has its own.
	Admission objhttp.Admission
}

// RedactedSAS stands for a root's SAS in a recorded command line: a resume
// of a run started with one must be given it again.
const RedactedSAS = "sig=[redacted]"

// ParseURL splits az://ACCOUNT/CONTAINER[/PREFIX][?SAS]. A query is the
// root's SAS when any of its parameters is named as a SAS parameter
// (sasParam), found by scanning the names whether or not the query parses;
// otherwise '?' belongs to the prefix. A SAS query must parse and carry a
// signature, or the root is refused. Errors never quote the SAS.
func ParseURL(u string) (account, container, prefix, sas string, err error) {
	shown := source.RedactRoot(RedactRoot(u))
	rest, ok := strings.CutPrefix(u, "az://")
	if !ok || rest == "" {
		return "", "", "", "", fmt.Errorf("not an az://ACCOUNT/CONTAINER[/PREFIX] URL: %q", shown)
	}
	if path, q, found := strings.Cut(rest, "?"); found && sasQuery(q) {
		if v, perr := url.ParseQuery(q); perr != nil || !v.Has("sig") {
			return "", "", "", "", fmt.Errorf("%s: the query names shared access signature parameters but is not a signed query string", shown)
		}
		rest, sas = path, q
	}
	account, rest, _ = strings.Cut(rest, "/")
	container, prefix, _ = strings.Cut(rest, "/")
	if account == "" || container == "" {
		return "", "", "", "", fmt.Errorf("no account or container in %q: want az://ACCOUNT/CONTAINER[/PREFIX]", shown)
	}
	return account, container, prefix, sas, nil
}

// sasParams are the query parameters of an Azure shared access signature
// (account, service and user delegation SAS): sig is the secret, the
// others say what it grants.
var sasParams = map[string]bool{"sig": true, "sv": true, "ss": true, "srt": true, "sp": true, "se": true, "st": true,
	"spr": true, "sip": true, "sr": true, "si": true, "sdd": true, "ses": true, "skoid": true, "sktid": true, "skt": true,
	"ske": true, "sks": true, "skv": true, "saoid": true, "suoid": true, "scid": true}

// sasQuery reports whether q names a SAS parameter, scanning its
// parameter names split at '&' and '?' (also ';'), each unescaped where it
// can be, without requiring q to parse: a malformed query that carries a
// signature is still a credential.
func sasQuery(q string) bool {
	for _, kv := range strings.FieldsFunc(q, func(r rune) bool { return r == '&' || r == '?' || r == ';' }) {
		name, _, _ := strings.Cut(kv, "=")
		if n, err := url.QueryUnescape(name); err == nil {
			name = n
		}
		if sasParams[strings.ToLower(strings.TrimSpace(name))] {
			return true
		}
	}
	return false
}

// RedactRoot replaces an az:// root's SAS query with RedactedSAS, for
// anything shown or recorded (registry.Factory.Redact); any other argument
// is returned as it is.
func RedactRoot(arg string) string {
	if !strings.HasPrefix(arg, "az://") {
		return arg
	}
	if path, q, found := strings.Cut(arg, "?"); found && sasQuery(q) {
		return path + "?" + RedactedSAS
	}
	return arg
}

// Stats are the provider's part of the run report.
type Stats struct {
	Anonymous   bool   `json:"anonymous"`
	Credentials string `json:"credentials,omitempty"`
	// Namespace is how admission found the namespace flat: "account
	// information", "filesystem properties" or "asserted" (--azure-assume-flat).
	Namespace   string            `json:"namespace"`
	NoVerifySSL bool              `json:"no_verify_ssl,omitempty"` // --no-verify-ssl: certificates were not verified
	MeanActive  float64           `json:"mean_active"`
	Counters    counters.Snapshot `json:"counters"`
}

// Provider is the range engine bound to List Blobs.
type Provider struct {
	*keyrange.Engine[Object]
	objhttp.Activity // the client's, observed once it is chosen
	opt              Options
	client           API
	anonymous        bool
	creds            string
	route            string // the URL the container's requests go to ("" with a supplied Client that is not Routed)
	namespace        string // how admission found the namespace flat (Stats.Namespace)
}

// Range is the engine's range.
type Range = keyrange.Range

var _ source.Provider[Range, Object] = (*Provider)(nil)

// New builds a provider and admits the account (admit): its namespace
// must be positively flat, or the run is refused (source.Unsupported)
// before any listing request: on a hierarchical-namespace account '/' sorts
// lowest, and after a byte cursor "a.z" the key "a/child" is still in range,
// but a seek that sorts '/' lowest skips it while every page still ascends,
// so nothing downstream could notice.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.AllVersions {
		return nil, source.Unsupported(errors.New("Azure Blob: --all-versions is not supported for az:// roots (versions, snapshots and deleted blobs are not listed yet)"))
	}
	p, own, err := connect(ctx, opt)
	if err != nil {
		return nil, err
	}
	if err := p.admit(ctx); err != nil {
		return nil, err
	}
	e, err := keyrange.New(p.engineOptions(own), p.description(), p.fetch)
	if err != nil {
		return nil, err
	}
	p.Engine = e
	return p, nil
}

// description is what the range engine must know about the provider.
func (p *Provider) description() keyrange.Description[Object] {
	return keyrange.Description[Object]{
		Target: p.target(),
		// The adapter serves every page in byte order, from any start
		// name, after After (exclusive) and up to Through.
		Listing:     keyrange.Listing,
		AcceptBound: AcceptBound,
		Requeue:     objhttp.Requeue,
		InvalidPage: objhttp.ErrInvalidPage,
		Size:        func(o *Object) int64 { return o.Size },
		Counters:    slices.Concat(client.CounterDefs, order.CounterDefs),
	}
}

// adapter is a byte-order adapter over the provider's client, counting in
// the provider's counters.
func (p *Provider) adapter() *order.Adapter {
	return order.New(p.client, p.opt.Container, p.Set(), p.ID(len(client.CounterDefs)))
}

// adapterKey carries an Expand's adapter on its context.
type adapterKey struct{}

// Expand lists one range (keyrange.Engine.Expand) with its own byte-order
// adapter, whose chains (one per continuation chain the range starts:
// the listing and its probes) are released however Expand returns (the
// specification's §5 lifetime: the engine joins its probes and seed work
// before Expand returns, and children carry bounds, not tokens).
func (p *Provider) Expand(ctx context.Context, r Range, e source.Emitter[Range, Object]) error {
	a := p.adapter()
	defer a.Release()
	return p.Engine.Expand(context.WithValue(ctx, adapterKey{}, a), r, e)
}

// admit refuses the run unless the account's namespace is flat: (1) Get
// Account Information on the blob endpoint, whose x-ms-is-hns-enabled
// false passes and true refuses; (2) when that call is refused (401, the
// answer to any anonymous caller, or 403) or the header is absent,
// Filesystem Get Properties on the account's Data Lake host (only for an
// ACCOUNT.blob.core.windows.net endpoint), whose x-ms-namespace-enabled
// false passes and true refuses; (3) anything else is unknown, which
// refuses unless Options.Flat asserts a flat namespace. Credentials that
// could not be had, or that the service refused (a signed caller's 401 or
// 403), are that failure (class auth or access denied), as on any later
// request: the Data Lake host would need the same.
func (p *Provider) admit(ctx context.Context) error {
	hns, err := p.client.AccountInfo(ctx, p.opt.Container)
	if ce := (*objhttp.CredentialError)(nil); errors.As(err, &ce) {
		return fmt.Errorf("Azure Blob client: %w", err)
	}
	how := "account information"
	if err != nil && ctx.Err() == nil && fallback(err) {
		if n, ok := p.client.(namespaced); ok {
			if h, nerr := n.NamespaceInfo(ctx, p.opt.Container); nerr == nil {
				hns, err, how = h, nil, "filesystem properties"
			} else if !errors.Is(nerr, client.ErrNoDFSEndpoint) {
				err = fmt.Errorf("%w; filesystem properties: %w", err, nerr)
			}
		}
	}
	switch {
	case err != nil && ctx.Err() != nil:
		return context.Cause(ctx)
	case err == nil && hns:
		return source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace (%s); only flat accounts are listed", p.opt.Account, how))
	case err == nil:
		p.namespace = how
	case p.opt.Flat:
		p.namespace = "asserted"
		if p.opt.Warn != nil {
			p.opt.Warn(fmt.Sprintf("Azure Blob: account %s: the namespace could not be read (%v); listing as flat, as --azure-assume-flat asserts", p.opt.Account, err))
		}
	case !p.unsigned() && refused(err):
		// Credentials the service rejected: what any request would meet.
		return fmt.Errorf("Azure Blob: account %s: %w", p.opt.Account, err)
	default:
		return source.Unsupported(fmt.Errorf("Azure Blob: account %s: cannot establish that its namespace is flat (%w); only flat accounts are listed (--azure-assume-flat asserts it)", p.opt.Account, err))
	}
	return nil
}

// unsigned says requests go anonymous, which Get Account Information
// refuses (401) whatever the namespace.
func (p *Provider) unsigned() bool {
	if c, ok := p.client.(interface{ Credentials() (string, bool) }); ok {
		_, anonymous := c.Credentials()
		return anonymous
	}
	return p.anonymous
}

// fallback: Get Account Information was refused or answered without the
// header, so the Data Lake host may tell.
func fallback(err error) bool {
	return errors.Is(err, client.ErrHNSUnknown) || refused(err)
}

// refused: the service refused the caller (class auth or access denied, by
// Azure's status and code).
func refused(err error) bool {
	if errors.Is(err, source.ErrUnsupported) {
		return false
	}
	f := objhttp.Normalize(err)
	if f == nil {
		return false
	}
	classified := (&client.Error{Status: f.Status, Code: f.Code}).Failure()
	return classified.Class == objhttp.ClassAuth || classified.Class == objhttp.ClassAccessDenied
}

// connect builds the client unless Options.Client supplies one (own).
func connect(ctx context.Context, opt Options) (*Provider, bool, error) {
	if opt.Account == "" || opt.Container == "" {
		return nil, false, errors.New("Azure Blob: no account or container")
	}
	p := &Provider{opt: opt, client: opt.Client}
	own := p.client == nil
	if own {
		lim, rate := opt.Admission.New(opt.Workers)
		cfg := client.Config{Retry: objhttp.Retry{Limiter: lim, Rate: rate}, Endpoint: opt.Endpoint, Account: opt.Account, Anonymous: opt.Anonymous,
			SAS: opt.SAS, RequestTimeout: opt.Timeout, Trust: opt.Trust, Workers: opt.Workers}
		if cfg.SAS == "" {
			cfg.SAS = os.Getenv(EnvSAS)
		}
		cfg.SharedKey = os.Getenv(EnvKey)
		c, err := client.New(ctx, cfg)
		if err != nil {
			return nil, false, fmt.Errorf("Azure Blob client: %w", err)
		}
		if n := c.Note(); n != "" && opt.Warn != nil {
			opt.Warn(n)
		}
		if opt.Trust.NoVerify && opt.Warn != nil {
			opt.Warn(source.NoVerifyWarning)
		}
		p.client = c
		p.creds, p.anonymous = c.Credentials()
	}
	if r, ok := p.client.(Routed); ok {
		p.route = r.Route(opt.Container)
	}
	p.Activity = objhttp.Observe(p.client)
	return p, own, nil
}

// defaultPageSize is the page a range asks for unless the policy sets one:
// 1,000, not the service's 5,000 (client.MaxPageSize). A range reads until
// its first non-member, so a range smaller than a page discards the rest of
// that page; at 5,000 most rows read were discarded, and 1,000 halved the
// wall time of a 1.84M-blob listing for three times the requests
// (measurements.md, Azure page size).
const defaultPageSize = 1000

// engineOptions is the range engine's options for p: pages of
// defaultPageSize unless the policy asks for another size.
func (p *Provider) engineOptions(own bool) keyrange.Options {
	opt := p.opt
	eo := opt.Policy.Options()
	eo.Prefix, eo.Workers, eo.ClientOff = opt.Prefix, opt.Workers, opt.Admission.Off(own)
	if eo.PageSize <= 0 {
		eo.PageSize = defaultPageSize
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

// Stats snapshots the counters.
func (p *Provider) Stats() Stats {
	p.Activity.Store(p.Set(), p.ID(0))
	if c, ok := p.client.(pooled); ok {
		p.Set().Set(p.ID(client.CPoolSize), int64(c.Transports()))
	}
	if c, ok := p.client.(capped); ok {
		p.Set().Set(p.ID(client.CRetryAfterCapped), c.RetryAfterCapped())
	}
	if c, ok := p.client.(formatted); ok {
		f := c.Formats()
		p.Set().Set(p.ID(client.CArrowPages), f.ArrowPages)
		p.Set().Set(p.ID(client.CArrowItems), f.ArrowItems)
		p.Set().Set(p.ID(client.CXMLPages), f.XMLPages)
		p.Set().Set(p.ID(client.CXMLItems), f.XMLItems)
	}
	e := p.Engine.Stats()
	st := Stats{Anonymous: p.anonymous, Credentials: p.creds, Namespace: p.namespace, NoVerifySSL: p.opt.Client == nil && p.opt.Trust.NoVerify,
		MeanActive: e.MeanActive, Counters: e.Counters}
	return st
}

// fetch lists one page in byte order through the Expand's adapter and
// converts its items to rows and prefixes.
func (p *Provider) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Object], error) {
	a, ok := ctx.Value(adapterKey{}).(*order.Adapter)
	if !ok || a == nil {
		return nil, source.Violation(errors.New("Azure Blob: fetch without an Expand adapter"))
	}
	out, err := a.Fetch(ctx, q)
	if err != nil {
		return nil, err
	}
	nblobs := 0
	for i := range out.Items {
		if !out.Items[i].Prefix {
			nblobs++
		}
	}
	page := &keyrange.Page[Object]{Rows: make([]Row, 0, nblobs), Truncated: out.Truncated, Token: out.Token}
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
