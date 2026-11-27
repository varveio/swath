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
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/azblob/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
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
	// (--azure-assume-flat); a reported hierarchical namespace takes precedence.
	// It is part of the resume identity and the report.
	Flat    bool
	Timeout time.Duration
	Trust   source.Trust // how the service's certificate is verified
	Workers int
	Warn    func(code, msg string) // one-time warnings for the user (registry.Request.Warn); may be nil
	Client  API                    // optional; built from the options above when nil
	// AllVersions lists every entry of each name (NewVersions): its
	// versions, snapshots and soft-deleted blobs beside its current blob.
	AllVersions bool
	// FetchMetadata, FetchTags and FetchImmutability add the user
	// metadata, the blob index tags, and the immutability policy and legal
	// hold to each row (include=metadata, tags, immutabilitypolicy and
	// legalhold); their columns are null otherwise. Tags need the right
	// to read them, which a listing does not: without it every request
	// fails.
	FetchMetadata, FetchTags, FetchImmutability bool
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
	Pruning     *filter.Pruning `json:"pruning,omitempty"`
	PrunedDirs  int64           `json:"pruned_dirs,omitempty"`
	Listing     string          `json:"listing,omitempty"`
	Anonymous   bool            `json:"anonymous"`
	Credentials string          `json:"credentials,omitempty"`
	// Namespace is how admission determined the namespace: "account
	// information", "filesystem properties", "filesystem endpoint blocked"
	// (the Data Lake host refused the flat account for soft delete or blob
	// events), "listing" (a hierarchical item) or "asserted" (--azure-assume-flat).
	Namespace                string            `json:"namespace"`
	NamespaceProbeRequests   int64             `json:"namespace_probe_requests,omitempty"`
	NamespaceProbeEmptyPages int64             `json:"namespace_probe_empty_pages,omitempty"`
	NamespaceProbeRequeues   int64             `json:"namespace_probe_requeues,omitempty"`
	NoVerifySSL              bool              `json:"no_verify_ssl,omitempty"` // --no-verify-ssl: certificates were not verified
	MeanActive               float64           `json:"mean_active"`
	Counters                 counters.Snapshot `json:"counters"`
}

// Provider is the range engine bound to List Blobs: the current blobs,
// rows of swath/azure-objects/v1.
type Provider = provider[Object]

// VersionsProvider is the --all-versions listing (Options.AllVersions):
// every entry of each name, its versions, snapshots and soft-deleted blobs
// beside its current blob (include=versions,snapshots,deleted), rows of
// swath/azure-versions/v1.
type VersionsProvider = provider[Version]

// provider is an Azure Blob listing with payload P.
type provider[P any] struct {
	*keyrange.Engine[P]
	objhttp.Activity // the client's, observed once it is chosen
	opt              Options
	client           API
	anonymous        bool
	creds            string
	route            string // the URL the container's requests go to ("" with a supplied Client that is not Routed)
	namespace        string // how admission determined the namespace (Stats.Namespace)
	probeRequests    int64
	probeEmptyPages  int64
	probeRequeues    int64 // exhausted namespace listing probes requeued during admission
	schema           *schema.Schema[P]
	row              func(*provider[P], *client.Item) P // a listed blob's payload
	size             func(*P) int64
}

// Range is the engine's range.
type Range = keyrange.Range

var (
	_ source.Provider[Range, Object]  = (*Provider)(nil)
	_ source.Provider[Range, Version] = (*VersionsProvider)(nil)
)

// New builds a provider of the current blobs and admits the account
// (admit): its namespace must be positively flat, or the run is refused
// (source.Unsupported) before any listing request: on a hierarchical-namespace
// account '/' sorts lowest, and after a byte cursor "a.z" the key "a/child" is
// still in range, but a seek that sorts '/' lowest skips it while every page
// still ascends, so nothing downstream could notice.
func New(ctx context.Context, opt Options) (*Provider, error) {
	if opt.AllVersions {
		return nil, errors.New("Azure Blob: Options.AllVersions lists every entry: use NewVersions")
	}
	return build(ctx, opt, false, &Schema, (*Provider).object, func(o *Object) int64 { return o.Size })
}

// NewVersions builds a provider of every entry of each name (versions,
// snapshots, soft-deleted blobs and the current blob), admitted as New
// admits. A name's entries are consecutive rows in Azure's order; the
// engine completes a name before it splits, cuts a part or records it
// (keyrange's group mode).
func NewVersions(ctx context.Context, opt Options) (*VersionsProvider, error) {
	opt.AllVersions = true
	return build(ctx, opt, false, &VersionSchema, (*VersionsProvider).version, func(v *Version) int64 { return v.Size })
}

func build[P any](ctx context.Context, opt Options, probeTree bool, s *schema.Schema[P], row func(*provider[P], *client.Item) P, size func(*P) int64) (*provider[P], error) {
	p, own, err := connect[P](ctx, opt)
	if err != nil {
		return nil, err
	}
	p.schema, p.row, p.size = s, row, size
	hns, err := p.admit(ctx, probeTree)
	if err != nil {
		return nil, err
	}
	if hns {
		if opt.AllVersions {
			return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace (%s); --all-versions requires a flat namespace", p.opt.Account, p.namespace))
		}
		return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace (%s); the flat provider lists only flat accounts", p.opt.Account, p.namespace))
	}
	if err := p.flat(own); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *provider[P]) flat(own bool) error {
	e, err := keyrange.New(p.engineOptions(own), p.description(), p.fetch)
	p.Engine = e
	return err
}

// description is what the range engine must know about the provider.
func (p *provider[P]) description() keyrange.Description[P] {
	d := keyrange.Description[P]{
		Target: p.target(),
		// The adapter serves every page in byte order, from any start
		// name, after After (exclusive: every entry of After is dropped)
		// and up to Through.
		Listing:     keyrange.Listing,
		AcceptBound: AcceptBound,
		Requeue:     objhttp.Requeue,
		InvalidPage: objhttp.ErrInvalidPage,
		Size:        p.size,
		Counters:    slices.Concat(client.CounterDefs, order.CounterDefs),
	}
	if p.schema.Groups {
		// A name's entries are a group: held until a later name or the
		// chain's end proves it complete, never split.
		d.Groups, d.Detach = true, p.schema.Detach
	}
	return d
}

// adapter is a byte-order adapter over the provider's client, counting in
// the provider's counters.
func (p *provider[P]) adapter() *order.Adapter {
	a := order.New(p.client, p.opt.Container, p.Set(), p.ID(len(client.CounterDefs)))
	a.Include = p.include()
	return a
}

// include is the listing's include= (the adapter leaves out of probes what
// only rows carry).
func (p *provider[P]) include() client.Include {
	all := p.opt.AllVersions
	return client.Include{Versions: all, Snapshots: all, Deleted: all,
		Metadata: p.opt.FetchMetadata, Tags: p.opt.FetchTags, Immutability: p.opt.FetchImmutability}
}

// fill sets what the run asked for and a blob has none of to its empty
// value, so that null means "not asked" (or, for metadata, unreadable: a
// blob encrypted with a customer-provided key lists none, whatever it
// holds): metadata and tags "{}", the legal hold false.
func (p *provider[P]) fill(o *Object) {
	if p.opt.FetchMetadata && o.Metadata == "" && o.CustomerProvidedKeySHA256 == "" {
		o.Metadata = "{}"
	}
	if p.opt.FetchTags && o.Tags == "" {
		o.Tags = "{}"
	}
	if p.opt.FetchImmutability && o.LegalHold == client.Unset {
		o.LegalHold = client.False
	}
}

// tagsRefused names what a listing that asked for tags lacks when Azure
// refuses it the way it refuses include=tags (403
// AuthorizationPermissionMismatch, or an anonymous caller's 401
// NoAuthenticationInformation); any other error, a rejected credential
// among them, is returned as it is.
func (p *provider[P]) tagsRefused(err error) error {
	f := objhttp.Normalize(err)
	if p.opt.FetchTags && f != nil && (f.Code == "AuthorizationPermissionMismatch" || p.unsigned() && f.Code == "NoAuthenticationInformation") {
		return fmt.Errorf("%w (--azure-fetch-tags needs the right to read blob index tags: a SAS with the t permission, "+
			"or an Entra ID role with the tags read data action such as Storage Blob Data Owner; an anonymous caller has none)", err)
	}
	return err
}

// adapterKey carries an Expand's adapter on its context.
type adapterKey struct{}

// Expand lists one range (keyrange.Engine.Expand) with its own byte-order
// adapter, whose chains (one per continuation chain the range starts:
// the listing and its probes) are released however Expand returns (the
// specification's §5 lifetime: the engine joins its probes and seed work
// before Expand returns, and children carry bounds, not tokens).
func (p *provider[P]) Expand(ctx context.Context, r Range, e source.Emitter[Range, P]) error {
	a := p.adapter()
	defer a.Release()
	return p.Engine.Expand(context.WithValue(ctx, adapterKey{}, a), r, e)
}

// admit establishes the namespace. namespaceOf reads the account or Data
// Lake headers, or recognizes a Data Lake response specific to flat
// accounts. Admission requeues transient failures for as long as the run
// lives. Inventory can probe a listing when namespace reads were refused.
// Flat asserts a namespace only when the reads offered no answer.
func (p *provider[P]) admit(ctx context.Context, probeTree bool) (bool, error) {
	var wait time.Duration
	hns, how, busy, rejected, err := p.namespaceOf(ctx)
	for busy && ctx.Err() == nil {
		if werr := keyrange.RequeueBackoff(ctx, &wait); werr != nil {
			return false, werr
		}
		hns, how, busy, rejected, err = p.namespaceOf(ctx)
	}
	if ce := (*objhttp.CredentialError)(nil); errors.As(err, &ce) {
		return false, fmt.Errorf("Azure Blob client: %w", err)
	}
	if ctx.Err() == nil && rejected {
		return false, fmt.Errorf("Azure Blob: account %s: %w", p.opt.Account, err)
	}
	if probeTree && ctx.Err() == nil && fallback(err) {
		proved, probeErr := p.probeNamespace(ctx)
		switch {
		case ctx.Err() != nil:
			return false, context.Cause(ctx)
		case errors.As(probeErr, new(*objhttp.CredentialError)):
			return false, fmt.Errorf("Azure Blob client: %w", probeErr)
		case !p.unsigned() && class(probeErr) == objhttp.ClassAuth:
			return false, fmt.Errorf("Azure Blob: account %s: %w", p.opt.Account, probeErr)
		case probeErr != nil && !p.opt.Flat && !refused(probeErr):
			return false, fmt.Errorf("Azure Blob: account %s: namespace listing probe: %w", p.opt.Account, probeErr)
		case proved:
			hns, how, err = true, "listing", nil
		}
	}
	switch {
	case err != nil && ctx.Err() != nil:
		return false, context.Cause(ctx)
	case err == nil:
		p.namespace = how
		return hns, nil
	case p.opt.Flat:
		p.namespace = "asserted"
		if p.opt.Warn != nil {
			p.opt.Warn(source.WarnFlatAssumed, fmt.Sprintf("Azure Blob: account %s: the namespace could not be read (%v); listing as flat, as --azure-assume-flat asserts", p.opt.Account, err))
		}
	case errors.Is(err, source.ErrUnreachable):
		// Nothing answered at all (objhttp's fail-fast): no answer about
		// the namespace either; the run stops as unreachable.
		return false, fmt.Errorf("Azure Blob: account %s: %w", p.opt.Account, err)
	case !p.unsigned() && refused(err):
		// Credentials the service rejected: what any request would meet.
		return false, fmt.Errorf("Azure Blob: account %s: %w", p.opt.Account, err)
	default:
		return false, source.Unsupported(fmt.Errorf("Azure Blob: account %s: cannot establish its namespace (%w); --azure-assume-flat asserts a flat namespace", p.opt.Account, err))
	}
	return false, nil
}

// probeNamespace looks for an affirmative resource type. A container SAS
// may list a directory without reading account information. Absence of a
// resource type cannot establish flatness.
func (p *provider[P]) probeNamespace(ctx context.Context) (bool, error) {
	dir := p.opt.Prefix[:strings.LastIndexByte(p.opt.Prefix, '/')+1]
	in := &client.ListInput{Container: p.opt.Container, Prefix: dir, Delimiter: "/", MaxResults: 1, Tree: true}
	var seen *keyrange.TokenWindow
	var wait time.Duration
	for {
		if ctx.Err() != nil {
			return false, context.Cause(ctx)
		}
		p.probeRequests++
		out, err := p.client.List(ctx, in)
		if objhttp.Requeue(err) {
			p.probeRequeues++
			if err := keyrange.RequeueBackoff(ctx, &wait); err != nil {
				return false, err
			}
			continue
		}
		if err != nil {
			return false, err
		}
		if len(out.Items) > 0 {
			return out.Items[0].Hierarchical, nil
		}
		if out.NextMarker == "" {
			return false, nil
		}
		if seen == nil {
			seen = keyrange.NewTokenWindow(in.Marker)
		}
		if !seen.Add(out.NextMarker) {
			return false, source.Violation(fmt.Errorf("%w: namespace listing probe repeats an empty continuation marker", objhttp.ErrInvalidPage))
		}
		p.probeEmptyPages++
		in.Marker, wait = out.NextMarker, 0
	}
}

// namespaceOf is one round of admit's calls: whether the namespace is
// hierarchical, which call told (how), or why none did (err). busy
// reports that the call deciding it spent its retries on transient
// failures (objhttp.Requeue), so asking again may answer. rejected means
// the Blob endpoint rejected a signed credential. A Data Lake-only
// authentication failure does not establish that about the Blob endpoint.
func (p *provider[P]) namespaceOf(ctx context.Context) (hns bool, how string, busy, rejected bool, err error) {
	hns, err = p.client.AccountInfo(ctx, p.opt.Container)
	if ce := (*objhttp.CredentialError)(nil); errors.As(err, &ce) {
		return false, "", false, false, err
	}
	how = "account information"
	if !p.unsigned() && class(err) == objhttp.ClassAuth {
		return false, how, false, true, err
	}
	if err != nil && ctx.Err() == nil && fallback(err) {
		if n, ok := p.client.(namespaced); ok {
			if h, nerr := n.NamespaceInfo(ctx, p.opt.Container); nerr == nil {
				return h, "filesystem properties", false, false, nil
			} else if flatFeatures(nerr) {
				return false, "filesystem endpoint blocked", false, false, nil
			} else if !errors.Is(nerr, client.ErrNoDFSEndpoint) {
				return hns, how, objhttp.Requeue(nerr), false, fmt.Errorf("%w; filesystem properties: %w", err, nerr)
			}
		}
	}
	return hns, how, objhttp.Requeue(err), false, err
}

// unsigned says requests go anonymous, which Get Account Information
// refuses (401) whatever the namespace.
func (p *provider[P]) unsigned() bool {
	if c, ok := p.client.(interface{ Credentials() (string, bool) }); ok {
		_, anonymous := c.Credentials()
		return anonymous
	}
	return p.anonymous
}

// codeEndpointUnsupported is the Data Lake host's refusal of an account
// for its Blob-only features.
const codeEndpointUnsupported = "EndpointUnsupportedAccountFeatures"

// flatFeatures: the Data Lake host refused the account with 409
// EndpointUnsupportedAccountFeatures ("This endpoint does not support
// BlobStorageEvents or SoftDelete"). Azure documents that blob soft delete
// blocks the Data Lake endpoint only on an account without a hierarchical
// namespace; with one, that endpoint is the account's own API and works
// with soft delete on. The answer therefore establishes a flat namespace.
// Most public open-data accounts answer it (60 of the 64 Planetary
// Computer containers; NOAA GOES, HRRR, GFS, NWM; NAIP; checked live on
// 2026-11-11).
func flatFeatures(err error) bool {
	f := objhttp.Normalize(err)
	return f != nil && f.Status == http.StatusConflict && f.Code == codeEndpointUnsupported
}

// fallback: Get Account Information was refused or answered without the
// header, so the Data Lake host may tell.
func fallback(err error) bool {
	return errors.Is(err, client.ErrHNSUnknown) || refused(err)
}

// refused: the service refused the caller (class auth or access denied, by
// Azure's status and code).
func refused(err error) bool {
	c := class(err)
	return c == objhttp.ClassAuth || c == objhttp.ClassAccessDenied
}

// class is the service's classification of err (by Azure's status and
// code); the zero class when err is not a service answer.
func class(err error) objhttp.Class {
	if errors.Is(err, source.ErrUnsupported) {
		return ""
	}
	f := objhttp.Normalize(err)
	if f == nil {
		return ""
	}
	return (&client.Error{Status: f.Status, Code: f.Code}).Failure().Class
}

// connect builds the client unless Options.Client supplies one (own).
func connect[P any](ctx context.Context, opt Options) (*provider[P], bool, error) {
	if opt.Account == "" || opt.Container == "" {
		return nil, false, errors.New("Azure Blob: no account or container")
	}
	p := &provider[P]{opt: opt, client: opt.Client}
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
			opt.Warn(source.WarnAnonymous, n)
		}
		if opt.Trust.NoVerify && opt.Warn != nil {
			opt.Warn(source.WarnTLSUnverified, source.NoVerifyWarning)
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
// defaultPageSize unless the policy asks for another size, and of the
// service's largest (client.MaxPageSize) with a counted prior, whose tiles
// hold whole pages of keys each, so a range's last page discards little;
// Azure bills per request.
func (p *provider[P]) engineOptions(own bool) keyrange.Options {
	opt := p.opt
	eo := opt.Policy.Options()
	eo.Prefix, eo.Workers, eo.ClientOff = opt.Prefix, opt.Workers, opt.Admission.Off(own)
	if eo.PageSize <= 0 {
		eo.PageSize, eo.PriorPageSize = defaultPageSize, client.MaxPageSize
	}
	return eo
}

// target is the listed root, without credentials.
func (p *provider[P]) target() string {
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
func (p *provider[P]) Stats() Stats { return p.stats(p.Engine) }

func (p *provider[P]) stats(eng *keyrange.Engine[P]) Stats {
	p.Activity.Store(eng.Set(), eng.ID(0))
	if c, ok := p.client.(pooled); ok {
		eng.Set().Set(eng.ID(client.CPoolSize), int64(c.Transports()))
	}
	if c, ok := p.client.(capped); ok {
		eng.Set().Set(eng.ID(client.CRetryAfterCapped), c.RetryAfterCapped())
	}
	if c, ok := p.client.(formatted); ok {
		f := c.Formats()
		eng.Set().Set(eng.ID(client.CArrowPages), f.ArrowPages)
		eng.Set().Set(eng.ID(client.CArrowItems), f.ArrowItems)
		eng.Set().Set(eng.ID(client.CXMLPages), f.XMLPages)
		eng.Set().Set(eng.ID(client.CXMLItems), f.XMLItems)
	}
	e := eng.Stats()
	st := Stats{Anonymous: p.anonymous, Credentials: p.creds, Namespace: p.namespace, NamespaceProbeRequests: p.probeRequests, NamespaceProbeEmptyPages: p.probeEmptyPages, NamespaceProbeRequeues: p.probeRequeues, NoVerifySSL: p.opt.Client == nil && p.opt.Trust.NoVerify,
		MeanActive: e.MeanActive, Counters: e.Counters}
	return st
}

// fetch lists one page in byte order through the Expand's adapter and
// converts its items to rows and prefixes.
func (p *provider[P]) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[P], error) {
	a, ok := ctx.Value(adapterKey{}).(*order.Adapter)
	if !ok || a == nil {
		return nil, source.Violation(errors.New("Azure Blob: fetch without an Expand adapter"))
	}
	out, err := a.Fetch(ctx, q)
	if err != nil {
		return nil, p.tagsRefused(err)
	}
	nblobs := 0
	for i := range out.Items {
		if !out.Items[i].Prefix {
			nblobs++
		}
	}
	page := &keyrange.Page[P]{Rows: make([]schema.Row[P], 0, nblobs), Truncated: out.Truncated, Token: out.Token}
	for i := range out.Items {
		it := &out.Items[i]
		if it.Prefix {
			page.Prefixes = append(page.Prefixes, it.Name)
			continue
		}
		page.Rows = append(page.Rows, schema.Row[P]{Key: it.Name, Data: p.row(p, it)})
	}
	return page, nil
}

// object is a listed blob's row payload.
func (p *provider[P]) object(it *client.Item) Object {
	o := itemObject(it)
	p.fill(&o)
	return o
}

func itemObject(it *client.Item) Object {
	return Object{Size: it.Size, Mtime: it.LastModified, CreationTime: it.CreationTime, ETag: it.ETag, ContentMD5: it.ContentMD5,
		ContentType: it.ContentType, BlobType: it.BlobType, AccessTier: it.AccessTier, Props: it.Props}
}
