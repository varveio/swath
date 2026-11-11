// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&Schema), Classify: objhttp.Label, Scheme: "az", KeyPrefix: keyPrefix,
		Flags: flags, Shared: []*source.FlagGroup{source.StoreGroup, keyrange.Group}, Defaults: defaults, Open: open, Redact: RedactRoot,
		Secrets: []registry.Secret{{Flag: "azure-sas-token", Env: EnvSAS}},
		FromURL: fromURL, Title: "Azure Blob", Example: "az://account/container", Usage: "az://account/container[/prefix] (or https://account.blob.core.windows.net/container[/prefix])"})
}

// flagged is the Azure flag group's value: the Azure options, the store
// group's values and the range engine's policy and HTTP admission, whose
// flags every engine provider shares.
type flagged struct {
	Options
	store  *source.Store
	common *keyrange.Common
}

func defaults() any { return &flagged{} }

// flags registers the Azure flag group beside the shared groups' values.
// The flags are prefixed: one flag set serves every provider.
func flags(f *pflag.FlagSet, diag bool, shared source.SharedValues) any {
	o := &flagged{store: source.Shared[source.Store](shared, source.StoreGroup), common: source.Shared[keyrange.Common](shared, keyrange.Group)}
	source.Define(f, source.Flag{Name: "azure-sas-token", Shown: source.ShownNever, Familiar: []string{"azblob-sas"}, Value: source.Text(&o.SAS), Metavar: "TOKEN", Usage: "a shared access signature (a query string); never recorded",
		Long: "Else a SAS in the source's query, else " + EnvSAS + "; with none, an account key in " + EnvKey + " (Shared Key), else Entra ID (DefaultAzureCredential). Give it again to swath resume."})
	source.Define(f, source.Flag{Name: "azure-fetch-metadata", Value: source.Switch(&o.FetchMetadata), Usage: "fill the metadata column with each blob's user metadata",
		Long: "A JSON object, keys sorted; {} for a blob without any, null for one encrypted with a customer-provided key. Up to 8 KiB a blob: it grows the responses, the memory a row holds and the dataset."})
	source.Define(f, source.Flag{Name: "azure-fetch-tags", Value: source.Switch(&o.FetchTags), Usage: "fill the tags column with each blob's index tags",
		Long: "A JSON object, keys sorted; {} for a blob without any. Needs the right to read tags (a SAS with the t permission, or an Entra ID role such as Storage Blob Data Owner); without it every request fails, anonymous ones included."})
	source.Define(f, source.Flag{Name: "azure-fetch-immutability", Value: source.Switch(&o.FetchImmutability), Usage: "fill the immutability policy and legal hold columns"})
	source.Define(f, source.Flag{Name: "azure-assume-flat", Value: source.Switch(&o.Flat), Usage: "assume a flat namespace where the account will not tell",
		Long: "For when Get Account Information and the Data Lake host's Filesystem Get Properties are both refused or silent (a Data Lake host that refuses the account for soft delete or blob events already establishes a flat one); a hierarchical namespace they report still refuses. Frozen for resume and shown in the report."})
	if diag {
		source.Define(f, source.Flag{Name: "azure-page-policy", Value: source.Text(&o.PagePolicy), Metavar: "fixed|one-read|grow|estimate",
			Usage: "experimental page policy (default one-read)",
			Long:  "fixed: pages of --page-size, read again to fill a page, as before one read per page became the default; one-read (the default) ends a page at its first read's last row instead of reading again for the rest; grow adds pages that double after full pages, up to 5,000, but to at most half the keys estimated left; estimate asks the finite remainder estimate plus two, 1,000 to 5,000. grow and estimate read once per page. The default is one-read."})
	}
	return o
}

// blobHost is the host suffix of an account's blob service URL.
const blobHost = ".blob.core.windows.net"

// fromURL is the az:// root of a blob service URL, as the portal and
// azcopy show it (registry.Factory.FromURL):
// https://ACCOUNT.blob.core.windows.net/CONTAINER[/PREFIX][?SAS].
func fromURL(root string) (string, bool) {
	rest, ok := strings.CutPrefix(root, "https://")
	if !ok {
		return "", false
	}
	host, path, _ := strings.Cut(rest, "/")
	account, ok := strings.CutSuffix(strings.ToLower(host), blobHost)
	if !ok || account == "" || strings.Contains(account, ".") {
		return "", false
	}
	return "az://" + account + "/" + path, true
}

// errSASNotRecorded: a resumed run whose root held a SAS, which the
// journal never records, given none again.
var errSASNotRecorded = errors.New("Azure Blob: the run's root carried a SAS, which is never recorded: give it again with --azure-sas-token or " + EnvSAS)

func open(ctx context.Context, req registry.Request) (registry.Source, error) {
	account, container, prefix, sas, err := ParseURL(req.Root)
	if err != nil {
		return nil, err
	}
	var opt Options
	switch o := req.Options.(type) {
	case *flagged: // the command line
		opt = o.Options
		if o.store != nil {
			opt.Endpoint, opt.Timeout, opt.Anonymous, opt.Trust = o.store.Endpoint, o.store.Timeout, o.store.Anonymous, o.store.Trust
		}
		if o.common != nil {
			opt.Policy, opt.Admission, opt.AllVersions = o.common.Policy, o.common.Admission, o.common.AllVersions
		}
		switch {
		case opt.Anonymous && opt.SAS != "":
			return nil, source.AnonymousConflict("--azure-sas-token")
		case opt.Anonymous && sas != "":
			return nil, source.AnonymousConflict("the SAS in the source URL")
		}
	case *Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("Azure Blob: options are %T, not *azblob.Options", req.Options)
	}
	if opt.SAS == "" && sas != "" {
		if sas == RedactedSAS {
			if os.Getenv(EnvSAS) == "" && !opt.Anonymous {
				return nil, errSASNotRecorded
			}
		} else {
			opt.SAS = sas
		}
	}
	opt.Account, opt.Container, opt.Prefix, opt.Workers, opt.Warn = account, container, prefix, req.Workers, req.Warn
	if opt.AllVersions {
		p, err := NewVersions(ctx, opt)
		if err != nil {
			return nil, err
		}
		return &opened[Version]{Bound: registry.Bind[Range, Version](p, &VersionSchema), Registered: p, p: p}, nil
	}
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &opened[Object]{Bound: registry.Bind[Range, Object](p, &Schema), Registered: p, p: p}, nil
}

type opened[P any] struct {
	registry.Bound[P]
	keyrange.Registered
	p *provider[P]
}

// Locator records the root without its SAS.
func (o *opened[P]) Locator() source.Locator {
	return source.Locator{Kind: "azblob", Root: o.p.target(), Container: o.p.opt.Container}
}

// endpoint is the account's blob service URL as the client requests it
// (the route without the container): https://ACCOUNT.blob.core.windows.net,
// or an endpoint with the account path segment.
func (o *opened[P]) endpoint() string {
	return strings.TrimSuffix(strings.TrimSuffix(o.p.route, o.p.opt.Container), "/")
}

// Identity freezes what a resume must list the same: the source (account,
// container, prefix), the endpoint requests go to (scheme and host case
// are the only equivalence), the account and the container. Credentials
// may change, a SAS among them. flat_asserted is --azure-assume-flat, so a
// resume cannot add or drop the assertion. A supplied client must name its
// route (Routed), or the run declines resume.
func (o *opened[P]) Identity() source.Identity {
	ep := o.endpoint()
	id := source.Identity{Provider: "azblob", Fields: []source.IdentityField{
		{Name: "source", Value: o.p.target()},
		{Name: "endpoint", Value: ep, Canonical: objhttp.CanonicalRoute(ep)},
		{Name: "account", Value: o.p.opt.Account},
		{Name: "container", Value: o.p.opt.Container},
		{Name: "flat_asserted", Value: strconv.FormatBool(o.p.opt.Flat)},
	}}
	// What a run asked for beyond the default columns, only when asked,
	// so a default run's identity is as before they existed.
	for _, f := range []struct {
		name string
		on   bool
	}{{"fetch_metadata", o.p.opt.FetchMetadata}, {"fetch_tags", o.p.opt.FetchTags}, {"fetch_immutability", o.p.opt.FetchImmutability}} {
		if f.on {
			id.Fields = append(id.Fields, source.IdentityField{Name: f.name, Value: "true"})
		}
	}
	return id
}

// Capabilities: the byte-order adapter serves byte-ordered pages from
// any start name, so the range engine applies; --sequential
// lists one marker chain. A run with an object-count cap, or through a
// supplied client that does not name its route, cannot resume (as for S3
// and GCS).
func (o *opened[P]) Capabilities() source.Capabilities {
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && o.p.route != "")
}

func (o *opened[P]) Report() source.Report {
	st := o.p.Stats()
	return keyrange.Report("azblob", o.p.target(), o.p.opt.Prefix, &st, st.Counters)
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
	_ registry.Source     = (*opened[Version])(nil)
	_ keyrange.Registered = (*opened[Version])(nil)
	_ source.Identified   = (*opened[Version])(nil)
	_ source.Capable      = (*opened[Version])(nil)
	_ source.Reporter     = (*opened[Version])(nil)
)

// keyPrefix is the name prefix a root names (registry.Factory.KeyPrefix).
func keyPrefix(root string) (string, error) {
	_, _, prefix, _, err := ParseURL(root)
	return prefix, err
}
