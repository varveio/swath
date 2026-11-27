// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

// Opened is an Azure Blob listing opened by Open or OpenVersions: the
// provider bound to its row schema (Bound), and what a run asks of an
// opened source besides its rows: the range engine's optional interfaces
// (keyrange.Registered, set to the provider, so nothing else of it is
// promoted), the locator, the resume identity, the capabilities and the
// report. It chooses no writer; the command joins it with one
// (internal/cli).
type Opened[P any] struct {
	keyrange.Registered
	bound bind.Bound[P]
	p     *provider[P]
}

// errSASNotRecorded: a resumed run whose root held a SAS, which the
// journal never records, given none again.
var errSASNotRecorded = errors.New("Azure Blob: the run's root carried a SAS, which is never recorded: give it again with --azure-sas-token or " + EnvSAS)

// Inventory is an opened listing of current Azure objects. The concrete
// source exposes the capabilities of its listing mode.
type Inventory interface {
	Bound() bind.Bound[Object]
	Options() Options
	Locator() source.Locator
	Listed() int64
	source.Identified
	source.Capable
	source.Reporter
}

// Open opens root, az://ACCOUNT/CONTAINER[/PREFIX][?SAS], for its current
// blobs: flat accounts list by range and hierarchical accounts list by
// directory. The account, container and prefix come from root
// (ParseURL), and the root's SAS where opt has none. A root whose SAS was
// redacted (RedactedSAS: a resumed run's) needs a SAS again, from opt or
// EnvSAS, or anonymity. opt.AllVersions is refused (New): OpenVersions
// lists every entry on flat accounts.
func Open(ctx context.Context, root string, opt Options) (Inventory, error) {
	return OpenNarrowed(ctx, root, "", opt)
}

// OpenNarrowed opens root after include-based narrowing. Original, when
// present, is the root before narrowing. A hierarchical listing can narrow
// only to a directory whose own row cannot match the inferred prefix.
func OpenNarrowed(ctx context.Context, root, original string, opt Options) (Inventory, error) {
	if err := opt.setRoot(root); err != nil {
		return nil, err
	}
	if opt.AllVersions {
		return nil, errors.New("Azure Blob: Options.AllVersions lists every entry: use OpenVersions")
	}
	narrowed := opt.Prefix
	if original != "" {
		account, container, prefix, _, err := ParseURL(original)
		if err != nil {
			return nil, err
		}
		if account != opt.Account || container != opt.Container || !strings.HasPrefix(narrowed, prefix) {
			return nil, errors.New("Azure Blob: narrowed root is outside the original scope")
		}
		opt.Prefix = prefix
	}
	p, own, err := connect[Object](ctx, opt)
	if err != nil {
		return nil, err
	}
	p.schema, p.row, p.size = &Schema, (*Provider).object, func(o *Object) int64 { return o.Size }
	hns, err := p.admit(ctx, true)
	if err != nil {
		return nil, err
	}
	if hns {
		// Preserve the user's original scope. A directory used as the
		// traversal root emits no row for itself, so a narrowed prefix that
		// ends in '/' must start from its parent if that row could match.
		if opt.Prefix == "" || strings.HasSuffix(opt.Prefix, "/") {
			dir := strings.TrimSuffix(narrowed, "/")
			dir = dir[:strings.LastIndexByte(dir, '/')+1]
			if len(dir) > len(opt.Prefix) {
				p.opt.Prefix = dir
			}
		}
		if original == "" {
			narrowed = ""
		}
		return openTree(p, own, narrowed)
	}
	p.opt.Prefix = narrowed
	if err := p.flat(own); err != nil {
		return nil, err
	}
	return opened(p, &Schema)
}

// OpenVersions is Open for every entry of each name (NewVersions), its
// rows of schema swath/azure-versions/v1.
func OpenVersions(ctx context.Context, root string, opt Options) (*Opened[Version], error) {
	if err := opt.setRoot(root); err != nil {
		return nil, err
	}
	opt.AllVersions = true
	p, err := build(ctx, opt, true, &VersionSchema, (*VersionsProvider).version, func(v *Version) int64 { return v.Size })
	if err != nil {
		return nil, err
	}
	return opened(p, &VersionSchema)
}

// setRoot sets what root names: the account, the container, the prefix
// and, where o has none, its SAS.
func (o *Options) setRoot(root string) error {
	account, container, prefix, sas, err := ParseURL(root)
	if err != nil {
		return err
	}
	if o.SAS == "" && sas != "" {
		if sas == RedactedSAS {
			if os.Getenv(EnvSAS) == "" && !o.Anonymous {
				return errSASNotRecorded
			}
		} else {
			o.SAS = sas
		}
	}
	o.Account, o.Container, o.Prefix = account, container, prefix
	return nil
}

// opened binds p to its row schema s (bind.Provider).
func opened[P any](p *provider[P], s *schema.Schema[P]) (*Opened[P], error) {
	b, err := bind.Provider[Range, P](p, s)
	if err != nil {
		return nil, err
	}
	return &Opened[P]{Registered: p, bound: b, p: p}, nil
}

// Bound is the provider bound to its row schema.
func (o *Opened[P]) Bound() bind.Bound[P] { return o.bound }

// Options is what the listing opened with: the options given, with the
// account, container and prefix the root names and, where the options had
// none, the root's SAS.
func (o *Opened[P]) Options() Options { return o.p.opt }

// Locator records the root without its SAS.
func (o *Opened[P]) Locator() source.Locator {
	return source.Locator{Kind: "azure", Root: o.p.target(), Container: o.p.opt.Container}
}

// endpoint is the account's blob service URL as the client requests it
// (the route without the container): https://ACCOUNT.blob.core.windows.net,
// or an endpoint with the account path segment.
func (o *Opened[P]) endpoint() string {
	return strings.TrimSuffix(strings.TrimSuffix(o.p.route, o.p.opt.Container), "/")
}

// Identity freezes what a resume must list the same: the source (account,
// container, prefix), the endpoint requests go to (scheme and host case
// are the only equivalence), the account and the container. Credentials
// may change, a SAS among them. flat_asserted is --azure-assume-flat, so a
// resume cannot add or drop the assertion. A supplied client must name its
// route (Routed), or the run declines resume.
func (o *Opened[P]) Identity() source.Identity {
	ep := o.endpoint()
	id := source.Identity{Provider: "azure", Fields: []source.IdentityField{
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
func (o *Opened[P]) Capabilities() source.Capabilities {
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && o.p.route != "")
}

// Report is the listing's statistics, naming the columns fetched beyond
// the default ones.
func (o *Opened[P]) Report() source.Report {
	st := o.p.Stats()
	return o.p.report(&st)
}

func (p *provider[P]) report(st *Stats) source.Report {
	r := keyrange.Report("azure", p.target(), p.opt.Prefix, st, st.Counters)
	if p.opt.FetchMetadata {
		r.Collected = append(r.Collected, "metadata")
	}
	if p.opt.FetchTags {
		r.Collected = append(r.Collected, "tags")
	}
	if p.opt.FetchImmutability {
		r.Collected = append(r.Collected, "immutability_policy_until", "immutability_policy_mode", "legal_hold")
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
	_ keyrange.Registered = (*Opened[Version])(nil)
	_ source.Identified   = (*Opened[Version])(nil)
	_ source.Capable      = (*Opened[Version])(nil)
	_ source.Reporter     = (*Opened[Version])(nil)
)

// KeyPrefix is the name prefix root names: az://a/c/logs/ names "logs/".
func KeyPrefix(root string) (string, error) {
	_, _, prefix, _, err := ParseURL(root)
	return prefix, err
}

// WithPrefix is root naming prefix instead of its own, its SAS kept byte
// for byte. An az:// root is not an escaped URL: '%' and '+' are a name's
// own bytes, and '?' belongs to the prefix unless the query names a SAS
// parameter (ParseURL). So the new root is taken only if it parses back
// to the same account, container and SAS and to prefix itself; a prefix
// it cannot carry (a '?' beside a SAS, or text after a '?' that would read
// as a SAS) is declined: root comes back as it was, as does a root that
// does not parse.
func WithPrefix(root, prefix string) string {
	account, container, _, sas, err := ParseURL(root)
	if err != nil {
		return root
	}
	narrowed := "az://" + account + "/" + container + "/" + prefix
	if sas != "" {
		narrowed += "?" + sas
	}
	if a, c, p, s, err := ParseURL(narrowed); err != nil || a != account || c != container || p != prefix || s != sas {
		return root
	}
	return narrowed
}
