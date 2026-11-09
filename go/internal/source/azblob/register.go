// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&Schema), Classify: classify, Scheme: "az", KeyPrefix: keyPrefix,
		Flags: flags, Shared: []*source.FlagGroup{source.StoreGroup, keyrange.Group}, Defaults: defaults, Open: open, Redact: RedactRoot,
		Secrets: []registry.Secret{{Flag: "azblob-sas", Env: EnvSAS}},
		Hints:   []source.Hint{{Flag: "azblob-sas"}, {Flag: "azblob-flat"}},
		Usage:   "az://account/container[/prefix]"})
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
func flags(f *flag.FlagSet, diag bool, shared source.SharedValues) any {
	o := &flagged{store: source.Shared[source.Store](shared, source.StoreGroup), common: source.Shared[keyrange.Common](shared, keyrange.Group)}
	f.StringVar(&o.SAS, "azblob-sas", "", "Azure Blob: shared access signature `TOKEN` (a query string; never recorded: give it again to swath resume); else a SAS in the root's query, else "+EnvSAS+"; with none, an account key in "+EnvKey+" (Shared Key), else Entra ID (DefaultAzureCredential); --anonymous sends none")
	f.BoolVar(&o.Flat, "azblob-flat", false, "Azure Blob: assert a flat namespace where the account will not tell (Get Account Information and the Data Lake host's Filesystem Get Properties both refused or silent); a hierarchical namespace they report still refuses; frozen for resume and shown in the report")
	return o
}

// errSASNotRecorded: a resumed run whose root held a SAS, which the
// journal never records, given none again.
var errSASNotRecorded = errors.New("azblob: the run's root carried a SAS, which is never recorded: give it again with --azblob-sas or " + EnvSAS)

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
			opt.Endpoint, opt.Timeout, opt.Anonymous = o.store.Endpoint, o.store.Timeout, o.store.Anonymous
		}
		if o.common != nil {
			opt.Policy, opt.Admission, opt.AllVersions = o.common.Policy, o.common.Admission, o.common.AllVersions
		}
	case *Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("azblob: options are %T, not *azblob.Options", req.Options)
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
	p, err := New(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &opened{Bound: registry.Bind[Range, Object](p, &Schema), p: p}, nil
}

type opened struct {
	registry.Bound[Object]
	p *Provider
}

// Locator records the root without its SAS.
func (o *opened) Locator() source.Locator {
	return source.Locator{Kind: "azblob", Root: o.p.target(), Container: o.p.opt.Container}
}

func (o *opened) Listed() int64 { return o.p.Listed() }

func (o *opened) Answered() int64 { return o.p.Answered() }

// Frontier is the engine's key-space progress (source.Advancing).
func (o *opened) Frontier() (advanced, emptyChained int64) { return o.p.Frontier() }

// Requests is the live request account (source.Requested).
func (o *opened) Requests() source.Requests { return o.p.Requests() }

// SeedTime is the engine's seed time (source.SeedTimed).
func (o *opened) SeedTime() (time.Duration, bool) { return o.p.SeedTime() }

func (o *opened) Faults() (throttles, timeouts int64) { return o.p.Faults() }

// Contention is the engine's decision-mutex wait and longest hold
// (source.Contended).
func (o *opened) Contention() (wait, maxHold time.Duration) { return o.p.Contention() }

// endpoint is the account's blob service URL as the client requests it
// (the route without the container): https://ACCOUNT.blob.core.windows.net,
// or an endpoint with the account path segment.
func (o *opened) endpoint() string {
	return strings.TrimSuffix(strings.TrimSuffix(o.p.route, o.p.opt.Container), "/")
}

// Identity freezes what a resume must list the same: the source (account,
// container, prefix), the endpoint requests go to (scheme and host case
// are the only equivalence), the account and the container. Credentials
// may change, a SAS among them. flat_asserted is --azblob-flat, so a
// resume cannot add or drop the assertion. A supplied client must name its
// route (Routed), or the run declines resume.
func (o *opened) Identity() source.Identity {
	ep := o.endpoint()
	return source.Identity{Provider: "azblob", Fields: []source.IdentityField{
		{Name: "source", Value: o.p.target()},
		{Name: "endpoint", Value: ep, Canonical: objhttp.CanonicalRoute(ep)},
		{Name: "account", Value: o.p.opt.Account},
		{Name: "container", Value: o.p.opt.Container},
		{Name: "flat_asserted", Value: strconv.FormatBool(o.p.opt.Flat)},
	}}
}

// Capabilities: the byte-order adapter serves byte-ordered pages from
// any start name, so the range engine applies; --sequential
// lists one marker chain. A run with an object-count cap, or through a
// supplied client that does not name its route, cannot resume (as for S3
// and GCS).
func (o *opened) Capabilities() source.Capabilities {
	return keyrange.Capabilities(o.p.opt.Sequential, o.p.opt.MaxKeys == 0 && o.p.route != "")
}

func (o *opened) Restart(after string) error {
	o.p.Restart(after)
	return nil
}

// SeedFrom starts the listing from a prior's cuts instead of the seed
// (source.Seeded, --seed-from).
func (o *opened) SeedFrom(pr *source.Prior) error { return o.p.SeedFrom(pr) }

// Reuse lists around a resume's reused coverage certificates
// (source.Reusing): the engine replays them, lists the gaps.
func (o *opened) Reuse(spans []source.Span) { o.p.Reuse(spans) }

// Fences reports the engine's coverage fences (source.Fencing).
func (o *opened) Fences() bool { return o.p.Fences() }

func (o *opened) Report() source.Report {
	st := o.p.Stats()
	return keyrange.Report("azblob", o.p.target(), o.p.opt.Prefix, &st, st.Counters)
}

var (
	_ source.Identified = (*opened)(nil)
	_ source.Resumable  = (*opened)(nil)
	_ source.Seeded     = (*opened)(nil)
	_ source.Reusing    = (*opened)(nil)
	_ source.Capable    = (*opened)(nil)
	_ source.Reporter   = (*opened)(nil)
)

// classify names the class of an Azure failure (client.Classify) for the
// run's outcome.
func classify(err error) string {
	switch c := client.Classify(err); c {
	case objhttp.ClassOther:
		return ""
	default:
		return string(c)
	}
}

// keyPrefix is the name prefix a root names (registry.Factory.KeyPrefix).
func keyPrefix(root string) (string, error) {
	_, _, prefix, _, err := ParseURL(root)
	return prefix, err
}
