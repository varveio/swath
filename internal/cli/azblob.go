// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&azblob.Schema), Schemas: []schema.Info{azblob.Schema.Info, azblob.VersionSchema.Info}, Classify: objhttp.Label, Scheme: "az", KeyPrefix: azblob.KeyPrefix, WithPrefix: azblob.WithPrefix,
		Flags: azblobFlags, Shared: []*registry.FlagGroup{StoreGroup, EngineGroup}, Defaults: azblobDefaults, Open: openAzblob, Redact: azblob.RedactRoot,
		Secrets: []registry.Secret{{Flag: "azure-sas-token", Env: azblob.EnvSAS}},
		FromURL: azblobFromURL, Title: "Azure Blob", Example: "az://account/container", Usage: "az://account/container[/prefix] (or https://account.blob.core.windows.net/container[/prefix])"})
}

// azblobFlagged is the Azure flag group's value: the Azure options, the
// store group's values and the range engine's policy and HTTP admission,
// whose flags every engine provider shares.
type azblobFlagged struct {
	azblob.Options
	store  *source.Store
	common *keyrange.Common
}

func azblobDefaults() any { return &azblobFlagged{} }

// azblobFlags registers the Azure flag group beside the shared groups'
// values. The flags are prefixed: one flag set serves every provider.
func azblobFlags(f *pflag.FlagSet, diag bool, shared registry.SharedValues) any {
	o := &azblobFlagged{store: registry.Shared[source.Store](shared, StoreGroup), common: registry.Shared[keyrange.Common](shared, EngineGroup)}
	Define(f, Flag{Name: "azure-sas-token", Shown: ShownNever, Familiar: []string{"azblob-sas"}, Value: Text(&o.SAS), Metavar: "TOKEN", Usage: "a shared access signature (a query string); never recorded",
		Long: "Else a SAS in the source's query, else " + azblob.EnvSAS + "; with none, an account key in " + azblob.EnvKey + " (Shared Key), else Entra ID (DefaultAzureCredential). Give it again to swath resume."})
	Define(f, Flag{Name: "azure-fetch-metadata", Value: Switch(&o.FetchMetadata), Usage: "fill the metadata column with each blob's user metadata",
		Long: "A JSON object, keys sorted; {} for a blob without any, null for one encrypted with a customer-provided key. Up to 8 KiB a blob: it grows the responses, the memory a row holds and the dataset."})
	Define(f, Flag{Name: "azure-fetch-tags", Value: Switch(&o.FetchTags), Usage: "fill the tags column with each blob's index tags",
		Long: "A JSON object, keys sorted; {} for a blob without any. Needs the right to read tags (a SAS with the t permission, or an Entra ID role such as Storage Blob Data Owner); without it every request fails, anonymous ones included."})
	Define(f, Flag{Name: "azure-fetch-immutability", Value: Switch(&o.FetchImmutability), Usage: "fill the immutability policy and legal hold columns"})
	Define(f, Flag{Name: "azure-assume-flat", Value: Switch(&o.Flat), Usage: "assume a flat namespace where the account will not tell",
		Long: "For when Get Account Information and the Data Lake host's Filesystem Get Properties are both refused or silent (a Data Lake host that refuses the account for soft delete or blob events already establishes a flat one); a hierarchical namespace they report uses tree listing for current blobs. Frozen for resume and shown in the report."})
	return o
}

// blobHost is the host suffix of an account's blob service URL.
const blobHost = ".blob.core.windows.net"

// azblobFromURL is the az:// root of a blob service URL, as the portal and
// azcopy show it (registry.Factory.FromURL):
// https://ACCOUNT.blob.core.windows.net/CONTAINER[/PREFIX][?SAS].
func azblobFromURL(root string) (string, bool) {
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

// openAzblob opens an Azure Blob root with the command line's options
// (azblobFlagged) or a library caller's (*azblob.Options).
func openAzblob(ctx context.Context, req registry.Request) (registry.Source, error) {
	_, _, _, sas, err := azblob.ParseURL(req.Root)
	if err != nil {
		return nil, err
	}
	var opt azblob.Options
	switch o := req.Options.(type) {
	case *azblobFlagged: // the command line
		if opt, err = o.options(sas); err != nil {
			return nil, err
		}
	case *azblob.Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("Azure Blob: options are %T, not *azblob.Options", req.Options)
	}
	opt.Workers, opt.Warn = req.Workers, req.Warn

	if opt.AllVersions {
		o, err := azblob.OpenVersions(ctx, req.Root, opt)
		if err != nil {
			return nil, err
		}
		return &azblobSource[azblob.Version]{registry.From(o.Bound()), o}, nil
	}
	o, err := azblob.OpenNarrowed(ctx, req.Root, req.OriginalRoot, opt)
	if err != nil {
		return nil, err
	}
	switch o := o.(type) {
	case *azblob.Opened[azblob.Object]:
		return &azblobSource[azblob.Object]{registry.From(o.Bound()), o}, nil
	case *azblob.OpenedTree:
		return &azblobTreeSource{registry.From(o.Bound()), o}, nil
	default:
		return nil, fmt.Errorf("Azure Blob: unexpected inventory type %T", o)
	}
}

// options are the Azure options the command line gives: the Azure flags
// and the shared groups' values. Anonymity beside a SAS, given by flag or
// in the source URL (sas, the root's), is refused.
func (o *azblobFlagged) options(sas string) (azblob.Options, error) {
	opt := o.Options
	if o.store != nil {
		opt.Endpoint, opt.Timeout, opt.Anonymous, opt.Trust = o.store.Endpoint, o.store.Timeout, o.store.Anonymous, o.store.Trust
	}
	if o.common != nil {
		opt.Policy, opt.Admission, opt.AllVersions = o.common.Policy, o.common.Admission, o.common.AllVersions
	}

	switch {
	case opt.Anonymous && opt.SAS != "":
		return azblob.Options{}, source.AnonymousConflict("--azure-sas-token")
	case opt.Anonymous && sas != "":
		return azblob.Options{}, source.AnonymousConflict("the SAS in the source URL")
	}
	return opt, nil
}

// azblobSource is an opened Azure Blob listing as a run takes it: its
// rows, bound when it opened, go where the run's Env sends them
// (registry.Bound, whose field hides the view's Bound method); the rest is
// the opened view's.
type azblobSource[P any] struct {
	registry.Bound[P]
	*azblob.Opened[P]
}

// The registered source's whole method set: registry.Source from both
// halves, and every optional interface of the view, which an ambiguous
// or missing method would silently turn off.
var (
	_ registry.Source     = (*azblobSource[azblob.Object])(nil)
	_ keyrange.Registered = (*azblobSource[azblob.Object])(nil)
	_ source.Identified   = (*azblobSource[azblob.Object])(nil)
	_ source.Capable      = (*azblobSource[azblob.Object])(nil)
	_ source.Reporter     = (*azblobSource[azblob.Object])(nil)
	_ registry.Source     = (*azblobSource[azblob.Version])(nil)
	_ keyrange.Registered = (*azblobSource[azblob.Version])(nil)
	_ source.Identified   = (*azblobSource[azblob.Version])(nil)
	_ source.Capable      = (*azblobSource[azblob.Version])(nil)
	_ source.Reporter     = (*azblobSource[azblob.Version])(nil)
)

// azblobTreeSource attaches output and recursive excludes to the tree listing.
type azblobTreeSource struct {
	registry.Bound[azblob.Object]
	*azblob.OpenedTree
}

func (o *azblobTreeSource) Run(ctx context.Context, env registry.Env) (order.Stats, parquetout.Encoder, error) {
	if err := o.PruneDirectories(env.Filter); err != nil {
		return order.Stats{}, nil, err
	}
	return o.Bound.Run(ctx, env)
}

var (
	_ registry.Source   = (*azblobTreeSource)(nil)
	_ source.Identified = (*azblobTreeSource)(nil)
	_ source.Capable    = (*azblobTreeSource)(nil)
	_ source.Reporter   = (*azblobTreeSource)(nil)
	_ source.Live       = (*azblobTreeSource)(nil)
	_ source.Faulted    = (*azblobTreeSource)(nil)
	_ source.Advancing  = (*azblobTreeSource)(nil)
	_ source.Requested  = (*azblobTreeSource)(nil)
)
