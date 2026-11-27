// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&gcs.Schema), Schemas: []schema.Info{gcs.Schema.Info, gcs.GenerationSchema.Info}, Classify: objhttp.Label, Scheme: "gs", KeyPrefix: gcs.KeyPrefix, WithPrefix: gcs.WithPrefix, Suggest: gcsSuggest,
		Flags: gcsFlags, Shared: []*registry.FlagGroup{StoreGroup, EngineGroup}, Defaults: gcsDefaults, Open: openGCS,
		Operational: []string{"gcs-credentials-file"},
		Title:       "GCS", Example: "gs://bucket", Usage: "gs://bucket[/prefix]"})
}

// gcsSuggest is the gs:// root of a source given as gcs:// or as a GCS
// https URL (storage.googleapis.com/BUCKET/KEY,
// BUCKET.storage.googleapis.com/KEY, storage.cloud.google.com/BUCKET/KEY)
// (registry.Factory.Suggest).
func gcsSuggest(root string) (string, bool) {
	u, err := url.Parse(root)
	if err != nil {
		return "", false
	}
	bucket, key := u.Host, u.Path
	switch scheme, host := strings.ToLower(u.Scheme), strings.ToLower(u.Hostname()); {
	case scheme == "gcs":
	case scheme != "https" && scheme != "http":
		return "", false
	case host == "storage.googleapis.com" || host == "storage.cloud.google.com":
		bucket, key, _ = strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	case strings.HasSuffix(host, ".storage.googleapis.com"):
		bucket = strings.TrimSuffix(host, ".storage.googleapis.com")
	default:
		return "", false
	}
	if bucket == "" {
		return "", false
	}
	if key = strings.TrimPrefix(key, "/"); key != "" {
		bucket += "/" + key
	}
	return "gs://" + bucket, true
}

// gcsFlagged is the GCS flag group's value: the GCS options, the store
// group's values and the range engine's policy and HTTP admission, whose
// flags every engine provider shares.
type gcsFlagged struct {
	gcs.Options
	store  *source.Store
	common *keyrange.Common
}

func gcsDefaults() any { return &gcsFlagged{} }

// gcsFlags registers the GCS flag group beside the shared groups' values.
// The GCS flags are prefixed: one flag set serves every provider.
func gcsFlags(f *pflag.FlagSet, diag bool, shared registry.SharedValues) any {
	o := &gcsFlagged{store: registry.Shared[source.Store](shared, StoreGroup), common: registry.Shared[keyrange.Common](shared, EngineGroup)}
	Define(f, Flag{Name: "gcs-credentials-file", Familiar: []string{"gcs-credentials"}, Value: Text(&o.CredentialsFile), Metavar: "FILE",
		Usage: "a service-account or authorized-user JSON file (default: ADC)",
		Long:  "Without it, Application Default Credentials."})
	Define(f, Flag{Name: "gcs-billing-project", Familiar: []string{"gcs-user-project"}, Value: Text(&o.UserProject), Metavar: "NAME",
		Usage: "bill a requester-pays bucket's requests to this project"})
	return o
}

// openGCS opens a GCS root with the command line's options (gcsFlagged)
// or a library caller's (*gcs.Options).
func openGCS(ctx context.Context, req registry.Request) (registry.Source, error) {
	if _, _, err := gcs.ParseURL(req.Root); err != nil {
		return nil, err
	}
	var opt gcs.Options
	switch o := req.Options.(type) {
	case *gcsFlagged: // the command line
		var err error
		if opt, err = o.options(); err != nil {
			return nil, err
		}
	case *gcs.Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("GCS: options are %T, not *gcs.Options", req.Options)
	}
	opt.Workers, opt.Warn = req.Workers, req.Warn

	if opt.AllVersions {
		o, err := gcs.OpenGenerations(ctx, req.Root, opt)
		if err != nil {
			return nil, err
		}
		return &gcsSource[gcs.Generation]{registry.From(o.Bound()), o}, nil
	}
	o, err := gcs.Open(ctx, req.Root, opt)
	if err != nil {
		return nil, err
	}
	return &gcsSource[gcs.Object]{registry.From(o.Bound()), o}, nil
}

// options are the GCS options the command line gives: the GCS flags and
// the shared groups' values. Anonymity beside a credentials file is
// refused.
func (o *gcsFlagged) options() (gcs.Options, error) {
	opt := o.Options
	if o.store != nil {
		opt.Endpoint, opt.Timeout, opt.Anonymous, opt.Trust = o.store.Endpoint, o.store.Timeout, o.store.Anonymous, o.store.Trust
	}
	if o.common != nil {
		opt.Policy, opt.Admission, opt.AllVersions = o.common.Policy, o.common.Admission, o.common.AllVersions
	}
	if opt.Anonymous && opt.CredentialsFile != "" {
		return gcs.Options{}, source.AnonymousConflict("--gcs-credentials-file")
	}
	if o.store != nil && o.store.Listing == source.ListingTree {
		return gcs.Options{}, errTreeUnsupported("GCS")
	}
	return opt, nil
}

// gcsSource is an opened GCS listing as a run takes it: its rows, bound
// when it opened, go where the run's Env sends them (registry.Bound, whose
// field hides the view's Bound method); the rest is the opened view's.
type gcsSource[P any] struct {
	registry.Bound[P]
	*gcs.Opened[P]
}

// The registered source's whole method set: registry.Source from both
// halves, and every optional interface of the view, which an ambiguous
// or missing method would silently turn off.
var (
	_ registry.Source     = (*gcsSource[gcs.Object])(nil)
	_ keyrange.Registered = (*gcsSource[gcs.Object])(nil)
	_ source.Identified   = (*gcsSource[gcs.Object])(nil)
	_ source.Capable      = (*gcsSource[gcs.Object])(nil)
	_ source.Reporter     = (*gcsSource[gcs.Object])(nil)
	_ source.Priced       = (*gcsSource[gcs.Object])(nil)
	_ registry.Source     = (*gcsSource[gcs.Generation])(nil)
	_ keyrange.Registered = (*gcsSource[gcs.Generation])(nil)
	_ source.Identified   = (*gcsSource[gcs.Generation])(nil)
	_ source.Capable      = (*gcsSource[gcs.Generation])(nil)
	_ source.Reporter     = (*gcsSource[gcs.Generation])(nil)
	_ source.Priced       = (*gcsSource[gcs.Generation])(nil)
)
