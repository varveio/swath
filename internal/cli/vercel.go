// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/vercel"
)

func init() {
	registry.Register(registry.Factory{
		Scheme: "vercel", Title: "Vercel Blob", Example: "vercel://store_ID", Usage: "vercel://store_ID[/prefix]",
		EncoderBytes: parquetout.EncoderBytes(&vercel.Schema), Schemas: []schema.Info{vercel.Schema.Info},
		Classify: objhttp.Label, KeyPrefix: vercel.KeyPrefix, WithPrefix: vercel.WithPrefix,
		Shared: []*registry.FlagGroup{StoreGroup, EngineGroup}, Flags: vercelFlags,
		Defaults: func() any { return &vercelFlagged{} }, Open: openVercel, CheckFilter: checkVercelFilter,
	})
}

type vercelFlagged struct {
	store  *source.Store
	common *keyrange.Common
	flags  *pflag.FlagSet
}

func vercelFlags(fs *pflag.FlagSet, _ bool, shared registry.SharedValues) any {
	return &vercelFlagged{store: registry.Shared[source.Store](shared, StoreGroup),
		common: registry.Shared[keyrange.Common](shared, EngineGroup), flags: fs}
}

func (o *vercelFlagged) options() (vercel.Options, error) {
	opt := vercel.Options{Token: os.Getenv(vercel.TokenEnv)}
	if o.store != nil {
		opt.Timeout, opt.Trust = o.store.Timeout, o.store.Trust
		switch {
		case o.store.Anonymous:
			return opt, source.Unsupported(fmt.Errorf("Vercel Blob inventory requires %s; --anonymous is unsupported", vercel.TokenEnv))
		case o.store.Endpoint != "":
			return opt, source.Unsupported(errors.New("Vercel Blob uses its fixed service endpoint; --endpoint-url is unsupported"))
		case o.store.Listing == source.ListingRange:
			return opt, source.Unsupported(errors.New("Vercel Blob has no arbitrary-key seek; --listing range is unsupported"))
		}
	}
	if o.common != nil {
		opt.Admission, opt.PageSize = o.common.Admission, o.common.Policy.PageSize
	}
	if o.flags != nil {
		for _, name := range []string{AllVersionsFlag, "no-seed", "structure", "slow-split", "sequential", "max-keys", TraceFlag, "seed-from", "seed-from-tile-pages", "seed-from-window-rows"} {
			if f := o.flags.Lookup(name); f != nil && f.Changed {
				return opt, source.Unsupported(fmt.Errorf("--%s is unsupported for Vercel Blob's current-object folder walk", name))
			}
		}
	}
	return opt, nil
}

func checkVercelFilter(_ any, f *filter.Spec) error {
	_, modified, class := f.Reads()
	if modified {
		return errors.New("--modified-since and --modified-until require modification times; Vercel Blob supplies upload times only")
	}
	if class {
		return errors.New("--storage-class: Vercel Blob has no storage classes")
	}
	return nil
}

func openVercel(ctx context.Context, req registry.Request) (registry.Source, error) {
	if _, _, err := vercel.ParseURL(req.Root); err != nil {
		return nil, err
	}
	var opt vercel.Options
	switch o := req.Options.(type) {
	case *vercelFlagged:
		var err error
		opt, err = o.options()
		if err != nil {
			return nil, err
		}
	case *vercel.Options:
		opt = *o
	default:
		return nil, fmt.Errorf("Vercel Blob: options are %T, not *vercel.Options", req.Options)
	}
	opt.Workers = req.Workers
	view, err := vercel.Open(ctx, req.Root, opt)
	if err != nil {
		return nil, err
	}
	if opt.HTTPClient == nil && opt.Trust.NoVerify && req.Warn != nil {
		req.Warn(source.WarnTLSUnverified, source.NoVerifyWarning)
	}
	return &vercelSource{registry.From(view.Bound()), view, req.Warn}, nil
}

type vercelSource struct {
	registry.Bound[vercel.Object]
	*vercel.Opened
	warn func(string, string)
}

func (o *vercelSource) Run(ctx context.Context, env registry.Env) (order.Stats, parquetout.Encoder, error) {
	if env.Router != nil && o.warn != nil {
		o.warn(source.WarnTreeListing, treeDatasetNotice)
	}
	return o.Bound.Run(ctx, env)
}

var (
	_ registry.Source   = (*vercelSource)(nil)
	_ source.Identified = (*vercelSource)(nil)
	_ source.Capable    = (*vercelSource)(nil)
	_ source.Reporter   = (*vercelSource)(nil)
	_ source.Live       = (*vercelSource)(nil)
	_ source.Advancing  = (*vercelSource)(nil)
	_ source.Requested  = (*vercelSource)(nil)
	_ source.Faulted    = (*vercelSource)(nil)
)
