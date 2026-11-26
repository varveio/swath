// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/internal/totals"
)

// The local filesystem takes roots without a scheme.
func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&localfs.Schema), Schemas: []schema.Info{localfs.Schema.Info}, Scheme: "", Flags: localFlags, Defaults: func() any { return &localfs.Options{LinkTarget: true} }, Open: openLocal, Workers: localfs.DefaultWorkers, CheckTotals: checkLocalTotals, CheckFilter: checkLocalFilter,
		Title: "Local", Example: "/data", Usage: "a local directory or file", Suggest: localSuggest})
}

// localSuggest is the local path of a file:// URL on this host
// (registry.Factory.Suggest): file:///data/x is /data/x, and
// file:///C:/x is C:\x on Windows.
func localSuggest(root string) (string, bool) {
	u, err := url.Parse(root)
	if err != nil || !strings.EqualFold(u.Scheme, "file") || u.Host != "" && u.Host != "localhost" || u.Path == "" {
		return "", false
	}
	p := u.Path
	if runtime.GOOS == "windows" && len(p) > 2 && p[0] == '/' && p[2] == ':' {
		p = p[1:]
	}
	return filepath.FromSlash(p), true
}

// fieldsValue is --metadata's value: a metadata tier (localfs.ParseFields).
type fieldsValue localfs.Fields

func (f *fieldsValue) Set(s string) error {
	v, err := localfs.ParseFields(s)
	if err == nil {
		*f = fieldsValue(v)
	}
	return err
}

func (f *fieldsValue) String() string { return localfs.Fields(*f).String() }

func (f *fieldsValue) Type() string { return "string" }

func localFlags(f *pflag.FlagSet, _ bool, _ registry.SharedValues) any {
	o := &localfs.Options{LinkTarget: true}
	Define(f, Flag{Name: "metadata", Value: (*fieldsValue)(&o.Fields), Metavar: "names|stat|statx|links", Default: o.Fields.String(),
		Usage: "what is read per entry (statx: Linux; links: Windows)",
		Long:  "statx adds created_at. links opens each entry for its link count, so that swath du counts a hard-linked file once; it costs an open per entry."})
	Define(f, Flag{Name: "no-dirs", Value: Switch(&o.NoDirs), Usage: "omit the directories' rows (kind dir)"})
	Define(f, Flag{Name: "no-link-target", Value: Off(&o.LinkTarget), Usage: "do not read symbolic links' targets"})
	Define(f, Flag{Name: "one-file-system", Short: "x", Familiar: []string{"one-filesystem"}, Value: Switch(&o.OneFilesystem),
		Usage: "stop at other filesystems (macOS: volumes; Windows: junctions, mounts)"})
	return o
}

// checkLocalTotals chooses and refuses the totals a local tree can give
// (registry.Factory.CheckTotals). The allocated bytes are read from each
// entry's stat: they lead by default, as du's, where the listing reads
// them (localfs.BlocksRead), and the size leads where it does not (the
// names tier, a platform whose listing reads none); --allocated is
// refused at the names tier. The hard
// links are read from the stat too, so the names tier, and a platform
// whose stat has no link count (localfs.LinksRead: Windows but at the
// links tier), count every name.
func checkLocalTotals(options any, t *totals.Options) error {
	o, ok := options.(*localfs.Options)
	if !ok {
		return nil
	}
	if o.Fields == localfs.Names && t.Primary == totals.AllocatedBytes {
		return errors.New("--allocated reads each entry's stat: not with --metadata names")
	}
	if t.Primary == "" {
		switch {
		case localfs.BlocksRead(o.Fields):
			// statx may leave blocks out of an entry (some network
			// filesystems), and so may a Windows directory's own
			// (localfs.BlocksMayLack): counted missing, not a failed count.
			t.Primary, t.Reason, t.Lenient = totals.AllocatedBytes, totals.ReasonDefault, localfs.BlocksMayLack(o.Fields)
		case o.Fields == localfs.Names:
			t.Primary, t.Reason = totals.Size, totals.ReasonNamesTier
		default:
			t.Primary, t.Reason = totals.Size, totals.ReasonPlatform
		}
	}
	if !localfs.LinksRead(o.Fields) {
		t.LinksOnce = false
	}
	return nil
}

// checkLocalFilter refuses filters on what a local tree does not read
// (registry.Factory.CheckFilter): storage classes ever, sizes and times at
// the names tier.
func checkLocalFilter(options any, f *filter.Spec) error {
	size, mtime, class := f.Reads()
	switch o, _ := options.(*localfs.Options); {
	case class:
		return errors.New("--storage-class: a local tree has no storage classes")
	case (size || mtime) && o != nil && o.Fields == localfs.Names:
		return errors.New("--min-size, --max-size, --modified-since and --modified-until read each entry's stat: not with --metadata names")
	}
	return nil
}

// openLocal opens a local root with its options, the command line's or a
// library caller's (*localfs.Options). SWATH_LOCALFS_ENGINE=portable
// selects the portable engine (comparisons).
func openLocal(_ context.Context, req registry.Request) (registry.Source, error) {
	o, ok := req.Options.(*localfs.Options)
	if !ok {
		return nil, fmt.Errorf("localfs: options are %T, not *localfs.Options", req.Options)
	}
	opt := *o
	if os.Getenv("SWATH_LOCALFS_ENGINE") == "portable" {
		opt.Portable = true
	}
	v, err := localfs.Open(req.Root, opt, req.Warn)
	if err != nil {
		return nil, err
	}
	return &localSource{registry.From(v.Bound()), v}, nil
}

// localSource is an opened local tree as a run takes it: its rows, bound
// when it opened, go where the run's Env sends them (registry.Bound, whose
// field hides the view's Bound method); the rest is the opened view's.
type localSource struct {
	registry.Bound[localfs.Entry]
	*localfs.Opened
}

// Run lists the tree once the view has begun the listing with the run's
// filters, its unreadable directories' observer and, for a count, the
// unordered listing and the processors a count runs with
// (localfs.Opened.Begin), which it ends when the listing returns.
func (s *localSource) Run(ctx context.Context, env registry.Env) (order.Stats, parquetout.Encoder, error) {
	end, err := s.Begin(env.Filter, env.Unreadable, env.Count, &env.Order)
	if err != nil {
		return order.Stats{}, nil, err
	}
	defer end()
	return s.Bound.Run(ctx, env)
}

var (
	_ registry.Source = (*localSource)(nil)
	_ source.Capable  = (*localSource)(nil)
	_ source.Excluder = (*localSource)(nil)
	_ source.Reporter = (*localSource)(nil)
)
