// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
)

// The local filesystem takes roots without a scheme.
func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&Schema), Scheme: "", BinaryKeys: true, Flags: flags, Defaults: func() any { return &Options{LinkTarget: true} }, Open: open})
}

// Set and String make Fields a flag value.
func (f *Fields) Set(s string) error {
	v, err := ParseFields(s)
	if err == nil {
		*f = v
	}
	return err
}

func flags(f *flag.FlagSet, _ bool, _ any) any {
	o := &Options{LinkTarget: true}
	f.Var(&o.Fields, "metadata", "local: metadata `tier` per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)")
	f.BoolVar(&o.NoDirs, "no-dirs", false, "local: omit kind=DIR rows")
	f.BoolFunc("no-link-target", "local: do not read symlink targets", func(string) error { o.LinkTarget = false; return nil })
	return o
}

// open validates the root. SWATH_LOCALFS_ENGINE=portable selects the
// portable engine (comparisons).
func open(_ context.Context, req registry.Request) (registry.Source, error) {
	o, ok := req.Options.(*Options)
	if !ok {
		return nil, fmt.Errorf("localfs: options are %T, not *localfs.Options", req.Options)
	}
	opt := *o
	if os.Getenv("SWATH_LOCALFS_ENGINE") == "portable" {
		opt.Portable = true
	}
	root, err := filepath.Abs(req.Root)
	if err != nil {
		return nil, err
	}
	p, err := New(root, opt)
	if err != nil {
		return nil, fmt.Errorf("root: %w", err)
	}
	return &opened{Bound: registry.Bind[Range, Entry](p, &Schema), p: p}, nil
}

type opened struct {
	registry.Bound[Entry]
	p *Provider
}

func (o *opened) Locator() source.Locator {
	return source.Locator{Kind: "local-fs", Root: o.p.root, Path: o.p.root}
}

func (o *opened) Listed() int64 {
	o.p.mu.Lock()
	defer o.p.mu.Unlock()
	if o.p.opt.NoDirs {
		return o.p.stats.Files
	}
	return o.p.stats.Files + o.p.stats.Dirs
}

// Exclude leaves out an output directory inside the root; it is called
// before listing starts.
func (o *opened) Exclude(out string) error {
	rel, inside, err := dataset.LocateInside(o.p.root, out)
	if err != nil {
		return fmt.Errorf("locate output: %w", err)
	}
	if inside {
		o.p.opt.Skip = map[string]bool{rel + "/": true}
	}
	return nil
}

// Report: an entry counts as skipped when an error kept it (or its subtree)
// out; a link whose target could not be read is still listed, and an entry
// that vanished is no longer part of the tree.
func (o *opened) Report() source.Report {
	st := o.p.Stats()
	cov := source.Coverage{Target: o.p.root, Complete: true, StopReason: "completed", Skipped: st.Errors() - st.ReadlinkErrors - st.Vanished}
	for k := range o.p.opt.Skip {
		cov.Exclusions = append(cov.Exclusions, k)
	}
	return source.Report{Name: "local", Stats: &st, Errors: st.Errors(), Fields: o.p.opt.Fields.String(), Coverage: cov,
		Brief: fmt.Sprintf("files=%d dirs=%d bytes=%d errors=%d", st.Files, st.Dirs, st.Bytes, st.Errors())}
}

// Capabilities: a directory is read whole and sorted; there is no seek and
// no resume.
func (o *opened) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekNone, Order: source.OrderNone}
}

var (
	_ source.Capable  = (*opened)(nil)
	_ source.Excluder = (*opened)(nil)
	_ source.Reporter = (*opened)(nil)
)
