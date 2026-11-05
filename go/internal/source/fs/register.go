// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package fs

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// The local filesystem takes roots without a scheme.
func init() {
	source.Register(source.Factory{Scheme: "", Flags: flags, Defaults: func() any { return &Options{LinkTarget: true} }, Open: open})
}

// Set and String make Fields a flag value.
func (f *Fields) Set(s string) error {
	v, err := ParseFields(s)
	if err == nil {
		*f = v
	}
	return err
}

func flags(f *flag.FlagSet) any {
	o := &Options{LinkTarget: true}
	f.Var(&o.Fields, "fields", "metadata `tier` per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)")
	f.BoolVar(&o.NoDirs, "no-dirs", false, "omit kind=DIR rows")
	f.BoolFunc("no-link-target", "do not read symlink targets", func(string) error { o.LinkTarget = false; return nil })
	return o
}

// open validates the root. SWATH_LOCAL_ENGINE=portable selects the
// portable engine (comparisons).
func open(_ context.Context, req source.Request) (source.Source, error) {
	o, ok := req.Options.(*Options)
	if !ok {
		return nil, fmt.Errorf("fs: options are %T, not *fs.Options", req.Options)
	}
	opt := *o
	if os.Getenv("SWATH_LOCAL_ENGINE") == "portable" {
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
	return &opened{Provider: source.Erase[Range](p), p: p}, nil
}

type opened struct {
	source.Provider[any]
	p *Provider
}

func (o *opened) Schema() *schema.Schema { return &schema.Local }

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

func (o *opened) Report() source.Report {
	st := o.p.Stats()
	return source.Report{Name: "listing", Stats: &st, Errors: st.Errors(), Fields: o.p.opt.Fields.String(),
		Brief: fmt.Sprintf("files=%d dirs=%d bytes=%d errors=%d", st.Files, st.Dirs, st.Bytes, st.Errors())}
}

var (
	_ source.Excluder = (*opened)(nil)
	_ source.Reporter = (*opened)(nil)
)
