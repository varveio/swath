// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package registry connects providers to the pipeline. Providers register a
// Factory by URL scheme from an init function; the command imports them only
// for that. An opened source is bound (Bind) to the typed pipeline stages
// with its range and row types, which the pipeline itself never names:
// listing, order, parquetout, dataset, schema and journal import no
// provider.
package registry

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/textout"
)

// A Factory opens one kind of source.
type Factory struct {
	// Scheme is the root's URL scheme ("s3" for s3://bucket/prefix). The
	// factory with the empty scheme takes roots without one (local paths).
	Scheme string
	// Flags registers the provider's flags on fs and returns its options
	// value, which the flags fill when fs is parsed. The value is opaque to
	// everything but the provider: it travels in Request.Options. diag adds
	// the provider's diagnostic flags (comparison modes and policy toggles,
	// for swath diag list only). shared is the value of the provider's
	// shared flag group (Shared), nil without one.
	Flags func(fs *flag.FlagSet, diag bool, shared any) any
	// Shared, if set, names a flag group several providers share (the range
	// engine's), which SharedFlags registers: the command registers each
	// named group once, from the first provider naming it, and passes its
	// value to every such provider's Flags.
	Shared      string
	SharedFlags func(fs *flag.FlagSet, diag bool) any
	// Defaults returns the options value used when none was given.
	Defaults func() any
	// Open validates the root and returns a source ready to list. It must
	// not touch the output directory.
	Open func(ctx context.Context, req Request) (Source, error)
	// BinaryKeys: the provider's keys are bytes, not UTF-8 (schema.Binary),
	// so its rows cannot be written as text; the command refuses a text run
	// of it before opening it.
	BinaryKeys bool
	// EncoderBytes is the working set admitted per Parquet encoder of the
	// provider's schema (parquetout.EncoderBytes), known before the source
	// is opened; 0 prices it as parquetout.DefaultEncoderBytes.
	EncoderBytes int64
	// Classify, if set, names the class of a failure the provider caused
	// ("throttle", "access_denied", ...), for the run's outcome; "" if it
	// does not know the error.
	Classify func(error) string
}

// Request is what the pipeline tells a factory.
type Request struct {
	Root    string       // as given, scheme included
	Options any          // the provider's options value (Flags or Defaults)
	Workers int          // the order stage's worker count
	Warn    func(string) // one-time warnings for the user; never nil
}

// Source is an opened provider bound to the pipeline (see Bind). It may
// also implement the optional interfaces of package source (Identified,
// Resumable, Capable, Excluder, Reporter).
type Source interface {
	Schema() schema.Info
	Locator() source.Locator
	// Listed is the number of rows emitted so far; after a listing it
	// must equal the rows the order stage handed on.
	Listed() int64
	// Run lists the source through the order stage into a part router
	// (or nowhere, for a dry run).
	Run(ctx context.Context, env Env) (order.Stats, parquetout.Encoder, error)
}

// Env is everything about a run except the row type.
type Env struct {
	Order order.Options
	// Router, if set, routes the rows into parts; nil discards them.
	Router *parquetout.Options
	// Count counts the rows without ordering them (order.Count): a dry
	// run, whose rows nothing consumes; Router and Text must be nil.
	Count bool
	// Empty lists nothing: a resume whose final part is durable.
	Empty bool
	// Ordered, if set, counts rows the order stage has handed on.
	Ordered *atomic.Int64
	// Text, if set (and Router nil), writes the rows as text there.
	Text *textout.Out
	// Filter, if set, drops the rows it does not accept before the order
	// stage, counting them in Filtered.
	Filter   *filter.Spec
	Filtered *atomic.Int64
}

// Bound is a provider bound to its schema; embed it in an opened source.
type Bound[P any] struct {
	p source.Provider[any, P]
	s *schema.Schema[P]
}

// Bind erases p's range type and binds it to its row schema.
func Bind[R, P any](p source.Provider[R, P], s *schema.Schema[P]) Bound[P] {
	return Bound[P]{source.Erase[R, P](p), s}
}

// Schema is the bound schema's description.
func (b Bound[P]) Schema() schema.Info { return b.s.Info }

// Run implements Source.Run with the provider's row type.
func (b Bound[P]) Run(ctx context.Context, env Env) (order.Stats, parquetout.Encoder, error) {
	keep, err := filter.Compile(env.Filter, b.s)
	if err != nil {
		return order.Stats{}, nil, err
	}
	var sink order.Sink[P] = discard[P]{}
	var enc parquetout.Encoder
	if env.Router != nil {
		r, err := parquetout.NewRouter(*env.Router, b.s)
		if err != nil {
			return order.Stats{}, nil, err
		}
		sink, enc = segSink[P]{r, env.Ordered}, r
	} else if env.Text != nil {
		t, err := textout.NewSink(env.Text, b.s)
		if err != nil {
			return order.Stats{}, nil, err
		}
		sink = t
	}
	if _, seg := sink.(segSink[P]); env.Ordered != nil && !seg { // segSink counts its own rows
		sink = countSink[P]{sink, env.Ordered}
	}
	p := b.p
	if env.Empty {
		p = source.Empty[P]{}
	} else if keep != nil {
		p = newFiltered(p, keep, b.s, env.Filtered)
	}
	if env.Count {
		st, err := order.Count(ctx, p, env.Ordered, env.Order)
		return st, nil, err
	}
	opt := env.Order
	if enc != nil {
		// Once listing has ended, the encoders the router reserved for
		// the drain are admitted (parquetout.Options.MaxEncoders).
		prev := opt.ListingDone
		opt.ListingDone = func() {
			enc.Admit()
			if prev != nil {
				prev()
			}
		}
	}
	st, err := order.Run(ctx, p, sink, b.s, opt)
	return st, enc, err
}

// segSink is the Parquet router as the order stage's sink: rows through
// Write, and once listing is done the rest as parallel segments
// (order.Segmenter), both counted in n when set (progress).
type segSink[P any] struct {
	r *parquetout.Router[P]
	n *atomic.Int64
}

func (s segSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	s.count(segs)
	return s.r.Write(ctx, segs)
}

func (s segSink[P]) count(segs [][]schema.Row[P]) {
	if s.n != nil {
		for _, seg := range segs {
			s.n.Add(int64(len(seg)))
		}
	}
}

func (s segSink[P]) Plan() (order.SegmentPlan, bool) {
	p, ok := s.r.Plan()
	return segmentPlan(p), ok
}

func (s segSink[P]) BeginSegments() (order.SegmentPlan, bool) {
	p, ok := s.r.BeginSegments()
	return segmentPlan(p), ok
}

func (s segSink[P]) AbandonSegments() { s.r.AbandonSegments() }

// segmentPlan is the router's plan as the order stage takes it.
func segmentPlan(p parquetout.SegmentPlan) order.SegmentPlan {
	return order.SegmentPlan{Limit: p.Limit, MaxRows: p.MaxRows, Logical: p.Logical, Rows: p.Rows, Open: p.Open, Full: p.Full,
		Encoders: p.Encoders, Queue: p.Queue, OpenQueue: p.OpenQueue}
}

func (s segSink[P]) OpenSegment(ctx context.Context, continueOpen bool) (order.SegmentSink[P], error) {
	seg, err := s.r.OpenSegment(ctx, continueOpen)
	if err != nil {
		return nil, err
	}
	return segPart[P]{seg, s}, nil
}

type segPart[P any] struct {
	*parquetout.Segment[P]
	s segSink[P]
}

func (p segPart[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	p.s.count(segs)
	return p.Segment.Write(ctx, segs)
}

type countSink[P any] struct {
	order.Sink[P]
	n *atomic.Int64
}

func (c countSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	for _, s := range segs {
		c.n.Add(int64(len(s)))
	}
	return c.Sink.Write(ctx, segs)
}

type discard[P any] struct{}

func (discard[P]) Write(context.Context, [][]schema.Row[P]) error { return nil }

var (
	regMu     sync.Mutex
	factories = map[string]Factory{}
)

// Register adds a factory; registering a scheme twice panics.
func Register(f Factory) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := factories[f.Scheme]; ok {
		panic("registry: scheme registered twice: " + f.Scheme)
	}
	factories[f.Scheme] = f
}

// Factories returns the registered factories, by scheme.
func Factories() []Factory {
	regMu.Lock()
	defer regMu.Unlock()
	out := make([]Factory, 0, len(factories))
	for _, f := range factories {
		out = append(out, f)
	}
	slices.SortFunc(out, func(a, b Factory) int { return strings.Compare(a.Scheme, b.Scheme) })
	return out
}

// Lookup returns the factory for root's scheme.
func Lookup(root string) (Factory, error) {
	scheme := ""
	if i := strings.Index(root, "://"); i > 0 {
		scheme = root[:i]
	}
	regMu.Lock()
	defer regMu.Unlock()
	f, ok := factories[scheme]
	if !ok {
		if scheme == "" {
			return f, fmt.Errorf("no provider for local paths is registered (root %q)", root)
		}
		return f, fmt.Errorf("unsupported root %q: no provider for %s://", root, scheme)
	}
	return f, nil
}
