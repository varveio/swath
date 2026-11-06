// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package keyrange is the range engine shared by the providers whose
// listing API returns keys in unsigned byte order and can start after any
// key the caller makes up (a fabricable seek, no rank oracle): Swath's range
// model, seed, demand-driven owner split, safe pivot synthesis, guard
// clauses, the probes, retry-and-requeue and their counters. A provider
// supplies one page-fetch function and a Description; it keeps its client,
// credentials, payload and registration.
//
// A range is (lo, hi]: keys strictly above lo up to and including hi (lo ""
// is ⊥; an open range has no hi, ⊤). The provider's own bound conventions
// are translated at the request (Description.StartInclusive, ExclusiveEnd).
package keyrange

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Query is one listing request in the engine's terms.
type Query struct {
	Prefix    string
	Delimiter string // "" or "/"
	// After: keys strictly above After ("" from the start of Prefix). A
	// provider whose start bound is inclusive sends it as is and declares
	// StartInclusive; the engine drops a key equal to After.
	After string
	// Through, when Bounded: the range ends at Through inclusive. The
	// engine trims keys past it, so a provider may ignore it, or bound the
	// request (an exclusive end bound is ExclusiveEnd(Through), when the
	// provider accepts it).
	Through string
	Bounded bool
	// Token: the continuation token of the previous page. Every other
	// field is the chain's and stays as on its first request; the
	// provider maps them onto its wire (S3 omits start-after with a token).
	Token string
	Max   int // page size
	// Probe: the page's objects are not emitted (seed, structure and 1-key
	// probes); a provider may leave optional fields out.
	Probe bool
}

// Page is one listing page, in listing order.
type Page[P any] struct {
	Rows      []schema.Row[P]
	Prefixes  []string // common prefixes, with a delimiter
	Truncated bool
	Token     string // the continuation token when Truncated
}

// Fetch lists one page.
type Fetch[P any] func(ctx context.Context, q *Query) (*Page[P], error)

// Description is what the engine must know about a provider.
type Description[P any] struct {
	// Target names the listing in errors (the listed URL).
	Target string
	// Listing is the provider's native listing: the engine applies only
	// where Listing.RangeEngine() holds (a fabricable seek, byte-ordered
	// pages, no rank oracle); New refuses any other.
	Listing source.Capabilities
	// StartInclusive: the provider's start bound lists keys >= After
	// (an inclusive start offset), not > After (an exclusive start-after).
	StartInclusive bool
	// AcceptBound is the provider's request admissibility: whether a
	// string may be sent as a start bound and so split at (pivots, cursors,
	// seed and structure boundaries, a seed top-up, a resume's start). The
	// engine itself needs only valid UTF-8 (bounds are compared as bytes).
	// nil is the conservative pivot alphabet (Safe), which S3 declares
	// explicitly. A refused pivot is a rejected split (guard clause
	// unsafe_pivot or unsafe_cursor), a refused boundary is not a cut.
	// Synthesized pivots always use the pivot alphabet.
	AcceptBound func(string) bool
	// Requeue reports whether a failed request is asked again after a
	// backoff instead of failing the run (the client's retries exhausted
	// on a transient failure).
	Requeue func(error) bool
	// InvalidPage is the error a malformed page wraps (order, token);
	// default ErrInvalidPage.
	InvalidPage error
	// Size is a row's byte size, for the bytes counter; nil counts none.
	Size func(*P) int64
	// Counters are the provider's own counters, after the engine's
	// (Engine.ID maps their index).
	Counters []counters.Def
}

// Listing is the native listing of a provider the engine applies to.
var Listing = source.Capabilities{Seek: source.SeekFabricable, Order: source.OrderBytes}

// ErrInvalidPage: a page broke the listing contract.
var ErrInvalidPage = errors.New("invalid listing page")

func (d *Description[P]) invalid() error {
	if d.InvalidPage != nil {
		return d.InvalidPage
	}
	return ErrInvalidPage
}

// safe: s is valid UTF-8 and the provider accepts it as a bound
// (AcceptBound, by default the pivot alphabet).
func (p *Engine[P]) safe(s string) bool {
	if p.d.AcceptBound == nil {
		return Safe(s)
	}
	return utf8.ValidString(s) && p.d.AcceptBound(s)
}

// safeStart is the longest prefix of key k, on a scalar boundary, that the
// engine may send as a start bound (safe); "" lists from the scope's start.
// It sorts at or before k, so listing after it and dropping keys <= k
// resumes after k.
func (p *Engine[P]) safeStart(k string) string {
	s := k
	for s != "" && !p.safe(s) {
		_, n := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-n]
	}
	return s
}

// ExclusiveEnd is the exclusive end bound that lists keys up to through
// inclusive, its immediate successor in byte order (through + NUL), and
// whether the provider accepts it (accept nil accepts it). A provider that
// does not accept it (NUL refused, or 1,025 bytes past a 1,024-byte limit)
// sends no end bound: the engine trims at Through itself.
func ExclusiveEnd(through string, accept func(string) bool) (string, bool) {
	end := through + "\x00"
	if accept != nil && !accept(end) {
		return "", false
	}
	return end, true
}

// Options configure an Engine.
type Options struct {
	Prefix      string  // the scope
	Sequential  bool    // one continuation-token chain, no ranges (the oracle)
	Seed        bool    // parallel: tile the scope from delimiter probes first
	SeedDescent bool    // the seed walks the folder hierarchy (seedplan.go); else one delimiter page
	Structure   bool    // pivot on folder boundaries from structure probes sent alongside pages
	Workers     int     // parallel: the order stage's worker count (split demand)
	MaxKeys     int64   // stop after this many objects (sequential only; 0 = all)
	Resume      *Resume // continue after a journaled run's last published key
	Probe       bool    // check each split's upper child with a 1-key request first (off by default)
	Floors      bool    // the gate-chain floors: no split of a range worth a few pages, nor into a tail child below two pages (off by default)

	// Test hooks: SplitCoin replaces the demand gate; Trace receives every
	// interval an Expand covered, (lo, hi] or (lo, ⊤].
	SplitCoin func() bool
	Trace     func(lo, hi string, open bool)
}

// Resume restarts a listing after the last published key.
type Resume struct {
	After string // the last published key: every key <= After is in the dataset
}

// PageKeys is the listing page size.
const PageKeys = 1000

// Stats is what a listing did: the time-weighted mean of ranges in Expand
// and the counter snapshot.
type Stats struct {
	MeanActive float64
	Counters   counters.Snapshot
}

// Engine lists one scope through a provider's Fetch. It implements
// source.Provider[Range, P].
type Engine[P any] struct {
	opt     Options
	d       Description[P]
	fetch   Fetch[P]
	objects atomic.Int64
	ids     atomic.Uint64
	start   time.Time

	c       *counters.Set // the tally (Counters, then the provider's)
	mu      sync.Mutex
	running map[uint64]*running // ranges in Expand
	queued  int
	area    float64 // ∫ active dt, for the mean
	areaAt  time.Time
}

// New returns an engine over fetch.
func New[P any](opt Options, d Description[P], fetch Fetch[P]) (*Engine[P], error) {
	if !opt.Sequential && opt.MaxKeys > 0 {
		return nil, errors.New("a key limit needs the sequential listing")
	}
	if !d.Listing.RangeEngine() {
		return nil, fmt.Errorf("%s: the range engine needs a fabricable seek, byte-ordered pages and no rank oracle (%+v)", d.Target, d.Listing)
	}
	if opt.Workers < 1 {
		opt.Workers = 1
	}
	now := time.Now()
	p := &Engine[P]{opt: opt, d: d, fetch: fetch, running: map[uint64]*running{}, start: now, areaAt: now}
	p.c = counters.New(append(append([]counters.Def(nil), Counters...), d.Counters...))
	return p, nil
}

// ID is the counter ID of the provider's i-th counter (Description.Counters).
func (p *Engine[P]) ID(i int) counters.ID { return counters.ID(len(Counters) + i) }

// Set is the counter set, for the provider's own counters.
func (p *Engine[P]) Set() *counters.Set { return p.c }

// Restart continues after the last published key (source.Resumable).
func (p *Engine[P]) Restart(after string) { p.opt.Resume = &Resume{After: after} }

// Options returns the options in force.
func (p *Engine[P]) Options() Options { return p.opt }

// Stats returns the counters so far.
func (p *Engine[P]) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	s := Stats{Counters: p.c.Snapshot(p.disabled()...)}
	if el := time.Since(p.start).Seconds(); el > 0 {
		s.MeanActive = p.area / el
	}
	return s
}

// list fetches one page that holds an entry, or the chain's last page.
// A provider with an inclusive start bound gets After as is; a key equal to
// it is dropped here. A continuing page with no entry (object stores may return them;
// also the page that held only the key at an inclusive bound) is followed
// by its token here, for every path (ranges, the sequential chain, the
// probes): each token of such a run must be new, or the page is invalid.
func (p *Engine[P]) list(ctx context.Context, in *Query) (*Page[P], error) {
	p.mu.Lock()
	p.c.Inc(cRequests)
	p.mu.Unlock()
	var seen map[string]bool // the tokens of a run of empty continuing pages
	wait := requeueBase
	for {
		out, err := p.fetch(ctx, in)
		if err == nil {
			if p.d.StartInclusive && in.Token == "" && in.After != "" && len(out.Rows) > 0 && out.Rows[0].Key == in.After {
				out.Rows = out.Rows[1:]
			}
			if !out.Truncated || len(out.Rows) > 0 || len(out.Prefixes) > 0 {
				return out, nil
			}
			if out.Token == "" {
				return nil, fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
			}
			if seen == nil {
				seen = map[string]bool{in.Token: true}
			}
			if seen[out.Token] {
				return nil, fmt.Errorf("%w: an empty page's continuation token makes no progress", p.d.invalid())
			}
			seen[out.Token] = true
			if ctx.Err() != nil {
				return nil, context.Cause(ctx)
			}
			next := *in
			next.Token = out.Token
			in, wait = &next, requeueBase
			p.mu.Lock()
			p.c.Inc(cRequests)
			p.c.Inc(cEmptyPages)
			p.mu.Unlock()
			continue
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, ctx.Err()
		}
		if p.d.Requeue != nil && p.d.Requeue(err) && ctx.Err() == nil {
			// Never fail the run on retry exhaustion: the page is asked
			// again after a backoff, for as long as the run lives. Whether
			// the run still makes progress is the command's watchdog's
			// decision, not this one's.
			p.c.Inc(cRequeues)
			t := time.NewTimer(wait/2 + rand.N(wait/2+1))
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, context.Cause(ctx)
			case <-t.C:
			}
			wait = min(2*wait, requeueMax)
			continue
		}
		return nil, fmt.Errorf("list %s: %w", p.d.Target, err)
	}
}

// requeueBase and requeueMax bound the backoff before a page whose
// client retries were exhausted is asked again.
var requeueBase, requeueMax = 2 * time.Second, 60 * time.Second

// SetRequeueBackoff sets the requeue backoff bounds (tests).
func SetRequeueBackoff(base, maxWait time.Duration) (restore func()) {
	b, m := requeueBase, requeueMax
	requeueBase, requeueMax = base, maxWait
	return func() { requeueBase, requeueMax = b, m }
}

// expandSequential lists the whole scope with one continuation-token chain.
func (p *Engine[P]) expandSequential(ctx context.Context, e source.Emitter[Range, P]) error {
	in := p.query()
	last := ""
	skip := ""
	if p.opt.Resume != nil {
		in.After, skip = p.safeStart(p.opt.Resume.After), p.opt.Resume.After
	}
	for {
		out, err := p.list(ctx, in)
		if err != nil {
			return err
		}
		for skip != "" && len(out.Rows) > 0 && out.Rows[0].Key <= skip {
			out.Rows = out.Rows[1:] // resume: already published
		}
		n := len(out.Rows)
		stop := false
		if p.opt.MaxKeys > 0 {
			left := p.opt.MaxKeys - p.objects.Add(int64(n)) + int64(n)
			if left <= int64(n) {
				n, stop = int(max(0, left)), true
			}
		}
		rows := out.Rows[:n]
		for i := range rows {
			if last != "" && rows[i].Key <= last {
				return fmt.Errorf("%w: listing out of order: %q after %q", p.d.invalid(), rows[i].Key, last)
			}
			last = rows[i].Key
		}
		if err := p.emit(rows, e); err != nil {
			return err
		}
		p.mu.Lock()
		if stop {
			p.c.Set(cTruncated, 1)
		}
		p.mu.Unlock()
		if stop || !out.Truncated {
			return nil
		}
		if out.Token == "" {
			return fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		in.Token = out.Token
	}
}

// emit counts and hands one page of rows to e.
func (p *Engine[P]) emit(rows []schema.Row[P], e source.Emitter[Range, P]) error {
	var bytes int64
	if p.d.Size != nil {
		for i := range rows {
			bytes += p.d.Size(&rows[i].Data)
		}
	}
	t := time.Now()
	err := e.Rows(rows)
	p.mu.Lock()
	p.c.Add(cSinkWaitNs, int64(time.Since(t)))
	p.c.Inc(cPages)
	p.c.Add(cObjects, int64(len(rows)))
	p.c.Add(cBytes, bytes)
	p.mu.Unlock()
	return err
}

// query is the request for the engine's scope.
func (p *Engine[P]) query() *Query {
	return &Query{Prefix: p.opt.Prefix, Max: PageKeys}
}

// Listed is the number of objects emitted so far.
func (p *Engine[P]) Listed() int64 { return p.c.Load(cObjects) }

// Get is counter id's value.
func (s *Stats) Get(id counters.ID) int64 { return s.Counters.Get(id) }
