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
	"slices"
	"strings"
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
	chain     *chainState // raw group ordering/progress, owned by this request chain
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
	// Probe: the page's objects are not emitted (the seed page and
	// structure probes); a provider may leave optional fields out.
	Probe bool
}

// Page is one listing page, in listing order.
type Page[P any] struct {
	Rows      []schema.Row[P]
	Prefixes  []string // common prefixes, with a delimiter; the engine sorts and deduplicates them
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
	// InclusiveStart, if set, is StartInclusive per request: whether the
	// start bound the provider sends for After lists keys >= After (GCS
	// generations send the exclusive successor After+NUL where its bound
	// policy accepts it, and After itself where not).
	InclusiveStart func(after string) bool
	// Groups: rows may repeat a key, as consecutive rows forming the key's
	// group, in the provider's order within the key (a versions listing).
	// It must agree with the bound schema's Info.Groups. The engine then
	// checks pages nondecreasing, holds back each continuing page's
	// trailing group until a later key or the chain's end proves it
	// complete, and never splits inside a group (groups.go).
	Groups bool
	// GroupOrdinal, when set, must strictly ascend within an equal-key
	// group, including native page seams and rows suppressed at a bound.
	GroupOrdinal func(*P) int64
	// Detach copies rows out of their page so they share no memory with
	// it (the bound schema's Detach); group mode needs it, to emit or hold
	// a few rows of a page without pinning the rest (groups.go).
	Detach func([]schema.Row[P]) []schema.Row[P]
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

// invalid is the error an invalid page wraps: a protocol violation
// (source.ErrProtocol) carrying the provider's InvalidPage.
func (d *Description[P]) invalid() error {
	if d.InvalidPage != nil {
		return source.Violation(d.InvalidPage)
	}
	return source.Violation(ErrInvalidPage)
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
	Prefix     string // the scope
	Sequential bool   // one continuation-token chain, no ranges (the oracle)
	Seed       bool   // parallel: tile the scope (or a resumed (K, ⊤]) from delimiter listings of its folders first
	// SeedFirstPage: with Seed, the scope's first data page is sent with
	// the seed's first wave and emitted by it; untruncated, it is the
	// whole listing and the seed stops (Policy sets it with Seed).
	SeedFirstPage bool
	// SlowSplit: rank ranges by time left (keys left x their page latency)
	// and split a latency outlier without an idle worker (slowSplitWanted).
	SlowSplit bool
	Structure Structure     // pivot on folder boundaries from structure probes sent alongside pages (zero: off)
	Workers   int           // parallel: the order stage's worker count (split demand)
	MaxKeys   int64         // stop after this many objects (sequential only; 0 = all)
	Resume    *Resume       // continue after a journaled run's last published key
	Prior     *source.Prior // parallel: tile the scope at a prior's cuts instead of the seed (SeedFrom)
	PageSize  int           // the nominal page size: data and seed requests (0 = PageKeys)
	// ClientOff names the features of the provider's HTTP client that are
	// off (objhttp.Admission.Off): the engine reports them, it does not
	// own the client's configuration.
	ClientOff []string

	// SplitMinPages: a bounded range is split only once it has listed this
	// many pages itself (0: off; diagnostic). The page's gauge errs long
	// on hierarchical keys, so its admission gates pass splits whose
	// children hold less than a page; a range's own pages are evidence
	// that it holds more.
	SplitMinPages int

	// Test hooks: SplitCoin replaces the demand gate; Trace receives every
	// interval an Expand covered, (lo, hi] or (lo, ⊤].
	SplitCoin func() bool
	Trace     func(lo, hi string, open bool)
}

// Resume restarts a listing after the last published key.
type Resume struct {
	After string // the last published key: every key <= After is in the dataset
}

// PageKeys is the default nominal page size (Options.PageSize).
const PageKeys = 1000

// MaxPageFlag is the largest diagnostic --page-size: Azure Blob's page. S3
// and GCS return at most PageKeys and ask for no more.
const MaxPageFlag = 5000

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
	seedNs  atomic.Int64 // the seed's wall time (SeedTime); 0: no seed ran
	d       Description[P]
	fetch   Fetch[P]
	objects atomic.Int64
	ids     atomic.Uint64
	// frontier counts key-space progress for the watchdog (Frontier): a
	// page that held an entry or ended its chain, and an empty continuing
	// page within the first EmptyChainLive of its run.
	frontier atomic.Int64
	start    time.Time

	c       *counters.Set       // the tally (Counters, then the provider's)
	mu      sync.Mutex          // the decision mutex: taken through lock and unlock, which time it
	held    time.Time           // when mu was last acquired; mu held
	running map[uint64]*running // ranges in Expand
	queued  int
	tiles   []tileAccount // the prior's bounded tiles (expandPrior), by Range.tile
	reuse   []source.Span // reused certificates a resume lists around (Reuse)
	// latSum/latN: the run's range pages' latency (ns), for SlowSplit;
	// slowQueued: children of slow splits not yet started.
	latSum     float64
	latN       int64
	slowQueued int
	area       float64 // ∫ active dt, for the mean
	areaAt     time.Time
}

// New returns an engine over fetch.
func New[P any](opt Options, d Description[P], fetch Fetch[P]) (*Engine[P], error) {
	if !opt.Sequential && opt.MaxKeys > 0 {
		return nil, errors.New("a key limit needs the sequential listing")
	}
	if !d.Listing.RangeEngine() {
		return nil, fmt.Errorf("%s: the range engine needs a fabricable seek, byte-ordered pages and no rank oracle (%+v)", d.Target, d.Listing)
	}
	if d.Groups && d.Detach == nil {
		return nil, fmt.Errorf("%s: a grouped listing needs Description.Detach", d.Target)
	}
	if opt.Workers < 1 {
		opt.Workers = 1
	}
	if opt.PageSize <= 0 {
		opt.PageSize = PageKeys
	}
	now := time.Now()
	p := &Engine[P]{opt: opt, d: d, fetch: fetch, running: map[uint64]*running{}, start: now, areaAt: now}
	p.c = counters.New(append(append([]counters.Def(nil), Counters...), d.Counters...))
	return p, nil
}

// lock takes the decision mutex, counting the wait (mutex.wait_ns).
func (p *Engine[P]) lock() {
	t := time.Now()
	p.mu.Lock()
	p.held = time.Now()
	p.c.Add(cMutexWait, int64(p.held.Sub(t)))
}

// unlock releases the decision mutex, raising mutex.max_hold_ns.
func (p *Engine[P]) unlock() {
	p.c.Max(cMutexMaxHold, int64(time.Since(p.held)))
	p.mu.Unlock()
}

// Contention is the decision mutex's total wait and longest hold so far
// (source.Contended).
func (p *Engine[P]) Contention() (wait, maxHold time.Duration) {
	return time.Duration(p.c.Load(cMutexWait)), time.Duration(p.c.Load(cMutexMaxHold))
}

// SeedTime is the seed's wall time, from its first delimiter request to
// its tiles (source.Seeded); ok is false where no seed ran.
func (p *Engine[P]) SeedTime() (time.Duration, bool) {
	d := time.Duration(p.seedNs.Load())
	return d, d > 0
}

// ID is the counter ID of the provider's i-th counter (Description.Counters).
func (p *Engine[P]) ID(i int) counters.ID { return counters.ID(len(Counters) + i) }

// Set is the counter set, for the provider's own counters.
func (p *Engine[P]) Set() *counters.Set { return p.c }

// Restart continues after the last published key (source.Resumable).
// It must be called before execution. The engine owns resume state; a
// provider can promote this method without keeping a second copy.
func (p *Engine[P]) Restart(after string) error {
	p.opt.Resume = &Resume{After: after}
	return nil
}

// Options returns the options in force.
func (p *Engine[P]) Options() Options { return p.opt }

// Stats returns the counters so far.
func (p *Engine[P]) Stats() Stats {
	p.lock()
	defer p.unlock()
	p.account(time.Now())
	p.priorErrors()
	s := Stats{Counters: p.c.Snapshot(p.disabled()...)}
	if el := time.Since(p.start).Seconds(); el > 0 {
		s.MeanActive = p.area / el
	}
	return s
}

// list fetches one page that holds an entry, or the chain's last page.
// A provider with an inclusive start bound gets After as is; a key equal to
// it, and a common prefix at or below it, are dropped here. A continuing page with no entry (object stores may return them;
// also the page that held only the key at an inclusive bound) is followed
// by its token here, for every path (ranges, the sequential chain, the
// probes): each token of such a run must differ from the last
// emptyTokenWindow ones, or the page is invalid. Only the first
// EmptyChainLive pages of a run count as progress (Frontier); past them a
// service that sends fresh tokens forever is followed, in bounded memory,
// until the command's watchdog stops the run as stuck (--stall-timeout,
// cause stuck_empty_chain) or its deadline (--max-duration).
func (p *Engine[P]) list(ctx context.Context, in *Query) (*Page[P], error) {
	return p.listFollowing(ctx, in, nil)
}

// listFollowing is list whose following of empty continuing pages asks
// follow first (nil: always follow): when follow refuses, the empty page
// is returned as it is, truncated, with its token. The seed charges each
// followed page to its budgets this way.
func (p *Engine[P]) listFollowing(ctx context.Context, in *Query, follow func() bool) (*Page[P], error) {
	p.c.Inc(cRequests)
	if p.d.Groups && in.chain == nil {
		in.chain = &chainState{last: strings.Clone(in.After)}
	}
	var seen *tokenWindow // the recent tokens of a run of empty continuing pages
	empties := 0          // the run's length
	// chainStart: no page of this chain has held an entry yet, so its first
	// row may be the key at an inclusive start bound (also after empty
	// pages followed here).
	chainStart := in.Token == ""
	wait := requeueBase
	for {
		out, err := p.fetch(ctx, in)
		if err == nil {
			if p.d.Groups {
				if err := p.observe(in, out); err != nil {
					return nil, err
				}
			}
			if chainStart && in.After != "" && p.inclusive(in.After) {
				dropped := 0
				// One row at the bound, or in group mode the bound key's
				// whole group, across pages: an empty continuing page left
				// by the drop is followed below, under the token window,
				// and the chain is still at its start.
				for dropped < len(out.Rows) && out.Rows[dropped].Key == in.After && (dropped == 0 || p.d.Groups) {
					dropped++
				}
				out.Rows = out.Rows[dropped:]
				if dropped > 0 && p.d.Groups {
					out.Rows = p.d.Detach(out.Rows)
				}
				// A delimiter page from an inclusive start also rolls the
				// keys at or after the bound up into a common prefix at or
				// below it (GCS returns "a/" for startOffset "a/" or
				// "a/b"); S3's StartAfter returns neither (both measured
				// on the live services), and the engine's bound is S3's.
				if n := len(out.Prefixes); n > 0 {
					out.Prefixes = slices.DeleteFunc(out.Prefixes, func(cp string) bool { return cp <= in.After })
					dropped += n - len(out.Prefixes)
				}
				if dropped > 0 {
					p.c.Add(cBoundDrops, int64(dropped))
				}
			}
			// A data chain follows prefix-only pages too; delimiter probes
			// return prefixes to their discovery caller.
			if !out.Truncated || len(out.Rows) > 0 || len(out.Prefixes) > 0 && (in.Delimiter != "" || !p.d.Groups) {
				if !strictlyAscending(out.Prefixes) {
					out.Prefixes = slices.Compact(slices.Sorted(slices.Values(out.Prefixes)))
					p.c.Inc(cPrefixesNormalized)
				}
				if !p.d.Groups {
					p.frontier.Add(1)
				}
				return out, nil
			}
			if out.Token == "" {
				return nil, fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
			}
			if seen == nil {
				seen = newTokenWindow(in.Token)
			}
			if !seen.add(out.Token) {
				return nil, fmt.Errorf("%w: an empty page's continuation token makes no progress", p.d.invalid())
			}
			if ctx.Err() != nil {
				return nil, context.Cause(ctx)
			}
			if follow != nil && !follow() {
				return out, nil
			}
			next := *in
			next.Token = out.Token
			in, wait = &next, requeueBase
			p.c.Inc(cRequests)
			p.c.Inc(cEmptyPages)
			if empties++; !p.d.Groups && empties > EmptyChainLive {
				p.c.Inc(cEmptyChainPages)
			}
			if !p.d.Groups && empties <= EmptyChainLive {
				p.frontier.Add(1)
			}
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

// inclusive: the start bound sent for after lists after itself.
func (p *Engine[P]) inclusive(after string) bool {
	if p.d.InclusiveStart != nil {
		return p.d.InclusiveStart(after)
	}
	return p.d.StartInclusive
}

// strictlyAscending: s is sorted without duplicates.
func strictlyAscending(s []string) bool {
	for i := 1; i < len(s); i++ {
		if s[i] <= s[i-1] {
			return false
		}
	}
	return true
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
	open := false     // group mode: last's group may continue
	var grp groups[P] // group mode: the progress window
	if p.opt.Resume != nil {
		in.After, skip = p.safeStart(p.opt.Resume.After), p.opt.Resume.After
	}
	for {
		out, err := p.list(ctx, in)
		if err != nil {
			return err
		}
		dropped := 0
		for dropped < len(out.Rows) && skip != "" && out.Rows[dropped].Key <= skip {
			dropped++
		}
		out.Rows = out.Rows[dropped:]
		if dropped > 0 && p.d.Groups {
			out.Rows = p.d.Detach(out.Rows)
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
		if p.d.Groups && n < len(out.Rows) {
			rows = p.d.Detach(rows)
		}
		if p.d.Groups {
			// One chain, no splits: a page is emitted whole, its last
			// group continued by the next page's first rows (the order
			// stage and the router keep part cuts out of a group).
			top := last
			if _, _, err := p.groupOrder(rows, last, open, "", "", true); err != nil {
				return err
			}
			if n > 0 {
				last, open = strings.Clone(rows[n-1].Key), true
			}
			first := p.measureGroups(rows, top, grp.open, grp.run)
			if n > 0 {
				j := n - 1
				for j > 0 && rows[j-1].Key == last {
					j--
				}
				grp.run = int64(n - j)
				if j == 0 {
					grp.run = first
				}
				grp.open = out.Truncated
				if j == 0 && out.Truncated {
					p.c.Inc(cGroupsOpenPages)
				}
			}
		} else {
			for i := range rows {
				if last != "" && rows[i].Key <= last {
					return fmt.Errorf("%w: listing out of order: %q after %q", p.d.invalid(), rows[i].Key, last)
				}
				last = rows[i].Key
			}
		}
		if err := p.emit(rows, e); err != nil {
			return err
		}
		if stop {
			p.c.Set(cTruncated, 1)
		}
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
	p.c.Add(cSinkWaitNs, int64(time.Since(t)))
	p.c.Inc(cPages)
	p.c.Add(cObjects, int64(len(rows)))
	p.c.Add(cBytes, bytes)
	return err
}

// query is the request for the engine's scope.
func (p *Engine[P]) query() *Query {
	return &Query{Prefix: p.opt.Prefix, Max: p.opt.PageSize}
}

// Listed is the number of objects emitted so far.
func (p *Engine[P]) Listed() int64 { return p.c.Load(cObjects) }

// Get is counter id's value.
func (s *Stats) Get(id counters.ID) int64 { return s.Counters.Get(id) }

// emptyTokenWindow is how many of the latest tokens of a run of empty
// continuing pages are kept to detect a token that makes no progress: a
// cycle within it is an invalid page; a longer one, like a chain of ever
// fresh tokens, is bounded by EmptyChainLive and the watchdog, not by
// memory.
const emptyTokenWindow = 1024

// EmptyChainLive is how many empty continuing pages in a run (one
// request's chain of them, before a page with an entry or the chain's end)
// count as progress for the watchdog (Frontier). It is the token window:
// the engine cannot tell a fresh token from a cycle longer than that
// window, so a longer run proves neither progress nor its absence. Past it
// the pages are still followed (a legitimate one ends when the service
// reaches an entry), counted in empty_chain_pages, and the watchdog stops
// the run as stuck when nothing else progresses for --stall-timeout.
const EmptyChainLive = emptyTokenWindow

// Frontier is the engine's key-space progress, for the watchdog: pages
// that held an entry or ended a chain, plus empty continuing pages within
// the first EmptyChainLive of their run; emptyChained is the empty pages
// past that bound (followed, but not progress).
func (p *Engine[P]) Frontier() (advanced, emptyChained int64) {
	return p.frontier.Load(), p.c.Load(cEmptyChainPages)
}

// tokenWindow holds the latest emptyTokenWindow tokens of a run.
type tokenWindow struct {
	set  map[string]bool
	ring []string
	next int
}

func newTokenWindow(first string) *tokenWindow {
	w := &tokenWindow{set: map[string]bool{}, ring: make([]string, 0, 16)}
	w.add(first)
	return w
}

// add records tok and reports whether it is not among the kept tokens.
func (w *tokenWindow) add(tok string) bool {
	if w.set[tok] {
		return false
	}
	w.set[tok] = true
	if len(w.ring) < emptyTokenWindow {
		w.ring = append(w.ring, tok)
		return true
	}
	delete(w.set, w.ring[w.next])
	w.ring[w.next] = tok
	w.next = (w.next + 1) % emptyTokenWindow
	return true
}
