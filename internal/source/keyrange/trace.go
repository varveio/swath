// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"math"
	"strings"
	"time"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/runtrace"
)

// The run trace (swath diag list --trace; docs/internals/trace.md is the
// schema): the engine's ranges as they are made, claimed, paged, split and
// ended. Every hook is behind p.tr != nil, so a run without a trace pays a
// nil check per event site and nothing else; with one, an event is encoded
// on the range's goroutine (outside the decision mutex) and handed to the
// writer without waiting (runtrace).

// bornName names a range's origin in the trace.
var bornName = [...]string{fromRoot: "root", fromSeed: "seed", fromLower: "lower", fromUpper: "upper"}

// TraceTo writes the run trace to w (source.Traced): called before
// execution, after Restart and SeedFrom, so run_start records the options
// the run lists with.
func (p *Engine[P]) TraceTo(w *runtrace.Writer) {
	p.tr = w
	b := buildinfo.Read()
	e := w.Event("run_start").Int("trace_version", runtrace.Version).Str("tool", b.Tool).Str("version", b.Version).
		Str("time", w.Start().UTC().Format(time.RFC3339Nano)).Key("prefix", p.opt.Prefix).Int("workers", int64(p.opt.Workers)).
		Int("page_size", int64(p.opt.PageSize)).Bool("sequential", p.opt.Sequential).Bool("seed", p.opt.Seed && p.opt.Prior == nil).
		Bool("prior", p.opt.Prior != nil).Str("structure", p.opt.Structure.String()).Bool("slow_split", p.opt.SlowSplit).Bool("versions", p.d.Groups)
	if b.Revision != "" {
		e = e.Str("revision", b.Revision)
	}
	if p.opt.Resume != nil {
		e = e.Key("resume_after", p.opt.Resume.After)
	}
	p.emitTrace(e)
}

// emitTrace hands e to the trace and counts it (trace.events; the writer
// counts the drops, read into trace.dropped by Stats).
func (p *Engine[P]) emitTrace(e runtrace.Event) {
	e.Emit()
	p.c.Inc(cTraceEvents)
}

// traceStats reads the writer's drops into trace.dropped; mu held.
func (p *Engine[P]) traceStats() {
	if p.tr != nil {
		p.c.Set(cTraceDropped, p.tr.Dropped())
	}
}

// bounds adds a range's bounds: lo, then hi or open, and after where a
// resume drops keys up to it.
func bounds(e runtrace.Event, r Range) runtrace.Event {
	e = e.Key("lo", r.lo)
	if r.open {
		e = e.Bool("open", true)
	} else {
		e = e.Key("hi", r.hi)
	}
	if r.after != "" {
		e = e.Key("after", r.after)
	}
	return e
}

// traceTiles records the ranges a seed, a prior or a resume tiled the
// scope into (seeded); a reused certificate's range is the order stage's
// to replay, never claimed.
func (p *Engine[P]) traceTiles(tiles []Range) {
	for _, t := range tiles {
		e := bounds(p.tr.Event("seeded").Int("id", int64(t.id)), t)
		if t.kind == reused {
			e = e.Bool("reused", true)
		}
		if t.prior && t.estOK {
			e = estimate(e, "expected", t.est)
		}
		p.emitTrace(e)
	}
}

// traceSeed records the delimiter seed: what it spent and why it stopped;
// keys is what its first data page emitted when that page held the whole
// scope (stop one_page: no tile follows).
func (p *Engine[P]) traceSeed(began time.Time, b *seedBudget, levels, cuts int, stop seedStop, keys int) {
	p.emitTrace(p.tr.Event("seed").Int("dur_ns", int64(time.Since(began))).Int("requests", int64(b.requests)).
		Int("round_trips", int64(b.trips)).Int("levels", int64(levels)).Int("cuts", int64(cuts)).
		Str("stop", strings.TrimPrefix(Counters[seedStopBy[stop]].Key, "seed.stop.")).Int("keys", int64(keys)))
}

// lane takes the lowest worker lane no range in Expand holds; mu held.
// The engine does not know the order stage's goroutines: a lane is a slot
// of concurrency, at most the workers plus the drainer.
func (p *Engine[P]) lane() int {
	for i, busy := range p.lanes {
		if !busy {
			p.lanes[i] = true
			return i
		}
	}
	p.lanes = append(p.lanes, true)
	return len(p.lanes) - 1
}

// traceClaim records a range entering Expand: the root of a run without
// a seed is seeded here, as it was made by no tiling.
func (p *Engine[P]) traceClaim(r Range, st *running) {
	if r.born == fromRoot {
		p.emitTrace(bounds(p.tr.Event("seeded").Int("id", int64(r.id)), r))
	}
	p.emitTrace(bounds(p.tr.Event("claimed").Int("id", int64(r.id)).Int("worker", int64(st.lane)), r).
		Int("depth", r.depth).Str("born", bornName[r.born]))
}

// tracePage records one page of r: the cursor before (after) and after
// it (last), the rows it emitted, the request's time (retries, requeues
// and empty continuing pages followed included) and whether it ended the
// range.
func (p *Engine[P]) tracePage(r Range, st *running, after, last string, rows int, ns float64, done bool) {
	p.emitTrace(p.tr.Event("page").Int("id", int64(r.id)).Int("worker", int64(st.lane)).Key("after", after).Key("last", last).
		Int("rows", int64(rows)).Int("dur_ns", int64(ns)).Bool("done", done))
}

// traceSplit records a committed split of r at cursor c: r ends, (c, m]
// and (m, hi] are its children; the decision's inputs as decide saw them.
func (p *Engine[P]) traceSplit(r Range, st *running, c string, pv pivot, lower, upper Range) {
	e := p.tr.Event("split").Int("id", int64(r.id)).Int("worker", int64(st.lane)).Key("cursor", c).Key("pivot", pv.m)
	if r.open {
		e = e.Bool("open", true)
	} else {
		e = e.Key("hi", r.hi)
	}
	reason := "demand"
	switch {
	case p.opt.SplitCoin != nil:
		reason = "coin"
	case pv.slow:
		reason = "slow"
	}
	e = e.Int("lower", int64(lower.id)).Int("upper", int64(upper.id)).Str("mechanism", pv.src.String()).Str("reason", reason).
		Int("hunger", int64(pv.hunger)).Int("rows", st.keys).Int("depth", lower.depth).Int("gen", int64(r.gen))
	e = estimate(e, "remainder", pv.rem)
	if pv.estOK {
		e = estimate(estimate(e, "est_lower", pv.lower), "est_upper", pv.upper)
		e = e.Bool("held", pv.prior)
	}
	if pv.persistent {
		e = e.Bool("persistent", true)
	}
	p.emitTrace(e)
}

// traceEnd records how r left Expand: listed through its bound
// (completed) or stopped by err (failed); a split range's split event
// ended it.
func (p *Engine[P]) traceEnd(r Range, st *running, err error) {
	switch {
	case st.split:
	case st.ended:
		p.emitTrace(p.tr.Event("completed").Int("id", int64(r.id)).Int("worker", int64(st.lane)).Int("rows", st.keys))
	default:
		e := p.tr.Event("failed").Int("id", int64(r.id)).Int("worker", int64(st.lane)).Int("rows", st.keys)
		if err != nil {
			e = e.Str("error", err.Error())
		}
		p.emitTrace(e)
	}
}

// estimate adds a key estimate as a count, or leaves it out where it is
// none: NaN (unknown), infinite (a bound the page could not read), or
// beyond what an int64 holds (a gauge's step over a long run of shared
// bytes can estimate more keys than that), which would convert to a
// meaningless negative.
func estimate(e runtrace.Event, name string, v float64) runtrace.Event {
	if math.IsNaN(v) || v < math.MinInt64 || v >= math.MaxInt64 {
		return e
	}
	return e.Int(name, int64(v))
}
