// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"flag"
	"fmt"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"math"
	"strconv"
)

// Policy is the engine's policy configuration, the same for every provider
// on the engine: one flag group (Flags), one value a provider passes on.
type Policy struct {
	Sequential  bool  // one continuation-token chain, no ranges (the oracle)
	Seed        bool  // parallel: tile the scope from delimiter probes first
	SeedDescent bool  // the seed walks the folder hierarchy (seedplan.go); else one delimiter page
	Structure   bool  // pivot on folder boundaries from structure probes sent alongside pages
	MaxKeys     int64 // stop after this many objects (sequential only; 0 = all)
	Probe       bool  // check each split's upper child with a 1-key request first (off by default)
	Floors      bool  // the gate-chain floors (off by default)
	// Limiter: the provider's client admits its HTTP attempts through an
	// adaptive limiter (objhttp.Limiter) of at most Workers permits.
	Limiter bool
	// RequestRate caps the client's HTTP attempts per second
	// (objhttp.Rate); 0: no cap.
	RequestRate float64
}

// Options are the engine options this policy sets.
func (p Policy) Options() Options {
	return Options{Sequential: p.Sequential, Seed: p.Seed, SeedDescent: p.SeedDescent, Structure: p.Structure,
		MaxKeys: p.MaxKeys, Probe: p.Probe, Floors: p.Floors, Limiter: p.Limiter, RequestRate: p.RequestRate}
}

// Flags registers the engine's policy flags on f and returns the Policy
// they fill when f is parsed. Every provider on the engine calls it from
// its own flag group; the flags are registered once per flag set and later
// callers get the same Policy. The policy toggles and comparison modes are
// diagnostic (swath diag list); -seed and -limiter are on swath list too.
func Flags(f *flag.FlagSet, diag bool) *Policy {
	if fl := f.Lookup("seed"); fl != nil {
		if b, ok := fl.Value.(*boolFlag); ok {
			return b.p
		}
		panic("keyrange: flag -seed is registered by something other than the range engine")
	}
	// The command's defaults: the seed descends, and the client admits its
	// requests through the adaptive limiter (Java's gauge rules);
	// --limiter=false turns it off. A library caller's zero Policy has
	// neither.
	p := &Policy{SeedDescent: true, Limiter: true}
	bf := func(v *bool, name, usage string) { f.Var(&boolFlag{p: p, v: v}, name, usage) }
	bf(&p.Limiter, "limiter", "requests: adaptive concurrency: a throttle answer lowers the requests in flight (x0.7), growth returns one per second after 10 s without one; false: a fixed --workers in flight")
	f.Var((*rateFlag)(&p.RequestRate), "request-rate", "requests: cap every HTTP attempt (pages, probes, retries, requeues) at `N` per second, after the limiter's permit; 0: no cap")
	bf(&p.Seed, "seed", "ranges: tile the keyspace from delimiter probes of the folder hierarchy before splitting")
	if diag {
		bf(&p.Structure, "structure", "ranges: structure probes: pivot on folder boundaries listed alongside a range's pages")
		bf(&p.SeedDescent, "seed-descent", "ranges: with -seed, walk the folder hierarchy (false: one delimiter page)")
		bf(&p.Floors, "floors", "ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages")
		bf(&p.Sequential, "sequential", "ranges: list with one continuation-token chain (no ranges; the reference)")
		bf(&p.Probe, "probe", "ranges: before each split, a 1-key request checks the upper child is not empty (off: it costs more than it saves, REPORT.md Step 5a)")
		f.Var((*int64Flag)(&p.MaxKeys), "max-keys", "ranges: with -sequential, stop after `N` objects (experiments; the dataset is marked incomplete)")
	}
	return p
}

// boolFlag is a bool flag that knows its Policy, so that a second
// registration on the same flag set finds it.
type boolFlag struct {
	p *Policy
	v *bool
}

func (b *boolFlag) IsBoolFlag() bool { return true }
func (b *boolFlag) String() string {
	if b == nil || b.v == nil {
		return "false"
	}
	return strconv.FormatBool(*b.v)
}
func (b *boolFlag) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*b.v = v
	return nil
}

type rateFlag float64

func (r *rateFlag) String() string {
	if r == nil || *r == 0 {
		return "0"
	}
	return strconv.FormatFloat(float64(*r), 'g', -1, 64)
}
func (r *rateFlag) Set(s string) error {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 || math.IsNaN(v) || math.IsInf(v, 0) || (v > 0 && v < objhttp.MinRate) {
		return fmt.Errorf("want a finite number of requests per second of at least %g, or 0 for no cap", objhttp.MinRate)
	}
	*r = rateFlag(v)
	return nil
}

type int64Flag int64

func (i *int64Flag) String() string {
	if i == nil {
		return "0"
	}
	return strconv.FormatInt(int64(*i), 10)
}
func (i *int64Flag) Set(s string) error {
	v, err := strconv.ParseInt(s, 0, 64)
	*i = int64Flag(v)
	return err
}
