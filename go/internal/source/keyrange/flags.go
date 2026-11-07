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
	Sequential bool  // one continuation-token chain, no ranges (the oracle)
	Seed       bool  // parallel: first cut the scope at the common prefixes of one delimiter page
	Structure  bool  // pivot on folder boundaries from structure probes sent alongside pages
	MaxKeys    int64 // stop after this many objects (sequential only; 0 = all)
	Floors     bool  // the gate-chain floors (off by default)
}

// Common is the flag group every provider on the engine shares: the
// engine's policy and its HTTP client's admission (objhttp.Admission),
// kept apart.
type Common struct {
	Policy    Policy
	Admission objhttp.Admission
}

// Options are the engine options this policy sets.
func (p Policy) Options() Options {
	return Options{Sequential: p.Sequential, Seed: p.Seed, Structure: p.Structure,
		MaxKeys: p.MaxKeys, Floors: p.Floors}
}

// FlagGroup names the engine's flag group (registry.Factory.Shared).
const FlagGroup = "keyrange"

// Flags registers the engine's flag group on f and returns the Common
// value it fills when f is parsed: the command registers it once and
// passes it to every provider's flag group (registry.Factory.Shared). The
// policy toggles and comparison modes are diagnostic (swath diag list);
// -seed and the admission flags are on swath list too.
func Flags(f *flag.FlagSet, diag bool) *Common {
	// The command's default: the client admits its requests through the
	// adaptive limiter (Java's gauge rules); --limiter=false turns it off. A
	// library caller's zero Admission has none.
	c := &Common{Admission: objhttp.Admission{Limiter: true}}
	p, a := &c.Policy, &c.Admission
	f.BoolVar(&a.Limiter, "limiter", a.Limiter, "requests: adaptive concurrency: a throttle answer lowers the requests in flight (x0.7), growth returns one per second after 10 s without one; false: a fixed --workers in flight")
	f.BoolVar(&a.SlowStart, "limiter-slow-start", a.SlowStart, "requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first throttle answer or timeout (Java swath's slow start); applies to swath resume too")
	f.Var((*rateFlag)(&a.RequestRate), "request-rate", "requests: cap every HTTP attempt (pages, probes, retries, requeues) at `N` per second, after the limiter's permit; refused when the spacing exceeds half --stall-timeout; 0: no cap")
	f.BoolVar(&p.Seed, "seed", p.Seed, "ranges: before splitting, cut the keyspace at the folders of one delimiter page ('/') at the scope's root")
	if diag {
		f.BoolVar(&p.Structure, "structure", p.Structure, "ranges: structure probes: pivot on folder boundaries listed alongside a range's pages")
		f.BoolVar(&p.Floors, "floors", p.Floors, "ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages")
		f.BoolVar(&p.Sequential, "sequential", p.Sequential, "ranges: list with one continuation-token chain (no ranges; the reference)")
		f.Var((*int64Flag)(&p.MaxKeys), "max-keys", "ranges: with -sequential, stop after `N` objects (experiments; the dataset is marked incomplete)")
	}
	return c
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
