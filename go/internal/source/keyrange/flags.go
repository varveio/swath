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
	Sequential bool      // one continuation-token chain, no ranges (the oracle)
	Seed       bool      // parallel: tile the scope from delimiter listings first (the command's default)
	Structure  Structure // pivot on folder boundaries from structure probes sent alongside pages
	MaxKeys    int64     // stop after this many objects (sequential only; 0 = all)
	Floors     bool      // the gate-chain floors (off by default)
	SlowSplit  bool      // split latency outliers without an idle worker (off by default)
	PageSize   int       // the page size asked of the service (0: PageKeys); diagnostic
}

// Common is the flag group every provider on the engine shares: the
// engine's policy and its HTTP client's admission (objhttp.Admission),
// kept apart.
type Common struct {
	Policy    Policy
	Admission objhttp.Admission
	// AllVersions lists every version of every key (--all-versions): S3
	// versions and delete markers, GCS generations, in the providers'
	// versions schemas.
	AllVersions bool
	// Anonymous sends no credentials (--anonymous): the one way an object
	// store is listed unauthenticated. Without it a source with no usable
	// credentials fails (class auth) before any listing request.
	Anonymous bool
}

// Options are the engine options this policy sets.
func (p Policy) Options() Options {
	return Options{Sequential: p.Sequential, Seed: p.Seed, SeedFirstPage: p.Seed, Structure: p.Structure,
		MaxKeys: p.MaxKeys, Floors: p.Floors, SlowSplit: p.SlowSplit, PageSize: p.PageSize}
}

// FlagGroup names the engine's flag group (registry.Factory.Shared).
const FlagGroup = "keyrange"

// Flags registers the engine's flag group on f and returns the Common
// value it fills when f is parsed: the command registers it once and
// passes it to every provider's flag group (registry.Factory.Shared). The
// policy toggles, comparison modes and the limiter's slow start are
// diagnostic (swath diag list): they exist to run an experiment. The
// limiter switch and the request rate are on swath list too: they are how
// an operator answers a service that throttles (docs/operating.md).
func Flags(f *flag.FlagSet, diag bool) *Common {
	// The command's defaults: the seed tiles the scope (--seed=false skips
	// it), structure probes engage on their evidence (StructureAuto), and
	// the client admits its requests through the adaptive limiter (Java's
	// gauge rules; --limiter=false turns it off: no limit at all, not a
	// fixed --workers, since seed and probe requests run beside the
	// workers' pages). A library caller's zero
	// Policy has no seed and no probes, its zero Admission no limiter.
	c := &Common{Policy: Policy{Seed: true, Structure: StructureAuto}, Admission: objhttp.Admission{Limiter: true}}
	p, a := &c.Policy, &c.Admission
	f.BoolVar(&a.Limiter, "limiter", a.Limiter, "requests: adaptive concurrency: throttle answers lower the requests in flight (x0.7, at most once per round trip, while 15 % or more of the recent answers are throttles), growth returns one per second while under 7.5 % are; false: no limit: each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them")
	f.BoolVar(&c.AllVersions, "all-versions", false, "list every version of every key, not only the current objects: S3 versions and delete markers (ListObjectVersions; needs s3:ListBucketVersions; schema swath/s3-versions/v1), GCS generations, live and noncurrent (versions=true; schema swath/gcs-generations/v1); a key's rows are consecutive and never split across parts")
	f.BoolVar(&c.Anonymous, "anonymous", false, "credentials: send no credentials, ignoring any configured (a public S3 bucket, GCS bucket or Azure container); without it a source with no usable credentials fails (auth) before listing")
	f.Var((*rateFlag)(&a.RequestRate), "request-rate", "requests: cap every HTTP attempt (pages, probes, retries, requeues) at `N` per second, after the limiter's permit; refused when the spacing exceeds half --stall-timeout; 0: no cap")
	if diag {
		f.BoolVar(&a.SlowStart, "limiter-slow-start", a.SlowStart, "requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first cut or timeout (Java swath's slow start); applies to swath resume too")
		f.BoolVar(&p.Seed, "seed", p.Seed, "ranges: tile the keyspace from delimiter listings of the folder hierarchy before splitting (the top level up to 4 pages, narrow levels descended up to 4 deep: 1 to 6 serial round trips before the first range lists, the first carrying the first data page, which lists a scope of one page alone; not with -sequential); false: start from one range")
		f.Var((*structureFlag)(&p.Structure), "structure", "ranges: structure probes (delimiter pages of 32 alongside a range's pages; a split pivots on the folder boundaries they list), written --structure=auto|on|off with '=' (the bare flag is on, so a separate word is the next argument): auto probes on the open frontier while its cursor is in a folder and in bounded ranges whose chain stays inside one folder; on (or true) probes every range in a folder; off (or false) none")
		f.BoolVar(&p.SlowSplit, "slow-split", p.SlowSplit, "ranges: rank ranges by time left (keys left x their page latency) and split a range without an idle worker when its pages take at least 4 times the run's mean latency and its time left is at least twice the running ranges' mean (one such split waiting at a time)")
		f.BoolVar(&p.Floors, "floors", p.Floors, "ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages")
		f.BoolVar(&p.Sequential, "sequential", p.Sequential, "ranges: list with one continuation-token chain (no ranges; the reference)")
		f.Func("page-size", "ranges: ask the service for pages of `N` entries (1 to 1000; default 1000): small pages exercise page seams, splits inside a version history and the probes", func(v string) error {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > PageKeys {
				return fmt.Errorf("want 1 to %d", PageKeys)
			}
			p.PageSize = n
			return nil
		})
		f.Int64Var(&p.MaxKeys, "max-keys", p.MaxKeys, "ranges: with -sequential, stop after `N` objects (experiments; the dataset is marked incomplete)")
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

// structureFlag is --structure: auto, on (true, or the bare flag) or off
// (false).
type structureFlag Structure

func (s *structureFlag) String() string {
	if s == nil {
		return StructureAuto.String()
	}
	return Structure(*s).String()
}

func (s *structureFlag) Set(v string) error {
	switch v {
	case "auto":
		*s = structureFlag(StructureAuto)
	case "on", "true":
		*s = structureFlag(StructureOn)
	case "off", "false":
		*s = structureFlag(StructureOff)
	default:
		return fmt.Errorf("want auto, on or off")
	}
	return nil
}

func (s *structureFlag) IsBoolFlag() bool { return true }
