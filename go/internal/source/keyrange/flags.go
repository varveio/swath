// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"math"
	"strconv"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
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
}

// Options are the engine options this policy sets.
func (p Policy) Options() Options {
	return Options{Sequential: p.Sequential, Seed: p.Seed, SeedFirstPage: p.Seed, Structure: p.Structure,
		MaxKeys: p.MaxKeys, Floors: p.Floors, SlowSplit: p.SlowSplit, PageSize: p.PageSize}
}

// Group is the engine's flag group, which every provider on the engine
// names (registry.Factory.Shared): the command registers it once.
var Group = &source.FlagGroup{
	Name:        "keyrange",
	Flags:       func(f *pflag.FlagSet, diag bool) any { return Flags(f, diag) },
	Operational: []string{"no-adaptive-limit", "adaptive-limit-slow-start", "request-rate"},
}

// Flags registers the engine's flag group on f and returns the Common
// value it fills when f is parsed: the command registers it once and
// passes it to every provider's flag group (Group). The
// policy toggles, comparison modes and the limiter's slow start are
// diagnostic (swath diag list): they exist to run an experiment. The
// limiter switch and the request rate are on swath list too: they are how
// an operator answers a service that throttles (docs/operating.md).
func Flags(f *pflag.FlagSet, diag bool) *Common {
	// The command's defaults: the seed tiles the scope (--no-seed skips
	// it), structure probes engage on their evidence (StructureAuto), and
	// the client admits its requests through the adaptive limiter (Java's
	// gauge rules; --no-adaptive-limit turns it off: no limit at all, not a
	// fixed --workers, since seed and probe requests run beside the
	// workers' pages). A library caller's zero
	// Policy has no seed and no probes, its zero Admission no limiter.
	c := &Common{Policy: Policy{Seed: true, Structure: StructureAuto}, Admission: objhttp.Admission{Limiter: true}}
	p, a := &c.Policy, &c.Admission
	source.Define(f, source.Flag{Name: source.AllVersionsFlag, Value: source.Switch(&c.AllVersions), Usage: "S3 and GCS: list every version, S3 delete markers too (not Azure Blob)",
		Long: "S3: versions and delete markers (needs s3:ListBucketVersions). GCS: generations, live and noncurrent. Refused on Azure Blob. A key's rows are consecutive and never split across parts."})
	source.Define(f, source.Flag{Name: "request-rate", Value: (*rateFlag)(&a.RequestRate), Metavar: "N", Group: source.GroupLimits,
		Usage: "cap every HTTP attempt at N per second (0: no cap)",
		Long:  "Set it to stay under a request quota shared with other clients. Refused when the spacing exceeds half --stall-timeout."})
	source.Define(f, source.Flag{Name: "no-adaptive-limit", Value: source.Off(&a.Limiter), Group: source.GroupLimits,
		Usage: "do not lower concurrency when the service throttles requests",
		Long:  "Turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1. Without it each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them."})
	if diag {
		source.Define(f, source.Flag{Name: "adaptive-limit-slow-start", Value: source.Switch(&a.SlowStart),
			Usage: "start the limit at 4 in flight, doubling each second to --workers"})
		source.Define(f, source.Flag{Name: "no-seed", Value: source.Off(&p.Seed),
			Usage: "start from one range, without the delimiter-listing seed"})
		fl := source.Define(f, source.Flag{Name: "structure", Value: (*structureFlag)(&p.Structure), Metavar: "auto|on|off", Default: StructureAuto.String(),
			Usage: "probes whose folder boundaries splits pivot on",
			Long:  "The bare flag is on, so a mode needs '='. auto probes on the open frontier while its cursor is in a folder, and in bounded ranges whose chain stays inside one folder; on probes every range in a folder."})
		fl.NoOptDefVal = "on"
		source.Define(f, source.Flag{Name: "slow-split", Value: source.Switch(&p.SlowSplit),
			Usage: "split a range whose pages are slow without waiting for an idle worker"})
		source.Define(f, source.Flag{Name: "floors", Value: source.Switch(&p.Floors),
			Usage: "never split a range of 4 pages or fewer, nor leave a child under 2"})
		source.Define(f, source.Flag{Name: "sequential", Value: source.Switch(&p.Sequential),
			Usage: "list with one continuation-token chain (no ranges; the reference)"})
		source.Define(f, source.Flag{Name: "max-keys", Value: source.Count64(&p.MaxKeys, 0), Metavar: "N",
			Usage: "with --sequential, stop after N objects (marked incomplete)"})
		source.Define(f, source.Flag{Name: "page-size", Metavar: "N", Usage: fmt.Sprintf("ask for pages of N entries (1 to %d; default the provider's)", PageKeys),
			Value: source.Func(func(v string) error {
				n, err := strconv.Atoi(v)
				if err != nil || n < 1 || n > PageKeys {
					return fmt.Errorf("want 1 to %d", PageKeys)
				}
				p.PageSize = n
				return nil
			})})
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

func (r *rateFlag) Type() string { return "float" }

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

func (s *structureFlag) Type() string { return "string" }

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
