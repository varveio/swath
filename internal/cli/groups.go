// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"math"
	"strconv"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

// The store group's flag names.
const (
	endpointFlag       = "endpoint-url"
	requestTimeoutFlag = "request-timeout"
	anonymousFlag      = "anonymous"
	caBundleFlag       = "ca-bundle"
	noVerifyFlag       = "no-verify-ssl"
)

// StoreGroup is the flag group every object store provider names: one
// --endpoint-url, --request-timeout, --anonymous, --ca-bundle and
// --no-verify-ssl, whichever provider owns the root, filling a
// *source.Store. --endpoint-url is part of each provider's identity (a
// resume refuses another); the timeout, anonymity and TLS trust are
// operational.
var StoreGroup = &registry.FlagGroup{
	Name: "store",
	Flags: func(fs *pflag.FlagSet, _ bool) any {
		s := &source.Store{Timeout: source.DefaultRequestTimeout}
		Define(fs, Flag{Name: anonymousFlag, Familiar: []string{"no-sign-request"}, Value: Switch(&s.Anonymous), Usage: "send no credentials (a public bucket or container)",
			Long: "Ignores any configured. Without it a source with no usable credentials fails (auth) before listing."})
		Define(fs, Flag{Name: endpointFlag, Familiar: []string{"endpoint"}, Shown: ShownURL, Value: Text(&s.Endpoint), Metavar: "URL", Usage: "the service URL in place of the provider's own (emulators, MinIO)",
			Long: "S3: path-style addressing unless --no-s3-path-style; default AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL. GCS: the JSON API base URL (fake-gcs-server). Azure Blob: the blob service URL, the account then its first path segment (Azurite). User information, a query and a fragment are refused. Part of the resume identity."})
		Define(fs, Flag{Name: caBundleFlag, Value: Text(&s.Trust.CABundle), Metavar: "FILE", Usage: "trust the certificates in this PEM file besides the system's",
			Long: "For a service behind a private certificate authority. S3: default AWS_CA_BUNDLE."})
		Define(fs, Flag{Name: noVerifyFlag, Value: Switch(&s.Trust.NoVerify), Usage: "do not verify the service's TLS certificate (warns)",
			Long: "For a test endpoint only. A resume replays it, and warns again."})
		Define(fs, Flag{Name: requestTimeoutFlag, Value: Duration(&s.Timeout), Metavar: "DURATION", Usage: "per-request timeout; a request over it is retried",
			Default: source.FormatDuration(source.DefaultRequestTimeout), Group: GroupLimits,
			Long: "Raise it for a slow endpoint or emulator whose pages take longer to answer."})
		return s
	},
	Operational: []string{anonymousFlag, requestTimeoutFlag, caBundleFlag, noVerifyFlag},
	URLs:        []string{endpointFlag},
}

// AllVersionsFlag names the range engine's switch that lists every version
// of every key. The command reads it by name for the terminal's nouns (a
// versions run counts rows, not keys).
const AllVersionsFlag = "all-versions"

// TraceFlag names the range engine's diagnostic --trace FILE, the run
// trace: the engine's flag group takes it, so only its providers do, and
// the command reads the path by name to check, open and close the file.
const TraceFlag = "trace"

// EngineGroup is the range engine's flag group, filling a
// *keyrange.Common, which every provider on the engine names: the command
// registers it once.
var EngineGroup = &registry.FlagGroup{
	Name:        "keyrange",
	Flags:       func(f *pflag.FlagSet, diag bool) any { return engineFlags(f, diag) },
	Operational: []string{"no-adaptive-limit", "adaptive-limit-slow-start", "request-rate"},
}

// engineFlags registers the engine's flag group on f and returns the
// keyrange.Common value it fills when f is parsed. The policy toggles,
// comparison modes and the limiter's slow start are diagnostic (swath diag
// list): they exist to run an experiment. The limiter switch and the
// request rate are on swath list too: they are how an operator answers a
// service that throttles (docs/watch-a-run.md).
func engineFlags(f *pflag.FlagSet, diag bool) *keyrange.Common {
	// The command's defaults: the seed tiles the scope (--no-seed skips
	// it), structure probes engage on their evidence (StructureAuto), and
	// the client admits its requests through the adaptive limiter (Java's
	// gauge rules; --no-adaptive-limit turns it off: no limit at all, not a
	// fixed --workers, since seed and probe requests run beside the
	// workers' pages). A library caller's zero
	// Policy has no seed and no probes, its zero Admission no limiter.
	c := &keyrange.Common{Policy: keyrange.Policy{Seed: true, Structure: keyrange.StructureAuto}, Admission: objhttp.Admission{Limiter: true}}
	p, a := &c.Policy, &c.Admission
	Define(f, Flag{Name: AllVersionsFlag, Value: Switch(&c.AllVersions), Usage: "list every version of every key, not only the current objects",
		Long: "S3: versions and delete markers (needs s3:ListBucketVersions). GCS: generations, live and noncurrent. Azure Blob: versions, snapshots and soft-deleted blobs. A key's rows are consecutive and never split across parts."})
	Define(f, Flag{Name: "request-rate", Value: (*rateFlag)(&a.RequestRate), Metavar: "N", Group: GroupLimits,
		Usage: "cap every HTTP attempt at N per second (0: no cap)",
		Long:  "Set it to stay under a request quota shared with other clients. Refused when the spacing exceeds half --stall-timeout."})
	Define(f, Flag{Name: "no-adaptive-limit", Familiar: []string{"limiter"}, Value: Off(&a.Limiter), Group: GroupLimits,
		Usage: "do not lower concurrency when the service throttles requests",
		Long:  "Turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1. Without it each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them."})
	if diag {
		Define(f, Flag{Name: "adaptive-limit-slow-start", Value: Switch(&a.SlowStart),
			Usage: "start the limit at 4 in flight, doubling each second to --workers"})
		Define(f, Flag{Name: "no-seed", Value: Off(&p.Seed),
			Usage: "start from one range, without the delimiter-listing seed"})
		fl := Define(f, Flag{Name: "structure", Value: (*structureFlag)(&p.Structure), Metavar: "auto|on|off", Default: keyrange.StructureAuto.String(),
			Usage: "probes whose folder boundaries splits pivot on",
			Long:  "The bare flag is on, so a mode needs '='. auto probes on the open frontier while its cursor is in a folder, and in bounded ranges whose chain stays inside one folder; on probes every range in a folder."})
		fl.NoOptDefVal = "on"
		Define(f, Flag{Name: "slow-split", Value: Switch(&p.SlowSplit),
			Usage: "split a range whose pages are slow without waiting for an idle worker"})
		Define(f, Flag{Name: "sequential", Value: Switch(&p.Sequential),
			Usage: "list with one continuation-token chain (no ranges; the reference)"})
		Define(f, Flag{Name: "max-keys", Value: Count64(&p.MaxKeys, 0), Metavar: "N",
			Usage: "with --sequential, stop after N objects (marked incomplete)"})
		Define(f, Flag{Name: TraceFlag, Value: Text(&c.Trace), Metavar: "FILE",
			Usage: "write the range engine's run trace (JSON Lines) to FILE",
			Long:  "Every range made, claimed, paged, split and ended, with its keys; tools/explainer reads it. Events the file cannot keep up with are dropped, never waited for (trace.dropped)."})
		Define(f, Flag{Name: "page-size", Metavar: "N", Usage: fmt.Sprintf("ask for pages of N entries (1 to %d; default the provider's)", keyrange.PageKeys),
			Value: Func(func(v string) error {
				n, err := strconv.Atoi(v)
				if err != nil || n < 1 || n > keyrange.PageKeys {
					return fmt.Errorf("want 1 to %d", keyrange.PageKeys)
				}
				p.PageSize = n
				return nil
			})})
		Define(f, Flag{Name: "seed-from-tile-pages", Metavar: "N", Usage: fmt.Sprintf("a counted --seed-from's tiles hold N pages (default %d)", keyrange.TilePages),
			Value: Func(func(v string) error {
				n, err := strconv.Atoi(v)
				if err != nil || n < 1 || n > keyrange.MaxTilePages {
					return fmt.Errorf("want 1 to %d", keyrange.MaxTilePages)
				}
				p.TilePages = n
				return nil
			})})
		Define(f, Flag{Name: "seed-from-window-rows", Value: Count64(&p.WindowRows, 0), Metavar: "N",
			Usage: "a counted --seed-from's window holds N rows (0: 8,000,000)"})
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
type structureFlag keyrange.Structure

func (s *structureFlag) String() string {
	if s == nil {
		return keyrange.StructureAuto.String()
	}
	return keyrange.Structure(*s).String()
}

func (s *structureFlag) Type() string { return "string" }

func (s *structureFlag) Set(v string) error {
	switch v {
	case "auto":
		*s = structureFlag(keyrange.StructureAuto)
	case "on", "true":
		*s = structureFlag(keyrange.StructureOn)
	case "off", "false":
		*s = structureFlag(keyrange.StructureOff)
	default:
		return fmt.Errorf("want auto, on or off")
	}
	return nil
}
