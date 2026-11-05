// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"github.com/varveio/swath/go/internal/counters"
)

// Counter IDs, the indices of Counters.
const (
	cObjects counters.ID = iota
	cBytes
	cPages
	cRequests
	cAttempts
	cRetries
	cThrottles
	cTimeouts
	cParseFailures
	cOverfetchKeys
	cTruncated
	cSeedPrefixes
	cSeedCuts
	cSeedUnsafe
	cSeedSubsampled
	cSeedTruncated
	cSeedSentinel
	cSentinelDeclinedTruncated
	cSentinelDeclinedObjectLast
	cSentinelDeclinedNoCeiling
	cSentinelDeclinedUnsafe
	cSplitConsidered
	cSplitExtrapolated
	cSplitAcceptedBounded
	cSplitAcceptedFrontier
	cSplitRejectedCursor
	cSplitRejectedBound
	cSplitRejectedDone
	cSplitRejectedNoPivot
	cSplitRejectedUnsafeCursor
	cSplitSkippedNoDemand
	cSplitSkippedOutranked
	cSplitSkippedNotContinuing
	cChildEmptyLower
	cChildEmptyUpper
	cResumedRanges
	cMaxActive
	cMaxQueued
	cBusyNs
	cSinkWaitNs
	cProbeRequests
	cProbeUpperEmpty
	cProbeMoved
	cSplitRejectedProbe
	cRequeues
	cSkippedRemainingFloor
	cSkippedTailFloor
	numCounters
)

// Counters is the S3 provider's tally: every engagement counter, in
// report order. The run report (source.stats.counters) and the table in
// docs/internals/counters.md are generated from it.
var Counters = []counters.Def{
	d("objects", counters.Count, "objects", "objects listed (emitted rows)"),
	d("bytes", counters.Count, "bytes", "sum of listed object sizes"),
	d("pages", counters.Count, "pages", "ListObjectsV2 pages processed (and the seed page)"),
	d("requests", counters.Count, "requests", "ListObjectsV2 calls, retries not counted"),
	d("attempts", counters.Count, "requests", "HTTP requests, retries included"),
	d("retries", counters.Count, "requests", "attempts repeated after a retryable failure"),
	d("throttles", counters.Count, "requests", "of the retries, 429/503/SlowDown answers"),
	d("timeouts", counters.Count, "requests", "of the retries, attempts past -request-timeout"),
	d("parse_failures", counters.Count, "responses", "200 answers whose body did not parse (retried)"),
	d("overfetch_keys", counters.Count, "keys", "keys listed past a range's hi (or at or below a resume key) and dropped"),
	d("truncated_at_max_keys", counters.Flag, "", "-max-keys stopped the listing; the dataset is incomplete", "max_keys"),
	d("seed.prefixes", counters.Count, "prefixes", "common prefixes on the seed page", "seed"),
	d("seed.cuts", counters.Count, "cuts", "cut points the seed tiled the scope at", "seed"),
	d("seed.unsafe", counters.Count, "prefixes", "prefixes dropped as unusable start-after values", "seed"),
	d("seed.subsampled", counters.Flag, "", "more than min(1000, 4×workers) usable prefixes: cuts were subsampled", "seed"),
	d("seed.truncated", counters.Flag, "", "the seed page was truncated", "seed"),
	d("seed.sentinel", counters.Flag, "", "the closing sentinel (pk, prefixCeil(pk)] was added", "seed"),
	d("seed.sentinel_declined.truncated", counters.Count, "decisions", "sentinel declined: the seed page was truncated", "seed"),
	d("seed.sentinel_declined.object_last", counters.Count, "decisions", "sentinel declined: the page ended with an object", "seed"),
	d("seed.sentinel_declined.no_ceiling", counters.Count, "decisions", "sentinel declined: no prefix ceiling", "seed"),
	d("seed.sentinel_declined.unsafe", counters.Count, "decisions", "sentinel declined: the ceiling is not a safe key", "seed"),
	d("split.considered", counters.Count, "decisions", "split decisions taken with demand and rank", "split"),
	d("split.pivot_extrapolated", counters.Count, "pivots", "pivots from page density (else midpoint or reflection)", "split"),
	d("split.accepted_bounded", counters.Count, "splits", "splits of bounded ranges", "split"),
	d("split.accepted_frontier", counters.Count, "splits", "splits of the open frontier", "split"),
	d("split.rejected.cursor", counters.Count, "decisions", "guard: pivot not above the cursor", "split"),
	d("split.rejected.bound", counters.Count, "decisions", "guard: pivot not below hi (or the scope ceiling)", "split"),
	d("split.rejected.done", counters.Count, "decisions", "guard: the range had ended", "split"),
	d("split.rejected.no_pivot", counters.Count, "decisions", "guard: no safe pivot exists", "split"),
	d("split.rejected.unsafe_cursor", counters.Count, "decisions", "guard: the cursor is not a usable start-after", "split"),
	d("split.skipped.no_demand", counters.Count, "decisions", "no idle worker (hunger <= 0)", "split"),
	d("split.skipped.outranked", counters.Count, "decisions", "larger ranges took the demand", "split"),
	d("split.skipped.not_continuing", counters.Count, "decisions", "the range's last page did not continue", "split"),
	d("child_empty.lower", counters.Count, "ranges", "lower split children that listed no key (the cost of not probing)", "split"),
	d("child_empty.upper", counters.Count, "ranges", "upper split children that listed no key", "split"),
	d("resumed_ranges", counters.Count, "ranges", "ranges a resume started with: (K, ⊤]", "resume"),
	d("max_active", counters.Max, "ranges", "peak ranges in Expand at once"),
	d("max_queued", counters.Max, "ranges", "peak ranges queued for a worker", "split"),
	d("busy_ns", counters.Count, "ns", "sum of Expand wall time"),
	d("sink_wait_ns", counters.Count, "ns", "time Expand spent handing rows to the order stage (backpressure)"),
	d("probe.requests", counters.Count, "requests", "1-key probes (max-keys=1 after a pivot), also counted in requests", "probe"),
	d("probe.upper_empty", counters.Count, "probes", "probes that found no key past the pivot: an empty upper child avoided", "probe"),
	d("probe.moved", counters.Count, "pivots", "pivots moved to the midpoint of (cursor, pivot] after an empty probe", "probe"),
	d("split.rejected.probe_empty", counters.Count, "decisions", "splits skipped: probes found no key past any pivot tried", "probe"),
	d("requeues", counters.Count, "requests", "pages asked again after the client's retries were exhausted (the run never fails on that)"),
	d("split.skipped.remaining_floor", counters.Count, "decisions", "bounded range with at most 4 pages of keys left by its density estimate: not split", "floors"),
	d("split.skipped.tail_floor", counters.Count, "decisions", "the child past the pivot would realize at most 2 pages of keys: not split", "floors"),
}

func d(key string, kind counters.Kind, unit, help string, feature ...string) counters.Def {
	def := counters.Def{Key: key, Kind: kind, Unit: unit, Help: help}
	if len(feature) > 0 {
		def.Feature = feature[0]
	}
	return def
}

// disabled lists the policies with counters that this run has turned off.
func (p *Provider) disabled() []string {
	var off []string
	if p.opt.Sequential {
		off = append(off, "split", "probe")
	} else if !p.opt.Probe {
		off = append(off, "probe")
	}
	if p.opt.Sequential || !p.opt.Floors {
		off = append(off, "floors")
	}
	if p.opt.Sequential || p.opt.Resume != nil || !p.opt.Seed {
		off = append(off, "seed")
	}
	if p.opt.Resume == nil {
		off = append(off, "resume")
	}
	if p.opt.MaxKeys == 0 {
		off = append(off, "max_keys")
	}
	return off
}
