// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"encoding/json"

	"github.com/varveio/swath/go/internal/counters"
)

// Counter IDs, the indices of Counters.
const (
	cObjects = iota
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
	cSeedSkipped
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
)

// Counters is the S3 provider's tally: every engagement counter, in
// summary order. The summary JSON ("go.s3") and the table in
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
	d("truncated_at_max_keys", counters.Flag, "", "-max-keys stopped the listing; the dataset is incomplete"),
	d("seed.skipped", counters.Flag, "", "no seed: the root was (⊥, ⊤]"),
	d("seed.prefixes", counters.Count, "prefixes", "common prefixes on the seed page"),
	d("seed.cuts", counters.Count, "cuts", "cut points the seed tiled the scope at"),
	d("seed.unsafe", counters.Count, "prefixes", "prefixes dropped as unusable start-after values"),
	d("seed.subsampled", counters.Flag, "", "more than min(1000, 4×workers) usable prefixes: cuts were subsampled"),
	d("seed.truncated", counters.Flag, "", "the seed page was truncated"),
	d("seed.sentinel", counters.Flag, "", "the closing sentinel (pk, prefixCeil(pk)] was added"),
	d("seed.sentinel_declined.truncated", counters.Count, "decisions", "sentinel declined: the seed page was truncated"),
	d("seed.sentinel_declined.object_last", counters.Count, "decisions", "sentinel declined: the page ended with an object"),
	d("seed.sentinel_declined.no_ceiling", counters.Count, "decisions", "sentinel declined: no prefix ceiling"),
	d("seed.sentinel_declined.unsafe", counters.Count, "decisions", "sentinel declined: the ceiling is not a safe key"),
	d("split.considered", counters.Count, "decisions", "split decisions taken with demand and rank"),
	d("split.pivot_extrapolated", counters.Count, "pivots", "pivots from page density (else midpoint or reflection)"),
	d("split.accepted_bounded", counters.Count, "splits", "splits of bounded ranges"),
	d("split.accepted_frontier", counters.Count, "splits", "splits of the open frontier"),
	d("split.rejected.cursor", counters.Count, "decisions", "guard: pivot not above the cursor"),
	d("split.rejected.bound", counters.Count, "decisions", "guard: pivot not below hi (or the scope ceiling)"),
	d("split.rejected.done", counters.Count, "decisions", "guard: the range had ended"),
	d("split.rejected.no_pivot", counters.Count, "decisions", "guard: no safe pivot exists"),
	d("split.rejected.unsafe_cursor", counters.Count, "decisions", "guard: the cursor is not a usable start-after"),
	d("split.skipped.no_demand", counters.Count, "decisions", "no idle worker (hunger <= 0)"),
	d("split.skipped.outranked", counters.Count, "decisions", "larger ranges took the demand"),
	d("split.skipped.not_continuing", counters.Count, "decisions", "the range's last page did not continue"),
	d("child_empty.lower", counters.Count, "ranges", "lower split children that listed no key (the cost of not probing)"),
	d("child_empty.upper", counters.Count, "ranges", "upper split children that listed no key"),
	d("resumed_ranges", counters.Count, "ranges", "tiles of (K, ⊤] a resume started with"),
	d("max_active", counters.Max, "ranges", "peak ranges in Expand at once"),
	d("max_queued", counters.Max, "ranges", "peak ranges queued for a worker"),
	d("busy_ns", counters.Count, "ns", "sum of Expand wall time"),
	d("sink_wait_ns", counters.Count, "ns", "time Expand spent handing rows to the order stage (backpressure)"),
}

func d(key string, kind counters.Kind, unit, help string) counters.Def {
	return counters.Def{Key: key, Kind: kind, Unit: unit, Help: help}
}

// MarshalJSON renders the counters by iterating the table, plus the
// non-counter fields.
func (s Stats) MarshalJSON() ([]byte, error) {
	m := s.tally
	if m == nil {
		m = map[string]any{}
	}
	m["anonymous"] = s.Anonymous
	m["mean_active"] = s.MeanActive
	if s.Credentials != "" {
		m["credentials"] = s.Credentials
	}
	if s.Region != "" {
		m["region"] = s.Region
	}
	return json.Marshal(m)
}
