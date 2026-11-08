// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"github.com/varveio/swath/go/internal/counters"
)

// Counter IDs, the indices of Counters.
const (
	cObjects counters.ID = iota
	cBytes
	cPages
	cRequests
	cOverfetchKeys
	cTruncated
	cSeedPrefixes
	cSeedCuts
	cSeedUnsafe
	cSeedSubsampled
	cSeedTruncated
	cSeedSentinel
	cSeedPages
	cSeedRequests
	cSeedRoundTrips
	cSeedLevels
	cSeedLevel0Cuts
	cSeedLevel1Cuts
	cSeedLevel2Cuts
	cSeedLevel3Cuts
	cSeedLevel4Cuts
	cSeedStopWide
	cSeedStopLeaf
	cSeedStopLevels
	cSeedStopRequests
	cSeedStopRoundTrips
	cSeedStopFailed
	cSeedStopOnePage
	cSeedStopFlat
	cSeedFirstPageKeys
	cSeedFirstPageDiscarded
	cSeedFlatChains
	cSeedResumed
	cSeedFromParts
	cSeedFromIndexed
	cSeedFromIndexFailed
	cSeedFromCuts
	cSeedFromUnsafe
	cSeedFromTiles
	cSeedFromExpected
	cSeedFromErrWithin
	cSeedFromErrGrown
	cSeedFromErrShrunk
	cSeedFromErrMaxGrown
	cSentinelDeclinedTruncated
	cSentinelDeclinedObjectLast
	cSentinelDeclinedNoCeiling
	cSentinelDeclinedUnsafe
	cSplitConsidered
	cSplitPivotReflect
	cSplitPivotMidpoint
	cSplitPivotDensityBounded
	cSplitPivotDensityFrontier
	cSplitSliverRefused
	cGallopMaxGen
	cGallopHalvings
	cFrontierAnchored
	cReflectCeiling
	cDensityShrinkPrevented
	cDensityRefusedShort
	cDensityRefusedAlphabet
	cDensityRefusedZeroSpan
	cDensityRefusedPastTop
	cDensityRefusedHole
	cDensityRefusedOutside
	cSplitAcceptedBounded
	cSplitAcceptedFrontier
	cSplitRejectedCursor
	cSplitRejectedBound
	cSplitRejectedNoPivot
	cSplitRejectedUnsafeCursor
	cSplitRejectedUnsafePivot
	cSplitSkippedNoDemand
	cSplitReasonSlowRange
	cSlowMaxRangeLatency
	cSlowRunLatency
	cSplitSkippedOutranked
	cSplitSkippedRemainder
	cSplitSkippedLowerTiny
	cSplitSkippedUpperTiny
	cSplitEstimateNone
	cSplitAdmittedPersistent
	cSplitMaxDepth
	cLeaves
	cLeafDepthSum
	cUnsplittable
	cUnsplittableKeys
	cIdleDemandNs
	cChildEmptyLower
	cChildEmptyUpper
	cChildEmptyReflectLower
	cChildEmptyReflectUpper
	cChildEmptyMidpointLower
	cChildEmptyMidpointUpper
	cChildEmptyDensityLower
	cChildEmptyDensityUpper
	cChildEmptyHiPivot
	cChildEmptyHiBound
	cChildSmallLower
	cChildSmallUpper
	cFrontierHandoutKeys
	cResumedRanges
	cMaxActive
	cMaxQueued
	cBusyNs
	cSinkWaitNs
	cRequeues
	cSkippedRemainingFloor
	cSkippedTailFloor
	cStructureRequests
	cStructureZero
	cStructureNoBoundary
	cStructureCapped
	cStructureSuppressed
	cStructureLate
	cStructureSentFrontier
	cStructureSentStall
	cStructureSentForced
	cStructureBoundaries
	cStructureFrontierPivots
	cStructureSuppressedInherited
	cSplitPivotStructure
	cChildEmptyStructureLower
	cChildEmptyStructureUpper
	cEmptyPages
	cEmptyChainPages
	cPrefixesNormalized
	cBoundDrops
	cMutexWait
	cMutexMaxHold
	numCounters
)

// Counters is the engine's tally: every engagement counter, in report
// order; a provider's own counters (Description.Counters) follow. The run
// report (source.stats.counters) and the provider tables in
// docs/internals/counters.md are generated from them.
var Counters = table([numCounters]counters.Def{
	cObjects:                      d("objects", counters.Count, "objects", "objects listed (emitted rows)"),
	cBytes:                        d("bytes", counters.Count, "bytes", "sum of listed object sizes"),
	cPages:                        d("pages", counters.Count, "pages", "listing pages processed (and the seed's probes)"),
	cRequests:                     d("requests", counters.Count, "requests", "logical listing requests (a call the client retried or the engine requeued is one); attempts counts HTTP requests"),
	cOverfetchKeys:                d("overfetch_keys", counters.Count, "keys", "keys listed past a range's hi (or at or below a resume key) and dropped"),
	cTruncated:                    d("truncated_at_max_keys", counters.Flag, "", "-max-keys stopped the listing; the dataset is incomplete", "max_keys"),
	cSeedPrefixes:                 d("seed.prefixes", counters.Count, "prefixes", "common prefixes on the top level's seed pages", "seed"),
	cSeedCuts:                     d("seed.cuts", counters.Count, "cuts", "cut points the seed tiled the scope at (every level, after the subsample)", "seed"),
	cSeedUnsafe:                   d("seed.unsafe", counters.Count, "prefixes", "prefixes dropped as unusable start-after values", "seed"),
	cSeedSubsampled:               d("seed.subsampled", counters.Flag, "", "more cut points than min(1000, 4×workers): cuts were subsampled", "seed"),
	cSeedTruncated:                d("seed.truncated", counters.Flag, "", "the top level had more pages than the seed followed (4)", "seed"),
	cSeedSentinel:                 d("seed.sentinel", counters.Flag, "", "the closing sentinel (pk, prefixCeil(pk)] was added", "seed"),
	cSentinelDeclinedTruncated:    d("seed.sentinel_declined.truncated", counters.Count, "decisions", "sentinel declined: the top level had more pages than the seed followed", "seed"),
	cSentinelDeclinedObjectLast:   d("seed.sentinel_declined.object_last", counters.Count, "decisions", "sentinel declined: the top level's last page ended with an object", "seed"),
	cSentinelDeclinedNoCeiling:    d("seed.sentinel_declined.no_ceiling", counters.Count, "decisions", "sentinel declined: no prefix ceiling above the last cut (and a resume key)", "seed"),
	cSentinelDeclinedUnsafe:       d("seed.sentinel_declined.unsafe", counters.Count, "decisions", "sentinel declined: the ceiling is not a safe key", "seed"),
	cSeedPages:                    d("seed.pages", counters.Count, "pages", "the top level's delimiter pages the seed followed (at most 4)", "seed"),
	cSeedRequests:                 d("seed.requests", counters.Count, "requests", "delimiter requests the seed sent, every level, the empty continuing pages followed included (at most 64; also in requests)", "seed"),
	cSeedRoundTrips:               d("seed.round_trips", counters.Count, "waves", "round trips of seed requests before any range lists: a wave counts its longest chain of requests, the empty continuing pages followed included (at most 6)", "seed"),
	cSeedLevels:                   d("seed.levels", counters.Count, "levels", "levels the seed descended below the top (at most 4)", "seed"),
	cSeedLevel0Cuts:               d("seed.level.0.cuts", counters.Count, "cuts", "usable folders found on the top level (before the subsample)", "seed"),
	cSeedLevel1Cuts:               d("seed.level.1.cuts", counters.Count, "cuts", "usable folders found one level below the top", "seed"),
	cSeedLevel2Cuts:               d("seed.level.2.cuts", counters.Count, "cuts", "usable folders found two levels below the top", "seed"),
	cSeedLevel3Cuts:               d("seed.level.3.cuts", counters.Count, "cuts", "usable folders found three levels below the top", "seed"),
	cSeedLevel4Cuts:               d("seed.level.4.cuts", counters.Count, "cuts", "usable folders found four levels below the top", "seed"),
	cSeedStopWide:                 d("seed.stop.wide", counters.Count, "seeds", "the seed stopped at a level with at least workers folders", "seed"),
	cSeedStopLeaf:                 d("seed.stop.leaf", counters.Count, "seeds", "the seed stopped at a level with no folder", "seed"),
	cSeedStopLevels:               d("seed.stop.levels", counters.Count, "seeds", "the seed stopped after descending 4 levels", "seed"),
	cSeedStopRequests:             d("seed.stop.requests", counters.Count, "seeds", "the seed stopped: the next level, or the next empty page a chain follows, would pass 64 requests", "seed"),
	cSeedStopRoundTrips:           d("seed.stop.round_trips", counters.Count, "seeds", "the seed stopped: 6 round trips spent (a wave costs its longest chain of requests, empty pages followed included)", "seed"),
	cSeedStopFailed:               d("seed.stop.failed", counters.Count, "seeds", "the seed stopped: a listing failed or the run was canceled (the seed counters are what it spent until then)", "seed"),
	cSeedStopOnePage:              d("seed.stop.one_page", counters.Count, "seeds", "the seed stopped: its first data page, sent with the top level's first delimiter request, was not truncated (the scope fits in one page; no tile)", "seed"),
	cSeedStopFlat:                 d("seed.stop.flat", counters.Count, "seeds", "the seed stopped at a level with no folder, after a chain whose page held no folder was not followed", "seed"),
	cSeedFirstPageKeys:            d("seed.first_page_keys", counters.Count, "objects", "keys the seed's first data page emitted: the whole scope, in one page", "seed"),
	cSeedFirstPageDiscarded:       d("seed.first_page_discarded", counters.Flag, "", "the seed's first data page was truncated: discarded, the tiles list the scope from its start (one request more)", "seed"),
	cSeedFlatChains:               d("seed.flat_chains", counters.Count, "chains", "delimiter chains not followed past a page that held no folder (objects only)", "seed"),
	cSeedResumed:                  d("seed.resumed", counters.Flag, "", "the seed tiled a resumed scope (K, ⊤]", "seed"),
	cSeedFromParts:                d("seed.from.parts", counters.Count, "parts", "parts of the --seed-from dataset (0 for a cut file)", "seed_from"),
	cSeedFromIndexed:              d("seed.from.indexed_parts", counters.Count, "parts", "parts whose key page index was read for finer cuts", "seed_from"),
	cSeedFromIndexFailed:          d("seed.from.index_failed", counters.Count, "parts", "parts whose key page index could not be read: their part bounds were cut at instead", "seed_from"),
	cSeedFromCuts:                 d("seed.from.cuts", counters.Count, "cuts", "cut keys the prior offered (before the merge into tiles)", "seed_from"),
	cSeedFromUnsafe:               d("seed.from.unsafe", counters.Count, "cuts", "cuts the provider does not accept as a bound: cut at their longest accepted prefix, or merged into the next", "seed_from"),
	cSeedFromTiles:                d("seed.from.tiles", counters.Count, "ranges", "ranges the prior tiled the scope into, the open frontier after its last cut included (at most min(1000, 4×workers))", "seed_from"),
	cSeedFromExpected:             d("seed.from.expected_keys", counters.Count, "keys", "keys the prior expected in its bounded tiles (0 without counts): against objects, the scope's growth", "seed_from"),
	cSeedFromErrWithin:            d("seed.from.error.within_2x", counters.Count, "ranges", "prior tiles whose ranges listed within 2x of the keys the prior expected (a tile's descendants included)", "seed_from"),
	cSeedFromErrGrown:             d("seed.from.error.grown", counters.Count, "ranges", "prior tiles whose ranges listed more than 2x the keys the prior expected: the segment grew (its first page's gauge replaced the estimate, and splits share it out)", "seed_from"),
	cSeedFromErrShrunk:            d("seed.from.error.shrunk", counters.Count, "ranges", "prior tiles whose ranges listed less than half the keys the prior expected: the segment shrank or emptied", "seed_from"),
	cSeedFromErrMaxGrown:          d("seed.from.error.max_grown", counters.Max, "x", "the largest ratio of a prior tile's listed keys to the prior's expectation, rounded", "seed_from"),
	cSplitConsidered:              d("split.considered", counters.Count, "decisions", "split decisions taken with demand and rank", "split"),
	cSplitPivotReflect:            d("split.pivot.reflect", counters.Count, "splits", "accepted splits whose pivot is the cursor reflected from its chain's anchor (forwardReflect at the scalar where they diverge)", "split"),
	cSplitPivotMidpoint:           d("split.pivot.midpoint", counters.Count, "splits", "accepted splits whose pivot is a safe scalar midpoint between the cursor and hi", "split"),
	cSplitPivotDensityBounded:     d("split.pivot.density.bounded", counters.Count, "splits", "accepted splits of bounded ranges whose pivot is the galloped density step (max(4, hunger) << generation page spans, at most halfway to hi), further than the anchor reflection", "split"),
	cSplitPivotDensityFrontier:    d("split.pivot.density.frontier", counters.Count, "splits", "accepted frontier splits whose pivot is the galloped density step, further than the anchor reflection", "split"),
	cSplitSliverRefused:           d("split.sliver_refused", counters.Count, "decisions", "splits not taken because the only pivot was a sliver just above the cursor (no density step, no safe midpoint): the range keeps listing and decides again after its next page", "split"),
	cGallopMaxGen:                 d("split.gallop.max_generation", counters.Max, "generations", "highest gallop generation (consecutive upper children along a chain) of a range that split: its density stride is max(4, hunger) << generation page spans", "split"),
	cGallopHalvings:               d("split.gallop.halvings", counters.Count, "halvings", "density stride halvings within decisions, after a target past the representable keys, a hole or outside (cursor, hi)", "split"),
	cFrontierAnchored:             d("split.frontier.anchored", counters.Count, "splits", "accepted frontier splits whose chain has an anchor (a seed or resume tile's lo, or the chain's first pivot): the reflection doubles the covered distance", "split"),
	cReflectCeiling:               d("split.reflect.ceiling", counters.Count, "decisions", "reflections at or past the range's ceiling (hi, or the scope's prefixCeil on the frontier): the pivot fell back to the midpoint between the cursor and the ceiling, and the upper child starts a new chain", "split"),
	cDensityShrinkPrevented:       d("split.density.shrink_prevented", counters.Count, "decisions", "decisions whose density step lay nearer the cursor than the anchor reflection: not taken (the density step may move a pivot further, never nearer)", "split"),
	cDensityRefusedShort:          d("split.density.refused.short_page", counters.Count, "decisions", "density step refused (after its halvings): fewer than 2 keys on the page", "split"),
	cDensityRefusedAlphabet:       d("split.density.refused.alphabet", counters.Count, "decisions", "density step refused (after its halvings): a byte on the page or the cursor is non-ASCII or a control character ('%' is read as a digit)", "split"),
	cDensityRefusedZeroSpan:       d("split.density.refused.zero_span", counters.Count, "decisions", "density step refused (after its halvings): the page spans no distance", "split"),
	cDensityRefusedPastTop:        d("split.density.refused.past_top", counters.Count, "decisions", "density step refused (after its halvings): the target is past the representable keys", "split"),
	cDensityRefusedHole:           d("split.density.refused.hole", counters.Count, "decisions", "density step refused (after its halvings): the target is not a string (an end-of-string digit inside it)", "split"),
	cDensityRefusedOutside:        d("split.density.refused.outside", counters.Count, "decisions", "density step refused (after its halvings): the target is not strictly inside (cursor, hi)", "split"),
	cSplitAcceptedBounded:         d("split.accepted_bounded", counters.Count, "splits", "splits of bounded ranges", "split"),
	cSplitAcceptedFrontier:        d("split.accepted_frontier", counters.Count, "splits", "splits of the open frontier", "split"),
	cSplitRejectedCursor:          d("split.rejected.cursor", counters.Count, "decisions", "guard: pivot not above the cursor", "split"),
	cSplitRejectedBound:           d("split.rejected.bound", counters.Count, "decisions", "guard: pivot not below hi (or the scope ceiling)", "split"),
	cSplitRejectedNoPivot:         d("split.rejected.no_pivot", counters.Count, "decisions", "guard: no safe pivot exists", "split"),
	cSplitRejectedUnsafeCursor:    d("split.rejected.unsafe_cursor", counters.Count, "decisions", "guard: the cursor is not a usable start-after", "split"),
	cSplitRejectedUnsafePivot:     d("split.rejected.unsafe_pivot", counters.Count, "decisions", "guard: the provider vetoes the pivot as a bound", "split"),
	cSplitSkippedNoDemand:         d("split.skipped.no_demand", counters.Count, "decisions", "no idle worker (hunger <= 0)", "split"),
	cSplitReasonSlowRange:         d("split.reason.slow_range", counters.Count, "splits", "--slow-split: splits taken without an idle worker: the range's pages took at least 4 times the run's mean page latency and its time left (keys left x its page latency) was at least twice the mean of the running ranges'", "slow_split"),
	cSlowMaxRangeLatency:          d("split.slow.max_range_latency_ns", counters.Max, "ns", "--slow-split: the highest page latency (EWMA) of a range", "slow_split"),
	cSlowRunLatency:               d("split.slow.run_latency_ns", counters.Gauge, "ns", "--slow-split: the run's mean range page latency", "slow_split"),
	cSplitSkippedOutranked:        d("split.skipped.outranked", counters.Count, "decisions", "larger ranges took the demand", "split"),
	cSplitSkippedRemainder:        d("split.skipped.remainder", counters.Count, "decisions", "bounded range whose remainder (cursor, hi] is under 2 pages by its last page's density: not split, the owner lists it in at most two more requests (decided again after the next page)", "split"),
	cSplitSkippedLowerTiny:        d("split.skipped.lower_tiny", counters.Count, "decisions", "bounded range whose pivot would leave the lower child (cursor, pivot] under 1 page by its last page's density: not split, the owner's next request covers it (decided again after the next page)", "split"),
	cSplitSkippedUpperTiny:        d("split.skipped.upper_tiny", counters.Count, "decisions", "bounded range whose pivot would leave the upper child (pivot, hi] under 1 page by its last page's density (often: past the remainder's last key): not split (decided again after the next page)", "split"),
	cSplitAdmittedPersistent:      d("split.admission.overridden", counters.Count, "decisions", "bounded split decisions the admission gates would refuse, taken because the range's previous 4 decisions (with demand) were refused: the policy kept proposing a pivot the gates refuse", "split"),
	cSplitEstimateNone:            d("split.estimate_none", counters.Count, "decisions", "bounded split decisions past the guard without a density estimate (a page of fewer than 2 keys or no span): the admission gates did not apply", "split"),
	cSplitMaxDepth:                d("split.max_depth", counters.Max, "splits", "longest chain of splits from a root or seed tile to a range: each step waits for a page, so at one page latency per step it bounds the wall time from below", "split"),
	cLeaves:                       d("split.leaves", counters.Count, "ranges", "ranges that listed through their bound without splitting (the tiling's leaves)", "split"),
	cLeafDepthSum:                 d("split.leaf_depth_sum", counters.Count, "splits", "sum of the leaves' depths: over split.leaves, the mean leaf depth", "split"),
	cUnsplittable:                 d("split.unsplittable", counters.Count, "ranges", "ranges proven unsplittable: no pivot exists above a safe cursor (no_pivot); an unsafe cursor declines one page only", "split"),
	cUnsplittableKeys:             d("split.unsplittable_keys", counters.Count, "keys", "keys listed by ranges after they became unsplittable (drained by one worker)", "split"),
	cIdleDemandNs:                 d("idle_demand_ns", counters.Count, "ns", "worker time idle with demand: max(0, workers - active - queued) integrated while a range is in Expand", "split"),
	cChildEmptyLower:              d("child_empty.lower", counters.Count, "ranges", "lower split children that listed no key", "split"),
	cChildEmptyUpper:              d("child_empty.upper", counters.Count, "ranges", "upper split children that listed no key", "split"),
	cChildEmptyReflectLower:       d("child_empty.by.reflect.lower", counters.Count, "ranges", "empty lower children of a reflect pivot", "split"),
	cChildEmptyReflectUpper:       d("child_empty.by.reflect.upper", counters.Count, "ranges", "empty upper children of a reflect pivot", "split"),
	cChildEmptyMidpointLower:      d("child_empty.by.midpoint.lower", counters.Count, "ranges", "empty lower children of a midpoint pivot", "split"),
	cChildEmptyMidpointUpper:      d("child_empty.by.midpoint.upper", counters.Count, "ranges", "empty upper children of a midpoint pivot", "split"),
	cChildEmptyDensityLower:       d("child_empty.by.density.lower", counters.Count, "ranges", "empty lower children of a density pivot", "split"),
	cChildEmptyDensityUpper:       d("child_empty.by.density.upper", counters.Count, "ranges", "empty upper children of a density pivot", "split"),
	cChildEmptyHiPivot:            d("child_empty.hi.pivot", counters.Count, "ranges", "empty children whose hi is a split's pivot", "split"),
	cChildEmptyHiBound:            d("child_empty.hi.bound", counters.Count, "ranges", "empty children whose hi is a real bound: a seed cut, the scope's end or ⊤", "split"),
	cChildSmallLower:              d("child_small.lower", counters.Count, "ranges", "lower split children that ended or split having listed fewer keys than one page (empty ones included)", "split"),
	cChildSmallUpper:              d("child_small.upper", counters.Count, "ranges", "upper split children that ended or split having listed fewer keys than one page (empty ones included)", "split"),
	cFrontierHandoutKeys:          d("split.frontier_handout_keys", counters.Count, "keys", "keys listed inside the lower children of frontier splits (the child and its descendants): over split.accepted_frontier, the work a frontier split hands out", "split"),
	cResumedRanges:                d("resumed_ranges", counters.Count, "ranges", "ranges a resume started with: (K, ⊤], or the seed's tiling of it", "resume"),
	cMaxActive:                    d("max_active", counters.Max, "ranges", "peak ranges in Expand at once"),
	cMaxQueued:                    d("max_queued", counters.Max, "ranges", "peak ranges queued for a worker", "split"),
	cBusyNs:                       d("busy_ns", counters.Count, "ns", "sum of Expand wall time"),
	cSinkWaitNs:                   d("sink_wait_ns", counters.Count, "ns", "time Expand spent handing rows to the order stage (backpressure)"),
	cRequeues:                     d("requeues", counters.Count, "requests", "pages asked again after the client's retries were exhausted (the run never fails on that)"),
	cSkippedRemainingFloor:        d("split.skipped.remaining_floor", counters.Count, "decisions", "bounded range with at most 4 pages of keys left by its estimate: not split", "floors"),
	cSkippedTailFloor:             d("split.skipped.tail_floor", counters.Count, "decisions", "the child past the pivot would realize at most 2 pages of keys: not split", "floors"),
	cStructureRequests:            d("structure.requests", counters.Count, "requests", "delimiter probes (max-keys 32) after a cursor, one per folder level; also counted in requests", "structure"),
	cStructureZero:                d("structure.zero_fanout", counters.Count, "probes", "probes that found no folder at any level", "structure"),
	cStructureNoBoundary:          d("structure.no_boundary", counters.Count, "probes", "probes whose folders all lay outside the range", "structure"),
	cStructureCapped:              d("structure.capped", counters.Count, "probes", "probes whose chosen level filled its page (the furthest folder is the pivot)", "structure"),
	cStructureSuppressed:          d("structure.suppressed", counters.Count, "decisions", "probes not sent: 8 in a row found no folder along this range's ancestry", "structure"),
	cStructureLate:                d("structure.late", counters.Count, "decisions", "split decisions taken while the range's probe was still in flight", "structure"),
	cStructureSentFrontier:        d("structure.sent.frontier", counters.Count, "probes", "probes sent by the open frontier (the scope root and the two folders above the cursor's own)", "structure"),
	cStructureSentStall:           d("structure.sent.stall", counters.Count, "probes", "probes sent by a bounded range whose parent's pivot stayed in the cursor's own folder", "structure"),
	cStructureSentForced:          d("structure.sent.forced", counters.Count, "probes", "probes sent by a bounded range without that evidence (--structure=on)", "structure"),
	cStructureBoundaries:          d("structure.boundaries", counters.Count, "folders", "folder boundaries the probes found inside their ranges", "structure"),
	cStructureFrontierPivots:      d("structure.frontier_pivots", counters.Count, "splits", "frontier splits whose pivot is a folder boundary, further than the anchor reflection (also in split.pivot.structure)", "structure"),
	cStructureSuppressedInherited: d("structure.suppressed_inherited", counters.Count, "decisions", "suppressed probes whose 8 empty probes were all an ancestor's: the range inherited the suppression", "structure"),
	cSplitPivotStructure:          d("split.pivot.structure", counters.Count, "splits", "accepted splits whose pivot is a folder boundary from a structure probe", "structure"),
	cChildEmptyStructureLower:     d("child_empty.by.structure.lower", counters.Count, "ranges", "empty lower children of a structure pivot", "structure"),
	cChildEmptyStructureUpper:     d("child_empty.by.structure.upper", counters.Count, "ranges", "empty upper children of a structure pivot", "structure"),
	cEmptyPages:                   d("empty_pages", counters.Count, "pages", "continuing pages with no entry, followed by their token (also counted in requests)"),
	cEmptyChainPages:              d("empty_chain_pages", counters.Count, "pages", "empty continuing pages past the 1,024th of their run: followed, but not progress for the watchdog (also in empty_pages)"),
	cPrefixesNormalized:           d("prefixes_normalized", counters.Count, "pages", "pages whose common prefixes came unsorted or repeated, sorted and deduplicated"),
	cBoundDrops:                   d("bound_drops", counters.Count, "keys", "keys equal to an inclusive start bound and common prefixes at or below it, listed again and dropped (not in overfetch_keys)"),
	cMutexWait:                    d("mutex.wait_ns", counters.Count, "ns", "time goroutines spent waiting for the engine's decision mutex (summed over goroutines)"),
	cMutexMaxHold:                 d("mutex.max_hold_ns", counters.Max, "ns", "the longest single hold of the engine's decision mutex"),
})

// pivotBy is the accepted-split counter of each pivot source (density is
// split by its branch in commit; a sliver is never a split's pivot).
var pivotBy = [...]counters.ID{srcReflect: cSplitPivotReflect, srcMidpoint: cSplitPivotMidpoint,
	srcStructure: cSplitPivotStructure}

// emptyBy is the empty-child counter of each parent pivot source, lower
// then upper.
var emptyBy = [...][2]counters.ID{
	srcReflect:   {cChildEmptyReflectLower, cChildEmptyReflectUpper},
	srcMidpoint:  {cChildEmptyMidpointLower, cChildEmptyMidpointUpper},
	srcDensity:   {cChildEmptyDensityLower, cChildEmptyDensityUpper},
	srcStructure: {cChildEmptyStructureLower, cChildEmptyStructureUpper},
}

// seedLevelCuts is the folders-found counter of each seed level.
var seedLevelCuts = [seedLevels + 1]counters.ID{cSeedLevel0Cuts, cSeedLevel1Cuts, cSeedLevel2Cuts, cSeedLevel3Cuts, cSeedLevel4Cuts}

// seedStopBy is the counter of each reason the seed stopped descending.
var seedStopBy = [...]counters.ID{stopWide: cSeedStopWide, stopLeaf: cSeedStopLeaf, stopLevels: cSeedStopLevels,
	stopRequests: cSeedStopRequests, stopRoundTrips: cSeedStopRoundTrips, stopFailed: cSeedStopFailed,
	stopOnePage: cSeedStopOnePage, stopFlat: cSeedStopFlat}

// probeSent is the counter of each reason a probe was sent.
var probeSent = [...]counters.ID{probeFrontier: cStructureSentFrontier, probeStall: cStructureSentStall, probeForced: cStructureSentForced}

// extRefused is the counter of each refusal of the density step.
var extRefused = [...]counters.ID{extShort: cDensityRefusedShort, extAlphabet: cDensityRefusedAlphabet,
	extZeroSpan: cDensityRefusedZeroSpan, extPastTop: cDensityRefusedPastTop, extHole: cDensityRefusedHole,
	extOutside: cDensityRefusedOutside}

// table is defs as a slice indexed by counter ID: an entry is keyed by its
// ID, so no position carries a meaning (TestCounterTable).
func table(defs [numCounters]counters.Def) []counters.Def { return defs[:] }

func d(key string, kind counters.Kind, unit, help string, feature ...string) counters.Def {
	def := counters.Def{Key: key, Kind: kind, Unit: unit, Help: help}
	if len(feature) > 0 {
		def.Feature = feature[0]
	}
	return def
}

// disabled lists the policies with counters that this run has turned off.
func (p *Engine[P]) disabled() []string {
	var off []string
	if p.opt.Sequential {
		off = append(off, "split")
	}
	if p.opt.Sequential || p.opt.Structure == StructureOff {
		off = append(off, "structure")
	}
	if p.opt.Sequential || !p.opt.Floors {
		off = append(off, "floors")
	}
	if p.opt.Sequential || !p.opt.Seed || p.opt.Prior != nil {
		off = append(off, "seed")
	}
	if p.opt.Sequential || p.opt.Prior == nil {
		off = append(off, "seed_from")
	}
	if p.opt.Sequential || !p.opt.SlowSplit {
		off = append(off, "slow_split")
	}
	if p.opt.Resume == nil {
		off = append(off, "resume")
	}
	if p.opt.MaxKeys == 0 {
		off = append(off, "max_keys")
	}
	return append(off, p.opt.ClientOff...)
}
