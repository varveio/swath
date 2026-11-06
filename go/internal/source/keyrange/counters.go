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
	cSplitRejectedUnsafePivot
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
	cSeedProbes
	cSeedProbesUnused
	cSeedRadixBands
	cSeedTopPaginated
	cSeedMassWeighted
	cSeedSampledLevels
	cSeedExplosions
	cSeedHeavyPrior
	cSeedFrontierContinued
	cSeedFlatTrivial
	cSentinelDeclinedNoPrefix
	cSentinelDeclinedNoGreatest
	cSentinelDeclinedNoSpare
	cSentinelDeclinedNotAbove
	cSeedLevelNarrow
	cSeedLevelFlatWide
	cSeedLevelFanout
	cSeedLevelHeavy
	cSeedLevelTinyLeaf
	cSeedLevelRadix
	cSeedLevelDelimiter
	cStructureRequests
	cStructureZero
	cStructureNoBoundary
	cStructureCapped
	cStructureSuppressed
	cStructureLate
	cSplitPivotStructure
	cEmptyPages
	cSeedTopUnsafe
	cPrefixesNormalized
	cBoundDrops
	cSeedShortContinuing
	cMutexWait
	cMutexMaxHold
	numCounters
)

// Counters is the engine's tally: every engagement counter, in report
// order; a provider's own counters (Description.Counters) follow. The run
// report (source.stats.counters) and the provider tables in
// docs/internals/counters.md are generated from them.
var Counters = table([numCounters]counters.Def{
	cObjects:                    d("objects", counters.Count, "objects", "objects listed (emitted rows)"),
	cBytes:                      d("bytes", counters.Count, "bytes", "sum of listed object sizes"),
	cPages:                      d("pages", counters.Count, "pages", "listing pages processed (and the seed's probes)"),
	cRequests:                   d("requests", counters.Count, "requests", "logical listing requests (a call the client retried or the engine requeued is one); attempts counts HTTP requests"),
	cOverfetchKeys:              d("overfetch_keys", counters.Count, "keys", "keys listed past a range's hi (or at or below a resume key) and dropped"),
	cTruncated:                  d("truncated_at_max_keys", counters.Flag, "", "-max-keys stopped the listing; the dataset is incomplete", "max_keys"),
	cSeedPrefixes:               d("seed.prefixes", counters.Count, "prefixes", "common prefixes on the top seed page", "seed"),
	cSeedCuts:                   d("seed.cuts", counters.Count, "cuts", "cut points the seed tiled the scope at", "seed"),
	cSeedUnsafe:                 d("seed.unsafe", counters.Count, "prefixes", "prefixes dropped as unusable start-after values", "seed"),
	cSeedSubsampled:             d("seed.subsampled", counters.Flag, "", "more cut points than min(1000, 4×workers): cuts were subsampled", "seed"),
	cSeedTruncated:              d("seed.truncated", counters.Flag, "", "the seed page was truncated", "seed"),
	cSeedSentinel:               d("seed.sentinel", counters.Flag, "", "the closing sentinel (pk, prefixCeil(pk)] was added", "seed"),
	cSentinelDeclinedTruncated:  d("seed.sentinel_declined.truncated", counters.Count, "decisions", "sentinel declined: the seed page was truncated", "seed"),
	cSentinelDeclinedObjectLast: d("seed.sentinel_declined.object_last", counters.Count, "decisions", "sentinel declined: the page ended with an object", "seed"),
	cSentinelDeclinedNoCeiling:  d("seed.sentinel_declined.no_ceiling", counters.Count, "decisions", "sentinel declined: no prefix ceiling", "seed"),
	cSentinelDeclinedUnsafe:     d("seed.sentinel_declined.unsafe", counters.Count, "decisions", "sentinel declined: the ceiling is not a safe key", "seed"),
	cSplitConsidered:            d("split.considered", counters.Count, "decisions", "split decisions taken with demand and rank", "split"),
	cSplitExtrapolated:          d("split.pivot_extrapolated", counters.Count, "pivots", "pivots from page density (else midpoint or reflection)", "split"),
	cSplitAcceptedBounded:       d("split.accepted_bounded", counters.Count, "splits", "splits of bounded ranges", "split"),
	cSplitAcceptedFrontier:      d("split.accepted_frontier", counters.Count, "splits", "splits of the open frontier", "split"),
	cSplitRejectedCursor:        d("split.rejected.cursor", counters.Count, "decisions", "guard: pivot not above the cursor", "split"),
	cSplitRejectedBound:         d("split.rejected.bound", counters.Count, "decisions", "guard: pivot not below hi (or the scope ceiling)", "split"),
	cSplitRejectedDone:          d("split.rejected.done", counters.Count, "decisions", "guard: the range had ended", "split"),
	cSplitRejectedNoPivot:       d("split.rejected.no_pivot", counters.Count, "decisions", "guard: no safe pivot exists", "split"),
	cSplitRejectedUnsafeCursor:  d("split.rejected.unsafe_cursor", counters.Count, "decisions", "guard: the cursor is not a usable start-after", "split"),
	cSplitRejectedUnsafePivot:   d("split.rejected.unsafe_pivot", counters.Count, "decisions", "guard: the provider vetoes the pivot as a bound", "split"),
	cSplitSkippedNoDemand:       d("split.skipped.no_demand", counters.Count, "decisions", "no idle worker (hunger <= 0)", "split"),
	cSplitSkippedOutranked:      d("split.skipped.outranked", counters.Count, "decisions", "larger ranges took the demand", "split"),
	cSplitSkippedNotContinuing:  d("split.skipped.not_continuing", counters.Count, "decisions", "the range's last page did not continue", "split"),
	cChildEmptyLower:            d("child_empty.lower", counters.Count, "ranges", "lower split children that listed no key (the cost of not probing)", "split"),
	cChildEmptyUpper:            d("child_empty.upper", counters.Count, "ranges", "upper split children that listed no key", "split"),
	cResumedRanges:              d("resumed_ranges", counters.Count, "ranges", "ranges a resume started with: (K, ⊤]", "resume"),
	cMaxActive:                  d("max_active", counters.Max, "ranges", "peak ranges in Expand at once"),
	cMaxQueued:                  d("max_queued", counters.Max, "ranges", "peak ranges queued for a worker", "split"),
	cBusyNs:                     d("busy_ns", counters.Count, "ns", "sum of Expand wall time"),
	cSinkWaitNs:                 d("sink_wait_ns", counters.Count, "ns", "time Expand spent handing rows to the order stage (backpressure)"),
	cProbeRequests:              d("probe.requests", counters.Count, "requests", "1-key probes (max-keys=1 after a pivot), also counted in requests", "probe"),
	cProbeUpperEmpty:            d("probe.upper_empty", counters.Count, "probes", "probes that found no key past the pivot: an empty upper child avoided", "probe"),
	cProbeMoved:                 d("probe.moved", counters.Count, "pivots", "pivots moved to the midpoint of (cursor, pivot] after an empty probe", "probe"),
	cSplitRejectedProbe:         d("split.rejected.probe_empty", counters.Count, "decisions", "splits skipped: probes found no key past any pivot tried", "probe"),
	cRequeues:                   d("requeues", counters.Count, "requests", "pages asked again after the client's retries were exhausted (the run never fails on that)"),
	cSkippedRemainingFloor:      d("split.skipped.remaining_floor", counters.Count, "decisions", "bounded range with at most 4 pages of keys left by its estimate: not split", "floors"),
	cSkippedTailFloor:           d("split.skipped.tail_floor", counters.Count, "decisions", "the child past the pivot would realize at most 2 pages of keys: not split", "floors"),
	cSeedProbes:                 d("seed.probes", counters.Count, "requests", "probes the seed descent asked for (a probe asked twice is fetched once; also in requests)", "seed"),
	cSeedProbesUnused:           d("seed.probes_unused", counters.Count, "requests", "probes fetched ahead that the descent did not ask for", "seed"),
	cSeedRadixBands:             d("seed.radix_bands", counters.Count, "cuts", "cuts synthesized after a dense flat folder ('!'..'~' without '%')", "seed"),
	cSeedTopPaginated:           d("seed.top_paginated", counters.Flag, "", "the top level was truncated and a second top page was read", "seed"),
	cSeedMassWeighted:           d("seed.mass_weighted", counters.Flag, "", "cuts were subsampled by sampled mass, not evenly", "seed"),
	cSeedSampledLevels:          d("seed.sampled_levels", counters.Count, "levels", "ambiguous truncated folders sampled (up to 3 children each)", "seed"),
	cSeedExplosions:             d("seed.explosions_confirmed", counters.Count, "levels", "sampled folders found one object per subfolder: left whole", "seed"),
	cSeedHeavyPrior:             d("seed.heavy_prior", counters.Count, "levels", "ambiguous folders classified from earlier samples (sample budget spent)", "seed"),
	cSeedFrontierContinued:      d("seed.frontier_continued", counters.Count, "levels", "the walk went on past a truncated folder", "seed"),
	cSeedFlatTrivial:            d("seed.flat_trivial", counters.Flag, "", "no folders and a complete top page: one range", "seed"),
	cSentinelDeclinedNoPrefix:   d("seed.sentinel_declined.no_prefix", counters.Count, "decisions", "sentinel declined: the top level has no folder", "seed"),
	cSentinelDeclinedNoGreatest: d("seed.sentinel_declined.no_greatest_item", counters.Count, "decisions", "sentinel declined: the top probe returned nothing", "seed"),
	cSentinelDeclinedNoSpare:    d("seed.sentinel_declined.no_spare_slot", counters.Count, "decisions", "sentinel declined: the cuts already fill min(1000, 4×workers)", "seed"),
	cSentinelDeclinedNotAbove:   d("seed.sentinel_declined.not_above_last_cut", counters.Count, "decisions", "sentinel declined: the ceiling is not above the last cut", "seed"),
	cSeedLevelNarrow:            d("seed.levels.narrow", counters.Count, "levels", "folders listed whole in one page: their subfolders are cuts and are walked", "seed"),
	cSeedLevelFlatWide:          d("seed.levels.flat_wide", counters.Count, "levels", "truncated folders of objects only", "seed"),
	cSeedLevelFanout:            d("seed.levels.fanout_tiled", counters.Count, "levels", "truncated folders of key=value partitions: up to workers of them are cuts", "seed"),
	cSeedLevelHeavy:             d("seed.levels.heavy_cut_banded", counters.Count, "levels", "truncated folders sampled heavy: up to workers subfolders are cuts", "seed"),
	cSeedLevelTinyLeaf:          d("seed.levels.tiny_leaf_explosion", counters.Count, "levels", "truncated folders of one-object subfolders: left whole", "seed"),
	cSeedLevelRadix:             d("seed.levels.dense_root_radix_banded", counters.Count, "levels", "the dense flat folder cut into radix bands", "seed"),
	cSeedLevelDelimiter:         d("seed.levels.delimiter_seeded", counters.Count, "levels", "a complete top level whose folders seeded the tiles (nothing else engaged)", "seed"),
	cStructureRequests:          d("structure.requests", counters.Count, "requests", "delimiter probes (max-keys 32) after a cursor, one per folder level; also counted in requests", "structure"),
	cStructureZero:              d("structure.zero_fanout", counters.Count, "probes", "probes that found no folder at any level", "structure"),
	cStructureNoBoundary:        d("structure.no_boundary", counters.Count, "probes", "probes whose folders all lay outside the range", "structure"),
	cStructureCapped:            d("structure.capped", counters.Count, "probes", "probes whose chosen level filled its page (the furthest folder is the pivot)", "structure"),
	cStructureSuppressed:        d("structure.suppressed", counters.Count, "decisions", "probes not sent: 8 in a row found no folder in this range", "structure"),
	cStructureLate:              d("structure.late", counters.Count, "decisions", "split decisions taken while the range's probe was still in flight", "structure"),
	cSplitPivotStructure:        d("split.pivot_structure", counters.Count, "pivots", "pivots on a folder boundary from a structure probe (else midpoint, density or reflection)", "structure"),
	cEmptyPages:                 d("empty_pages", counters.Count, "pages", "continuing pages with no entry, followed by their token (also counted in requests)"),
	cSeedTopUnsafe:              d("seed.top_paginated_declined.unsafe", counters.Count, "probes", "top-up probes not sent: the top page's last entry is not a bound the provider accepts", "seed"),
	cPrefixesNormalized:         d("prefixes_normalized", counters.Count, "pages", "pages whose common prefixes came unsorted or repeated, sorted and deduplicated"),
	cBoundDrops:                 d("bound_drops", counters.Count, "keys", "keys equal to an inclusive start bound and common prefixes at or below it, listed again and dropped (not in overfetch_keys)"),
	cSeedShortContinuing:        d("seed.short_continuing_estimated", counters.Count, "samples", "continuing seed samples shorter than a nominal page, weighed (or classed dense) as a full page: an estimate", "seed"),
	cMutexWait:                  d("mutex.wait_ns", counters.Count, "ns", "time goroutines spent waiting for the engine's decision mutex (summed over goroutines)"),
	cMutexMaxHold:               d("mutex.max_hold_ns", counters.Max, "ns", "the longest single hold of the engine's decision mutex"),
})

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
		off = append(off, "split", "probe")
	} else if !p.opt.Probe {
		off = append(off, "probe")
	}
	if p.opt.Sequential || !p.opt.Structure {
		off = append(off, "structure")
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
	return append(off, p.opt.ClientOff...)
}

// seedMarks maps the seed descent's engagements onto counters; the
// run-level classifications are counted per level (seedLevels).
var seedMarks = map[string]counters.ID{
	"descent_cuts_subsampled":              cSeedSubsampled,
	"top_truncated":                        cSeedTruncated,
	"sentinel":                             cSeedSentinel,
	"sentinel_declined.top_truncated":      cSentinelDeclinedTruncated,
	"sentinel_declined.object_last":        cSentinelDeclinedObjectLast,
	"sentinel_declined.no_ceiling":         cSentinelDeclinedNoCeiling,
	"sentinel_declined.no_prefix":          cSentinelDeclinedNoPrefix,
	"sentinel_declined.no_greatest_item":   cSentinelDeclinedNoGreatest,
	"sentinel_declined.no_spare_slot":      cSentinelDeclinedNoSpare,
	"sentinel_declined.not_above_last_cut": cSentinelDeclinedNotAbove,
	"top_paginated":                        cSeedTopPaginated,
	"top_paginated_declined.unsafe":        cSeedTopUnsafe,
	"mass_weighted_subsample":              cSeedMassWeighted,
	"heavy_cut_descended":                  cSeedSampledLevels,
	"explosion_confirmed":                  cSeedExplosions,
	"heavy_prior_applied":                  cSeedHeavyPrior,
	"frontier_continued_past_explosion":    cSeedFrontierContinued,
	"flat_trivial":                         cSeedFlatTrivial,
	"short_continuing_estimated":           cSeedShortContinuing,
}

var seedLevels = map[string]counters.ID{
	"narrow":                  cSeedLevelNarrow,
	"flat_wide":               cSeedLevelFlatWide,
	"fanout_tiled":            cSeedLevelFanout,
	"heavy_cut_banded":        cSeedLevelHeavy,
	"tiny_leaf_explosion":     cSeedLevelTinyLeaf,
	"dense_root_radix_banded": cSeedLevelRadix,
	"delimiter_seeded":        cSeedLevelDelimiter,
}
