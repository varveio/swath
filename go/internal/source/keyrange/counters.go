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
	numCounters
)

// Counters is the engine's tally: every engagement counter, in report
// order; a provider's own counters (Description.Counters) follow. The run
// report (source.stats.counters) and the provider tables in
// docs/internals/counters.md are generated from them.
var Counters = []counters.Def{
	d("objects", counters.Count, "objects", "objects listed (emitted rows)"),
	d("bytes", counters.Count, "bytes", "sum of listed object sizes"),
	d("pages", counters.Count, "pages", "listing pages processed (and the seed's probes)"),
	d("requests", counters.Count, "requests", "listing requests, retries not counted"),
	d("overfetch_keys", counters.Count, "keys", "keys listed past a range's hi (or at or below a resume key) and dropped"),
	d("truncated_at_max_keys", counters.Flag, "", "-max-keys stopped the listing; the dataset is incomplete", "max_keys"),
	d("seed.prefixes", counters.Count, "prefixes", "common prefixes on the top seed page", "seed"),
	d("seed.cuts", counters.Count, "cuts", "cut points the seed tiled the scope at", "seed"),
	d("seed.unsafe", counters.Count, "prefixes", "prefixes dropped as unusable start-after values", "seed"),
	d("seed.subsampled", counters.Flag, "", "more cut points than min(1000, 4×workers): cuts were subsampled", "seed"),
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
	d("split.rejected.unsafe_pivot", counters.Count, "decisions", "guard: the provider vetoes the pivot as a bound", "split"),
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
	d("split.skipped.remaining_floor", counters.Count, "decisions", "bounded range with at most 4 pages of keys left by its estimate: not split", "floors"),
	d("split.skipped.tail_floor", counters.Count, "decisions", "the child past the pivot would realize at most 2 pages of keys: not split", "floors"),
	d("seed.probes", counters.Count, "requests", "probes the seed descent asked for (a probe asked twice is fetched once; also in requests)", "seed"),
	d("seed.probes_unused", counters.Count, "requests", "probes fetched ahead that the descent did not ask for", "seed"),
	d("seed.radix_bands", counters.Count, "cuts", "cuts synthesized after a dense flat folder ('!'..'~' without '%')", "seed"),
	d("seed.top_paginated", counters.Flag, "", "the top level was truncated and a second top page was read", "seed"),
	d("seed.mass_weighted", counters.Flag, "", "cuts were subsampled by sampled mass, not evenly", "seed"),
	d("seed.sampled_levels", counters.Count, "levels", "ambiguous truncated folders sampled (up to 3 children each)", "seed"),
	d("seed.explosions_confirmed", counters.Count, "levels", "sampled folders found one object per subfolder: left whole", "seed"),
	d("seed.heavy_prior", counters.Count, "levels", "ambiguous folders classified from earlier samples (sample budget spent)", "seed"),
	d("seed.frontier_continued", counters.Count, "levels", "the walk went on past a truncated folder", "seed"),
	d("seed.flat_trivial", counters.Flag, "", "no folders and a complete top page: one range", "seed"),
	d("seed.sentinel_declined.no_prefix", counters.Count, "decisions", "sentinel declined: the top level has no folder", "seed"),
	d("seed.sentinel_declined.no_greatest_item", counters.Count, "decisions", "sentinel declined: the top probe returned nothing", "seed"),
	d("seed.sentinel_declined.no_spare_slot", counters.Count, "decisions", "sentinel declined: the cuts already fill min(1000, 4×workers)", "seed"),
	d("seed.sentinel_declined.not_above_last_cut", counters.Count, "decisions", "sentinel declined: the ceiling is not above the last cut", "seed"),
	d("seed.levels.narrow", counters.Count, "levels", "folders listed whole in one page: their subfolders are cuts and are walked", "seed"),
	d("seed.levels.flat_wide", counters.Count, "levels", "truncated folders of objects only", "seed"),
	d("seed.levels.fanout_tiled", counters.Count, "levels", "truncated folders of key=value partitions: up to workers of them are cuts", "seed"),
	d("seed.levels.heavy_cut_banded", counters.Count, "levels", "truncated folders sampled heavy: up to workers subfolders are cuts", "seed"),
	d("seed.levels.tiny_leaf_explosion", counters.Count, "levels", "truncated folders of one-object subfolders: left whole", "seed"),
	d("seed.levels.dense_root_radix_banded", counters.Count, "levels", "the dense flat folder cut into radix bands", "seed"),
	d("seed.levels.delimiter_seeded", counters.Count, "levels", "a complete top level whose folders seeded the tiles (nothing else engaged)", "seed"),
	d("structure.requests", counters.Count, "requests", "delimiter probes (max-keys 32) after a cursor, one per folder level; also counted in requests", "structure"),
	d("structure.zero_fanout", counters.Count, "probes", "probes that found no folder at any level", "structure"),
	d("structure.no_boundary", counters.Count, "probes", "probes whose folders all lay outside the range", "structure"),
	d("structure.capped", counters.Count, "probes", "probes whose chosen level filled its page (the furthest folder is the pivot)", "structure"),
	d("structure.suppressed", counters.Count, "decisions", "probes not sent: 8 in a row found no folder in this range", "structure"),
	d("structure.late", counters.Count, "decisions", "split decisions taken while the range's probe was still in flight", "structure"),
	d("split.pivot_structure", counters.Count, "pivots", "pivots on a folder boundary from a structure probe (else midpoint, density or reflection)", "structure"),
	d("empty_pages", counters.Count, "pages", "continuing pages with no entry, followed by their token (also counted in requests)"),
	d("seed.top_paginated_declined.unsafe", counters.Count, "probes", "top-up probes not sent: the top page's last entry is not a bound the provider accepts", "seed"),
	d("prefixes_normalized", counters.Count, "pages", "pages whose common prefixes came unsorted or repeated, sorted and deduplicated"),
	d("bound_drops", counters.Count, "keys", "keys equal to an inclusive start bound, listed again and dropped (not in overfetch_keys)"),
}

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
	return off
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
