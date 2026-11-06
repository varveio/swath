// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"math"
	"slices"
	"strings"
)

// The seed descent, ported from Swath's HybridSeedPlanner (mass-aware, with
// fan-out tiling and radix bands, its defaults). It walks the folder
// hierarchy with delimiter pages, most promising folders first, within a
// probe budget, classifies each truncated level, and returns cut points that
// tile the scope. It is a pure state machine over the probe results; the
// provider issues the probes (seed.go), several at once where the order of
// the walk is already known.

const (
	seedDelimiter = "/"
	maxSeeds      = 1000 // the most cuts a seed makes
	// sampleWidth children of an ambiguous truncated folder are probed to
	// tell a heavy subtree from a one-object-per-folder explosion.
	sampleWidth = 3
	// sampleBudget caps sample probes (children and weights) within the
	// probe budget.
	sampleBudget = 32
	// sampleDenseMin: a sampled child with this many objects (or truncated,
	// or with subfolders) is evidence of a heavy subtree.
	sampleDenseMin = 8
	// minWeightSamples weights are needed before the cut subsample is
	// weighted by mass.
	minWeightSamples = sampleBudget / 4
	// Radix bands: a dense flat folder is cut after each of up to bandSpan
	// printable ASCII bytes ('!'..'~' without '%').
	bandLo, bandHi = 0x21, 0x7E
	bandSpan       = bandHi - bandLo
	minBands       = 8
)

// keyspaceCeiling is the span score's upper bound when the scope has none.
var keyspaceCeiling = strings.Repeat("\xff", maxKeyLen)

// seedProbe is one delimiter page: prefix, start-after.
type seedProbe struct{ prefix, after string }

// seedOutcome is what the planner reads from a probe's page.
type seedOutcome struct {
	prefixes []string
	more     bool   // the page continues (a token): the level was not listed whole
	objects  int    // direct objects on the page (with prefixes, the observed entries)
	last     string // greatest key or common prefix on the page
	hasLast  bool
}

// seedResult is a finished plan: cut points in order (the tiles are
// (⊥, c1], …, (ck, ⊤]), the radix-band cuts among them, the probes it used,
// and what it engaged (reason -> count) and how it classified each level.
type seedResult struct {
	cuts        []string
	synthesized int
	probes      int
	marks       map[string]int
	levels      map[string]int
}

type seedPhase uint8

const (
	phaseTop seedPhase = iota
	phaseTopExtra
	phaseDescent
	phaseSampleChild
	phaseWeightSample
)

type levelDecision struct {
	prefix, class string
}

// descent is one run's seed walk.
type descent struct {
	prefix      string
	workers     int
	targetSeeds int
	maxProbes   int
	topCeil     string

	cuts      []string // sorted, unique
	frontier  spanFrontier
	decisions []levelDecision
	marks     map[string]int

	totalProbes, sampleProbes int
	phase                     seedPhase

	topCapped                    bool
	topGreatest, topGreatestCP   string
	hasTopGreatest, hasTopCP     bool
	topObjects                   int
	extraFired, extraCapped      bool
	flatWide                     string
	hasFlatWide                  bool
	tinyLeaf, fanoutTiled, heavy bool
	massWeighted                 bool
	weights                      map[string]int64

	descentCeiling             int
	reorderedMark, levelMarked bool
	heavySamples, sampledLevel int
	currentDir                 string
	currentDepth               int
	currentCps                 []string

	sampleN, sampleJ, sampleIdx, sampleDense, sampleSampled int

	weightAll     []string
	weightBudget  int
	weightIdx     int
	currentWeight string

	safe func(string) bool // the provider's bound predicate: a start-after the walk may send
	page int               // the nominal page size: a probe is one page, never paginated
}

func newDescent(prefix string, workers, page int, safe func(string) bool) *descent {
	w := max(1, workers)
	target := min(maxSeeds, 4*w)
	d := &descent{prefix: prefix, workers: w, targetSeeds: target, maxProbes: min(256, max(1, target)),
		marks: map[string]int{}, safe: safe, page: page}
	d.topCeil = scopeCeiling(prefix)
	return d
}

func scopeCeiling(p string) string {
	if c, ok := prefixCeil(p); ok {
		return c
	}
	return keyspaceCeiling
}

func (d *descent) mark(reason string) { d.marks[reason]++ }

// start is the top probe.
func (d *descent) start() (seedProbe, *seedResult) {
	d.phase = phaseTop
	d.totalProbes++
	return seedProbe{prefix: d.prefix}, nil
}

// next feeds a probe's outcome and returns the next probe or the plan.
func (d *descent) next(o seedOutcome) (seedProbe, *seedResult) {
	switch d.phase {
	case phaseTop:
		return d.onTop(o)
	case phaseTopExtra:
		return d.onTopExtra(o)
	case phaseDescent:
		return d.onDescentProbe(o)
	case phaseSampleChild:
		return d.onSampleChild(o)
	default:
		return d.onWeightSample(o)
	}
}

// lookahead lists up to n probes the walk will ask for after the one just
// issued, in order, if no level detours into sampling: the folders queued
// at the current depth, or the rest of the current sample. The provider
// fetches them ahead; one the walk never asks for is wasted, never wrong.
func (d *descent) lookahead(n int) []seedProbe {
	var out []seedProbe
	switch d.phase {
	case phaseDescent:
		left := min(n, d.descentCeiling-d.totalProbes)
		for _, e := range d.frontier.peek(left) {
			if e.depth != d.currentDepth {
				break
			}
			out = append(out, seedProbe{prefix: e.cut})
		}
	case phaseSampleChild:
		for i := d.sampleIdx; i < d.sampleJ && len(out) < n; i++ {
			out = append(out, seedProbe{prefix: d.currentCps[sampleIndex(i, d.sampleJ, d.sampleN)]})
		}
	case phaseWeightSample:
		for i := d.weightIdx; i < d.weightBudget && len(out) < n; i++ {
			out = append(out, seedProbe{prefix: d.weightAll[sampleIndex(i, d.weightBudget, len(d.weightAll))]})
		}
	}
	return out
}

func sampleIndex(i, j, n int) int {
	if j == 1 {
		return n / 2
	}
	return int(int64(i) * int64(n-1) / int64(j-1))
}

func (d *descent) onTop(o seedOutcome) (seedProbe, *seedResult) {
	d.topCapped, d.topObjects = o.more, o.objects
	d.observeTop(o)
	d.addCuts(o.prefixes, d.topCeil)
	if d.topCapped && len(d.cuts) > 0 && d.totalProbes < d.maxProbes && o.hasLast {
		// The top-up starts after the page's last entry; a provider that
		// vetoes it as a bound never receives it (no top-up).
		if d.safe != nil && !d.safe(o.last) {
			d.mark("top_paginated_declined.unsafe")
			return d.afterTop()
		}
		d.phase = phaseTopExtra
		d.totalProbes++
		return seedProbe{prefix: d.prefix, after: o.last}, nil
	}
	return d.afterTop()
}

func (d *descent) onTopExtra(o seedOutcome) (seedProbe, *seedResult) {
	d.mark("top_paginated")
	d.extraFired, d.extraCapped = true, o.more
	d.observeTop(o)
	d.addCuts(o.prefixes, d.topCeil)
	return d.afterTop()
}

func (d *descent) observeTop(o seedOutcome) {
	if o.hasLast && (!d.hasTopGreatest || o.last > d.topGreatest) {
		d.topGreatest, d.hasTopGreatest = o.last, true
	}
	for _, cp := range o.prefixes {
		if !d.hasTopCP || cp > d.topGreatestCP {
			d.topGreatestCP, d.hasTopCP = cp, true
		}
	}
}

func (d *descent) afterTop() (seedProbe, *seedResult) {
	if len(d.cuts) == 0 {
		// No folders: a small flat scope (one range), or one dense flat
		// folder at the top, cut into radix bands.
		if d.topCapped && d.topObjects > 0 {
			d.flatWide, d.hasFlatWide = d.prefix, true
			d.decisions = append(d.decisions, levelDecision{d.prefix, "flat_wide"})
		} else {
			d.decisions = append(d.decisions, levelDecision{d.prefix, "narrow"})
		}
		return d.finalize()
	}
	// A truncated top with folders is more than a page of top-level
	// folders: left whole, no descent.
	if d.topCapped {
		d.tinyLeaf = true
		d.decisions = append(d.decisions, levelDecision{d.prefix, "tiny_leaf_explosion"})
	} else {
		d.decisions = append(d.decisions, levelDecision{d.prefix, "narrow"})
	}
	if !d.topCapped {
		d.phase = phaseDescent
		d.descentCeiling = d.maxProbes - min(sampleBudget, d.maxProbes/2)
		return d.continueDescent()
	}
	return d.finalize()
}

func (d *descent) continueDescent() (seedProbe, *seedResult) {
	if d.frontier.empty() || d.totalProbes >= d.descentCeiling {
		return d.afterDescent()
	}
	if !d.reorderedMark && d.frontier.size() > 1 {
		d.mark("frontier_reordered")
		d.reorderedMark = true
	}
	if !d.levelMarked && d.frontier.multipleDepths() {
		d.mark("frontier_level_ordered")
		d.levelMarked = true
	}
	e := d.frontier.poll()
	d.currentDir, d.currentDepth = e.cut, e.depth
	d.totalProbes++
	return seedProbe{prefix: d.currentDir}, nil
}

func (d *descent) onDescentProbe(o seedOutcome) (seedProbe, *seedResult) {
	if o.more {
		d.currentCps = slices.Sorted(slices.Values(o.prefixes))
		return d.classifyTruncated(o)
	}
	d.addCuts(o.prefixes, scopeCeiling(d.currentDir))
	d.decisions = append(d.decisions, levelDecision{d.currentDir, "narrow"})
	return d.continueDescent()
}

func (d *descent) classifyTruncated(o seedOutcome) (seedProbe, *seedResult) {
	if len(o.prefixes) == 0 && o.objects > 0 { // flat and wide
		if !d.hasFlatWide {
			d.flatWide, d.hasFlatWide = d.currentDir, true
		}
		return d.finishLevel("flat_wide")
	}
	if isPartitionFanout(d.currentCps) {
		d.addSubsample(d.currentCps, d.workers)
		d.fanoutTiled = true
		if d.sampleProbes < sampleBudget && d.totalProbes < d.maxProbes {
			d.mark("banding_deferred_to_fanout")
		}
		return d.finishLevel("fanout_tiled")
	}
	// Ambiguous: a heavy subtree (band it) or a one-object-per-folder
	// explosion (leave it whole).
	if d.sampleProbes < sampleBudget && d.totalProbes < d.maxProbes {
		return d.beginSample()
	}
	heavy := false
	if d.sampledLevel > 0 {
		heavy = d.heavySamples*2 >= d.sampledLevel
		d.mark("heavy_prior_applied")
		if heavy {
			d.mark("heavy_prior_banded")
		} else {
			d.mark("heavy_prior_left_whole")
		}
	}
	return d.finishAmbiguous(heavy)
}

func (d *descent) finishLevel(class string) (seedProbe, *seedResult) {
	d.decisions = append(d.decisions, levelDecision{d.currentDir, class})
	if !d.frontier.empty() && d.totalProbes < d.maxProbes {
		d.mark("frontier_continued_past_explosion")
	}
	d.phase = phaseDescent
	return d.continueDescent()
}

func (d *descent) afterDescent() (seedProbe, *seedResult) {
	if !d.topCapped && len(d.cuts) > d.targetSeeds && d.sampleProbes < sampleBudget {
		d.weightAll = slices.Clone(d.cuts)
		d.weightBudget = min(sampleBudget-d.sampleProbes, len(d.weightAll))
		d.weightIdx = 0
		d.weights = map[string]int64{}
		d.phase = phaseWeightSample
		return d.nextWeight()
	}
	return d.finalize()
}

func (d *descent) beginSample() (seedProbe, *seedResult) {
	n := len(d.currentCps)
	if n == 0 {
		d.sampledLevel++
		d.mark("heavy_cut_descended")
		d.mark("explosion_confirmed")
		return d.finishAmbiguous(false)
	}
	d.sampleN, d.sampleJ = n, min(sampleWidth, n)
	d.sampleIdx, d.sampleDense, d.sampleSampled = 0, 0, 0
	return d.nextSample()
}

func (d *descent) nextSample() (seedProbe, *seedResult) {
	if d.sampleIdx >= d.sampleJ || d.sampleProbes >= sampleBudget || d.totalProbes >= d.maxProbes {
		return d.finishSample()
	}
	child := d.currentCps[sampleIndex(d.sampleIdx, d.sampleJ, d.sampleN)]
	d.totalProbes++
	d.sampleProbes++
	d.sampleSampled++
	d.sampleIdx++
	d.phase = phaseSampleChild
	return seedProbe{prefix: child}, nil
}

func (d *descent) onSampleChild(o seedOutcome) (seedProbe, *seedResult) {
	dense := len(o.prefixes) > 0 || o.objects >= sampleDenseMin || (o.more && o.entries() >= d.page)
	if !dense && o.more {
		dense = true // a short continuing page: taken as a full one (estimate)
		d.mark("short_continuing_estimated")
	}
	if dense {
		d.sampleDense++
	}
	return d.nextSample()
}

// entries is what the page held: its prefixes and objects.
func (o seedOutcome) entries() int { return len(o.prefixes) + o.objects }

func (d *descent) finishSample() (seedProbe, *seedResult) {
	heavy := d.sampleSampled > 0 && d.sampleDense*2 >= d.sampleSampled
	d.sampledLevel++
	if heavy {
		d.heavySamples++
	}
	d.mark("heavy_cut_descended")
	if !heavy {
		d.mark("explosion_confirmed")
	}
	d.phase = phaseDescent
	return d.finishAmbiguous(heavy)
}

func (d *descent) finishAmbiguous(heavy bool) (seedProbe, *seedResult) {
	if heavy {
		d.addSubsample(d.currentCps, min(len(d.currentCps), d.workers))
		d.heavy = true
		d.mark("heavy_cut_banded")
		return d.finishLevel("heavy_cut_banded")
	}
	d.tinyLeaf = true
	return d.finishLevel("tiny_leaf_explosion")
}

func (d *descent) nextWeight() (seedProbe, *seedResult) {
	if d.weightIdx >= d.weightBudget || d.sampleProbes >= sampleBudget || d.totalProbes >= d.maxProbes {
		if len(d.weights) >= minWeightSamples {
			d.massWeighted = true
		} else {
			d.weights = nil
		}
		return d.finalize()
	}
	d.currentWeight = d.weightAll[sampleIndex(d.weightIdx, d.weightBudget, len(d.weightAll))]
	d.totalProbes++
	d.sampleProbes++
	d.weightIdx++
	return seedProbe{prefix: d.currentWeight}, nil
}

func (d *descent) onWeightSample(o seedOutcome) (seedProbe, *seedResult) {
	// The weight is the observed entries. A continuing sample holds more than
	// it showed, by an unknown amount: when it showed less than a nominal
	// page (a short continuing page), the nominal page is the estimate (a
	// heuristic, counted).
	w := int64(o.entries())
	if o.more && w < int64(d.page) {
		w = int64(d.page)
		d.mark("short_continuing_estimated")
	}
	d.weights[d.currentWeight] = max(1, w)
	return d.nextWeight()
}

func (d *descent) finalize() (seedProbe, *seedResult) {
	res := &seedResult{probes: d.totalProbes, marks: d.marks, levels: map[string]int{}}
	if len(d.cuts) == 0 && !d.hasFlatWide {
		d.mark("flat_trivial")
		for _, l := range d.decisions {
			res.levels[l.class]++
		}
		return seedProbe{}, res
	}
	if len(d.cuts) > d.targetSeeds {
		d.mark("descent_cuts_subsampled")
	}
	var set []string
	if d.massWeighted && d.weights != nil {
		set = massWeightedSubsample(d.cuts, d.targetSeeds, d.weights)
		d.mark("mass_weighted_subsample")
	} else {
		set = subsampleEvenly(d.cuts, d.targetSeeds)
	}
	synthesized := 0
	if d.hasFlatWide {
		var bands []string
		for i := range min(bandSpan, max(minBands, d.targetSeeds)) {
			bands = append(bands, d.flatWide+string(rune(spreadScalar(i, min(bandSpan, max(minBands, d.targetSeeds))))))
		}
		var added int
		set, added = mergeCount(set, bands)
		synthesized = added
	}
	cuts := d.appendSentinel(set)
	if synthesized > 0 {
		d.mark("dense_root_radix_banded")
	}
	if d.tinyLeaf {
		d.mark("tiny_leaf_explosion")
	}
	if d.fanoutTiled {
		d.mark("fanout_tiled")
	}
	generic := synthesized == 0 && !d.tinyLeaf && !d.fanoutTiled && !d.heavy
	if generic {
		d.mark("delimiter_seeded")
	}
	if d.topCapped {
		d.mark("top_truncated")
	} else {
		d.mark("top_complete")
	}
	for i, l := range d.decisions {
		c := l.class
		if synthesized > 0 && d.hasFlatWide && l.prefix == d.flatWide {
			c = "dense_root_radix_banded"
		} else if i == 0 && generic && c == "narrow" {
			c = "delimiter_seeded"
		}
		res.levels[c]++
	}
	res.cuts, res.synthesized = cuts, synthesized
	return seedProbe{}, res
}

// appendSentinel bounds the last tile at prefixCeil of the greatest
// top-level folder when the top level is complete, ends with that folder,
// and a seed slot is spare, so the mass in it is not the open frontier.
func (d *descent) appendSentinel(cuts []string) []string {
	switch {
	case d.topCapped && !(d.extraFired && !d.extraCapped):
		d.mark("sentinel_declined.top_truncated")
	case !d.hasTopCP:
		d.mark("sentinel_declined.no_prefix")
	case !d.hasTopGreatest:
		d.mark("sentinel_declined.no_greatest_item")
	case d.topGreatest != d.topGreatestCP:
		d.mark("sentinel_declined.object_last")
	case len(cuts) >= d.targetSeeds:
		d.mark("sentinel_declined.no_spare_slot")
	default:
		u, ok := prefixCeil(d.topGreatestCP)
		switch {
		case !ok:
			d.mark("sentinel_declined.no_ceiling")
		case len(cuts) > 0 && u <= cuts[len(cuts)-1]:
			d.mark("sentinel_declined.not_above_last_cut")
		default:
			d.mark("sentinel")
			return append(cuts, u)
		}
	}
	return cuts
}

// addCuts adds a level's folders to the cut set and queues the new ones.
func (d *descent) addCuts(prefixes []string, scopeUpper string) {
	sorted := slices.Compact(slices.Sorted(slices.Values(prefixes)))
	var added []string
	for _, p := range sorted {
		if _, found := slices.BinarySearch(d.cuts, p); !found {
			added = append(added, p)
		}
	}
	d.cuts, _ = mergeCount(d.cuts, added)
	for _, p := range added {
		d.frontier.offer(p, strings.Count(p, seedDelimiter), spanScore(p, d.cuts, scopeUpper))
	}
}

// addSubsample adds an even subsample of a level's folders to the cut set
// (not to the frontier: the level is not descended).
func (d *descent) addSubsample(sortedCps []string, max int) {
	d.cuts, _ = mergeCount(d.cuts, subsampleEvenly(slices.Compact(slices.Clone(sortedCps)), max))
}

// mergeCount merges sorted b into sorted, unique a; it returns the merged
// set and how many of b were new.
func mergeCount(a, b []string) ([]string, int) {
	b = slices.Compact(slices.Sorted(slices.Values(b)))
	out := make([]string, 0, len(a)+len(b))
	i, j, added := 0, 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j == len(b) || (i < len(a) && a[i] < b[j]):
			out = append(out, a[i])
			i++
		case i == len(a) || b[j] < a[i]:
			out = append(out, b[j])
			j++
			added++
		default:
			out = append(out, a[i])
			i, j = i+1, j+1
		}
	}
	return out, added
}

func subsampleEvenly(all []string, max int) []string {
	n := len(all)
	if n <= max {
		return slices.Clone(all)
	}
	if max <= 1 {
		return []string{all[n/2]}
	}
	out := make([]string, 0, max)
	for i := range max {
		out = append(out, all[int(int64(i)*int64(n-1)/int64(max-1))])
	}
	return slices.Compact(out)
}

// massWeightedSubsample walks the cuts accumulating sampled weights (1
// where unsampled) and keeps one cut per total/max of weight.
func massWeightedSubsample(all []string, max int, weights map[string]int64) []string {
	n := len(all)
	if n <= max {
		return slices.Clone(all)
	}
	var total int64
	w := make([]int64, n)
	for i, c := range all {
		w[i] = 1
		if x, ok := weights[c]; ok {
			w[i] = x
		}
		total += w[i]
	}
	step := float64(total) / float64(max)
	next, acc := step, 0.0
	var out []string
	for i := 0; i < n && len(out) < max; i++ {
		acc += float64(w[i])
		if acc+1e-9 >= next {
			out = append(out, all[i])
			next = acc + step
		}
	}
	if len(out) == 0 {
		out = append(out, all[n/2])
	}
	return out
}

func spreadScalar(i, n int) int {
	rank := (bandSpan - 1) / 2
	if n != 1 {
		rank = int(math.Round(float64(i) * float64(bandSpan-1) / float64(n-1)))
	}
	c := bandLo + rank
	if c >= '%' {
		c++
	}
	return c
}

// isPartitionFanout: most folders of the level are key=value partitions.
func isPartitionFanout(cps []string) bool {
	if len(cps) == 0 {
		return false
	}
	n := 0
	for _, cp := range cps {
		seg := strings.TrimSuffix(cp, seedDelimiter)
		if i := strings.LastIndex(seg, seedDelimiter); i >= 0 {
			seg = seg[i+1:]
		}
		if strings.Contains(seg, "=") {
			n++
		}
	}
	return n*2 > len(cps)
}

// spanScore ranks a folder by the byte gap to the next cut (or the scope's
// ceiling): wider first.
func spanScore(cut string, cuts []string, upper string) int64 {
	next := upper
	if i, found := slices.BinarySearch(cuts, cut); found && i+1 < len(cuts) {
		next = cuts[i+1]
	} else if !found && i < len(cuts) {
		next = cuts[i]
	}
	m := min(len(cut), len(next))
	for i := range m {
		if dlt := int64(next[i]) - int64(cut[i]); dlt != 0 {
			return int64(m-i)<<8 + dlt
		}
	}
	return int64(len(next) - len(cut))
}

// spanFrontier orders queued folders by depth, then widest span first, then
// arrival (Java's priority queue leaves ties unordered).
type spanFrontier struct {
	q      []frontierEntry
	seq    int
	depths map[int]int
}

type frontierEntry struct {
	cut   string
	depth int
	score int64
	seq   int
}

func (f *spanFrontier) less(a, b frontierEntry) bool {
	if a.depth != b.depth {
		return a.depth < b.depth
	}
	if a.score != b.score {
		return a.score > b.score
	}
	return a.seq < b.seq
}

func (f *spanFrontier) offer(cut string, depth int, score int64) {
	if f.depths == nil {
		f.depths = map[int]int{}
	}
	e := frontierEntry{cut, depth, score, f.seq}
	f.seq++
	i, _ := slices.BinarySearchFunc(f.q, e, func(x, y frontierEntry) int {
		if f.less(x, y) {
			return -1
		}
		return 1
	})
	f.q = slices.Insert(f.q, i, e)
	f.depths[depth]++
}

func (f *spanFrontier) poll() frontierEntry {
	e := f.q[0]
	f.q = f.q[1:]
	if f.depths[e.depth]--; f.depths[e.depth] == 0 {
		delete(f.depths, e.depth)
	}
	return e
}

func (f *spanFrontier) peek(n int) []frontierEntry {
	return f.q[:min(max(n, 0), len(f.q))]
}

func (f *spanFrontier) empty() bool          { return len(f.q) == 0 }
func (f *spanFrontier) size() int            { return len(f.q) }
func (f *spanFrontier) multipleDepths() bool { return len(f.depths) > 1 }
