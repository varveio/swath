/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.StealMath;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntPredicate;

/**
 * The HYBRID seed descent (algorithms.md §8) as a {@link SeedPlanner}: the span-priority frontier,
 * probe-budget accounting, per-level classification (narrow / partition fan-out / flat-wide radix
 * banding / tiny-leaf explosion vs. heavy-cut), and cut-set assembly plus the mass-weighted subsample
 * to the target seed count — moved here verbatim from {@code SeedStep}'s {@code collectCutPoints}/
 * {@code shallow}, with every RPC, page decode, and node insertion left to the executor.
 *
 * <p>Deterministic and single-threaded: this planner (and every {@link SeedDescent} it begins) holds
 * no ambient clock or randomness — the whole descent is a pure function of the sequence of
 * {@link SeedProbeOutcome}s fed back to it, which is exactly what {@code HybridSeedPlannerDescentTest}
 * exercises with a canned sequence and zero I/O.
 */
public final class HybridSeedPlanner implements SeedPlanner {

    /** The folder delimiter for the structure probe — a store convention, not a tuned value. */
    public static final byte[] DELIMITER = {'/'};

    /** S3's first-page cap; the probe never pages past it at any level. */
    public static final int PROBE_PAGE = 1000;

    // ---- Mass-aware seed descent (§8; behind mass_aware_seed, default ON) ------
    /**
     * Second-level sample width {@code j} (§8): how many children of an ambiguous
     * truncated-with-prefixes cut are probed to disambiguate heavy-subtree from 1:1 explosion.
     * {@code j=1} can misclassify a skewed subtree; 2–3 costs a couple of probes and cuts that error.
     */
    static final int SAMPLE_WIDTH = 3;
    /**
     * The bounded second-level sample sub-budget (§8), carved OUT of {@code maxProbes} (never added
     * on top): {@code structure probes + sample probes <= maxProbes <= 256}, with the sample probes
     * themselves capped at this many so deep descent can never starve sampling and sampling can never
     * storm the wallet.
     */
    static final int SAMPLE_BUDGET = 32;
    /**
     * A sampled child is "dense" (evidence of a heavy subtree, not a 1:1 leaf) when it is itself
     * page-capped, has its own sub-directories, or holds at least this many direct objects — well
     * above the ~1 object of an {@code essential-web}-style 1:1 explosion (INT-8), well below a real
     * partition's mass. The load-bearing distinction is 1-object-per-leaf vs. many.
     */
    static final int SAMPLE_DENSE_MIN_OBJECTS = 8;
    /**
     * The floor {@link #sampleCutWeights} must clear before the over-cap reduction trusts its
     * estimate for {@link #massWeightedSubsample}: fewer sampled weights than this (including zero,
     * e.g. when the descent's own structure probing already left no room in the probe budget for this
     * pass to run) is too sparse to be a meaningful mass estimate, and every unsampled cut defaults to
     * weight 1 in {@link #massWeightedSubsample} regardless — below this floor the "weighted" walk
     * would really just be an unweighted one wearing the wrong label. Set at a quarter of
     * {@link #SAMPLE_BUDGET}: enough samples to say something about SEVERAL distinct regions of the
     * cut-index range (not just one lucky/unlucky one), while still tolerating a heavily-contended
     * sample sub-budget (e.g. shared with mid-descent heavy-cut sampling) that only had a partial
     * slice left to spend here.
     */
    static final int MIN_WEIGHT_SAMPLES = SAMPLE_BUDGET / 4;

    // ---- Per-depth yield quota (issue #15; mass-aware descent only) -----------
    /**
     * The recent-probe window a depth's yield is judged over, AND (since the window must be full
     * before a verdict is trusted) the grace period every depth gets before the quota can cut it off —
     * one constant serving both roles rather than two independently-tuned ones. Small on purpose: a
     * depth that is genuinely productive clears the break-even floor (below) easily inside 4 probes,
     * while a depth quietly grinding through dead leaves (the porotomo shape: 220 of 224 descent
     * decisions at fanout ≤ 4, one 238-wide sibling never reached) does not get to spend a large
     * multiple of the budget proving it before the frontier is allowed to look elsewhere.
     */
    static final int YIELD_WINDOW = 4;
    /**
     * A depth's last {@link #YIELD_WINDOW} probes must collectively have produced STRICTLY MORE than
     * this many new cuts to stay alive — cut off otherwise. Deliberately a floor on the WHOLE window's
     * total rather than a strict per-probe break-even (avg {@code >= 1} cut/probe): a single unlucky
     * dead-end probe sitting amid otherwise-productive siblings (e.g. a lone near-empty directory
     * ranked first purely because it has no successor yet, scoring the span-priority ceiling fallback)
     * must not tip a whole depth into cutoff on one bad roll — {@code
     * SeedFrontierLevelOrderTest}'s heavy cut sits behind exactly nine other depth-1 siblings whose
     * probes mix kept-0 and kept-1 outcomes, and depth ordering alone (no quota help needed) already
     * reaches it there since the whole level fits the descent's budget. The quota exists for the
     * OPPOSITE shape: a depth that is genuinely, sustained-ly unproductive across its whole recent
     * window (the porotomo shape: 220 of 224 descent decisions at fanout ≤ 4, one 238-wide sibling
     * never reached) — set at "at most one cut total across 4 probes", i.e. an average no better than
     * 1-in-4, clearly below what any single lucky/unlucky probe can produce or erase on its own.
     */
    static final int YIELD_WINDOW_MIN_CUTS = 2;
    /**
     * The depth threshold the cheap keyspace-classification signal buckets a cutoff into: at or below
     * this depth is "shallow" (the wide-shallow-level half of the issue's pathology — a top-level or
     * one-level-in fan-out that stops yielding), strictly deeper is "deep" (the narrow-chain half). A
     * fixed, tiny bucket count (as {@code PIVOT_BYTE}'s four buckets) keeps the counter's cardinality
     * bounded regardless of how deep a pathological descent goes.
     */
    static final int YIELD_QUOTA_SHALLOW_MAX_DEPTH = 2;

    /**
     * The smallest / largest SAFE ASCII code points a synthesized cut appends after a flat region's
     * prefix. The populated alphabet of a flat region (hex, base64, uuid, ISO timestamps, …) lives in
     * printable ASCII, so appending every printable-ASCII byte isolates each first-character bucket
     * into its own range — no single range keeps a disproportionate share. {@code 0x7F} (DEL) is
     * excluded (not a printable / XML-safe scalar).
     */
    private static final int APPEND_LO = 0x21;   // '!'
    private static final int APPEND_HI = 0x7E;   // '~'
    /**
     * {@code '%'} (0x25) is excluded from the appendable alphabet — see the historical note in
     * {@code SeedStep}'s javadoc predecessor: a synthesized cut becomes a {@code start-after}/
     * {@code prefix} the engine later sends on the wire, and the SDK's response-decoding interceptor
     * strict-decodes those fields, so a lone {@code '%'} there crashes the run.
     */
    private static final int UNSAFE_SCALAR = '%';
    /** Width of the single-byte printable-ASCII radix alphabet [APPEND_LO, APPEND_HI], minus {@code '%'}. */
    private static final int SPAN = APPEND_HI - APPEND_LO;
    /** The floor number of radix bands a dense flat region always gets (even at low worker counts). */
    private static final int MIN_BANDS = 8;

    /**
     * S3's own maximum object key length in bytes — the length {@link #KEYSPACE_CEILING} is filled to.
     */
    private static final int MAX_S3_KEY_LEN = 1024;

    /**
     * A purely-local sentinel for {@link #spanScore}'s scope-upper-bound fallback (see
     * {@link #scopeCeiling}) when the enclosing scope has no real ceiling to fall back to.
     */
    private static final byte[] KEYSPACE_CEILING = keyspaceCeiling();

    private static byte[] keyspaceCeiling() {
        byte[] b = new byte[MAX_S3_KEY_LEN];
        Arrays.fill(b, (byte) 0xFF);
        return b;
    }

    private final byte[] prefix;
    private final int workerCount;
    private final int targetSeeds;
    private final int maxProbes;
    private final EngineToggles toggles;
    private final byte[] topScopeCeiling;

    /**
     * @param prefix      the listing prefix {@code P} ({@code null}/empty = whole bucket)
     * @param workerCount the engine's target concurrency; the descent aims for roughly a small
     *                    multiple of this many ranges (enough parallelism, never over-fragmenting)
     * @param toggles     {@code toggles.radixBands() == false} skips the flat-wide radix-banding
     *                    synthesis entirely; {@code toggles.fanoutTiling() == false} skips the
     *                    partition-fan-out tiling classifier
     */
    public HybridSeedPlanner(byte[] prefix, int workerCount, EngineToggles toggles) {
        this.prefix = prefix;
        this.toggles = toggles;
        int w = Math.max(1, workerCount);
        this.workerCount = w;
        // ~4xW ranges: every worker has headroom to steal from, while the seed-range count
        // (and thus the per-range LIST overhead) stays bounded regardless of directory count.
        this.targetSeeds = Math.min(PROBE_PAGE, 4 * w);
        // The descent probe budget is bounded by the same target (one-time, ~O(seed) RPCs).
        this.maxProbes = Math.min(256, Math.max(1, this.targetSeeds));
        this.topScopeCeiling = scopeCeiling(this.prefix);
    }

    @Override
    public int probeBudget() {
        return maxProbes;
    }

    @Override
    public SeedDescent beginDescent() {
        return new Descent();
    }

    /**
     * One run's descent state machine. Explicit phases (mirroring, by hand, what a generator would
     * otherwise desugar to): {@code TOP} -> optional {@code TOP_EXTRA} (the bounded +1-page top-level
     * pagination) -> {@code DESCENT} (the frontier-poll loop), which for an ambiguous truncated level
     * detours through a bounded {@code SAMPLE_CHILD} sub-loop before resuming -> once the frontier is
     * exhausted or the budget runs out, an optional bounded {@code WEIGHT_SAMPLE} sub-loop -> the
     * terminal {@link SeedPlan}.
     */
    private final class Descent implements SeedDescent {

        private enum Phase { TOP, TOP_EXTRA, DESCENT, SAMPLE_CHILD, WEIGHT_SAMPLE }

        private final boolean massAware = toggles.massAwareSeed();
        private final TreeSet<byte[]> cuts = new TreeSet<>(Arrays::compareUnsigned);
        private final Frontier frontier = massAware ? new SpanPriorityFrontier() : new FifoFrontier();
        private final List<SeedLevelDecision> decisions = new ArrayList<>();
        private final List<Engagement> pending = new ArrayList<>();

        private int totalProbes;
        private int sampleProbes;

        private Phase phase;

        // -- top-level bookkeeping --
        private boolean topPageCapped;
        private int topObjectCount;
        private int topFanout;
        private int[] topCounts;
        private boolean extraFired;
        private int extraFanout;
        private int[] extraCounts;
        private boolean extraPageCapped;
        private byte[] flatWideRegion;

        // -- run-wide classification flags --
        private boolean tinyLeafExplosion;
        private boolean fanoutTiled;
        private boolean heavyCutBanded;
        private boolean massWeighted;
        private NavigableMap<byte[], Long> weights;

        // -- descent-loop bookkeeping --
        private int descentCeiling;
        private boolean frontierReorderedFired;
        private boolean frontierLevelOrderedFired;
        private int heavySamples;
        private int sampledLevels;

        // -- the truncated level currently being classified --
        private byte[] currentDir;
        private int currentSubFanout;
        private List<byte[]> currentSubCps;
        // -- currentDir's own depth + its quota state AT POLL TIME (issue #15), snapshotted once in
        // continueDescentLoop() so both onDescentProbe/finishClassification's decision record and the
        // frontier.recordProbeResult callback below use the SAME depth without recomputing it twice.
        private int currentDirDepth;
        private boolean currentDirWasCutOff;

        // -- sample-child sub-loop --
        private int sampleN;
        private int sampleJ;
        private int sampleIdx;
        private int sampleDense;
        private int sampleSampled;

        // -- weight-sample sub-loop --
        private List<byte[]> weightAll;
        private int weightBudget;
        private int weightIdx;
        private byte[] currentWeightCut;

        @Override
        public SeedAction start() {
            phase = Phase.TOP;
            totalProbes++;
            return new RequestSeedProbe(prefix, null, drain());
        }

        @Override
        public SeedAction onProbeResult(SeedProbeOutcome outcome) {
            return switch (phase) {
                case TOP -> onTop(outcome);
                case TOP_EXTRA -> onTopExtra(outcome);
                case DESCENT -> onDescentProbe(outcome);
                case SAMPLE_CHILD -> onSampleChild(outcome);
                case WEIGHT_SAMPLE -> onWeightSample(outcome);
            };
        }

        private void mark(String reason) {
            pending.add(new Engagement("SEED", reason));
        }

        private List<Engagement> drain() {
            if (pending.isEmpty()) {
                return List.of();
            }
            List<Engagement> d = List.copyOf(pending);
            pending.clear();
            return d;
        }

        private SeedAction issueProbe(byte[] probePrefix, byte[] startAfter) {
            return new RequestSeedProbe(probePrefix, startAfter, drain());
        }

        /** The scan prefix's own depth — the {@code decisions[]} depth for the TOP/TOP_EXTRA entries,
         *  which precede the depth-ordered descent frontier (and its per-depth yield quota) entirely. */
        private int topDepth() {
            return depthOf(prefix == null ? new byte[0] : prefix);
        }

        // ---- TOP / TOP_EXTRA ------------------------------------------------------------------

        private SeedAction onTop(SeedProbeOutcome outcome) {
            topPageCapped = outcome.pageCapped();
            topObjectCount = outcome.objectCount();
            topFanout = outcome.commonPrefixes().size();
            topCounts = addCutsCounted(outcome.commonPrefixes(), cuts, frontier, topScopeCeiling);
            if (massAware && topPageCapped && !cuts.isEmpty() && totalProbes < maxProbes
                    && outcome.lastKey() != null) {
                phase = Phase.TOP_EXTRA;
                totalProbes++;
                return issueProbe(prefix, outcome.lastKey());
            }
            return afterTop();
        }

        private SeedAction onTopExtra(SeedProbeOutcome outcome) {
            mark("top_probe_paginated");
            extraFired = true;
            extraFanout = outcome.commonPrefixes().size();
            extraPageCapped = outcome.pageCapped();
            extraCounts = addCutsCounted(outcome.commonPrefixes(), cuts, frontier, topScopeCeiling);
            return afterTop();
        }

        private SeedAction afterTop() {
            if (cuts.isEmpty()) {
                // Flat top: no delimiter=/ structure. Either a small flat bucket (not page-capped) ->
                // one (⊥, null] range same as NONE, or a single dense flat directory at the root
                // (page-capped, direct objects, no commonPrefixes) -> flatWideRegion for finalize() to
                // pre-cut into radix bands.
                flatWideRegion = (topPageCapped && topObjectCount > 0)
                        ? (prefix == null ? new byte[0] : prefix) : null;
                // Record the NORMALIZED region as this level's prefix when there is one (issue #33):
                // finalizeDecisions() matches the banded level by Arrays.equals(d.prefix(),
                // flatWideRegion), and a whole-bucket scan arrives here with prefix == null while the
                // region above is normalized to new byte[0]. Storing the raw null made
                // Arrays.equals(null, new byte[0]) false, so a dense flat ROOT kept the flat_wide label
                // and never took the dense_root_radix_banded rewrite its run-level mark already fired.
                // Every descent level already stores the same currentDir it puts in flatWideRegion, so
                // this makes the root consistent with them rather than special-casing it.
                decisions.add(new SeedLevelDecision(flatWideRegion != null ? flatWideRegion : prefix,
                        topFanout, topPageCapped,
                        flatWideRegion != null ? "flat_wide" : "narrow", topCounts[0], topCounts[1],
                        topDepth(), false));
                return finalizePlan();
            }
            // A page-capped top WITH common prefixes fans out into >1000 top-level directories -- a
            // tiny-leaf explosion left whole (the descent below is skipped).
            if (topPageCapped) {
                tinyLeafExplosion = true;
            }
            decisions.add(new SeedLevelDecision(prefix, topFanout, topPageCapped,
                    topPageCapped ? "tiny_leaf_explosion" : "narrow", topCounts[0], topCounts[1],
                    topDepth(), false));
            // The top-page pagination probe (mass_aware_seed=on only) is a second, distinct LIST
            // against the same top-level prefix -- its own decisions[] entry, classified
            // "top_probe_paginated", never folded into the top-level entry's counts.
            if (extraFired) {
                decisions.add(new SeedLevelDecision(prefix, extraFanout, extraPageCapped,
                        "top_probe_paginated", extraCounts[0], extraCounts[1], topDepth(), false));
            }
            if (!topPageCapped) {
                return enterDescent();
            }
            return finalizePlan();
        }

        // ---- DESCENT ---------------------------------------------------------------------------

        private SeedAction enterDescent() {
            phase = Phase.DESCENT;
            descentCeiling = massAware ? maxProbes - Math.min(SAMPLE_BUDGET, maxProbes / 2) : maxProbes;
            frontierReorderedFired = false;
            frontierLevelOrderedFired = false;
            heavySamples = 0;
            sampledLevels = 0;
            return continueDescentLoop();
        }

        private SeedAction continueDescentLoop() {
            if (frontier.isEmpty() || totalProbes >= descentCeiling) {
                return afterDescentLoop();
            }
            if (massAware && !frontierReorderedFired && frontier.size() > 1) {
                mark("frontier_reordered");
                frontierReorderedFired = true;
            }
            if (massAware && !frontierLevelOrderedFired && frontier.spansMultipleDepths()) {
                mark("frontier_level_ordered");
                frontierLevelOrderedFired = true;
            }
            currentDir = frontier.poll();
            currentDirDepth = depthOf(currentDir);
            currentDirWasCutOff = frontier.isCutOff(currentDirDepth);
            totalProbes++;
            return issueProbe(currentDir, null);
        }

        private SeedAction onDescentProbe(SeedProbeOutcome outcome) {
            currentSubFanout = outcome.commonPrefixes().size();
            if (outcome.pageCapped()) {
                currentSubCps = sortedCommonPrefixes(outcome.commonPrefixes());
                return classifyTruncatedLevel(outcome);
            }
            int[] counts = addCutsCounted(outcome.commonPrefixes(), cuts, frontier, scopeCeiling(currentDir));
            recordYield(counts[0]);
            decisions.add(new SeedLevelDecision(currentDir, currentSubFanout, false, "narrow",
                    counts[0], counts[1], currentDirDepth, currentDirWasCutOff));
            return continueDescentLoop();
        }

        /**
         * Rolls {@code currentDir}'s own kept-cuts count into its depth's yield-quota window (issue
         * #15) and, the first time this crosses that depth into cut-off, records the engagement
         * counter plus its cheap shallow/deep keyspace-classification bucket. A no-op on
         * {@link FifoFrontier} ({@code mass_aware_seed=off}), which has no notion of depth.
         */
        private void recordYield(int kept) {
            if (frontier.recordProbeResult(currentDirDepth, kept)) {
                mark("yield_quota_cutoff");
                mark(currentDirDepth <= YIELD_QUOTA_SHALLOW_MAX_DEPTH
                        ? "yield_quota_cutoff_shallow" : "yield_quota_cutoff_deep");
            }
        }

        private SeedAction classifyTruncatedLevel(SeedProbeOutcome outcome) {
            if (isFlatWide(outcome)) {
                if (flatWideRegion == null) {
                    flatWideRegion = currentDir;
                }
                return finishClassification("flat_wide", 0, 0);
            }
            if (toggles.fanoutTiling() && isPartitionFanout(currentSubCps)) {
                int[] counts = tilePartitionFanout(currentSubCps, cuts);
                fanoutTiled = true;
                if (massAware && sampleProbes < SAMPLE_BUDGET && totalProbes < maxProbes) {
                    mark("banding_deferred_to_fanout");
                }
                return finishClassification("fanout_tiled", counts[0], counts[1]);
            }
            // Ambiguous: a heavy deep subtree (band it) vs. a 1:1 tiny-leaf explosion (leave whole).
            if (massAware && sampleProbes < SAMPLE_BUDGET && totalProbes < maxProbes) {
                return beginSampleChild();
            }
            boolean heavy = false;
            if (massAware && sampledLevels > 0) {
                // The sample budget is spent, but this level is just as ambiguous as the ones that
                // got sampled: carry the empirical prior from the siblings already sampled this
                // descent instead of defaulting to "left whole" on zero evidence.
                heavy = heavySamples * 2 >= sampledLevels;
                mark("heavy_prior_applied");
                mark(heavy ? "heavy_prior_banded" : "heavy_prior_left_whole");
            }
            return finishAmbiguousClassification(heavy);
        }

        private SeedAction finishClassification(String classification, int kept, int discarded) {
            recordYield(kept);
            decisions.add(new SeedLevelDecision(currentDir, currentSubFanout, true, classification,
                    kept, discarded, currentDirDepth, currentDirWasCutOff));
            if (!frontier.isEmpty() && totalProbes < maxProbes) {
                mark("frontier_continued_past_explosion");
            }
            phase = Phase.DESCENT;
            return continueDescentLoop();
        }

        private SeedAction afterDescentLoop() {
            if (massAware && !topPageCapped && cuts.size() > targetSeeds && sampleProbes < SAMPLE_BUDGET) {
                return beginWeightSample();
            }
            return finalizePlan();
        }

        // ---- SAMPLE_CHILD (nested inside a DESCENT iteration) ----------------------------------

        private SeedAction beginSampleChild() {
            int n = currentSubCps.size();
            if (n == 0) {
                sampledLevels++;
                mark("heavy_cut_descended");
                mark("explosion_confirmed");
                return finishAmbiguousClassification(false);
            }
            sampleN = n;
            sampleJ = Math.min(SAMPLE_WIDTH, n);
            sampleIdx = 0;
            sampleDense = 0;
            sampleSampled = 0;
            return issueNextSample();
        }

        private SeedAction issueNextSample() {
            if (sampleIdx >= sampleJ || sampleProbes >= SAMPLE_BUDGET || totalProbes >= maxProbes) {
                return finishSampleChild();
            }
            int idx = (sampleJ == 1) ? sampleN / 2 : (int) ((long) sampleIdx * (sampleN - 1) / (sampleJ - 1));
            byte[] child = currentSubCps.get(idx);
            totalProbes++;
            sampleProbes++;
            sampleSampled++;
            sampleIdx++;
            phase = Phase.SAMPLE_CHILD;
            return issueProbe(child, null);
        }

        private SeedAction onSampleChild(SeedProbeOutcome outcome) {
            boolean dense = outcome.pageCapped() || !outcome.commonPrefixes().isEmpty()
                    || outcome.objectCount() >= SAMPLE_DENSE_MIN_OBJECTS;
            if (dense) {
                sampleDense++;
            }
            return issueNextSample();
        }

        private SeedAction finishSampleChild() {
            boolean heavy = sampleSampled > 0 && sampleDense * 2 >= sampleSampled;
            sampledLevels++;
            if (heavy) {
                heavySamples++;
            }
            mark("heavy_cut_descended");
            if (!heavy) {
                mark("explosion_confirmed");
            }
            phase = Phase.DESCENT;
            return finishAmbiguousClassification(heavy);
        }

        private SeedAction finishAmbiguousClassification(boolean heavy) {
            if (heavy) {
                int[] counts = bandHeavyCut(currentSubCps, cuts);
                heavyCutBanded = true;
                mark("heavy_cut_banded");
                return finishClassification("heavy_cut_banded", counts[0], counts[1]);
            }
            tinyLeafExplosion = true;
            return finishClassification("tiny_leaf_explosion", 0, 0);
        }

        // ---- WEIGHT_SAMPLE (post-descent, bounded) ---------------------------------------------

        private SeedAction beginWeightSample() {
            weightAll = new ArrayList<>(cuts);
            int n = weightAll.size();
            weightBudget = Math.min(SAMPLE_BUDGET - sampleProbes, n);
            weightIdx = 0;
            weights = new TreeMap<>(Arrays::compareUnsigned);
            phase = Phase.WEIGHT_SAMPLE;
            return issueNextWeightSample();
        }

        private SeedAction issueNextWeightSample() {
            if (weightIdx >= weightBudget || sampleProbes >= SAMPLE_BUDGET || totalProbes >= maxProbes) {
                return finishWeightSample();
            }
            int n = weightAll.size();
            int idx = (weightBudget == 1) ? n / 2 : (int) ((long) weightIdx * (n - 1) / (weightBudget - 1));
            currentWeightCut = weightAll.get(idx);
            totalProbes++;
            sampleProbes++;
            weightIdx++;
            return issueProbe(currentWeightCut, null);
        }

        private SeedAction onWeightSample(SeedProbeOutcome outcome) {
            long weight = outcome.pageCapped()
                    ? PROBE_PAGE
                    : (long) outcome.commonPrefixes().size() + outcome.objectCount();
            weights.put(currentWeightCut, Math.max(1L, weight));
            return issueNextWeightSample();
        }

        private SeedAction finishWeightSample() {
            if (weights.size() >= MIN_WEIGHT_SAMPLES) {
                massWeighted = true;
            } else {
                weights = null;
            }
            return finalizePlan();
        }

        // ---- Finalize ---------------------------------------------------------------------------

        private SeedAction finalizePlan() {
            if (cuts.isEmpty() && flatWideRegion == null) {
                mark("flat_trivial");
                return new SeedPlan(List.of(), 0, 0, totalProbes, List.copyOf(decisions), drain());
            }

            int cutsDiscovered = cuts.size();
            TreeSet<byte[]> cutSet = new TreeSet<>(Arrays::compareUnsigned);
            if (cuts.size() > targetSeeds) {
                mark("descent_cuts_subsampled");
            }
            if (massWeighted && weights != null) {
                cutSet.addAll(massWeightedSubsample(cuts, targetSeeds, weights));
                mark("mass_weighted_subsample");
            } else {
                cutSet.addAll(subsampleEvenly(cuts, targetSeeds));
            }

            int synthesized = toggles.radixBands() ? subdivideFlatWideRegion(cutSet, flatWideRegion) : 0;
            if (!toggles.radixBands() && flatWideRegion != null) {
                mark("radix_bands_toggle_disabled");
            }

            List<byte[]> finalCuts = new ArrayList<>(cutSet);

            if (synthesized > 0) {
                mark("dense_root_radix_banded");
            }
            if (tinyLeafExplosion) {
                mark("tiny_leaf_explosion");
            }
            if (fanoutTiled) {
                mark("fanout_tiled");
            }
            boolean genericDelimiterSeeded =
                    synthesized == 0 && !tinyLeafExplosion && !fanoutTiled && !heavyCutBanded;
            if (genericDelimiterSeeded) {
                mark("delimiter_seeded");
            }
            mark(topPageCapped ? "top_truncated" : "top_complete");

            List<SeedLevelDecision> finalDecisions =
                    finalizeDecisions(decisions, flatWideRegion, synthesized > 0, genericDelimiterSeeded);

            return new SeedPlan(finalCuts, synthesized, cutsDiscovered, totalProbes, finalDecisions, drain());
        }
    }

    /**
     * Rewrites the classification for the (at most one) level whose flat-wide region was actually
     * radix-banded, and for the top level when the run's overall shape is the generic
     * delimiter-tiled case — both are known only once the whole descent's cut assembly is finished.
     */
    private static List<SeedLevelDecision> finalizeDecisions(List<SeedLevelDecision> decisions,
            byte[] flatWideRegion, boolean banded, boolean genericDelimiterSeeded) {
        List<SeedLevelDecision> out = new ArrayList<>(decisions.size());
        for (int i = 0; i < decisions.size(); i++) {
            SeedLevelDecision d = decisions.get(i);
            String classification = d.classification();
            if (banded && Arrays.equals(d.prefix(), flatWideRegion)) {
                classification = "dense_root_radix_banded";
            } else if (i == 0 && genericDelimiterSeeded && "narrow".equals(classification)) {
                classification = "delimiter_seeded";
            }
            out.add(new SeedLevelDecision(d.prefix(), d.fanout(), d.truncated(), classification,
                    d.cutsKept(), d.cutsDiscarded(), d.depth(), d.quotaCutOff()));
        }
        return out;
    }

    /** A page-capped level that is flat (direct objects, no sub-directory rollups) — a wide flat region. */
    private static boolean isFlatWide(SeedProbeOutcome sub) {
        return sub.commonPrefixes().isEmpty() && sub.objectCount() > 0;
    }

    /**
     * Is this page-capped-with-common-prefixes level a Hive/Spark {@code key=value/} PARTITION
     * fan-out (tile it), or a plain {@code <hex>/} directory explosion (leave it whole)? A level is a
     * partition fan-out iff a MAJORITY of its common prefixes' final path segments contain {@code '='}.
     */
    private static boolean isPartitionFanout(List<byte[]> sortedCommonPrefixes) {
        if (sortedCommonPrefixes.isEmpty()) {
            return false;
        }
        int partitionLike = 0;
        for (byte[] cp : sortedCommonPrefixes) {
            if (lastSegmentHasEquals(cp)) {
                partitionLike++;
            }
        }
        return partitionLike * 2 > sortedCommonPrefixes.size();
    }

    /**
     * Does the final path segment of a common prefix {@code cp} (which ends in the {@link #DELIMITER}
     * byte) contain a {@code '='}? i.e. is it a {@code key=value/} partition directory.
     */
    private static boolean lastSegmentHasEquals(byte[] cp) {
        int end = cp.length;
        if (end > 0 && cp[end - 1] == DELIMITER[0]) {
            end--;
        }
        int start = end;
        while (start > 0 && cp[start - 1] != DELIMITER[0]) {
            start--;
        }
        for (int i = start; i < end; i++) {
            if (cp[i] == '=') {
                return true;
            }
        }
        return false;
    }

    /** The store may not return common prefixes sorted — sort defensively. */
    private static List<byte[]> sortedCommonPrefixes(List<byte[]> raw) {
        List<byte[]> cps = new ArrayList<>(raw);
        cps.sort(Arrays::compareUnsigned);
        return cps;
    }

    /**
     * Adds this page's common prefixes to the global cut set and the descent frontier, returning
     * {@code {kept, discarded}}. Adds every new cut to {@code cuts} FIRST, then offers each one to
     * {@code frontier} — so {@link SpanPriorityFrontier#offer} scores every cut in this page against
     * the OTHER cuts from the SAME page (its immediate siblings), not just cuts from earlier pages.
     */
    private static int[] addCutsCounted(List<byte[]> raw, TreeSet<byte[]> cuts, Frontier frontier,
            byte[] scopeUpper) {
        List<byte[]> added = new ArrayList<>();
        int kept = 0;
        int discarded = 0;
        for (byte[] b : sortedCommonPrefixes(raw)) {
            if (cuts.add(b)) {
                added.add(b);
                kept++;
            } else {
                discarded++;
            }
        }
        for (byte[] b : added) {
            frontier.offer(b, cuts, scopeUpper);
        }
        return new int[] {kept, discarded};
    }

    /**
     * Add up to {@code W} (worker count) evenly-subsampled cut-points from a partition fan-out's
     * ALREADY-PROBED common prefixes into the global cut set. The tiled prefixes are NOT added to the
     * frontier — the region is tiled here and descended no further (zero extra probes). Returns
     * {@code {kept, discarded}}.
     */
    private int[] tilePartitionFanout(List<byte[]> sortedCps, TreeSet<byte[]> cuts) {
        TreeSet<byte[]> observed = new TreeSet<>(Arrays::compareUnsigned);
        observed.addAll(sortedCps);
        int kept = 0;
        int discarded = 0;
        for (byte[] cut : subsampleEvenly(observed, workerCount)) {
            if (cuts.add(cut)) {
                kept++;
            } else {
                discarded++;
            }
        }
        return new int[] {kept, discarded};
    }

    /**
     * Band a confirmed-heavy cut: tile it along the child prefixes already in its probed page — an
     * evenly-subsampled subset capped at {@code workerCount}. Zero extra probes (the page is already
     * in hand). Returns {@code {kept, discarded}}.
     */
    private int[] bandHeavyCut(List<byte[]> sortedCps, TreeSet<byte[]> cuts) {
        TreeSet<byte[]> observed = new TreeSet<>(Arrays::compareUnsigned);
        observed.addAll(sortedCps);
        int budget = Math.min(observed.size(), workerCount);
        int kept = 0;
        int discarded = 0;
        for (byte[] cut : subsampleEvenly(observed, budget)) {
            if (cuts.add(cut)) {
                kept++;
            } else {
                discarded++;
            }
        }
        return new int[] {kept, discarded};
    }

    /**
     * Pre-cut the recorded <b>heavy dense range</b> into alphabet-uniform leading-byte RADIX bands —
     * an even spread over the printable-ASCII leading alphabet, since no keys are observed yet at
     * seed time. Returns the number of cut-points added (0 when {@code dir} is {@code null}).
     */
    private int subdivideFlatWideRegion(TreeSet<byte[]> cutSet, byte[] dir) {
        if (dir == null) {
            return 0;
        }
        int perRegion = Math.min(SPAN, Math.max(MIN_BANDS, targetSeeds));
        return appendSpread(cutSet, dir, perRegion);
    }

    /**
     * Add up to {@code allow} leading-byte radix cut-points inside {@code (dir, prefixCeil(dir)]},
     * spread uniformly over the SAFE printable-ASCII alphabet.
     */
    private static int appendSpread(TreeSet<byte[]> cutSet, byte[] dir, int allow) {
        int n = Math.min(allow, SPAN);
        if (n <= 0) {
            return 0;
        }
        int added = 0;
        for (int i = 0; i < n; i++) {
            if (cutSet.add(withScalar(dir, spreadScalar(i, n)))) {
                added++;
            }
        }
        return added;
    }

    /** The {@code i}-th of {@code n} leading scalars spread evenly over the safe alphabet. */
    private static int spreadScalar(int i, int n) {
        int rank = (n == 1)
                ? (SPAN - 1) / 2
                : (int) Math.round((double) i * (SPAN - 1) / (n - 1));
        return safeScalar(rank);
    }

    /** Map a 0-based rank in {@code [0, SPAN)} to its ASCII scalar, skipping {@code UNSAFE_SCALAR}. */
    private static int safeScalar(int rank) {
        int candidate = APPEND_LO + rank;
        return candidate >= UNSAFE_SCALAR ? candidate + 1 : candidate;
    }

    /** {@code dir} with a single-byte, XML-safe printable-ASCII scalar {@code 0x21..0x7E} appended. */
    private static byte[] withScalar(byte[] dir, int scalar) {
        byte[] out = Arrays.copyOf(dir, dir.length + 1);
        out[dir.length] = (byte) scalar;
        return out;
    }

    /** Evenly pick at most {@code max} cut-points from the sorted set (a subset still tiles exactly). */
    private static List<byte[]> subsampleEvenly(TreeSet<byte[]> cuts, int max) {
        List<byte[]> all = new ArrayList<>(cuts);
        int n = all.size();
        if (n <= max) {
            return all;
        }
        if (max <= 1) {
            return List.of(all.get(n / 2));
        }
        List<byte[]> picked = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            int idx = (int) ((long) i * (n - 1) / (max - 1));
            picked.add(all.get(idx));
        }
        return picked;
    }

    /**
     * Weight-proportional subsample: allocate the {@code max}-cut budget so a heavy (high-weight)
     * region keeps proportionally more interior cuts than an empty one — mass-weighting is a
     * PREFERENCE among candidates, never a mechanism that can starve the budget itself (issue #83). A
     * cut absent from {@code weights} counts as weight 1.
     *
     * <p><b>Two passes, in order:</b>
     * <ol>
     *   <li><b>Certainty selection</b> ({@link #selectCertain}): a candidate whose weight ALREADY
     *       claims its own full proportional share (or more) of what remains is picked with certainty
     *       and removed from the running total/budget, heaviest first. This is the fix for issue #83's
     *       collapse: the naive single threshold-walk below anchors its next threshold to the weight
     *       actually accumulated at each pick ({@code next = acc + step}, the existing fix for the
     *       CONVERSE {@code HybridSeedPlannerMassWeightedSubsampleTest} landslide, where a heavy cut's
     *       credit must not spill onto its sorted neighbors as an unbroken run) — but a SET can only
     *       pick one heavy candidate ONCE, so any credit beyond a single step that a heavy candidate
     *       banks is credit the walk can never cash in elsewhere; on a shape with several such
     *       candidates (issue #83's porotomo repro: a handful of page-capped samples among mostly
     *       unsampled weight-1 cuts) that uncashable credit compounds until the walk collapses to a
     *       small fraction of {@code max}. Certainty selection removes exactly the candidates that
     *       would trigger this before the walk ever sees them, so what the walk sees afterward can
     *       never overshoot by more than one step's worth again.
     *   <li><b>Systematic walk over the residual</b> ({@link #systematicWalk}): once every remaining
     *       candidate's weight is bounded by the (now-fixed) residual share, the threshold advances by
     *       a plain {@code step} each pick (not re-anchored to {@code acc}) — safe here (no candidate
     *       left can overshoot by more than one step, so no landslide risk), and it lands within one
     *       pick of the exact residual budget instead of drifting low the way anchoring to {@code acc}
     *       does at fine (near-1) granularity.
     * </ol>
     * Fully deterministic (ties break on original sort-order index, never on iteration/hash order) and
     * O(n log n) (one sort, two linear passes) — this subsample runs once per seed, not per probe.
     */
    static List<byte[]> massWeightedSubsample(TreeSet<byte[]> cuts, int max, NavigableMap<byte[], Long> weights) {
        List<byte[]> all = new ArrayList<>(cuts);
        int n = all.size();
        if (n <= max) {
            return all;
        }
        long[] w = new long[n];
        long total = 0;
        for (int i = 0; i < n; i++) {
            Long ww = weights.get(all.get(i));
            w[i] = (ww == null) ? 1L : ww;
            total += w[i];
        }

        boolean[] selected = new boolean[n];
        long[] residual = new long[1];   // {remainingTotal} — selectCertain's other out-param is the return value
        int remainingBudget = selectCertain(w, total, max, selected, residual);
        if (remainingBudget > 0 && residual[0] > 0) {
            systematicWalk(w, selected, residual[0], remainingBudget);
        }

        List<byte[]> picked = new ArrayList<>(max);
        for (int i = 0; i < n; i++) {
            if (selected[i]) {
                picked.add(all.get(i));
            }
        }
        if (picked.isEmpty()) {
            picked.add(all.get(n / 2));
        }
        return picked;
    }

    /**
     * Marks every candidate that already claims its full share of the remaining budget as
     * {@code selected[i] = true} (heaviest-first; stops at the first, and therefore every remaining,
     * candidate that does NOT), returning the leftover budget and (via {@code residualTotalOut[0]})
     * the leftover weight for {@link #systematicWalk} to spend on the rest.
     */
    private static int selectCertain(long[] w, long total, int max, boolean[] selected, long[] residualTotalOut) {
        int n = w.length;
        Integer[] byWeightDesc = new Integer[n];
        for (int i = 0; i < n; i++) {
            byWeightDesc[i] = i;
        }
        Arrays.sort(byWeightDesc, Comparator.<Integer>comparingLong(i -> w[i]).reversed());
        long remainingTotal = total;
        int remainingBudget = max;
        for (int idx : byWeightDesc) {
            if (remainingBudget <= 0) {
                break;
            }
            double share = (double) remainingTotal / remainingBudget;
            if (w[idx] < share - 1e-9) {
                break;   // sorted heaviest-first: no lighter candidate can clear the share either
            }
            selected[idx] = true;
            remainingTotal -= w[idx];
            remainingBudget--;
        }
        residualTotalOut[0] = remainingTotal;
        return remainingBudget;
    }

    /**
     * Spends {@code remainingBudget} picks over the NOT-{@code selected} candidates in their original
     * (sorted-cut) order, proportional to weight. Safe to advance the threshold by a plain fixed
     * {@code step} per pick (never re-anchored to the running sum) precisely because {@link
     * #selectCertain} has already removed every candidate that could otherwise overshoot by more than
     * one step.
     */
    private static void systematicWalk(long[] w, boolean[] selected, long remainingTotal, int remainingBudget) {
        double step = (double) remainingTotal / remainingBudget;
        double next = step;
        double acc = 0;
        int picked = 0;
        for (int i = 0; i < w.length && picked < remainingBudget; i++) {
            if (selected[i]) {
                continue;
            }
            acc += w[i];
            if (acc + 1e-9 >= next) {
                selected[i] = true;
                picked++;
                next += step;
            }
        }
    }

    /**
     * The upper bound of the scope a cut was discovered in — {@link #spanScore}'s fallback measure
     * for a cut with no successor in the global cut set yet.
     */
    private static byte[] scopeCeiling(byte[] scopePrefix) {
        byte[] ceil = StealMath.prefixCeil(scopePrefix);
        return ceil != null ? ceil : KEYSPACE_CEILING;
    }

    /**
     * A monotone score for the keyspace gap from {@code cut} to its next sibling cut: an EARLIER
     * first differing byte and a LARGER delta both mean a wider span. A cut with no successor yet
     * falls back to {@code scopeUpper} (see {@link #scopeCeiling}).
     */
    private static long spanScore(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper) {
        byte[] next = cuts.higher(cut);
        if (next == null) {
            next = scopeUpper;
        }
        int min = Math.min(cut.length, next.length);
        for (int i = 0; i < min; i++) {
            int d = (next[i] & 0xFF) - (cut[i] & 0xFF);
            if (d != 0) {
                return ((long) (min - i) << 8) + d;
            }
        }
        return next.length - cut.length;
    }

    /** Depth = the number of {@link #DELIMITER} bytes in {@code cut} (the top-level scan prefix, or
     *  {@code new byte[0]}, is depth 0; each descended level adds one). Shared by the frontier
     *  (level-order poll key) and the per-depth yield quota (issue #15) — both key on the identical
     *  notion of depth, so this lives once at the planner level rather than duplicated per caller. */
    private static int depthOf(byte[] cut) {
        int d = 0;
        for (byte b : cut) {
            if (b == DELIMITER[0]) {
                d++;
            }
        }
        return d;
    }

    /**
     * The descent frontier of not-yet-probed directory prefixes: {@link FifoFrontier} (mass-aware
     * OFF) or {@link SpanPriorityFrontier} (mass-aware ON, best-first by {@link #spanScore}).
     */
    private interface Frontier {
        boolean isEmpty();

        int size();

        byte[] poll();

        /** Offer a newly-discovered cut into the frontier; {@code cuts} is the FULL global cut set at
         *  the time of the offer, and {@code scopeUpper} is the upper bound of the scope {@code cut}
         *  was discovered in. */
        void offer(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper);

        /** Whether cuts at more than one depth are currently queued. */
        default boolean spansMultipleDepths() {
            return false;
        }

        /** Whether {@code depth}'s per-depth yield quota (issue #15) has cut it off. Always
         *  {@code false} on a frontier with no notion of depth. */
        default boolean isCutOff(int depth) {
            return false;
        }

        /**
         * Roll one just-probed level's outcome into {@code depth}'s yield-quota window; returns
         * {@code true} the first time this call crosses that depth into cut-off (a one-shot signal
         * for the engagement counter). A no-op returning {@code false} on a frontier with no notion
         * of depth.
         */
        default boolean recordProbeResult(int depth, int cutsKept) {
            return false;
        }
    }

    /** The mass-aware OFF frontier: plain FIFO. */
    private static final class FifoFrontier implements Frontier {
        private final Deque<byte[]> queue = new ArrayDeque<>();

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public byte[] poll() {
            return queue.poll();
        }

        @Override
        public void offer(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper) {
            queue.addLast(cut);
        }
    }

    /**
     * The mass-aware ON frontier (§3.1): a genuine best-first order maintained across insertions, not
     * a one-shot pre-pass. Level-ordered first (depth primary), span-ordered within a level, and
     * (issue #15) a per-depth yield quota that reorders WITHIN that level-first bound rather than
     * replacing it: a depth whose own recent probes stop paying for themselves ({@link
     * #recordProbeResult}) is skipped by {@link #poll}'s first pass in favor of any other depth that
     * still has queued work, however much deeper — but never abandoned outright. If every depth with
     * queued entries is cut off (nothing better anywhere), {@link #poll}'s fallback pass resumes
     * strict shallowest-first exactly as before the quota existed, so a depth can be deferred but never
     * starved: this is the "strict shallow-first order is preserved as the starvation bound" the issue
     * calls for, and it is why a bottomless narrow chain (one queued entry per depth, never enough
     * probes at any single depth to fill the quota's own judging window) never trips the quota at all
     * — {@link SeedDescentRightmostChainDoesNotStarveWideNonLastSiblingTest}-shaped fixtures see
     * byte-identical poll order to the pre-quota frontier.
     */
    private static final class SpanPriorityFrontier implements Frontier {
        private record Entry(byte[] cut, long score) {
        }

        /** Depth -> its still-queued entries, best-span-first; ascending key iteration order is
         *  exactly the level-order poll's shallowest-first scan. A depth's bucket is removed the
         *  instant it drains, so {@link #spansMultipleDepths}/{@link #poll} never see a hollow entry. */
        private final NavigableMap<Integer, PriorityQueue<Entry>> byDepth = new TreeMap<>();
        private int size;

        /** Depth -> its yield-quota window state (issue #15). */
        private final Map<Integer, DepthYield> yields = new HashMap<>();

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public byte[] poll() {
            byte[] cut = pollWhere(depth -> !isCutOff(depth));
            return cut != null ? cut : pollWhere(depth -> true);   // fallback: the starvation bound
        }

        /** Shallowest depth whose bucket is non-empty AND satisfies {@code eligible}; {@code null} if
         *  none does. */
        private byte[] pollWhere(IntPredicate eligible) {
            for (Iterator<Map.Entry<Integer, PriorityQueue<Entry>>> it = byDepth.entrySet().iterator();
                    it.hasNext();) {
                Map.Entry<Integer, PriorityQueue<Entry>> e = it.next();
                if (!eligible.test(e.getKey())) {
                    continue;
                }
                PriorityQueue<Entry> q = e.getValue();
                Entry head = q.poll();
                size--;
                if (q.isEmpty()) {
                    it.remove();
                }
                return head.cut();
            }
            return null;
        }

        @Override
        public void offer(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper) {
            int depth = depthOf(cut);
            byDepth.computeIfAbsent(depth, d -> new PriorityQueue<>(Comparator.comparingLong(Entry::score).reversed()))
                    .add(new Entry(cut, spanScore(cut, cuts, scopeUpper)));
            size++;
        }

        @Override
        public boolean spansMultipleDepths() {
            return byDepth.size() > 1;
        }

        @Override
        public boolean isCutOff(int depth) {
            DepthYield y = yields.get(depth);
            return y != null && y.cutOff;
        }

        @Override
        public boolean recordProbeResult(int depth, int cutsKept) {
            return yields.computeIfAbsent(depth, d -> new DepthYield()).record(cutsKept);
        }

        /**
         * One depth's yield-quota window (issue #15): the last {@link #YIELD_WINDOW} probes issued
         * against THIS depth, judged once the window fills. O(1) per probe (a fixed-size ring buffer
         * plus a running sum) and fully deterministic — no clock, no randomness.
         */
        private static final class DepthYield {
            private final int[] window = new int[YIELD_WINDOW];
            private int next;
            private int filled;
            private int sum;
            private boolean cutOff;

            /**
             * Rolls {@code cutsKept} into the window; returns {@code true} the FIRST time the window
             * fills with a total at or below {@link #YIELD_WINDOW_MIN_CUTS} — a one-way gate (once a
             * depth stops paying for its own probes it never earns priority back over a shallower or
             * still-productive depth), though {@link SpanPriorityFrontier#poll}'s fallback pass still
             * drains it once nothing else remains.
             */
            boolean record(int cutsKept) {
                if (cutOff) {
                    return false;
                }
                sum += cutsKept - window[next];
                window[next] = cutsKept;
                next = (next + 1) % window.length;
                if (filled < window.length) {
                    filled++;
                }
                if (filled == window.length && sum <= YIELD_WINDOW_MIN_CUTS) {
                    cutOff = true;
                    return true;
                }
                return false;
            }
        }
    }
}
