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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;

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
        /** Greatest sort key the TOP scope returned (direct objects and common prefixes combined). */
        private byte[] topGreatestItem;
        /** Greatest {@code CommonPrefix} the TOP scope returned, or {@code null} if it returned none. */
        private byte[] topGreatestCommonPrefix;
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

        // ---- TOP / TOP_EXTRA ------------------------------------------------------------------

        private SeedAction onTop(SeedProbeOutcome outcome) {
            topPageCapped = outcome.pageCapped();
            topObjectCount = outcome.objectCount();
            topFanout = outcome.commonPrefixes().size();
            observeTopScopeExtent(outcome);
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
            observeTopScopeExtent(outcome);
            extraCounts = addCutsCounted(outcome.commonPrefixes(), cuts, frontier, topScopeCeiling);
            return afterTop();
        }

        /**
         * Track the greatest item the TOP scope returned, and whether that item is a {@code
         * CommonPrefix} — the two facts {@link #appendOpenTileSentinel} needs. Called for the top probe
         * and for the {@code TOP_EXTRA} continuation, so a paginated top is accounted across both
         * pages.
         *
         * <p>{@code lastKey} is already the combined maximum over direct objects AND common prefixes
         * ({@code SeedStep#lastSeenKey}), which is exactly the comparison that decides whether a
         * direct object sorts past the last rolled-up prefix.
         */
        private void observeTopScopeExtent(SeedProbeOutcome outcome) {
            byte[] last = outcome.lastKey();
            if (last != null && (topGreatestItem == null || Arrays.compareUnsigned(last, topGreatestItem) > 0)) {
                topGreatestItem = last;
            }
            for (byte[] cp : outcome.commonPrefixes()) {
                if (cp != null && (topGreatestCommonPrefix == null
                        || Arrays.compareUnsigned(cp, topGreatestCommonPrefix) > 0)) {
                    topGreatestCommonPrefix = cp;
                }
            }
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
                        flatWideRegion != null ? "flat_wide" : "narrow", topCounts[0], topCounts[1]));
                return finalizePlan();
            }
            // A page-capped top WITH common prefixes fans out into >1000 top-level directories -- a
            // tiny-leaf explosion left whole (the descent below is skipped).
            if (topPageCapped) {
                tinyLeafExplosion = true;
            }
            decisions.add(new SeedLevelDecision(prefix, topFanout, topPageCapped,
                    topPageCapped ? "tiny_leaf_explosion" : "narrow", topCounts[0], topCounts[1]));
            // The top-page pagination probe (mass_aware_seed=on only) is a second, distinct LIST
            // against the same top-level prefix -- its own decisions[] entry, classified
            // "top_probe_paginated", never folded into the top-level entry's counts.
            if (extraFired) {
                decisions.add(new SeedLevelDecision(prefix, extraFanout, extraPageCapped,
                        "top_probe_paginated", extraCounts[0], extraCounts[1]));
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
            decisions.add(new SeedLevelDecision(currentDir, currentSubFanout, false, "narrow",
                    counts[0], counts[1]));
            return continueDescentLoop();
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
            decisions.add(new SeedLevelDecision(currentDir, currentSubFanout, true, classification,
                    kept, discarded));
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
                return new SeedPlan(List.of(), 0, totalProbes, List.copyOf(decisions), drain());
            }

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
            appendOpenTileSentinel(finalCuts);

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

            return new SeedPlan(finalCuts, synthesized, totalProbes, finalDecisions, drain());
        }

        /**
         * <b>Close the top scope, so its rightmost mass stops being unsplittable.</b> {@code SeedStep}
         * always tiles the cut set as {@code (⊥,c1], …, (c_last, null]}, and the owner-split governor
         * refuses every range whose {@code hi} is {@code null} ({@code OwnerSplitSkipReason#OPEN_FRONTIER})
         * — so whatever mass lands past the last cut is drained by one worker, serially, no matter how
         * much of the bucket it is. On {@code nrel-pds-porotomo} that is 95.2% of 4.97M keys
         * (measured: {@code open_frontier.keys_emitted} 4,735,963 of 4,973,335).
         *
         * <p>When the top scope was listed to completion and its greatest item is a {@code CommonPrefix
         * p/}, every key in the scope is strictly below {@code prefixCeil(p/)} — a bound we already
         * hold, for free, with no extra probe. Appending it as one more cut turns the final tile into
         * {@code (c_last, u], (u, null]}: the open tile becomes EMPTY and the mass-bearing tile gains a
         * finite {@code hi}, which makes it eligible for owner self-split and for the thief's
         * delimiter-structure path. The static tiling is not enlarged — this hands the region to the
         * DYNAMIC splitter, which is the mechanism a corpus smoke proved load-bearing when more static
         * cuts regressed 6 of 10 fixtures.
         *
         * <p><b>Every precondition below is load-bearing</b> (checked in this order; each refusal
         * marks its own counter):
         * <ul>
         *   <li>the top is complete — either never page-capped, or page-capped and RECOVERED by the
         *       {@code TOP_EXTRA} continuation. A truncated, unrecovered top means unseen siblings
         *       may sort past what we saw, so no bound is provable. Do nothing.</li>
         *   <li>a greatest {@code CommonPrefix} exists — a flat scope offers no rolled-up prefix
         *       whose ceiling could bound it.</li>
         *   <li>a combined greatest returned item exists — with none reported there is nothing to
         *       verify the bound against. Distinct from the direct-object case below: that one KNOWS
         *       a key sorts past the prefix; this one knows nothing at all.</li>
         *   <li>the greatest item IS the greatest common prefix — if a direct object sorts past the
         *       last rolled-up prefix, {@code prefixCeil} of that prefix would NOT bound it, and the
         *       sentinel would cut the keyspace short. This is the correctness guard; it is a
         *       comparison, not an assumption.</li>
         *   <li>a spare seed slot ({@code size() < targetSeeds}) — never evict a real boundary for a
         *       sentinel. Replacing the maximal cut instead of appending would merge two useful
         *       regions and spend the freed slot on an empty range: same cardinality, less initial
         *       parallelism.</li>
         *   <li>{@code prefixCeil} is bounded — an all-{@code 0xFF} prefix has no ceiling, so there
         *       is nothing to append.</li>
         *   <li>{@code u} strictly above the current last cut — otherwise it is a duplicate or would
         *       break sort order, and it buys nothing.</li>
         * </ul>
         *
         * <p>Marked either way so the outcome is attributable from counters rather than inferred: the
         * declined case is as informative as the applied one when reading a slow run.
         *
         * @return {@code true} iff a sentinel was appended
         */
        private boolean appendOpenTileSentinel(List<byte[]> finalCuts) {
            // A top that was page-capped but then RECOVERED by the TOP_EXTRA continuation is complete:
            // the +1-page pagination read the overflow prefixes, so what we hold is the whole top
            // level. Gating on the first page's truncation alone would decline every paginated-top
            // bucket for no reason.
            boolean topComplete = !topPageCapped || (extraFired && !extraPageCapped);
            if (!topComplete) {
                mark("open_tile_sentinel_declined_top_truncated");
                return false;
            }
            if (topGreatestCommonPrefix == null) {
                mark("open_tile_sentinel_declined_no_prefix");
                return false;
            }
            if (topGreatestItem == null) {
                // No combined maximum was reported for the scope, so there is nothing to verify the
                // bound against. Distinct from the direct-object case below: that one KNOWS a key
                // sorts past the prefix; this one knows nothing at all.
                mark("open_tile_sentinel_declined_no_greatest_item");
                return false;
            }
            if (Arrays.compareUnsigned(topGreatestItem, topGreatestCommonPrefix) != 0) {
                // A direct object sorts past the last rolled-up prefix, so that prefix's ceiling does
                // not bound it. Declining keeps the bound honest; appending anyway would leave the
                // mass-bearing tile cut short of the keys above it (they stay covered by the final
                // open tile, so this is a parallelism loss, not a key loss).
                mark("open_tile_sentinel_declined_direct_object_tail");
                return false;
            }
            if (finalCuts.size() >= targetSeeds) {
                mark("open_tile_sentinel_declined_no_spare_slot");
                return false;
            }
            byte[] u = StealMath.prefixCeil(topGreatestCommonPrefix);
            if (u == null) {
                mark("open_tile_sentinel_declined_unbounded_ceiling");
                return false;
            }
            if (!finalCuts.isEmpty()
                    && Arrays.compareUnsigned(u, finalCuts.get(finalCuts.size() - 1)) <= 0) {
                mark("open_tile_sentinel_declined_not_above_last_cut");
                return false;
            }
            finalCuts.add(u);
            mark("open_tile_sentinel_appended");
            return true;
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
                    d.cutsKept(), d.cutsDiscarded()));
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
     * Weight-proportional subsample: allocate the {@code max}-cut budget by walking the sorted cuts
     * and emitting one each time the running weight crosses a {@code total/max} threshold, so a heavy
     * (high-weight) region keeps proportionally more interior cuts than an empty one. A cut absent
     * from {@code weights} counts as weight 1. Package-visible for
     * {@code HybridSeedPlannerMassWeightedSubsampleTest}, which reconstructs the wide-sibling shape
     * this guards against directly.
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
        List<byte[]> picked = new ArrayList<>(max);
        double step = (double) total / max;
        double next = step;
        double acc = 0;
        for (int i = 0; i < n && picked.size() < max; i++) {
            acc += w[i];
            if (acc + 1e-9 >= next) {
                picked.add(all.get(i));
                next = acc + step;
            }
        }
        if (picked.isEmpty()) {
            picked.add(all.get(n / 2));
        }
        return picked;
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
     * a one-shot pre-pass. Level-ordered first (depth primary), span-ordered within a level.
     */
    private static final class SpanPriorityFrontier implements Frontier {
        private record Entry(byte[] cut, int depth, long score) {
        }

        private final PriorityQueue<Entry> queue = new PriorityQueue<>(
                Comparator.comparingInt(Entry::depth)
                        .thenComparing(Comparator.comparingLong(Entry::score).reversed()));

        /** Depth -> count of entries at that depth currently queued. */
        private final Map<Integer, Integer> queuedDepths = new HashMap<>();

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
            Entry e = queue.poll();
            if (e != null) {
                queuedDepths.computeIfPresent(e.depth(), (depth, count) -> count == 1 ? null : count - 1);
            }
            return e == null ? null : e.cut();
        }

        @Override
        public void offer(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper) {
            int depth = depthOf(cut);
            queuedDepths.merge(depth, 1, Integer::sum);
            queue.add(new Entry(cut, depth, spanScore(cut, cuts, scopeUpper)));
        }

        @Override
        public boolean spansMultipleDepths() {
            return queuedDepths.size() > 1;
        }

        /** Depth = the number of {@link #DELIMITER} bytes in the cut. */
        private static int depthOf(byte[] cut) {
            int d = 0;
            for (byte b : cut) {
                if (b == DELIMITER[0]) {
                    d++;
                }
            }
            return d;
        }
    }
}
