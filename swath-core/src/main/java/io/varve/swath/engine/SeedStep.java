/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HYBRID seed step (algorithms.md §8). Before the {@code WorkStealingScan}
 * engine starts stealing, this issues a bounded {@code delimiter=/} structure
 * probe of the listing prefix {@code P} and turns the top-level common prefixes
 * {@code p1 < p2 < … < pk} into a tiling range set that exactly covers
 * {@code (⊥, null]}: {@code (⊥, p1], (p1, p2], …, (pk, null]} (the I2/I3
 * invariant at seed time). A deep-nested keyspace (the whole keyspace under one
 * or a few top-level prefixes) then parallelizes immediately instead of running
 * the single root range near-serially.
 *
 * <p><b>The probe is only a structure probe, never an emitting pass.</b> It uses
 * the {@code CommonPrefixes} as cut-points and <b>discards the {@code Contents}</b>
 * it also returns — every object (including top-level ones) is (re-)listed by a
 * range worker, so there is no double-emit between the seed and the ranges.
 *
 * <p><b>Bounded — never an exhaustive prefix enumeration (INT-8).</b> The cut-point set
 * is capped at roughly a small multiple of the worker count, so the seed never
 * over-fragments a directory explosion into one tiny range per leaf (which would cost
 * ~1 LIST per directory instead of ~{@code ceil(N/1000)} total). Only the first page
 * (≤1000) of common prefixes is read at any level; a truncated/broad level is never
 * paged further (default), and the engine flat-scans + steals it — the ONE exception
 * is the top level with {@code mass_aware_seed} ON (the default), which reads at most
 * one extra top page so mass-weighting/tiling can see the overflow prefixes past the
 * first page; sub-levels are never paged further either way.
 * A second {@code delimiter=/} level is descended ONLY when the first level is narrow
 * (not broad) and finer seeds are wanted. Each exploding (truncated) sub-level the descent
 * hits is classified and disposed of on its own — the descent then CONTINUES over the rest
 * of the frontier instead of abandoning it, since an unrelated sibling exploding first must
 * not strand a sibling with real, splittable mass at one giant range (a 1-object-per-leaf
 * Hive/Spark tree still costs one seed probe per top prefix + a flat scan, never a
 * per-directory walk — that sub-level is simply left whole, not descended, same as before). The
 * descent itself stops only on the probe budget ({@code maxProbes}) or natural frontier
 * exhaustion — the cut-point cap ({@code targetSeeds}) no longer bounds how far it reaches, only
 * how many of its cuts survive: once the descent completes, a cut set over {@code targetSeeds} is
 * reduced by mass-weighted subsampling, so cut density stays proportional to each heavy region's
 * sampled mass regardless of which one the frontier happened to reach first.
 *
 * <p><b>Dense-leaf radix banding.</b> A truncated
 * {@code delimiter=/} level is <em>classified</em> from its shape, using no extra probes:
 * <ul>
 *   <li><b>tiny-leaf explosion</b> — a truncated level returning {@code CommonPrefixes} that are
 *       plain (non-{@code key=value/}) directory names (e.g. ~1000 {@code <hex>/} each with ~1
 *       object): LEFT WHOLE and handed to work-stealing (the INT-8 shape; never enumerated, since a
 *       1:1 tree flat-scans in ~{@code ceil(N/1000)} LIST calls).</li>
 *   <li><b>partition fan-out</b> — a truncated level whose {@code CommonPrefixes} are
 *       Hive/Spark {@code key=value/} PARTITION directories (e.g. a {@code date=…/}-partitioned
 *       table, each partition holding real data mass): TILED at seed time along a {@code
 *       W}-capped subset of the partition prefixes ALREADY in the probed page (zero extra probes),
 *       instead of breaking and discarding the region, which would collapse the fleet to a serial
 *       tail. Behind the {@code fanout_tiling} engine toggle (default on).</li>
 *   <li><b>heavy dense range</b> — a truncated level with <em>no</em> {@code CommonPrefixes},
 *       only direct objects (a flat leaf, e.g. a UUID-keyed mega-day directory, or a
 *       single dense directory at the root): PRE-CUT at seed time into alphabet-uniform
 *       leading-byte RADIX bands — an even spread over the printable-ASCII leading alphabet (no
 *       keys observed yet at seed time, unlike the runtime rank-space pivots used once a worker has
 *       fetched pages) — so the fleet parallelizes the tail from the first moment, instead of
 *       draining it near-serially.</li>
 * </ul>
 * The initial split is presence/absence of {@code CommonPrefixes} on the truncated level
 * (flat-wide vs. directory-structured). Among directory-structured levels, the {@code key=value/}
 * partition-naming signal separates a partition fan-out from a plain explosion, and — when {@code
 * mass_aware_seed} is enabled — a bounded child sample further separates a genuinely heavy subtree
 * from a 1:1 explosion (see {@code isPartitionFanout} / {@code sampleProvesHeavy}). A flat-wide
 * level ({@link #isFlatWide}: truncated, no {@code CommonPrefixes}, at least one direct object) is
 * classified from that page shape alone — the seed inspects no key bytes and issues no further
 * probe — and is pre-cut straight into leading-byte radix bands.
 */
public final class SeedStep {

    private static final Logger log = LoggerFactory.getLogger(SeedStep.class);

    /** The folder delimiter for the structure probe. */
    static final byte[] DELIMITER = {'/'};

    /** S3's first-page cap; the probe never pages past it at any level. */
    private static final int PROBE_PAGE = 1000;

    // ---- Mass-aware seed descent (§8; behind mass_aware_seed, default ON) ------
    /**
     * Second-level sample width {@code j} (§8): how many children of an ambiguous
     * truncated-with-prefixes cut are probed to disambiguate heavy-subtree from 1:1 explosion.
     * {@code j=1} can misclassify a skewed subtree; 2–3 costs a couple of probes and cuts that error.
     */
    private static final int SAMPLE_WIDTH = 3;
    /**
     * The bounded second-level sample sub-budget (§8), carved OUT of {@link #maxProbes}
     * (never added on top): {@code structure probes + sample probes <= maxProbes <= 256}, with the
     * sample probes themselves capped at this many so deep descent can never starve sampling and
     * sampling can never storm the wallet.
     */
    private static final int SAMPLE_BUDGET = 32;
    /**
     * A sampled child is "dense" (evidence of a heavy subtree, not a 1:1 leaf) when it is itself
     * truncated, has its own sub-directories, or holds at least this many direct objects — well above
     * the ~1 object of an {@code essential-web}-style 1:1 explosion (INT-8), well below a real
     * partition's mass. The load-bearing distinction is 1-object-per-leaf vs. many.
     */
    private static final int SAMPLE_DENSE_MIN_OBJECTS = 8;
    /**
     * The floor {@link #sampleCutWeights} must clear before the over-cap reduction trusts its
     * estimate for {@link #massWeightedSubsample}: fewer sampled weights than this (including zero,
     * e.g. when the descent's own structure probing already left no room in {@link #maxProbes} for
     * this pass to run) is too sparse to be a meaningful mass estimate, and every unsampled cut
     * defaults to weight 1 in {@link #massWeightedSubsample} regardless — below this floor the
     * "weighted" walk would really just be an unweighted one wearing the wrong label. Set at a
     * quarter of {@link #SAMPLE_BUDGET}: enough samples to say something about SEVERAL distinct
     * regions of the cut-index range (not just one lucky/unlucky one), while still tolerating a
     * heavily-contended sample sub-budget (e.g. shared with mid-descent heavy-cut sampling) that
     * only had a partial slice left to spend here.
     */
    private static final int MIN_WEIGHT_SAMPLES = SAMPLE_BUDGET / 4;

    private final PageFetcher fetcher;
    private final byte[] prefix;
    private final int workerCount;
    private final int targetSeeds;
    private final int maxProbes;
    private final RunMetrics metrics;
    private final EngineToggles toggles;
    /** The scan's own top-level scope ceiling — see {@link #scopeCeiling} for what this measures. */
    private final byte[] topScopeCeiling;

    /**
     * @param prefix      the listing prefix {@code P} ({@code null}/empty = whole bucket)
     * @param workerCount the engine's target concurrency; the seed aims for roughly a small
     *                    multiple of this many ranges (enough parallelism, never over-fragmenting)
     * @param metrics     the run's metrics sink, or {@code null} when unwired (e.g. a test)
     * @param toggles     {@code toggles.radixBands() == false} skips {@link
     *                    #subdivideFlatWideRegion} entirely, leaving a dense flat region as one
     *                    un-subdivided range (see {@link EngineToggles}'s javadoc)
     */
    public SeedStep(PageFetcher fetcher, byte[] prefix, int workerCount, RunMetrics metrics, EngineToggles toggles) {
        this.metrics = metrics;
        this.fetcher = fetcher;
        this.prefix = prefix;
        this.toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        // The once-per-instantiation TOGGLE.radix_bands_off / TOGGLE.fanout_tiling_off marks (§5) —
        // both are SeedStep-only toggles (never WorkStealingScan/Thief's, which do not re-construct
        // SeedStep). The toggle-name → mark-string derivation itself is single-sourced in
        // EngineToggles#recordOffMarks — this only lists which toggles this unit owns.
        if (metrics != null) {
            this.toggles.recordOffMarks(metrics, "radix_bands", "fanout_tiling");
        }
        // Engagement mark, inverted from the *_off ablation marks above — fires whenever
        // mass-aware seed descent is ON (the default) so post-hoc analysis sees it even on a bucket
        // whose shape never triggered a second-level sample.
        if (this.toggles.massAwareSeed() && metrics != null) {
            metrics.recordStealReason("TOGGLE", "mass_aware_seed_on");
        }
        int w = Math.max(1, workerCount);
        this.workerCount = w;
        // ~4×W ranges: every worker has headroom to steal from, while the seed-range count
        // (and thus the per-range LIST overhead) stays bounded regardless of directory count.
        this.targetSeeds = Math.min(PROBE_PAGE, 4 * w);
        // The descent probe budget is bounded by the same target (one-time, ~O(seed) RPCs).
        this.maxProbes = Math.min(256, Math.max(1, this.targetSeeds));
        // The whole scan's own upper bound — the scope every top-level cut's unmeasured tail
        // falls back to in spanScore (see scopeCeiling).
        this.topScopeCeiling = scopeCeiling(this.prefix);
    }

    /**
     * The seed worklist for a <b>fresh</b> run: a set of {@link NodeSpec} that exactly
     * tiles {@code (⊥, null]}. Resume never calls this (it reloads existing nodes via
     * {@code loadResumable}).
     */
    public List<NodeSpec> seedSpecs(long runId, SeedMode mode) throws SwathException, InterruptedException {
        return switch (mode) {
            case NONE -> {
                recordSeedSummary("none", 0, 0, 0, 1, List.of());
                yield List.of(NodeSpec.rootRange(runId));
            }
            case SHALLOW -> shallow(runId);
            case HINTS -> throw new InvalidConfigException(
                    "--seed hints requires a --hints cut-points file, which is not yet implemented");
        };
    }

    private List<NodeSpec> shallow(long runId) throws SwathException, InterruptedException {
        Collected collected = collectCutPoints();
        if (collected.cuts.isEmpty() && collected.flatWideRegion == null) {
            // Flat top (no common prefixes) that is NOT a dense leaf: one (⊥, null] range, same as
            // NONE. A dense flat root (flatWideRegion non-null) falls through to radix banding.
            recordSeed("flat_trivial");   // §5: trivial/flat seed shape
            recordSeedSummary("shallow", collected.probes, 0, 0, 1, collected.decisions);
            return List.of(NodeSpec.rootRange(runId));
        }
        // Cap the cut-points (subsample evenly) so the seed-range count — and thus the
        // per-range LIST overhead — never exceeds the target, even when one probe returned
        // up to 1000 prefixes (a broad top), or the descent (no longer capped by cuts.size(),
        // above) accumulated interior cuts from several heavy regions. Any subset of cut-points
        // still tiles exactly.
        TreeSet<byte[]> cutSet = new TreeSet<>(Arrays::compareUnsigned);
        if (collected.cuts.size() > targetSeeds) {
            // The descent's own reach is no longer bounded by targetSeeds (see collectCutPoints),
            // so this is the actual point the cut-point cap bites — record it distinctly from
            // mass_weighted_subsample below, which fires only on the weighted branch, so post-hoc
            // analysis can see every trim (weighted or positional) that reduced the descent's cuts.
            recordSeed("descent_cuts_subsampled");
        }
        if (collected.massWeighted && collected.weights != null) {
            // An over-cap cut set is subsampled weight-proportionally (§4), so a heavy
            // region that is a small fraction of the cut-index range keeps MORE interior cuts than an
            // empty one — instead of the positional even spacing that starves it. Any subset still
            // tiles (⊥, null] exactly (I2/I3).
            cutSet.addAll(massWeightedSubsample(collected.cuts, targetSeeds, collected.weights));
            recordSeed("mass_weighted_subsample");
        } else {
            cutSet.addAll(subsampleEvenly(collected.cuts, targetSeeds));
        }
        // A `delimiter=/` sub-level that is *flat and wide* (truncated with direct
        // objects, no sub-directory structure — e.g. `zz/<uuid>` under a `zz/` prefix) would be
        // left as ONE giant range covering the majority of the keyspace, so its tail lists
        // near-serially. Subdivide that region at seed time by synthesizing cut-points inside
        // it (pure byte synthesis of the next code point after the region's prefix — no extra
        // probe), so the fleet can parallelize the tail from the first moment. Any subset still
        // tiles (⊥, null] exactly (I2/I3); the synthesized cuts are strictly between the region's
        // prefix and its prefix-ceiling.
        int synthesized = toggles.radixBands() ? subdivideFlatWideRegion(cutSet, collected.flatWideRegion) : 0;
        if (!toggles.radixBands() && collected.flatWideRegion != null) {
            // The toggle suppressed a banding that WOULD otherwise have fired — a
            // per-attempt engagement mark distinct from the once-per-run TOGGLE.radix_bands_off mark
            // (ListCommand), so post-hoc analysis can tell an ablation run that actually hit the
            // dense-flat-region shape from one that never would have banded anyway.
            recordSeed("radix_bands_toggle_disabled");
        }
        List<byte[]> cuts = new ArrayList<>(cutSet);
        List<NodeSpec> specs = tile(runId, cuts);
        // Seed engagement counters + classification signals (§5): which seed shape a bucket got, and
        // (for the dense-root radix-banding path) how many radix bands and the observed keyspace shape.
        if (synthesized > 0) {
            recordSeed("dense_root_radix_banded");
            if (metrics != null) {
                metrics.recordSeedBands(synthesized);
            }
        }
        if (collected.tinyLeafExplosion) {
            recordSeed("tiny_leaf_explosion");
        }
        // The engagement counter for a truncated `key=value/` partition fan-out that was tiled
        // (W-capped) instead of broken-and-discarded, so a post-hoc run can confirm the path fired.
        if (collected.fanoutTiled) {
            recordSeed("fanout_tiled");
        }
        // The generic/base seed shape — plain delimiter=/ structure tiled into cut-points, no
        // dense-root radix banding or tiny-leaf explosion applied.
        boolean genericDelimiterSeeded =
                synthesized == 0 && !collected.tinyLeafExplosion && !collected.fanoutTiled
                        && !collected.heavyCutBanded;
        if (genericDelimiterSeeded) {
            recordSeed("delimiter_seeded");
        }
        recordSeed(collected.topTruncated ? "top_truncated" : "top_complete");
        // Promote the run's overall disposition onto the per-level decision trace — a level's
        // OWN probe result only distinguishes narrow/tiny_leaf_explosion/flat_wide (see
        // collectCutPoints); the two run-level-only classifications (dense_root_radix_banded — a
        // flat_wide region that actually got banded; delimiter_seeded — the generic plain-tiled run,
        // applied to the top level only) are known exclusively AFTER this method's post-processing.
        List<RunMetrics.SeedProbeDecision> decisions = finalizeDecisions(
                collected, synthesized > 0, genericDelimiterSeeded);
        recordSeedSummary("shallow", collected.probes, cuts.size(), synthesized, specs.size(), decisions);
        log.info("seed_shallow run_id={} probes={} cut_points={} synthesized_cuts={} seed_ranges={}",
                runId, collected.probes, cuts.size(), synthesized, specs.size());
        return specs;
    }

    /**
     * Rewrites {@link Collected#decisions}' classification for the (at most one) level whose
     * flat-wide region was actually radix-banded, and for the top level when the run's overall shape
     * is the generic delimiter-tiled case — see {@link #shallow}'s call site for why these two
     * classifications can only be known after the whole collection is in hand.
     */
    private static List<RunMetrics.SeedProbeDecision> finalizeDecisions(
            Collected collected, boolean banded, boolean genericDelimiterSeeded) {
        List<RunMetrics.SeedProbeDecision> out = new ArrayList<>(collected.decisions.size());
        for (int i = 0; i < collected.decisions.size(); i++) {
            RunMetrics.SeedProbeDecision d = collected.decisions.get(i);
            String classification = d.classification();
            if (banded && Arrays.equals(d.prefix(), collected.flatWideRegion)) {
                classification = "dense_root_radix_banded";
            } else if (i == 0 && genericDelimiterSeeded && "narrow".equals(classification)) {
                classification = "delimiter_seeded";
            }
            out.add(new RunMetrics.SeedProbeDecision(d.prefix(), d.fanout(), d.truncated(), classification,
                    d.cutsKept(), d.cutsDiscarded()));
        }
        return out;
    }

    /** Record a seed engagement/classification counter (§5) — surfaced in {@code SEED.<reason>}. */
    private void recordSeed(String reason) {
        if (metrics != null) {
            metrics.recordStealReason("SEED", reason);
        }
    }

    /**
     * Promotes the seed step's already-computed shape (§5) plus the per-level decision trace into
     * the JSON run-summary's {@code seed} block ({@link RunMetrics#recordSeedSummary}). A no-op
     * when metrics are not wired (e.g. a unit test using the metrics-less constructor).
     */
    private void recordSeedSummary(String mode, long probes, long cutPoints, long synthesizedCuts, long ranges,
            List<RunMetrics.SeedProbeDecision> decisions) {
        if (metrics != null) {
            metrics.recordSeedSummary(mode, probes, cutPoints, synthesizedCuts, ranges, decisions);
        }
    }

    /**
     * {@code flatWideRegion} is at most ONE region, never a list: the flat-top branch
     * records the root and returns immediately, and the descent loop keeps only the FIRST
     * truncated flat-wide sub-level it sees — a later flat-wide sibling is still classified
     * and left whole, but does not overwrite the banding slot (the descent no longer stops
     * there; it continues probing the rest of the frontier). {@code decisions} is one entry
     * per probed level, in probe order (index 0 is always the top-level probe) — bounded by
     * {@link #maxProbes}, the same cap {@code probes} is already bounded by.
     */
    private record Collected(TreeSet<byte[]> cuts, int probes, byte[] flatWideRegion,
                             boolean topTruncated, boolean tinyLeafExplosion, boolean fanoutTiled,
                             boolean heavyCutBanded, boolean massWeighted,
                             NavigableMap<byte[], Long> weights,
                             List<RunMetrics.SeedProbeDecision> decisions) {
    }

    /**
     * The descent frontier of not-yet-probed directory prefixes: {@link FifoFrontier} (mass-aware
     * OFF, byte-identical to the original plain {@link Deque}) or {@link SpanPriorityFrontier}
     * (mass-aware ON, best-first by {@link #spanScore}). {@link #collectCutPoints} picks the
     * implementation once, up front, from {@code massAware} alone — never per-call — so both paths
     * share every insertion/poll call site below.
     */
    private interface Frontier {
        boolean isEmpty();

        int size();

        byte[] poll();

        /** Offer a newly-discovered cut into the frontier; {@code cuts} is the FULL global cut set
         *  at the time of the offer (used by {@link SpanPriorityFrontier} to score it), and {@code
         *  scopeUpper} is the upper bound of the scope {@code cut} was discovered in (the ceiling of
         *  the directory being probed, or the scan's own {@link #topScopeCeiling} at the top level —
         *  see {@link #scopeCeiling}) — the fallback {@link #spanScore} measures {@code cut}'s tail
         *  against when it has no successor in {@code cuts} yet. */
        void offer(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper);
    }

    /** The mass-aware OFF frontier: plain FIFO, byte-identical to the original {@link ArrayDeque}. */
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
     * The mass-aware ON frontier (§3.1): a genuine best-first order <b>maintained across
     * insertions</b>, not a one-shot pre-pass — a candidate enqueued mid-descent (e.g. a wide
     * subtree discovered several probes into the descent) is ranked against every entry already
     * queued, including ones enqueued earlier, so it is polled before them whenever its span score
     * is higher. This replaces the original {@code reorderBySpanDescending}, which sorted the
     * frontier once before the loop started and left every later insertion in plain append order —
     * adequate only while the descent stopped at the first exploding sub-level; now that it runs to
     * the probe budget (§8), a narrow region discovered early could otherwise starve a wide one
     * discovered late purely by insertion order.
     *
     * <p><b>Scored at insertion, not at poll.</b> {@link #spanScore} measures the keyspace gap to a
     * cut's next sibling in the (evolving) global cut set, so its exact value can only get MORE
     * accurate as later probes insert cuts between two already-scored siblings (shrinking a stale
     * gap the queue has not re-measured). Re-scoring every entry on every poll would keep the score
     * exact but is asymptotically dearer (an {@code O(n)} rescan per poll instead of an {@code O(log
     * n)} heap operation); insertion-time scoring is the cheaper, slightly-stale call, consistent
     * with {@link #spanScore}'s own javadoc describing it as a weak-but-cheap proxy in the first
     * place — a proxy chosen for a bounded probe budget, not an ordering the descent depends on
     * being exact. When {@code cut} has no successor yet in {@code cuts}, {@link #spanScore} falls
     * back to the offering scope's own {@code scopeUpper} bound instead of treating the tail as
     * infinite (see {@link #scopeCeiling}).
     */
    private static final class SpanPriorityFrontier implements Frontier {
        private record Entry(byte[] cut, long score) {
        }

        private final PriorityQueue<Entry> queue =
                new PriorityQueue<>(Comparator.comparingLong(Entry::score).reversed());

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
            return e == null ? null : e.cut();
        }

        @Override
        public void offer(byte[] cut, TreeSet<byte[]> cuts, byte[] scopeUpper) {
            queue.add(new Entry(cut, spanScore(cut, cuts, scopeUpper)));
        }
    }

    private Collected collectCutPoints() throws SwathException, InterruptedException {
        boolean massAware = toggles.massAwareSeed();
        TreeSet<byte[]> cuts = new TreeSet<>(Arrays::compareUnsigned);
        // OFF: byte-identical FIFO. ON: best-first by span score, maintained across every
        // insertion (see SpanPriorityFrontier) — chosen once, up front, since massAware never
        // changes mid-descent.
        Frontier expandable = massAware ? new SpanPriorityFrontier() : new FifoFrontier();
        List<RunMetrics.SeedProbeDecision> decisions = new ArrayList<>();
        // probeState[0] = total LIST probes (structure + samples); probeState[1] = sample probes only,
        // the sub-budget carved OUT of maxProbes (§8). Boxed so the helpers can advance it.
        int[] probeState = {1, 0};
        boolean tinyLeafExplosion = false;
        boolean fanoutTiled = false;
        boolean heavyCutBanded = false;

        ListPage top = probe(prefix);
        boolean topTruncated = top.truncated();
        int[] topCounts = addCutsCounted(top, cuts, expandable, topScopeCeiling);
        // A truncated TOP level with common prefixes (§5) hides its overflow prefixes
        // (1001..2000) past the first page — read at most ONE extra page (TOP level only; never a
        // sub-level, which would be O(levels x pages) and re-expose the cold-probe risk) so
        // mass-weighting/tiling can see them. Behind mass_aware_seed; zero effect on the OFF path.
        // The extra page is its own probe/LIST — it gets its own seed.decisions[]
        // entry (extraPageDecision below, appended right after the top-level entry) so decisions[] stays
        // in exact 1:1 correspondence with the probes/delimiter_fanout counters it is cross-checked against.
        int[] extraCounts = null;
        int extraFanout = 0;
        boolean extraTruncated = false;
        if (massAware && topTruncated && !cuts.isEmpty() && probeState[0] < maxProbes) {
            byte[] after = lastSeenKey(top);
            if (after != null) {
                ListPage extra = probe(prefix, after);
                probeState[0]++;
                recordSeed("top_probe_paginated");
                extraCounts = addCutsCounted(extra, cuts, expandable, topScopeCeiling);
                extraFanout = extra.commonPrefixes().size();
                extraTruncated = extra.truncated();
            }
        }
        if (cuts.isEmpty()) {
            // Flat top: no delimiter=/ structure. Two shapes:
            //   (a) a small flat bucket (not truncated) → one (⊥, null] range, same as NONE;
            //   (b) a single DENSE flat directory at the root (truncated, direct objects, no
            //       CommonPrefixes — the isFlatWide shape): no key-entropy test and no further
            //       probe, fall straight to leading-byte RADIX bands: record the root as the
            //       flat-wide region for the caller to pre-cut. Any subset of synthesized cuts
            //       still tiles exactly.
            byte[] flatWide = (top.truncated() && isFlatWide(top))
                    ? (prefix == null ? new byte[0] : prefix) : null;
            // Top-level decision — "flat_wide" when it is the dense-root banding candidate,
            // "narrow" for the small/trivial flat top (see finalizeDecisions for the run-level
            // dense_root_radix_banded/delimiter_seeded rewrites this classification can still take).
            decisions.add(new RunMetrics.SeedProbeDecision(prefix, top.commonPrefixes().size(), topTruncated,
                    flatWide != null ? "flat_wide" : "narrow", topCounts[0], topCounts[1]));
            return new Collected(cuts, probeState[0], flatWide, topTruncated, tinyLeafExplosion, fanoutTiled,
                    false, false, null, decisions);   // flat top
        }
        // A truncated top WITH common prefixes fans out into >1000 top-level directories — a tiny-leaf
        // explosion left whole (the descent below is skipped), the INT-8 shape (§5 classification).
        if (topTruncated) {
            tinyLeafExplosion = true;
        }
        decisions.add(new RunMetrics.SeedProbeDecision(prefix, top.commonPrefixes().size(), topTruncated,
                topTruncated ? "tiny_leaf_explosion" : "narrow", topCounts[0], topCounts[1]));
        // The top-page pagination probe (mass_aware_seed=on only) is a
        // second, distinct LIST against the same top-level prefix — record it as its own decision entry,
        // classified "top_probe_paginated" (matching the recordSeed reason above) so it never gets
        // silently folded into the top-level entry's counts. Only ever reached when the pagination probe
        // above actually fired (extraCounts != null); OFF (or no pagination) leaves decisions unchanged.
        if (extraCounts != null) {
            decisions.add(new RunMetrics.SeedProbeDecision(prefix, extraFanout, extraTruncated,
                    "top_probe_paginated", extraCounts[0], extraCounts[1]));
        }

        // Adaptive descent: refine the cut-points ONLY when the top level is narrow
        // (not truncated). Descend a sub-level only while it stays narrow; an exploding
        // (truncated) sub-level is disposed of PER-CUT and classified from its shape — a
        // `key=value/` partition fan-out is tiled, a flat-wide region is radix-banded, and a
        // plain 1:1 directory explosion is left whole to be flat-scanned + work-stolen (INT-8)
        // — but does NOT stop the descent: the remaining frontier siblings are unrelated
        // subtrees that may still carry real mass, so probing continues past them. The probe
        // budget (maxProbes) and natural frontier exhaustion are the only stops on the descent
        // itself — cuts.size() reaching targetSeeds is deliberately NOT one of them: capping the
        // descent there is first-come-first-served (whichever subtree the frontier reaches first
        // keeps every cut, and a sibling with equally real mass a few frontier entries later is
        // never even probed — the wide-sibling regression this reverses). targetSeeds instead
        // bounds the FINAL range count, below, by mass-weighted subsampling once the whole
        // descent is in hand, so cut density stays proportional to sampled mass across every
        // heavy region the descent found, not to probe order.
        byte[] flatWide = null;
        if (!top.truncated()) {
            // Best-first frontier (§3.1) — the likeliest-heavy sibling is always polled first,
            // using the child's interval byte-span to its next sibling as the enqueue-time
            // heaviness proxy (SpanPriorityFrontier). The order is maintained continuously, not
            // reordered once up front: a wide candidate discovered several probes into the descent
            // still outranks a narrow one already queued. OFF (FifoFrontier) keeps the plain FIFO
            // (sorted) order byte-for-byte.
            //
            // Reserve headroom for the post-descent mass-weighting pass (sampleCutWeights, below
            // this method): a descent that runs the WHOLE wallet on structure probes (the FS3
            // shape — the descent never runs dry of narrow sub-levels to expand) leaves probeState[0]
            // already at maxProbes by the time shallow() wants to weigh an over-cap cut set, so
            // sampleCutWeights returns nothing and the reduction silently falls back to
            // unweighted (see the MIN_WEIGHT_SAMPLES floor below). Cap this loop below maxProbes so
            // SAMPLE_BUDGET's worth of probes is still available afterward — but never reserve more
            // than half of a (possibly small) maxProbes: SAMPLE_BUDGET (32) can exceed a
            // low-worker-count run's entire wallet, and the structure descent itself must keep a
            // meaningful share even when the later weighting pass has to make do with fewer than
            // SAMPLE_BUDGET samples as a result. Mid-descent heavy-cut sampling (sampleProvesHeavy,
            // below) shares the same SAMPLE_BUDGET sub-budget (probeState[1]) but is bounded by the
            // absolute maxProbes ceiling, not this reduced one — it fires only for a cut already
            // popped inside the descent's own share, and the few probes it spends confirming
            // heavy/tiny may spill a little into the reserved tail; that shared, slightly-overlapping
            // use is deliberate (see SAMPLE_BUDGET's own javadoc).
            int descentCeiling = massAware ? maxProbes - Math.min(SAMPLE_BUDGET, maxProbes / 2) : maxProbes;
            boolean frontierReorderedFired = false;
            while (!expandable.isEmpty() && probeState[0] < descentCeiling) {
                if (massAware && !frontierReorderedFired && expandable.size() > 1) {
                    // (§5 instrumentation rule): only counts a REAL engagement — fires the first
                    // time the priority order actually has more than one candidate to rank, checked
                    // AT EVERY POLL rather than once before the loop starts, so a frontier that
                    // starts with a single entry but expands mid-descent (its newly-discovered
                    // children queued alongside whatever is already left) still counts as an
                    // engagement the moment a real choice exists — the original check (frontier
                    // size before the loop even started) missed that case.
                    recordSeed("frontier_reordered");
                    frontierReorderedFired = true;
                }
                byte[] dir = expandable.poll();
                ListPage sub = probe(dir);
                probeState[0]++;
                if (sub.truncated()) {
                    String classification;
                    int kept = 0;
                    int discarded = 0;
                    if (isFlatWide(sub)) {
                        // A large FLAT region (>1000 direct objects, no sub-directories): the seed
                        // would otherwise leave it as one giant near-serial range. Record it so the
                        // caller synthesizes interior cut-points. A truncated sub-level
                        // WITH common prefixes is a directory explosion instead — handled below.
                        // flatWideRegion is at most one region (see the Collected javadoc): the
                        // descent no longer stops at the first truncated sub-level, so a LATER
                        // flat-wide sibling keeps its own "flat_wide" classification (still left
                        // whole, still disposed of) but does not steal the banding slot from the
                        // first one found.
                        if (flatWide == null) {
                            flatWide = dir;
                        }
                        classification = "flat_wide";
                    } else if (toggles.fanoutTiling() && isPartitionFanout(sub)) {
                        // A truncated sub-level whose common prefixes are Hive/Spark
                        // `key=value/` PARTITION directories (e.g. a partitioned table's `date=…/`
                        // dirs). Each partition holds real data mass, so leaving the whole region as
                        // one near-serial range collapses the fleet to a serial tail. TILE it along a
                        // W-capped subset of the partition prefixes ALREADY in this probed page — zero
                        // extra probes, and any sorted subset of cut-points still tiles (⊥, null]
                        // exactly (I2/I3). A plain (non-`key=value`) `<hex>/` directory explosion is
                        // NOT tiled — it falls through to the tiny-leaf branch below and is left whole
                        // (the INT-8 shape), since a 1:1 one-object-per-leaf tree costs ~ceil(N/1000)
                        // LIST calls flat-scanned.
                        //
                        // This sharp, zero-probe syntactic partition signal is evaluated
                        // and, when it matches, taken BEFORE the mass-aware heavy-cut sample below —
                        // the sample is SHORT-CIRCUITED entirely (no probes spent, no banding) so
                        // `fanout_tiling` composes identically whether `mass_aware_seed` is off or on.
                        // Mass-aware banding still fully owns every heavy cut that is NOT a partition
                        // fanout (the else branch below).
                        int[] counts = tilePartitionFanout(sub, cuts);
                        kept = counts[0];
                        discarded = counts[1];
                        fanoutTiled = true;
                        classification = "fanout_tiled";
                        if (massAware && probeState[1] < SAMPLE_BUDGET && probeState[0] < maxProbes) {
                            // (§5 instrumentation rule): this cut WOULD have been sampled by
                            // mass-aware banding (sample budget available) had the partition-fanout
                            // signal not taken precedence and short-circuited it first — record so
                            // post-hoc analysis can see how often the precedence rule engages.
                            recordSeed("banding_deferred_to_fanout");
                        }
                    } else {
                        // A truncated sub-level WITH common prefixes is AMBIGUOUS on one page: a heavy
                        // deep subtree (band it) vs. a 1:1 tiny-leaf explosion (leave whole — INT-8).
                        // Sample j children to disambiguate (§2), carving the sample probes
                        // out of maxProbes (<= SAMPLE_BUDGET). OFF skips sampling → heavy stays false
                        // and the fanout/tiny-leaf classification below is byte-identical.
                        boolean heavy = false;
                        if (massAware && probeState[1] < SAMPLE_BUDGET && probeState[0] < maxProbes) {
                            heavy = sampleProvesHeavy(sub, probeState);
                            recordSeed("heavy_cut_descended");   // the bounded second-level dive fired
                            if (!heavy) {
                                recordSeed("explosion_confirmed");   // sample proved 1:1 (INT-8 shape)
                            }
                        }
                        if (heavy) {
                            // A confirmed-heavy cut (§3.2) is BANDED — tiled along the
                            // child prefixes already in this probed page (the recurse-one-level option),
                            // so its mass parallelizes into many ranges instead of one serial tail.
                            // Only reachable here for a NON-partition-fanout heavy cut (the
                            // partition-fanout case above already short-circuited).
                            int[] counts = bandHeavyCut(sub, cuts);
                            kept = counts[0];
                            discarded = counts[1];
                            heavyCutBanded = true;
                            recordSeed("heavy_cut_banded");
                            classification = "heavy_cut_banded";
                        } else {
                            tinyLeafExplosion = true;   // directory explosion left whole (INT-8 shape)
                            classification = "tiny_leaf_explosion";
                        }
                    }
                    // cuts_kept/discarded are 0/0 for a flat_wide or tiny_leaf level (nothing
                    // tiled from it); a fanout_tiled/heavy_cut_banded level reports the cuts it added.
                    decisions.add(new RunMetrics.SeedProbeDecision(dir, sub.commonPrefixes().size(), true,
                            classification, kept, discarded));
                    // Per-cut disposal, not a global abandon: this cut's own region is already
                    // tiled/banded/left whole by the classification above, but the OTHER frontier
                    // siblings are unrelated subtrees (e.g. commoncrawl's projects/headers-testing/
                    // sitting next to an exploding sibling) that may still carry real, splittable
                    // mass — CONTINUE probing them instead of abandoning the rest of the frontier.
                    // The while-loop's own caps — probe budget and frontier exhaustion — remain the
                    // only stops on the descent itself (targetSeeds no longer gates it; see above).
                    if (!expandable.isEmpty() && probeState[0] < maxProbes) {
                        // (§5 instrumentation rule): only counts when the frontier actually still
                        // holds work that will now be probed — exactly the case where the old
                        // unconditional break would have abandoned it.
                        recordSeed("frontier_continued_past_explosion");
                    }
                    continue;
                }
                int[] counts = addCutsCounted(sub, cuts, expandable, scopeCeiling(dir));
                decisions.add(new RunMetrics.SeedProbeDecision(dir, sub.commonPrefixes().size(), false,
                        "narrow", counts[0], counts[1]));
            }
        }

        // An over-cap collection (§4) (more cut-points than targetSeeds) is subsampled
        // POSITIONALLY otherwise, starving whichever heavy region is a small slice of the
        // cut-index range. This used to be gated to the plain wide-top case only (flatWide == null
        // && !heavyCutBanded) — back when the descent stopped at the first exploding sub-level, a
        // heavy cut's own local banding was the only heaviness signal in play, so cuts rarely grew
        // much past targetSeeds. Now the descent continues past every exploding sibling (above), so
        // the final cut set can hold interior cuts from SEVERAL heavy regions (banded and/or tiled)
        // well past targetSeeds — exactly the shape that needs mass-proportional, not positional,
        // trimming, so run the same weighted sample whenever the cap is exceeded, regardless of
        // which shapes the descent hit. Sample a bounded subset of the cuts to estimate per-region
        // mass so shallow() can allocate the budget weight-proportionally. Behind mass_aware_seed;
        // carved out of the same sample sub-budget.
        NavigableMap<byte[], Long> weights = null;
        boolean massWeighted = false;
        if (massAware && !topTruncated && cuts.size() > targetSeeds && probeState[1] < SAMPLE_BUDGET) {
            NavigableMap<byte[], Long> sampled = sampleCutWeights(cuts, probeState);
            if (sampled.size() >= MIN_WEIGHT_SAMPLES) {
                weights = sampled;
                massWeighted = true;
            }
            // Fewer than MIN_WEIGHT_SAMPLES weights (including zero — most often because the
            // descent's own probing already reached its ceiling before this pass could spend more
            // than a couple of probes, or the reserved SAMPLE_BUDGET headroom above was itself
            // largely spent by mid-descent heavy-cut sampling): too sparse an estimate to trust.
            // Fall through with massWeighted left false so shallow() takes the subsampleEvenly
            // branch instead — a real trim (still recorded via descent_cuts_subsampled), just never
            // reported as mass_weighted_subsample when it silently degraded to unweighted (an
            // external review flagged the previous unconditional massWeighted=true here as a false
            // signal: every cut defaults to weight 1 when weights is empty, so the "weighted" walk
            // was actually the SAME degenerate positional-ish walk subsampleEvenly already does,
            // just reported as something it wasn't).
        }

        return new Collected(cuts, probeState[0], flatWide, topTruncated, tinyLeafExplosion, fanoutTiled,
                heavyCutBanded, massWeighted, weights, decisions);
    }

    /** A truncated sub-level that is flat (direct objects, no sub-directory rollups) — a wide flat region. */
    private static boolean isFlatWide(ListPage sub) {
        return sub.commonPrefixes().isEmpty() && !sub.entries().isEmpty();
    }

    /**
     * Is this truncated-with-common-prefixes level a Hive/Spark {@code key=value/}
     * PARTITION fan-out (tile it), or a plain {@code <hex>/} directory explosion (leave it whole — the
     * INT-8 tiny-leaf shape)? Decided with ZERO extra probes, from the common-prefix names already in
     * the probed page: a level is a partition fan-out iff a MAJORITY of its common prefixes' final path
     * segments contain {@code '='} (the {@code date=…/}, {@code pid=…/} partition-naming convention).
     *
     * <p>This is the honest, zero-probe signal available in hand: the {@code N}-objects-per-partition
     * Hive shape (e.g. {@code date=…/}) and the 1:1 one-object-per-leaf explosion
     * ({@code data/<hex>/}) produce IDENTICAL page shapes (~1000 common prefixes, no direct Contents,
     * truncated) — per-leaf density is unobservable without a leaf probe. The {@code key=value/}
     * convention is the shape this discriminator targets, and is exactly what distinguishes a
     * partitioned table (mass per partition) from a content-addressed directory explosion. A 1:1
     * explosion that DOES use {@code key=value/} names still tiles, but only into {@code <= W} ranges
     * (see {@link #tilePartitionFanout}) — {@code W} extra ranges cost {@code <= W} extra flat-scan
     * LIST calls, so INT-8's {@code ~ceil(N/1000)} budget still holds.
     */
    private static boolean isPartitionFanout(ListPage sub) {
        List<KeyBytes> cps = sub.commonPrefixes();
        if (cps.isEmpty()) {
            return false;
        }
        int partitionLike = 0;
        for (KeyBytes cp : cps) {
            if (lastSegmentHasEquals(cp.rawUnsafe())) {
                partitionLike++;
            }
        }
        return partitionLike * 2 > cps.size();
    }

    /**
     * Does the final path segment of a common prefix {@code cp} (which ends in the {@code /} delimiter)
     * contain a {@code '='}? i.e. is it a {@code key=value/} partition directory. E.g. the final segment
     * of {@code v1.0/btc/transactions/date=2009-01-03/} is {@code date=2009-01-03} → {@code true}; the
     * final segment of {@code data/5bd1e995/} is {@code 5bd1e995} → {@code false}.
     */
    private static boolean lastSegmentHasEquals(byte[] cp) {
        int end = cp.length;
        if (end > 0 && cp[end - 1] == DELIMITER[0]) {
            end--;   // drop the trailing delimiter
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

    /**
     * Add up to {@code W} (worker count) evenly-subsampled cut-points from a partition fan-out's
     * ALREADY-PROBED common prefixes into the global cut set. Capped at {@code W} — enough to fill every
     * worker from t=0, without turning a true 1:1 tiny-leaf explosion (that happened to use
     * {@code key=value/} names) into hundreds of tiny scans (widening beyond W needs measurement first).
     * Returns {@code {kept, discarded}} (new distinct cuts added vs. duplicates already present).
     * The tiled prefixes are NOT added to {@code expandable} — the region is tiled here and descended no
     * further (zero extra probes). Any sorted subset of the observed prefixes still tiles {@code (⊥,
     * null]} exactly (I2/I3), the same invariant {@link #addCutsCounted} and {@code tile} already rely on.
     *
     * <p><b>First-page bound.</b> {@code sub} is
     * the level's FIRST (and only, per the zero-extra-probes budget) page, so at most {@code
     * PROBE_PAGE} (1000) partition prefixes are ever visible to tile from — a partition table with
     * MORE than 1000 partitions leaves everything past the first page's worth as one final open
     * range, still exact but under-parallelized for the tail. Sub-levels are never paginated past
     * their first page (unlike the top level under {@code mass_aware_seed}, see
     * {@code collectCutPoints}), so mass-aware seed descent does not close this gap either.
     */
    private int[] tilePartitionFanout(ListPage sub, TreeSet<byte[]> cuts) {
        TreeSet<byte[]> observed = new TreeSet<>(Arrays::compareUnsigned);
        for (KeyBytes cp : sortedCommonPrefixes(sub)) {
            observed.add(cp.raw());
        }
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

    /** Build the tiling {@code (⊥, c1], (c1, c2], …, (c_last, null]} from the sorted cut-points. */
    private static List<NodeSpec> tile(long runId, List<byte[]> cuts) {
        List<NodeSpec> specs = new ArrayList<>(cuts.size() + 1);
        byte[] lo = null;
        for (byte[] hi : cuts) {
            specs.add(seedTile(runId, lo, hi));
            lo = hi;
        }
        specs.add(seedTile(runId, lo, null));   // final open range (c_last, null]
        return specs;
    }

    /**
     * A fresh seed tile {@code (lo, hi]}. The engine resumes a fresh node from its
     * {@code cursor} (RangeScanner {@code start_after}) and uses {@code rangeStart} only
     * for split math, so the cursor starts at the lower bound {@code lo} — exactly like a
     * split child {@code (m, H]} whose cursor begins at {@code m}. {@code lo == null} is ⊥.
     */
    private static NodeSpec seedTile(long runId, byte[] lo, byte[] hi) {
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, lo, null);
    }

    /**
     * Adds this page's common prefixes to the global cut set and the descent frontier, returning
     * {@code {kept, discarded}}: how many of this page's common prefixes were newly added to the
     * global cut set vs. were already present (a duplicate cut byte string, e.g. two probed levels
     * sharing a boundary) — this level's OWN contribution to the seed decision trace.
     *
     * <p>Adds every new cut to {@code cuts} FIRST, then offers each one to {@code frontier} — so
     * {@link SpanPriorityFrontier#offer} scores every cut in this page against the OTHER cuts from
     * the SAME page (its immediate siblings), not just cuts from earlier pages. Scoring a cut before
     * its own siblings are in the set would blind {@link #spanScore} to the very sibling gap it is
     * measuring.
     *
     * @param scopeUpper the upper bound of the scope this page was probed in ({@link #scopeCeiling}
     *                   of the directory just probed, or {@link #topScopeCeiling} for the top level)
     *                   — the fallback {@link #spanScore} measures a cut's tail against when it has
     *                   no successor in {@code cuts} yet (see {@link Frontier#offer})
     */
    private static int[] addCutsCounted(ListPage page, TreeSet<byte[]> cuts, Frontier frontier, byte[] scopeUpper) {
        List<byte[]> added = new ArrayList<>();
        int kept = 0;
        int discarded = 0;
        for (KeyBytes cp : sortedCommonPrefixes(page)) {
            byte[] b = cp.raw();
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

    /** S3 returns common prefixes sorted, but the mock/other stores may not — sort defensively. */
    private static List<KeyBytes> sortedCommonPrefixes(ListPage page) {
        List<KeyBytes> cps = new ArrayList<>(page.commonPrefixes());
        cps.sort(Comparator.comparing(KeyBytes::rawUnsafe, Arrays::compareUnsigned));
        return cps;
    }

    /**
     * The smallest / largest SAFE ASCII code points a synthesized cut appends after a flat region's
     * prefix. The populated alphabet of a flat region (hex, base64, uuid, ISO timestamps, …) lives
     * in printable ASCII, so appending every printable-ASCII byte isolates each first-character
     * bucket into its own range — no single range keeps a disproportionate share. {@code 0x7F} (DEL)
     * is excluded (not a printable / XML-safe scalar).
     */
    private static final int APPEND_LO = 0x21;   // '!'
    private static final int APPEND_HI = 0x7E;   // '~'
    /**
     * {@code '%'} (0x25) is excluded from the appendable alphabet. A synthesized cut
     * becomes a {@code start-after}/{@code prefix} the engine later sends on the wire; at least
     * LocalStack echoes that parameter back verbatim (not re-percent-encoded) in the
     * {@code StartAfter}/{@code Prefix}/{@code Marker} response fields under {@code
     * encoding-type=url}. The AWS SDK's own (always-on, unconfigurable)
     * {@code DecodeUrlEncodedResponseInterceptor} strict-decodes those fields with {@code
     * java.net.URLDecoder} while unmarshalling the response, so a lone {@code '%'} there throws
     * {@code IllegalArgumentException("URLDecoder: Incomplete trailing escape (%) pattern")} and
     * aborts the whole listing — a self-inflicted crash, not an S3 error. No other printable-ASCII
     * scalar in {@code [APPEND_LO, APPEND_HI]} triggers this (only {@code '%'} starts an escape).
     */
    private static final int UNSAFE_SCALAR = '%';
    /** Width of the single-byte printable-ASCII radix alphabet [APPEND_LO, APPEND_HI], minus {@code '%'}. */
    private static final int SPAN = APPEND_HI - APPEND_LO;   // 93: the endpoint difference, which equals
                                                              // the 94-slot alphabet minus the one excluded
                                                              // '%' only because exactly one scalar is excluded
    /** The floor number of radix bands a dense flat region always gets (even at low worker counts). */
    private static final int MIN_BANDS = 8;

    /**
     * Pre-cut the recorded <b>heavy dense range</b> (a truncated {@code delimiter=/} level — or the
     * root — with only direct objects) into alphabet-uniform leading-byte RADIX bands —
     * an even spread over the printable-ASCII leading alphabet, since no keys are observed yet at seed
     * time (the observed-alphabet rank-space pivots apply only once a worker has fetched pages).
     *
     * <p><b>At most one region.</b> {@link #collectCutPoints} can record at most one flat-wide
     * region per run — the flat-top branch returns immediately after recording the root, and the
     * descent loop keeps only the FIRST truncated flat-wide sub-level it sees (a later one is still
     * classified and left whole, just does not take the banding slot — the descent itself no longer
     * stops there) — so this takes a single (possibly {@code null}) region rather than a list; there
     * is no live multi-region case to divide a shared band budget across.
     *
     * <p><b>Budget by mass/risk, not leftover.</b> A dense flat region is a serial-tail risk in its
     * own right, so its band count {@code K} is worker-proportional (the same spirit as
     * {@code targetSeeds = min(1000, 4·W)}) and <b>independent</b> of how much cut-point budget the
     * directory descent already spent — even a descent that used the whole seed budget must still band
     * a dense leaf. {@code K} is capped at the single-byte radix span ({@value #SPAN}), so a sparse
     * region can never explode into a probe/seed storm (over-cutting a sparse region is
     * correctness-harmless — any subset tiles — but stays bounded here).
     *
     * <p>Every populated key in the region is {@code dir·<scalar>·…} and sorts strictly below
     * {@code prefixCeil(dir)}, so appending a spread of safe scalars to {@code dir} yields cut-points
     * strictly inside {@code (dir, prefixCeil(dir)]} that partition the region by its leading scalar.
     * Returns the number of cut-points added (0 when {@code dir} is {@code null}). Preserves exact
     * tiling: each synthesized cut is {@code > dir} and strictly below the region's ceiling (hence
     * below the next existing cut).
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
     * spread uniformly over the SAFE printable-ASCII alphabet {@code [APPEND_LO, APPEND_HI] \
     * {UNSAFE_SCALAR}} <b>inclusive</b> (an even partition of the leading alphabet, not a
     * mass-weighted one — no keys are observed at seed time).
     *
     * <p>Each cut is a single-scalar band boundary {@code dir·<scalar>} synthesized directly by
     * {@link #withScalar} (I12 code-point-safe: {@code <scalar>} is a single XML-safe printable-ASCII
     * byte, never {@code '%'}). The cut points are placed at scalars evenly spaced across
     * the FULL span endpoints included, so a full-span band count enumerates every SAFE printable-ASCII
     * first character ({@code 0x21..0x7E} minus {@code '%'}) into its own band. This directly enumerates
     * the endpoints rather than interpolating strictly
     * <i>between</i> two anchors, which — by strict betweenness — could only ever emit interior scalars
     * ({@code 0x22..0x7D}), leaving the first/last printable characters unbanded. Every cut is
     * {@code > dir} and strictly below the region's ceiling {@code prefixCeil(dir)} (it shares {@code dir}
     * then appends one larger scalar), so any subset still tiles {@code (⊥, null]} exactly (I2/I3).
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

    /**
     * The {@code i}-th of {@code n} leading scalars spread evenly over the safe alphabet {@code
     * [APPEND_LO, APPEND_HI] \ {UNSAFE_SCALAR}}: {@code n == SPAN} enumerates every safe printable
     * scalar; {@code n == 1} degenerates to the alphabet midpoint. The blind band cuts (no keys
     * observed at seed time).
     */
    private static int spreadScalar(int i, int n) {
        int rank = (n == 1)
                ? (SPAN - 1) / 2
                : (int) Math.round((double) i * (SPAN - 1) / (n - 1));
        return safeScalar(rank);
    }

    /**
     * Map a 0-based rank in {@code [0, SPAN)} to its ASCII scalar in {@code [APPEND_LO, APPEND_HI]},
     * skipping {@code UNSAFE_SCALAR} ({@code '%'}) by shifting every rank at or past it up
     * by one. Monotonic and injective, so distinct ranks never collide on the same byte.
     */
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
            // The evenly-spaced-index formula below divides by (max - 1), which would be 0 when
            // max == 1 — an unhandled ArithmeticException. tilePartitionFanout's
            // `subsampleEvenly(observed, workerCount)` can call this with exactly that single-slot
            // cap (`workerCount` is floored at 1, not 4×W like `targetSeeds`, and
            // `--max-parallel-listings 1` is a valid config) while n > 1 cut-points are available, so
            // this case is handled first: a max<=1 cap degenerates to a single representative
            // cut-point (the sorted midpoint, avoiding either extreme corner) rather than crashing —
            // any subset (including a singleton) of a sorted cut set still tiles (⊥, null] exactly
            // (I2/I3), so this is correctness-neutral, just less parallel. `max <= 0` degenerates to
            // the same singleton rather than an empty/negative-size list.
            return List.of(all.get(n / 2));
        }
        List<byte[]> picked = new ArrayList<>(max);
        // Evenly spaced indices across [0, n-1] inclusive, deduped (already strictly increasing).
        for (int i = 0; i < max; i++) {
            int idx = (int) ((long) i * (n - 1) / (max - 1));
            picked.add(all.get(idx));
        }
        return picked;
    }

    // ---- Mass-aware seed descent helpers (all no-ops on the OFF path — never reached) ----------

    /**
     * S3's own maximum object key length in bytes — the length {@link #KEYSPACE_CEILING} is filled
     * to (see its javadoc for why).
     */
    private static final int MAX_S3_KEY_LEN = 1024;

    /**
     * A purely-local sentinel for {@link #spanScore}'s scope-upper-bound fallback (see {@link
     * #scopeCeiling}) when the enclosing scope has no real ceiling to fall back to: an unprefixed
     * (whole-bucket) top level, or the (never seen in practice) all-{@code 0xFF}-bytes edge case of
     * {@link StealMath#prefixCeil}. {@code 0xFF} repeated {@link #MAX_S3_KEY_LEN} bytes — the highest
     * possible byte value, for {@link #MAX_S3_KEY_LEN} bytes so {@link #spanScore}'s {@code
     * min(cut.length, next.length)} is never truncated shorter than any real key/cut by the
     * sentinel's own length (S3 keys are never longer than that).
     *
     * <p>Unlike {@link StealMath}'s wire-safe {@code MAX_UTF8_KEY} — which is transmitted on the wire
     * as a {@code start-after}/{@code prefix} parameter and so must stay valid UTF-8 — this value only
     * ever feeds {@link #spanScore}'s local {@code long} arithmetic and is discarded immediately after,
     * so it is free to use non-UTF8 {@code 0xFF} filler instead of being capped to a short valid-UTF-8
     * ceiling (which would truncate {@code min()} above and understate the tail's true width).
     */
    private static final byte[] KEYSPACE_CEILING = keyspaceCeiling();

    private static byte[] keyspaceCeiling() {
        byte[] b = new byte[MAX_S3_KEY_LEN];
        Arrays.fill(b, (byte) 0xFF);
        return b;
    }

    /**
     * The upper bound of the scope a cut was discovered in — {@link #spanScore}'s fallback measure
     * for a cut with no successor in the global cut set yet (see {@link Frontier#offer}):
     * {@link StealMath#prefixCeil(byte[])} of {@code scopePrefix} (nothing sorting under {@code
     * scopePrefix} can sort past its own ceiling), or {@link #KEYSPACE_CEILING} when {@code
     * scopePrefix} has no real ceiling (empty/{@code null} — a whole-bucket top level — or the
     * all-{@code 0xFF} edge case).
     */
    private static byte[] scopeCeiling(byte[] scopePrefix) {
        byte[] ceil = StealMath.prefixCeil(scopePrefix);
        return ceil != null ? ceil : KEYSPACE_CEILING;
    }

    /**
     * A monotone score for the keyspace gap from {@code cut} to its next sibling cut: an EARLIER first
     * differing byte (a coarser alphabet jump) and a LARGER delta both mean a wider span.
     *
     * <p>A cut with no successor <b>yet</b> in the global cut set ({@code cuts.higher(cut) == null})
     * falls back to {@code scopeUpper} — the upper bound of the scope {@code cut} was DISCOVERED in
     * (see {@link #scopeCeiling}): the ceiling of the directory being probed when the cut was offered,
     * or, for a top-level cut, the scan's own upper bound ({@link #topScopeCeiling}). That bound is
     * KNOWN at offer time — nothing sorting under the discovering scope can sort past it — so the tail
     * is scored as a genuine, FINITE gap by this same arithmetic, not pinned to {@link Long#MAX_VALUE}.
     *
     * <p>This replaces an earlier version that scored every successor-less cut {@link Long#MAX_VALUE}
     * unconditionally, regardless of scope. That fixed the top-level starvation this method's history
     * still needs to guard against — {@code s3://commoncrawl}'s top level has exactly two entries,
     * {@code contrib/} and {@code projects/}; {@code projects/} sorts last (no successor) and held
     * ~8.1M of the bucket's ~13.7M objects, but {@code contrib/}'s ever-deepening chain of narrow
     * subdirectories kept discovering new deepest cuts, and the ORIGINAL 0-for-no-successor scoring
     * pinned {@code projects/} at the lowest possible priority forever, so {@code projects/} was never
     * probed even once — but unconditional {@link Long#MAX_VALUE} overcorrected: once {@code
     * projects/} was finally probed, its OWN lexically-last child inherited the same unconditional
     * MAX_VALUE (it, too, had no successor yet), and so did that child's lexically-last grandchild, for
     * as long as the descent kept finding one — the frontier chased a single rightmost spine
     * depth-first through near-leaf directories instead of spreading across the wallet, the opposite
     * starvation. Scoping the fallback to the DISCOVERING prefix's own ceiling fixes both: a
     * lexically-last child's tail is bounded by its immediate parent's ceiling — an ordinary, narrow
     * span once the parent is not itself the scan's own top level — so only a genuine top-level tail
     * (the one case the original regression depended on) still scores wide.
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
     * Second-level disambiguation (§2): probe up to {@link #SAMPLE_WIDTH} evenly-spaced children
     * of a truncated-with-prefixes cut and decide heavy-subtree vs. 1:1 explosion. A child is "dense"
     * (heavy evidence) when it is itself truncated, has its own sub-directories, or holds at least
     * {@link #SAMPLE_DENSE_MIN_OBJECTS} direct objects; the cut is heavy iff a MAJORITY of sampled
     * children are dense (so one skewed sample cannot flip the call). Advances {@code probeState}
     * (total + sample sub-budget); never probes more than the remaining wallet allows.
     */
    private boolean sampleProvesHeavy(ListPage sub, int[] probeState) throws SwathException, InterruptedException {
        List<KeyBytes> cps = sortedCommonPrefixes(sub);
        int n = cps.size();
        if (n == 0) {
            return false;
        }
        int j = Math.min(SAMPLE_WIDTH, n);
        int dense = 0;
        int sampled = 0;
        for (int s = 0; s < j; s++) {
            if (probeState[1] >= SAMPLE_BUDGET || probeState[0] >= maxProbes) {
                break;
            }
            int idx = (j == 1) ? n / 2 : (int) ((long) s * (n - 1) / (j - 1));
            ListPage child = probe(cps.get(idx).raw());
            probeState[0]++;
            probeState[1]++;
            sampled++;
            if (child.truncated() || !child.commonPrefixes().isEmpty()
                    || child.entries().size() >= SAMPLE_DENSE_MIN_OBJECTS) {
                dense++;
            }
        }
        return sampled > 0 && dense * 2 >= sampled;
    }

    /**
     * Band a confirmed-heavy cut (§3.2): tile it along the child prefixes already in its probed
     * page — an evenly-subsampled subset capped at {@code workerCount} (the same {@code W} width
     * {@link #tilePartitionFanout} uses) — so its mass spreads over MANY seed ranges instead of one
     * serial tail. Zero extra probes (the page is already in hand). Reuses {@link #subsampleEvenly};
     * any sorted subset still tiles {@code (⊥, null]} exactly (I2/I3). Returns {@code {kept,
     * discarded}}.
     *
     * <p><b>A fixed width, not a remaining-capacity share.</b> This used to cap the band at {@code
     * targetSeeds - cuts.size()} — the budget still left under the (then global) cut-point cap. Now
     * that the descent runs past every exploding sibling instead of stopping at the first (see the
     * descent loop above), a cut reached late still deserves its full band even though the running
     * total has already passed {@code targetSeeds}: sizing the band off leftover capacity would
     * degenerate to a single cut for every heavy region the descent reaches after the first, no
     * matter how much real mass it holds — exactly the starvation this whole change fixes. The final
     * mass-weighted subsample (once the descent completes) is what allocates the {@code targetSeeds}
     * budget across every heavy region proportionally; this band only needs to be wide enough to give
     * that step real interior structure to weigh.
     */
    private int[] bandHeavyCut(ListPage sub, TreeSet<byte[]> cuts) {
        TreeSet<byte[]> observed = new TreeSet<>(Arrays::compareUnsigned);
        for (KeyBytes cp : sortedCommonPrefixes(sub)) {
            observed.add(cp.raw());
        }
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
     * Estimate per-cut mass for an over-cap wide top (§4): probe up to the remaining sample
     * sub-budget of evenly-spaced cuts and weigh each by its immediate fan-out — {@link #PROBE_PAGE}
     * when the child is itself truncated (unknown-large), else its common-prefix + object count, floored
     * at 1. Cuts not sampled default to weight 1 in {@link #massWeightedSubsample}. Advances
     * {@code probeState}.
     */
    private NavigableMap<byte[], Long> sampleCutWeights(TreeSet<byte[]> cuts, int[] probeState)
            throws SwathException, InterruptedException {
        NavigableMap<byte[], Long> weights = new TreeMap<>(Arrays::compareUnsigned);
        List<byte[]> all = new ArrayList<>(cuts);
        int n = all.size();
        int budget = Math.min(SAMPLE_BUDGET - probeState[1], n);
        for (int s = 0; s < budget; s++) {
            if (probeState[1] >= SAMPLE_BUDGET || probeState[0] >= maxProbes) {
                break;
            }
            int idx = (budget == 1) ? n / 2 : (int) ((long) s * (n - 1) / (budget - 1));
            byte[] cut = all.get(idx);
            ListPage page = probe(cut);
            probeState[0]++;
            probeState[1]++;
            long weight = page.truncated()
                    ? PROBE_PAGE
                    : (long) page.commonPrefixes().size() + page.entries().size();
            weights.put(cut, Math.max(1L, weight));
        }
        return weights;
    }

    /**
     * Weight-proportional subsample (§4): allocate the {@code max}-cut budget by walking the
     * sorted cuts and emitting one each time the running weight crosses a {@code total/max} threshold,
     * so a heavy (high-weight) region keeps proportionally more interior cuts than an empty one — versus
     * {@link #subsampleEvenly}'s positional spacing. A cut absent from {@code weights} counts as weight
     * 1. Any subset of the sorted cut set still tiles {@code (⊥, null]} exactly (I2/I3).
     *
     * <p><b>The next threshold is anchored to the accumulated weight actually banked at each pick, not
     * to a fixed {@code prevThreshold + step} grid.</b> A single very-heavy cut (e.g. an unprobed
     * subtree sampled at {@link #PROBE_PAGE}, weight 1000, sitting among a sea of weight-1 cuts) can
     * jump {@code acc} far past several grid thresholds in one step. Advancing the threshold by a
     * fixed {@code step} would leave it far behind {@code acc}, so every ORDINARY-weight cut
     * immediately following the heavy one would also satisfy the (stale) crossing check and get
     * auto-picked — a landslide of consecutive picks that drains the whole {@code max} budget on
     * whatever happens to sort right after the heavy cut, instead of spreading proportionally across
     * the rest of the sorted set (package-visible for {@code SeedStepMassWeightedSubsampleTest}, which
     * reconstructs this exact wide-sibling shape).
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
                next = acc + step;   // anchor to the banked weight, not the stale grid — see javadoc
            }
        }
        if (picked.isEmpty()) {
            picked.add(all.get(n / 2));
        }
        return picked;
    }

    /** The greatest sort key on a page (max of direct objects and common prefixes) — the +1-page cursor. */
    private static byte[] lastSeenKey(ListPage page) {
        byte[] last = null;
        for (ListEntry e : page.entries()) {
            byte[] k = e.key().raw();
            if (last == null || Arrays.compareUnsigned(k, last) > 0) {
                last = k;
            }
        }
        for (KeyBytes cp : page.commonPrefixes()) {
            byte[] k = cp.rawUnsafe();
            if (last == null || Arrays.compareUnsigned(k, last) > 0) {
                last = k;
            }
        }
        return last;
    }

    private ListPage probe(byte[] probePrefix) throws SwathException, InterruptedException {
        return probe(probePrefix, null);
    }

    /**
     * As {@link #probe(byte[])}, paginating from {@code startAfter} (exclusive) — used only by the
     * bounded +1-page TOP-level pagination (§5) to recover a truncated top's overflow prefixes.
     */
    private ListPage probe(byte[] probePrefix, byte[] startAfter) throws SwathException, InterruptedException {
        ListPage page = fetcher.fetchPage(
                PageRequest.objectsDelimited(probePrefix, DELIMITER, startAfter, PROBE_PAGE));
        // The delimiter=/ fan-out this seed probe observed (§5 classification).
        if (metrics != null) {
            metrics.recordDelimiterFanout(page.commonPrefixes().size());
            // The seed step's own bounded descent (up to maxProbes ~256 sequential probes,
            // algorithms.md §8) is armed by the watchdog BEFORE the engine's first page ever
            // completes, so this call advances progressSignal() once per completed probe (never per
            // key — the seed never enumerates keys): a slow/throttled fresh seed on a real endpoint
            // (each probe bounded by the apiCallTimeout, not by seed-step logic) could otherwise
            // exceed the stall window with the signal pinned at 0 and be falsely cancelled
            // (STUCK)/halted despite each probe actually completing. A seed truly wedged inside a
            // single probe (zero probes ever completing) still correctly trips the watchdog.
            metrics.markProgress();
        }
        return page;
    }
}
