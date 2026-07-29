/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.HybridSeedPlanner;
import io.varve.swath.engine.policy.RequestSeedProbe;
import io.varve.swath.engine.policy.SeedAction;
import io.varve.swath.engine.policy.SeedDescent;
import io.varve.swath.engine.policy.SeedLevelDecision;
import io.varve.swath.engine.policy.SeedPlan;
import io.varve.swath.engine.policy.SeedPlanner;
import io.varve.swath.engine.policy.SeedProbeOutcome;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * <p><b>The descent itself — the frontier, the per-level classification, and the cut-set
 * assembly — is {@link HybridSeedPlanner}, a {@link SeedPlanner} decided by observed probe
 * results alone (no RPC, no page decode, no node insertion); this class drives that decision to
 * completion by issuing the probes it requests via {@link #probe}, decoding each page into a
 * {@link SeedProbeOutcome}, and turning the finished {@link SeedPlan} into fresh {@code NodeSpec}
 * tiles.</b>
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
 * from a 1:1 explosion (see {@link HybridSeedPlanner}). A flat-wide level (truncated, no {@code
 * CommonPrefixes}, at least one direct object) is classified from that page shape alone — the seed
 * inspects no key bytes and issues no further probe — and is pre-cut straight into leading-byte
 * radix bands.
 */
public final class SeedStep {

    private static final Logger log = LoggerFactory.getLogger(SeedStep.class);

    private final PageFetcher fetcher;
    private final RunMetrics metrics;
    private final EngineToggles toggles;
    private final SeedPlanner planner;

    /**
     * @param prefix      the listing prefix {@code P} ({@code null}/empty = whole bucket)
     * @param workerCount the engine's target concurrency; the seed aims for roughly a small
     *                    multiple of this many ranges (enough parallelism, never over-fragmenting)
     * @param metrics     the run's metrics sink, or {@code null} when unwired (e.g. a test)
     * @param toggles     {@code toggles.radixBands() == false} skips the flat-wide radix banding
     *                    entirely, leaving a dense flat region as one un-subdivided range (see
     *                    {@link EngineToggles}'s javadoc)
     */
    public SeedStep(PageFetcher fetcher, byte[] prefix, int workerCount, RunMetrics metrics, EngineToggles toggles) {
        this.metrics = metrics;
        this.fetcher = fetcher;
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
        this.planner = new HybridSeedPlanner(prefix, workerCount, this.toggles);
        if (metrics != null) {
            metrics.recordSeedProbeBudget(planner.probeBudget());   // the seed phase's exact denominator
        }
    }

    /**
     * The seed worklist for a <b>fresh</b> run: a set of {@link NodeSpec} that exactly
     * tiles {@code (⊥, null]}. Resume never calls this (it reloads existing nodes via
     * {@code loadResumable}).
     */
    public List<NodeSpec> seedSpecs(long runId, SeedMode mode) throws SwathException, InterruptedException {
        return switch (mode) {
            case NONE -> {
                recordSeedSummary("none", 0, 0, 0, 0, 1, List.of());
                yield List.of(NodeSpec.rootRange(runId));
            }
            case SHALLOW -> shallow(runId);
            case HINTS -> throw new InvalidConfigException(
                    "--seed hints requires a --hints cut-points file, which is not yet implemented");
        };
    }

    /**
     * Drives a fresh {@link SeedDescent} to its terminal {@link SeedPlan}: issue every
     * {@link RequestSeedProbe} it returns, decode the resulting page into a {@link SeedProbeOutcome},
     * and feed that back — mirroring how {@code Thief} drives {@code ThiefPolicy}'s
     * {@code StealAttempt} (algorithms.md §3) through the identical request/response shape.
     */
    private List<NodeSpec> shallow(long runId) throws SwathException, InterruptedException {
        SeedDescent descent = planner.beginDescent();
        SeedAction action = descent.start();
        while (true) {
            applyEngagements(action.engagements());
            if (action instanceof RequestSeedProbe request) {
                ListPage page = probe(request.probePrefix(), request.startAfter());
                action = descent.onProbeResult(toOutcome(page));
            } else if (action instanceof SeedPlan plan) {
                return finish(runId, plan);
            } else {
                throw new IllegalStateException("unhandled SeedAction: " + action);
            }
        }
    }

    /** Record every {@link Engagement} the planner returned — the executor's own metrics-presence gate. */
    private void applyEngagements(List<Engagement> engagements) {
        if (metrics == null) {
            return;
        }
        for (Engagement e : engagements) {
            metrics.recordStealReason(e.category(), e.reason());
        }
    }

    /**
     * Tiles the planner's finished cut set into fresh {@code NodeSpec} ranges and reports the run's
     * seed-phase summary (§5) — the JSON run-summary's {@code seed} block, including the per-level
     * decision trace, and the {@code seed_shallow} log line.
     */
    private List<NodeSpec> finish(long runId, SeedPlan plan) {
        List<NodeSpec> specs = tile(runId, plan.cuts());
        if (plan.synthesizedCuts() > 0 && metrics != null) {
            metrics.recordSeedBands(plan.synthesizedCuts());
        }
        recordSeedSummary("shallow", plan.probes(), plan.cuts().size(), plan.synthesizedCuts(),
                plan.cutsDiscovered(), specs.size(), toRunMetricsDecisions(plan.decisions()));
        log.info("seed_shallow run_id={} probes={} cut_points={} synthesized_cuts={} cuts_discovered={} "
                        + "seed_ranges={}",
                runId, plan.probes(), plan.cuts().size(), plan.synthesizedCuts(), plan.cutsDiscovered(),
                specs.size());
        return specs;
    }

    /** Promotes the policy-domain decision trace, field-for-field, into the observability shape. */
    private static List<RunMetrics.SeedProbeDecision> toRunMetricsDecisions(List<SeedLevelDecision> decisions) {
        List<RunMetrics.SeedProbeDecision> out = new ArrayList<>(decisions.size());
        for (SeedLevelDecision d : decisions) {
            out.add(new RunMetrics.SeedProbeDecision(d.prefix(), d.fanout(), d.truncated(), d.classification(),
                    d.cutsKept(), d.cutsDiscarded(), d.depth(), d.quotaCutOff()));
        }
        return out;
    }

    /**
     * Promotes the seed step's already-computed shape (§5) plus the per-level decision trace into
     * the JSON run-summary's {@code seed} block ({@link RunMetrics#recordSeedSummary}). A no-op
     * when metrics are not wired (e.g. a unit test using the metrics-less constructor).
     */
    private void recordSeedSummary(String mode, long probes, long cutPoints, long synthesizedCuts,
            long cutsDiscovered, long ranges, List<RunMetrics.SeedProbeDecision> decisions) {
        if (metrics != null) {
            metrics.recordSeedSummary(mode, probes, cutPoints, synthesizedCuts, cutsDiscovered, ranges, decisions);
        }
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
     * Distills a decoded page into the policy-domain {@link SeedProbeOutcome} the planner consumes —
     * the source-agnostic seam (contracts.md §2.1): {@link ListPage}, {@link KeyBytes}, and
     * {@link ListEntry} never cross into {@code io.varve.swath.engine.policy}.
     */
    private static SeedProbeOutcome toOutcome(ListPage page) {
        List<byte[]> commonPrefixes = new ArrayList<>(page.commonPrefixes().size());
        for (KeyBytes cp : page.commonPrefixes()) {
            commonPrefixes.add(cp.raw());
        }
        return new SeedProbeOutcome(commonPrefixes, page.truncated(), page.entries().size(), lastSeenKey(page));
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
        ListPage page = fetcher.fetchPage(PageRequest.objectsDelimited(
                probePrefix, HybridSeedPlanner.DELIMITER, startAfter, HybridSeedPlanner.PROBE_PAGE));
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
            // One completed probe, for the live seed-phase display: the count against maxProbes is
            // the only exact completion figure this phase has, and the age of the last one is the
            // only evidence a seeding run (zero objects, zero pages, zero workers) is still alive.
            metrics.recordSeedProbe();
        }
        return page;
    }
}
