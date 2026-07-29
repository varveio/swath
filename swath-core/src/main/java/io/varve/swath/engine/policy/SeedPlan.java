/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The descent's terminal decision (algorithms.md §8): the finished, sorted cut-point set — already
 * capped to the target seed count and, when applicable, mass-weighted-subsampled and radix-band
 * synthesized — plus the per-level decision trace (with the two run-level-only classification
 * rewrites, {@code dense_root_radix_banded}/{@code delimiter_seeded}, already applied). The executor
 * tiles {@link #cuts} into {@code NodeSpec} ranges and reports {@link #probes}/{@code cuts.size()}/
 * {@link #synthesizedCuts}/the tiled range count/{@link #decisions} to the run summary verbatim — no
 * further classification decision remains to be made on the executor side.
 *
 * <p>Deliberately does NOT carry {@code topTruncated}, {@code tinyLeafExplosion}, {@code fanoutTiled},
 * {@code heavyCutBanded}, or {@code massWeighted}: every engagement mark those flags used to gate
 * (algorithms.md §5) is already resolved into this action's own {@link #engagements()}, and nothing
 * else on the executor side branches on them — carrying them here would expose how the current HYBRID
 * descent happens to be internally structured, not what a planner's output need be.
 *
 * @param cuts            the final, sorted cut-point set — a subset that still tiles {@code (⊥, null]}
 *                        exactly, whatever it contains, including empty (the flat-trivial case)
 * @param synthesizedCuts how many of {@code cuts} were synthesized (dense-root radix banding) rather
 *                        than observed from a probe
 * @param cutsDiscovered  how many DISTINCT cut points the descent actually found via probing, BEFORE
 *                        any over-cap subsample reduced that set toward {@code targetSeeds} (issue
 *                        #83's instrumentation: a {@code cuts.size() > cutsDiscovered} reader alone
 *                        cannot be sure whether a low final cut count means the descent found little,
 *                        or found plenty and a subsample discarded most of it — this makes that
 *                        distinction directly readable post-hoc rather than requiring a probe-log
 *                        replay). Equal to {@code cuts.size()} whenever no subsample ran.
 * @param probes          the total number of probes this descent issued
 * @param decisions       the per-level decision trace, in probe order
 */
public record SeedPlan(List<byte[]> cuts, int synthesizedCuts, int cutsDiscovered, int probes,
                       List<SeedLevelDecision> decisions, List<Engagement> engagements) implements SeedAction {
}
