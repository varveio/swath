/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The numeric inputs {@link OwnerSplitGovernor#decide} read on its way to a terminal gate, plus the
 * reason string that gate reports — the payload of the {@code owner_split_decision} trace event
 * (docs/internals/metrics-internals.md §7). Carried on the {@link OwnerSplitDecision} because the
 * gate chain is a PURE policy: it never holds the {@code TraceSink}, so the executor ({@code
 * OwnerSelfSplit}/{@code WorkStealingScan}) emits the event out of what the decision returns,
 * exactly as it already records the decision's {@link Engagement}s (contracts.md §2.1).
 *
 * <p><b>Short-circuit convention.</b> The chain short-circuits, so an input only a gate BELOW the
 * terminal one would have read was never computed: those components carry {@link #NOT_COMPUTED}
 * ({@code NaN}) rather than a plausible-looking zero (an integral one would carry {@code -1}).
 * Today that is exactly {@code farAheadFraction}/{@code densityRatio} on the three gates above the
 * observed-mass floor ({@code remaining_est_floor}/{@code rate_limited}/{@code demand_gated}) —
 * every other component is read before the first gate — but the convention is part of the contract
 * so a later gate reordering stays expressible without a schema break.
 *
 * <p><b>The reason is the GATE CHAIN's terminal reason</b>, not the executor's outcome: a carve the
 * chain admitted can still fail to publish executor-side (a lost confetti probe-slot claim, a
 * rejected split CAS), which the trace shows as the absence of a following {@code owner_split}
 * event rather than as a different reason here.
 *
 * @param reason                  the terminal gate's reason string — the same code the {@code
 *                                OWNER_SPLIT.*} counters use ({@link OwnerSplitSkipReason#code()},
 *                                {@code confetti_probe}, {@code pivot_reflect_clamped}, {@code
 *                                pivot_reflect_lifted}, or {@code self_published} for a plain carve)
 * @param est                     the position sensor's {@code estRemaining} reading for this commit
 * @param pagesSinceLastSelfSplit {@code committed - lastSelfSplitPage}, the rate limit's input
 * @param outstanding             the live-node count the demand gate read
 * @param workerCount             the configured {@code Tmax} the demand gate compared it against
 * @param farAheadFraction        the toggle-resolved far-ahead pivot fraction {@code f}
 * @param densityRatio            the toggle-resolved observed local-vs-average density ratio
 * @param keysEmitted             keys emitted so far on this range (the sensor's own input)
 */
public record OwnerSplitGateInputs(
        String reason,
        double est,
        long pagesSinceLastSelfSplit,
        long outstanding,
        int workerCount,
        double farAheadFraction,
        double densityRatio,
        long keysEmitted) {

    /** The sentinel for a double input the short-circuiting chain never computed. */
    public static final double NOT_COMPUTED = Double.NaN;
}
