/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The named pivot-placement mechanism a steal attempt committed on, or engaged mid-cascade
 * (algorithms.md §3/§3.1/§3.3). {@link #code()} is the exact lower-snake-case
 * {@code RunMetrics#recordStealReason("PIVOT", code())} reason string the current engine already
 * emits — unchanged by the extraction (AGENTS.md: counter names never change).
 *
 * <p>Twelve of these ({@link #MIDPOINT} through {@link #FLAT_LEAF}) are the pivot cascade's pinned
 * mechanisms (decision-trace-goldens.md's coverage matrix); {@link #EXTRAPOLATE} is the thirteenth
 * {@code PIVOT.*} value the engine emits — the trivial open-frontier case, which has no cascade
 * (density extrapolation toward the prefix ceiling, never a bisection/structure/reflect step).
 */
public enum PivotMechanism {
    /** The plain code-point midpoint — also the far-ahead step-back's re-probed fallback. */
    MIDPOINT("midpoint"),
    /** The far-ahead interpolated pivot (fraction {@code > 0.5}, algorithms.md §3.3). */
    FAR_AHEAD("far_ahead"),
    /** Far-ahead placement stepped back to the plain midpoint after an empty upper probe. */
    STEP_BACK("step_back"),
    /** The empty-upper retry-nearer-cursor bisection committed. */
    BISECT("bisect"),
    /** The density-reflected pivot committed (probe hit). */
    REFLECT("reflect"),
    /** Engagement mark: the density-reflection probe hit (fires before {@link #REFLECT} commits). */
    REFLECT_HIT("reflect_hit"),
    /** Engagement mark: the density-reflection probe missed (seeds the bisection interval instead). */
    REFLECT_EMPTY("reflect_empty"),
    /** The empty-upper {@code delimiter=/} structure probe's median boundary committed. */
    STRUCTURE_PROBE("structure_probe"),
    /** As {@link #STRUCTURE_PROBE}, but the probe page was truncated (furthest proved boundary). */
    STRUCTURE_CAPPED("structure_capped"),
    /** The parent-empty sliver's coarse→fine adaptive structure back-out committed. */
    ADAPTIVE_STRUCTURE("adaptive_structure"),
    /** As {@link #ADAPTIVE_STRUCTURE}, but the winning probe page was truncated. */
    ADAPTIVE_STRUCTURE_CAPPED("adaptive_structure_capped"),
    /** The parent-empty sliver's flat-leaf density-reflected pivot committed. */
    FLAT_LEAF("flat_leaf"),
    /** The open-frontier density-extrapolated pivot (no bounded cascade — algorithms.md §3.1). */
    EXTRAPOLATE("extrapolate");

    private final String code;

    PivotMechanism(String code) {
        this.code = code;
    }

    /** The exact {@code RunMetrics#recordStealReason("PIVOT", ...)} reason string. */
    public String code() {
        return code;
    }
}
