/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Why one page-commit's owner-side proactive self-split consideration produced {@link Skip}
 * (algorithms.md §3.3). {@link #code()} is the {@code RunMetrics#recordStealReason("OWNER_SPLIT",
 * code())} reason string a gate's {@link Engagement} uses when it does fire one — not every gate
 * does: unlike {@link NoVictimReason}/{@link RetryReason}, whose owning executor always records the
 * terminal reason verbatim, {@link Skip#reason()} here is a plain discriminator the executor and
 * tests read directly; whether a gate ALSO contributes a counted {@link Engagement} is a separate,
 * per-gate call documented below (AGENTS.md's instrument-every-path law, applied gate by gate).
 */
public enum OwnerSplitSkipReason {
    /**
     * The range is unbounded ({@code hi == null}). Deliberately UNCOUNTED: this is not a gate
     * declining a carve, it is a path that structurally does not apply — an open frontier has no
     * far tail to carve off, so there is nothing for post-hoc analysis to learn from a rate here
     * that {@code hi}-bounded-ness itself doesn't already say. Never emits an {@link Engagement}.
     */
    OPEN_FRONTIER("open_frontier"),
    /**
     * The estimated remaining work does not clear {@code SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys}
     * — a genuine suppressed carve, currently UNCOUNTED (issue #16: no {@link Engagement} fires
     * here yet, violating AGENTS.md's instrument-every-path law).
     */
    REMAINING_EST_FLOOR("remaining_est_floor"),
    /**
     * Fewer than {@code SELF_SPLIT_MIN_PAGES_BETWEEN} committed pages have passed since this
     * range's last published self-split — a genuine suppressed carve (the O(1)-per-drain
     * rate-limit engaging), currently UNCOUNTED: no {@link Engagement} fires here yet.
     */
    RATE_LIMITED("rate_limited"),
    /** The demand/saturation gate suppressed the carve: {@code OWNER_SPLIT.demand_gated}. */
    DEMAND_GATED("demand_gated"),
    /**
     * The observed-mass child-tail floor blocked a confetti-sized carve:
     * {@code OWNER_SPLIT.floor_reflected_blocked}.
     */
    FLOOR_REFLECTED_BLOCKED("floor_reflected_blocked"),
    /**
     * The realized-child-mass confetti feedback gate suppressed the carve:
     * {@code OWNER_SPLIT.confetti_suppressed}.
     */
    CONFETTI_SUPPRESSED("confetti_suppressed"),
    /**
     * The synthesized pivot was {@code null}, or not strictly inside {@code (cursorTo, hi]} —
     * unsplittable this page, or the interpolation didn't land in range. Uncounted today (moved
     * verbatim from {@code OwnerSelfSplit}, not a gap this extraction introduces or resolves — see
     * this slice's report for the open question of whether it deserves one).
     */
    UNSPLITTABLE_PIVOT("unsplittable_pivot");

    private final String code;

    OwnerSplitSkipReason(String code) {
        this.code = code;
    }

    /** The {@code RunMetrics#recordStealReason("OWNER_SPLIT", ...)} reason string, when recorded. */
    public String code() {
        return code;
    }
}
