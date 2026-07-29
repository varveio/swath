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
     * — a genuine suppressed carve: {@code OWNER_SPLIT.remaining_est_floor} (issue #16).
     */
    REMAINING_EST_FLOOR("remaining_est_floor"),
    /**
     * Fewer than {@code SELF_SPLIT_MIN_PAGES_BETWEEN} committed pages have passed since this
     * range's last published self-split — a genuine suppressed carve (the O(1)-per-drain
     * rate-limit engaging): {@code OWNER_SPLIT.rate_limited}.
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
     * The realized-child-mass carve brake suppressed the carve — distinct from {@link
     * #CONFETTI_SUPPRESSED}: the brake reads the recent window-average MASS TREND of tagged
     * children rather than confetti's binary degenerate/substantial rate (campaign memo §5, the
     * serial-tail over-carving cure). {@code OWNER_SPLIT.carve_braked}. Gated on {@code
     * --engine-toggle carve_brake=MODE} (default {@code off} in this commit).
     */
    CARVE_BRAKED("carve_braked"),
    /**
     * The synthesized pivot was {@code null}, or not strictly inside {@code (cursorTo, hi]} — no
     * safe key exists strictly between {@code cursorTo} and {@code hi} THIS page, or the
     * interpolation didn't land in range. RECURS, unlike a one-off edge case: {@code
     * StealMath.estRemaining}'s span heuristic can diverge from true byte-adjacency on a deep shared
     * prefix — the same measurement/reality gap algorithms.md §3.2 documents on the thief side — so
     * this path is reachable in production, not merely a defensive check. Recorded as {@code
     * OWNER_SPLIT.unsplittable_pivot}.
     *
     * <p><b>Not the same durability as {@link UnsplittableReason#NO_PIVOT}</b> ({@code
     * UNSPLITTABLE.no_pivot}): the thief's terminal outcome permanently caches the victim as
     * unsplittable ({@code VictimMutation.Kind#SET_UNSPLITTABLE}) because a thief's lock-guarded
     * snapshot is coherent — a `null` pivot there really is a dead range. This gate's `Skip` is
     * transient and per-attempt: the owner took no coherent snapshot (owner-split is zero-probe by
     * construction), nothing here is cached, and the SAME range is reconsidered at its next
     * qualifying page-commit, with a fresh {@code cursorTo} that may no longer be adjacent to {@code
     * hi}. Deliberately a distinct counter, not a reuse of {@code UNSPLITTABLE.no_pivot}, so post-hoc
     * analysis never conflates a permanently-dead range with a range that just hasn't drained past
     * this page's transient adjacency yet.
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
