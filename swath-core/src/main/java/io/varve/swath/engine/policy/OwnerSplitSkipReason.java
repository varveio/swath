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
     * The range is unbounded ({@code hi == null}) — {@code OWNER_SPLIT.open_frontier}. Not a gate
     * declining a carve: it is a path that structurally does not apply, because an owner carve
     * interpolates a midpoint between {@code lo} and {@code hi}, and a {@code null} upper bound has
     * no midpoint. <b>That interpolation argument holds — but the corollary once drawn from it, "so
     * there is nothing for post-hoc analysis to learn from a rate here", does not (issue #76).</b>
     * {@code (lastCut, null]} can hold an arbitrary share of a bucket's mass — most of it, on a
     * keyspace whose mass sits past the last seed cut — and it is the one range the owner can never
     * self-split off. Left uncounted, the metrics cannot tell "the tail sat on the open frontier"
     * apart from "the tail was gate-blocked" when a run's tail turns out serial. So — unlike the
     * gates below, whose rate this counter's neighbors is compared against — this counter's own
     * population (qualifying page commits against an unbounded range) is the diagnostic: alongside
     * {@code swath.open_frontier.keys_emitted} (the keys drained from that range), it tells whether a
     * slow tail sat here at all, and how much of the run it carried.
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
