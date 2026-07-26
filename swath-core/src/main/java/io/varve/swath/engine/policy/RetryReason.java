/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The reason a steal attempt's <b>pre-lock, policy-owned</b> cascade returned {@code RETRY}
 * (algorithms.md §3). {@link #code()} is the exact {@code RunMetrics#recordStealReason("RETRY",
 * code())} reason string the current engine already emits.
 *
 * <p>Only the cascade's own RETRYs are here. The lock-guarded re-validate-and-narrow hand-off's
 * RETRYs ({@code bound_moved}, {@code cursor_passed_pivot}, {@code split_aborted}) and the
 * probe-throttle fail-fast ({@code probe_retry_cap_failfast}) stay executor-owned string literals —
 * they arise entirely from live lock/CAS/RPC-exception state the policy never sees (the executor
 * owns the lock, the snapshot→CAS re-validate, and RPC issuing — seam-notes.md).
 */
public enum RetryReason {
    /** This victim's exact {@code (cursor, hi)} snapshot already proved non-productive. */
    UNCHANGED_NONPRODUCTIVE_SNAPSHOT("unchanged_nonproductive_snapshot"),
    /** The victim's cursor has already reached or passed the speculatively-read {@code hi}. */
    CURSOR_AT_OR_PAST_HI("cursor_at_or_past_hi"),
    /** An open frontier whose owner hasn't committed page 1 yet — transient, never cached. */
    UNSTARTED_FRONTIER("unstarted_frontier"),
    /** The empty-upper bisection produced a {@code null} pivot (cursor and the live head adjacent). */
    RETRY_PIVOT_ADJACENT("retry_pivot_adjacent"),
    /** The log-scaled empty-upper bisection budget exhausted without a non-empty probe. */
    BISECT_BUDGET_EXHAUSTED("bisect_budget_exhausted");

    private final String code;

    RetryReason(String code) {
        this.code = code;
    }

    /** The exact {@code RunMetrics#recordStealReason("RETRY", ...)} reason string. */
    public String code() {
        return code;
    }
}
