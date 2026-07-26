/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Which of the four structurally-different situations selection's aggregate {@code
 * NO_VICTIM.no_splittable_victim} refusal folds together (algorithms.md §3), or
 * {@link #NO_SPLITTABLE_VICTIM} itself, the aggregate always fired alongside exactly one
 * discriminator so the two counter series stay reconcilable. {@link #code()} is the exact
 * {@code RunMetrics#recordStealReason("NO_VICTIM", code())} reason string the current engine
 * already emits.
 */
public enum NoVictimReason {
    /** The pool passed to selection was empty. */
    POOL_EMPTY("pool_empty"),
    /** Every eligible candidate read {@code estRemaining <= 0}. */
    ALL_NO_REMAINING_SPAN("all_no_remaining_span"),
    /** Every candidate is in a per-victim futility cooldown. */
    ALL_FUTILITY_PACED("all_futility_paced"),
    /** Every candidate is cached unsplittable. */
    ALL_UNSPLITTABLE("all_unsplittable"),
    /** No single skip reason explains every candidate's exclusion. */
    MIXED_SKIPS("mixed_skips"),
    /** The aggregate refusal, always fired alongside exactly one of the above. */
    NO_SPLITTABLE_VICTIM("no_splittable_victim");

    private final String code;

    NoVictimReason(String code) {
        this.code = code;
    }

    /** The exact {@code RunMetrics#recordStealReason("NO_VICTIM", ...)} reason string. */
    public String code() {
        return code;
    }
}
