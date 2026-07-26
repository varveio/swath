/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Why one {@code AlphabetDigest.chooseScalar} consult produced no scalar (algorithms.md §3.3).
 * {@code io.varve.swath.engine}'s {@code AlphabetDigest} reports this into a caller-owned {@link
 * Engagement} collector rather than a {@code RunMetrics} field of its own (issue #19's fix) — the
 * two call sites that consult it ({@code ThiefPolicy}'s pivot cascade, {@code
 * OwnerSplitGovernor}'s carve) already thread a {@code List<Engagement>} through every other
 * decision point, so the alphabet consult now uses the identical mechanism instead of a private
 * side channel to the executor.
 */
public enum AlphabetFallback {
    /**
     * The consult position fell outside the tracked/clean window: {@code
     * ALPHABET.fallback_out_of_window}.
     */
    FALLBACK_OUT_OF_WINDOW("fallback_out_of_window"),
    /**
     * No room for any scalar strictly between the consult's bounds: {@code
     * ALPHABET.fallback_no_room}.
     */
    FALLBACK_NO_ROOM("fallback_no_room"),
    /** The observed alphabet had no value populating the consult's gap: {@code ALPHABET.window_gap}. */
    WINDOW_GAP("window_gap");

    private final String code;

    AlphabetFallback(String code) {
        this.code = code;
    }

    /** The {@code RunMetrics#recordStealReason("ALPHABET", ...)} reason string. */
    public String code() {
        return code;
    }
}
