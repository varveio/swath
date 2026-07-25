/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Invoked by {@link SortTransform} once, when the merge stops folding intermediates and starts
 * writing the output it will publish — the seam {@code ListRunner} wires to {@code
 * RunMetrics.startFinalMergePass}, so the {@code swath.phase} gauge reaches {@code WRITING} instead
 * of folding the whole merge+publish into {@code MERGING}. {@link #NO_OP} observes nothing.
 */
@FunctionalInterface
public interface FinalPassListener {

    /** Observes nothing — the null-object implementation. */
    FinalPassListener NO_OP = stagedRowsAreTheDenominator -> { };

    /**
     * @param stagedRowsAreTheDenominator whether the work from here on is exactly ONE pass over the
     *         staged rows, and so has an honest completion fraction. False whenever the remaining
     *         work still cascades — the serial path's cascade passes are complete by the time this
     *         fires, but a parallel range merge's are not, and a cascading range rewrites its rows
     *         once per pass, which is how cumulative merge work runs past the staged total.
     */
    void onFinalPassStarting(boolean stagedRowsAreTheDenominator);
}
