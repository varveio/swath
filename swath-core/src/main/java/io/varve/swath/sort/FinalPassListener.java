/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.output.sorted.SortedDatasetCoordinator;

/**
 * Invoked by {@link SortedDatasetCoordinator} once, when the merge stops folding intermediates and starts
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
     *         staged rows, and so has an honest completion fraction. False whenever remaining work
     *         can still rewrite rows before the final pass.
     */
    void onFinalPassStarting(boolean stagedRowsAreTheDenominator);
}
