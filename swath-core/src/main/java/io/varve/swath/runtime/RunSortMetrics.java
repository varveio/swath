/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.sort.SortMetrics;

/**
 * Wires the sort library's {@link SortMetrics} hook to the live {@link RunMetrics}.
 *
 * <p>This exists as a named class rather than the method reference it replaced
 * ({@code ctx.metrics()::recordStealReason}) for one reason: {@link SortMetrics} is a
 * {@code @FunctionalInterface}, so a method reference can only ever supply
 * {@code recordStealReason} and silently inherits the no-op default for
 * {@link SortMetrics#markProgress()}. That is precisely how the parallel range merge came to run
 * its two pre-emission scan phases without advancing the liveness signal, and how a 120 s watchdog
 * came to halt healthy billion-object listings.
 *
 * <p>A named bridge is testable, and {@code RunSortMetricsTest} asserts both methods forward — so
 * adding a third hook to {@link SortMetrics} cannot quietly go unwired again.
 */
record RunSortMetrics(RunMetrics metrics) implements SortMetrics {

    @Override
    public void recordStealReason(String outcome, String reason) {
        metrics.recordStealReason(outcome, reason);
    }

    @Override
    public void markProgress() {
        metrics.markProgress();
    }
}
