/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.SeedStep;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageFetcher;

/**
 * Terse {@link SeedStep} factories for engine tests: the same effective defaults the (now removed)
 * short {@link SeedStep} constructor overloads supplied — no metrics sink and the canonical toggle
 * defaults. A test that needs a non-default seam passes it explicitly to the matching overload here,
 * or constructs {@link SeedStep} directly.
 */
public final class SeedSteps {

    private SeedSteps() {
    }

    /** A metrics-less, default-toggles {@link SeedStep} — the deleted 3-arg overload's construction. */
    public static SeedStep of(PageFetcher fetcher, byte[] prefix, int workerCount) {
        return of(fetcher, prefix, workerCount, null);
    }

    /**
     * As {@link #of(PageFetcher, byte[], int)}, but with a caller-supplied metrics sink — the deleted
     * 4-arg overload's construction.
     */
    public static SeedStep of(PageFetcher fetcher, byte[] prefix, int workerCount, RunMetrics metrics) {
        return of(fetcher, prefix, workerCount, metrics, EngineToggles.DEFAULT);
    }

    /**
     * As {@link #of(PageFetcher, byte[], int, RunMetrics)}, but with a caller-supplied {@link
     * EngineToggles} — the canonical constructor, exposed here for symmetry with the other factories.
     */
    public static SeedStep of(PageFetcher fetcher, byte[] prefix, int workerCount, RunMetrics metrics,
            EngineToggles toggles) {
        return new SeedStep(fetcher, prefix, workerCount, metrics, toggles);
    }
}
