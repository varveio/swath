/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;

/**
 * The tier-selection signals of {@link SimStoreFactory}, mirroring the replay server's
 * {@code serving.path} / {@code serving.fallback} pair. Which backend actually served a fixture,
 * and why an arena was declined, must be answerable from a sweep's metrics alone — otherwise a
 * result set carries no record of what produced it, and a threshold regression looks like a
 * throughput regression.
 */
public final class SimStoreMetrics {

    /** Bumped once per resolved store, tagged with the backend that will serve the fixture. */
    public static final String BACKEND_METRIC = "swath.sim.store.backend";

    /** Bumped when an {@link SimStoreBackend#AUTO} resolution declines the arena tier. */
    public static final String ARENA_DECLINE_METRIC = "swath.sim.store.arena.decline";

    /** {@link #ARENA_DECLINE_METRIC} reason: the fixture's keys exceed the configured byte budget. */
    public static final String DECLINE_OVER_BUDGET = "over-budget";

    private static final String BACKEND_TAG = "backend";
    private static final String REASON_TAG = "reason";

    private final MeterRegistry registry;

    public SimStoreMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordBackend(SimStoreBackend resolved) {
        Counter.builder(BACKEND_METRIC).tag(BACKEND_TAG, tagValue(resolved)).register(registry).increment();
    }

    public void recordArenaDecline(String reason) {
        Counter.builder(ARENA_DECLINE_METRIC).tag(REASON_TAG, reason).register(registry).increment();
    }

    /** The lowercase spelling of a backend, matching how the replay server tags its serving path. */
    public static String tagValue(SimStoreBackend backend) {
        return backend.name().toLowerCase(Locale.ROOT);
    }
}
