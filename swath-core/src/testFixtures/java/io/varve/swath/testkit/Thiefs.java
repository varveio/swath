/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.Thief;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageFetcher;

/**
 * Terse {@link Thief} factories for engine tests: the same effective defaults the (now removed)
 * short {@link Thief} constructor overloads supplied — no metrics sink, the canonical toggle
 * defaults, and no trace sink. A test that needs a non-default seam passes it explicitly to the
 * matching overload here, or constructs {@link Thief} directly.
 */
public final class Thiefs {

    private Thiefs() {
    }

    /** A metrics-less {@link Thief} — the effective construction the deleted 6-arg overload built. */
    public static Thief of(CheckpointStore store, PageFetcher fetcher, long runId, byte[] prefix,
                           ListingMode mode, Thief.ChildSink childSink) {
        return new Thief(store, fetcher, runId, prefix, mode, childSink, null, null, null, null);
    }

    /**
     * As {@link #of(CheckpointStore, PageFetcher, long, byte[], ListingMode, Thief.ChildSink)}, but with
     * a caller-supplied metrics sink — the effective construction the deleted 7-arg overload built.
     */
    public static Thief of(CheckpointStore store, PageFetcher fetcher, long runId, byte[] prefix,
                           ListingMode mode, Thief.ChildSink childSink, RunMetrics metrics) {
        return new Thief(store, fetcher, runId, prefix, mode, childSink, metrics, null, null, null);
    }

    /**
     * As {@link #of(CheckpointStore, PageFetcher, long, byte[], ListingMode, Thief.ChildSink, RunMetrics)},
     * but with a caller-supplied {@link EngineToggles} — the effective construction the deleted 8-arg
     * overload built.
     */
    public static Thief of(CheckpointStore store, PageFetcher fetcher, long runId, byte[] prefix,
                           ListingMode mode, Thief.ChildSink childSink, RunMetrics metrics,
                           EngineToggles toggles) {
        return new Thief(store, fetcher, runId, prefix, mode, childSink, metrics, toggles, null, null);
    }
}
