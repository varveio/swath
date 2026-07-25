/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/**
 * How the replay server decides which {@link io.varve.swath.replay.store.ListingStore} backs a fixture
 * (§2). The CLI accepts the lowercase spellings ({@code auto}, {@code duckdb},
 * {@code sorted}); {@link ReplayServingFactory} resolves {@link #AUTO} to one of the two concrete
 * paths and, in that case only, records a {@code serving.fallback} reason when it declines sorted.
 */
public enum ServingMode {

    /** Serve sorted when the fixture is a stamped, objects-mode, strictly-sorted file; else DuckDB. */
    AUTO,

    /** Force the DuckDB role-1 path (materialised) — the conformance oracle, works on any capture. */
    DUCKDB,

    /** Require the sorted role-2 path; fail fast if the fixture is not sorted-eligible. */
    SORTED
}
