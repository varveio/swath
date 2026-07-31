/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

/** Backends for a simulator fixture. {@link #AUTO} resolves one backend; comparisons use concrete choices. */
public enum SimStoreBackend {

    /** Arena within budget; then sorted-eligible streaming; otherwise Parquet. Never windowed. */
    AUTO,

    /** Force the keys-only in-memory arena; fail fast when the fixture does not fit its budget. */
    ARENA,

    /** Force keys-only decode-once streaming; rejects a fixture that is not sorted-eligible. */
    STREAMING,

    /**
     * Force the windowed row-group prefetch over the replay module's sorted-Parquet store; fail fast
     * when the fixture is not sorted-eligible.
     *
     * <p>Forced-only because the BLOB key has no usable zonemap, so every window fill scans the
     * whole Parquet key column and scales with fixture size. It is the memory-bounded full-metadata
     * path and a conformance comparison for {@link #STREAMING}.
     */
    WINDOWED,

    /** Force the replay module's Parquet-backed store — full metadata, the differential reference. */
    PARQUET
}
