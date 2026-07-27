/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

/**
 * Which {@link io.varve.swath.replay.store.ListingStore} backs a simulator fixture, following the
 * replay server's {@code --serving-mode} precedent.
 *
 * <p>The forced choices exist for the same reason that one does: an automatic choice resolves to
 * exactly one backend per fixture, so a conformance or differential run driven through {@link #AUTO}
 * could never compare two backends on the same keys. Callers that mean to compare iterate the
 * concrete constants explicitly.
 */
public enum SimStoreBackend {

    /** Arena within {@code arena-max-encoded-bytes}, otherwise {@link #PARQUET}. */
    AUTO,

    /** Force the keys-only in-memory arena; fail fast when the fixture does not fit its budget. */
    ARENA,

    /** Force the replay module's Parquet-backed store — full metadata, the differential reference. */
    PARQUET
}
