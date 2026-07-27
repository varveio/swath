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

    /** Arena within {@code arena-max-encoded-bytes}; else {@link #STREAMING} when sorted-eligible;
     *  else {@link #PARQUET}. {@link #WINDOWED} is never resolved automatically — see its own note. */
    AUTO,

    /** Force the keys-only in-memory arena; fail fast when the fixture does not fit its budget. */
    ARENA,

    /** Force the keys-only decode-once streaming tier over a sorted fixture's row groups; fail fast
     *  when the fixture is not sorted-eligible (§ {@link io.varve.swath.replay.fixture.SortedEligibility}). */
    STREAMING,

    /**
     * Force the windowed row-group prefetch over the replay module's sorted-Parquet store; fail fast
     * when the fixture is not sorted-eligible.
     *
     * <p><b>Forced-only.</b> {@link #AUTO} never resolves here: this tier decodes Parquet inside the
     * serving loop, one range query per window fill, and a {@code BLOB} key column has no usable
     * zonemaps, so each fill scans the whole key column and the per-call cost scales with the
     * fixture rather than the window. It stays as the memory-bounded path that carries <b>full
     * metadata</b> (the keys-only tiers do not) and as the conformance comparison for
     * {@link #STREAMING} — chosen deliberately, never by default.
     */
    WINDOWED,

    /** Force the replay module's Parquet-backed store — full metadata, the differential reference. */
    PARQUET
}
