/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

/**
 * Simulator-store configuration, read through the {@code swath.sim.*} system-property idiom the
 * replay server already uses for {@code swath.replay.*} (these tools sit outside swath's CLI config
 * system, so a plain system property is the right surface).
 *
 * @param arenaMaxEncodedBytes the largest {@linkplain KeyArena#encodedBytes() encoded arena} the
 *                             {@link SimStoreBackend#ARENA} tier will build. A byte budget, never a
 *                             key count: keys run to {@value KeyArena#MAX_KEY_BYTES} bytes each, so
 *                             count predicts footprint only to within three orders of magnitude.
 */
public record SimStoreConfig(long arenaMaxEncodedBytes) {

    /**
     * Default arena budget. Sized so a typical multi-million-key fixture loads outright while a
     * fixture large enough to threaten the heap resolves to the Parquet tier instead.
     */
    public static final long DEFAULT_ARENA_MAX_ENCODED_BYTES = 4L << 30;

    /** System property overriding {@link #DEFAULT_ARENA_MAX_ENCODED_BYTES}. */
    public static final String ARENA_MAX_ENCODED_BYTES_PROPERTY = "swath.sim.arena.max-encoded-bytes";

    public SimStoreConfig {
        if (arenaMaxEncodedBytes < 1) {
            throw new IllegalArgumentException("arena max-encoded-bytes must be at least 1, got "
                    + arenaMaxEncodedBytes);
        }
    }

    public static SimStoreConfig defaults() {
        return new SimStoreConfig(DEFAULT_ARENA_MAX_ENCODED_BYTES);
    }

    public static SimStoreConfig fromSystemProperties() {
        String value = System.getProperty(ARENA_MAX_ENCODED_BYTES_PROPERTY);
        if (value == null) {
            return defaults();
        }
        return new SimStoreConfig(Long.parseLong(value.trim()));
    }
}
