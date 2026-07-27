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
 * @param arenaMaxEncodedBytes      the largest {@linkplain KeyArena#encodedBytes() encoded arena} the
 *                                  {@link SimStoreBackend#ARENA} tier will build. A byte budget, never
 *                                  a key count: keys run to {@value KeyArena#MAX_KEY_BYTES} bytes each,
 *                                  so count predicts footprint only to within three orders of magnitude.
 * @param streamingMaxResidentBytes how much decoded key data the {@link SimStoreBackend#STREAMING}
 *                                  tier keeps resident before evicting behind its cursors. Bounds the
 *                                  tier's memory independently of the fixture's size — the property
 *                                  that lets one budget serve a 500M-key fixture.
 */
public record SimStoreConfig(long arenaMaxEncodedBytes, long streamingMaxResidentBytes) {

    /**
     * Default arena budget. Sized so a typical multi-million-key fixture loads outright while a
     * fixture large enough to threaten the heap resolves to the streaming tier instead.
     */
    public static final long DEFAULT_ARENA_MAX_ENCODED_BYTES = 4L << 30;

    /**
     * Default streaming residency budget: ~30 row groups of a fixture whose groups hold ~300K keys of
     * ~100 bytes, i.e. headroom for a fleet of ~30 concurrent cursors. Deliberately an order of
     * magnitude below the arena budget — a tier that holds the whole fixture must be allowed to, a
     * tier that holds a moving window must not need to.
     */
    public static final long DEFAULT_STREAMING_MAX_RESIDENT_BYTES = 1L << 30;

    /** System property overriding {@link #DEFAULT_ARENA_MAX_ENCODED_BYTES}. */
    public static final String ARENA_MAX_ENCODED_BYTES_PROPERTY = "swath.sim.arena.max-encoded-bytes";

    /** System property overriding {@link #DEFAULT_STREAMING_MAX_RESIDENT_BYTES}. */
    public static final String STREAMING_MAX_RESIDENT_BYTES_PROPERTY = "swath.sim.streaming.max-resident-bytes";

    public SimStoreConfig {
        if (arenaMaxEncodedBytes < 1) {
            throw new IllegalArgumentException("arena max-encoded-bytes must be at least 1, got "
                    + arenaMaxEncodedBytes);
        }
        if (streamingMaxResidentBytes < 1) {
            throw new IllegalArgumentException("streaming max-resident-bytes must be at least 1, got "
                    + streamingMaxResidentBytes);
        }
    }

    public static SimStoreConfig defaults() {
        return new SimStoreConfig(DEFAULT_ARENA_MAX_ENCODED_BYTES, DEFAULT_STREAMING_MAX_RESIDENT_BYTES);
    }

    public static SimStoreConfig fromSystemProperties() {
        return new SimStoreConfig(
                longProperty(ARENA_MAX_ENCODED_BYTES_PROPERTY, DEFAULT_ARENA_MAX_ENCODED_BYTES),
                longProperty(STREAMING_MAX_RESIDENT_BYTES_PROPERTY, DEFAULT_STREAMING_MAX_RESIDENT_BYTES));
    }

    private static long longProperty(String name, long fallback) {
        String value = System.getProperty(name);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a valid long, got \"" + value + "\"", e);
        }
    }
}
