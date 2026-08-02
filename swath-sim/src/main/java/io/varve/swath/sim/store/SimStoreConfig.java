/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

/** Simulator-store configuration, optionally overridden by {@code swath.sim.*} system properties.
 *
 * @param arenaMaxEncodedBytes      the largest {@linkplain KeyArena#encodedBytes() encoded arena} the
 *                                  {@link SimStoreBackend#ARENA} tier will build. A byte budget, never
 *                                  a key count.
 * @param streamingMaxResidentBytes how much decoded key data the {@link SimStoreBackend#STREAMING}
 *                                  tier keeps resident before evicting behind its cursors
 */
public record SimStoreConfig(long arenaMaxEncodedBytes, long streamingMaxResidentBytes) {

    /** Default arena byte budget. */
    public static final long DEFAULT_ARENA_MAX_ENCODED_BYTES = 4L << 30;

    /** Default streaming residency byte budget. */
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
