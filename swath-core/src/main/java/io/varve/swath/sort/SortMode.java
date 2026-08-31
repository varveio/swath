/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.Optional;

/**
 * Whether a sorted output file may contain multiple rows per key — the {@code --all-versions}
 * knob the caller already knows at final-file-write time (§0.6). Stamped verbatim into the
 * footer via {@link io.varve.swath.output.parquet.sorted.SortedParquetWriter#MODE_KEY}, read back by
 * {@link io.varve.swath.output.parquet.sorted.SortedParquetStamp}.
 */
public enum SortMode {

    OBJECTS("objects"),
    VERSIONS("versions");

    private final String value;

    SortMode(String value) {
        this.value = value;
    }

    /** The exact footer KV string for this mode. */
    public String value() {
        return value;
    }

    /** Inverse of {@link #value()}; empty for any unrecognized string (incl. {@code null}). */
    public static Optional<SortMode> fromValue(String value) {
        for (SortMode mode : values()) {
            if (mode.value.equals(value)) {
                return Optional.of(mode);
            }
        }
        return Optional.empty();
    }
}
