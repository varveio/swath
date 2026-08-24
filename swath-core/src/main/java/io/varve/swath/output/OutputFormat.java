/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

/** Output formats, selected via {@code --format}. */
public enum OutputFormat {
    PARQUET,
    JSONL,
    TSV,
    TABLE,
    /** Diagnostic sink that drains and tallies rows without serializing or writing them. */
    DISCARD;

    /** Default ({@code auto}): {@code table} on a TTY, {@code tsv} otherwise (piped). */
    public static OutputFormat defaultFor(boolean tty) {
        return tty ? TABLE : TSV;
    }
}
