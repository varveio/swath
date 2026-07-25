/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.io.Writer;

/** Builds a text {@link EntryFormatter} for a {@link Writer} sink. */
public final class Formatters {

    private Formatters() {
    }

    public static EntryFormatter text(OutputFormat format, Writer out, boolean escape) {
        return switch (format) {
            case JSONL -> new JsonlFormatter(out);
            case TSV -> new TsvFormatter(out, escape);
            case TABLE -> new AlignedFormatter(out, escape);
            case PARQUET -> throw new IllegalArgumentException(
                    "Parquet is a path-based sink, not a text Writer");
        };
    }
}
