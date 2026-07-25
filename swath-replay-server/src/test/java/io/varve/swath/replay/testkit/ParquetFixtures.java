/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartWriter;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Opens a {@link PartWriter} against the module's canonical Parquet schema — the two-line
 * try-with-resources boilerplate that 14 capture-fixture test files each reimplemented.
 */
public final class ParquetFixtures {

    private ParquetFixtures() {
    }

    public static PartWriter open(Path path) throws IOException {
        return new PartWriter(path, ParquetSchema.canonical());
    }

    /** Writes a fixed set of entries to a single capture file in one call. */
    public static void write(Path path, ObjectEntry... entries) throws IOException {
        try (PartWriter writer = open(path)) {
            for (ObjectEntry entry : entries) {
                writer.write(entry);
            }
        }
    }
}
