/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Path;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Validates the Parquet toolchain (parquet-mr + ZSTD via zstd-jni + LocalOutput/
 * InputFile): write a few canonical rows, read them back, schema intact.
 */
class ParquetRoundTripTest {

    @Test
    void writesAndReadsCanonicalRows(@TempDir Path dir) throws Exception {
        Path part = dir.resolve("part-00000.parquet");
        MessageType schema = ParquetSchema.canonical();

        try (PartWriter w = new PartWriter(part, schema)) {
            w.write(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("a/1"), 10, 1_700_000_000_000_000L,
                    "etag1", "STANDARD", null, true, null, null));
            w.write(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("a/2"), 20, 1_700_000_000_000_001L,
                    "etag2", "GLACIER", null, true, null, null));
        }

        // Schema round-trips with the canonical column set.
        assertThat(ParquetReads.schema(part).getColumns())
                .extracting(c -> c.getPath()[0])
                .contains("key", "size", "last_modified", "etag", "storage_class", "row_type", "is_delete_marker");

        List<Group> rows = ParquetReads.readAll(part);
        assertThat(rows).hasSize(2);
        assertThat(ParquetReads.keys(part)).containsExactly("a/1", "a/2");
        assertThat(rows.getFirst().getString("row_type", 0)).isEqualTo("OBJECT");
        assertThat(rows.getFirst().getBoolean("is_delete_marker", 0)).isFalse();
        assertThat(rows.getFirst().getLong("size", 0)).isEqualTo(10);
    }
}
