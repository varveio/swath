/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.testkit.PageBatches;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PartWriterStreamingDigestTest {

    @Test
    void streamsTheExactFinalParquetBytesAndExposesThemOnlyAfterDurableClose(@TempDir Path dir)
            throws Exception {
        Path path = dir.resolve("part.parquet");
        PartWriter writer = new PartWriter(path, ParquetSchema.canonical());
        writer.write(PageBatches.batch(0, 0, 0, 1).entries().getFirst());

        assertThatThrownBy(writer::md5).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before durable close");
        writer.close();

        assertThat(writer.md5()).isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(path)));
        assertThat(writer.digestNanos()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void discardCannotExposeADigest(@TempDir Path dir) throws Exception {
        PartWriter writer = new PartWriter(dir.resolve("part.parquet"), ParquetSchema.canonical());
        writer.write(PageBatches.batch(0, 0, 0, 1).entries().getFirst());

        writer.discard();

        assertThatThrownBy(writer::md5).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before durable close");
    }
}
