/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.dataset.PeriodicDataSync;
import io.varve.swath.testkit.PageBatches;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PartWriterStreamingDigestTest {

    @Test
    void streamsTheExactFinalParquetBytesAndExposesThemOnlyAfterDurableClose(@TempDir Path dir)
            throws Exception {
        Path path = dir.resolve("part.parquet");
        PartWriter writer = new PartWriter(path, ParquetSchema.canonical());
        writer.write(PageBatches.batch(0, 0, 0, 1).entries().getFirst());

        assertThat(writer.maybeSyncData()).isZero();
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

    @Test
    void enabledAdapterIsByteIdenticalWhenNoNaturalRowGroupHasEmitted(@TempDir Path dir)
            throws Exception {
        Path controlPath = dir.resolve("control.parquet");
        Path candidatePath = dir.resolve("candidate.parquet");
        AtomicInteger forces = new AtomicInteger();
        var entries = PageBatches.batch(0, 0, 0, 1000).entries();

        try (PartWriter control = new PartWriter(controlPath, ParquetSchema.canonical());
             PartWriter candidate = new PartWriter(candidatePath, ParquetSchema.canonical(),
                     PeriodicDataSync.MIN_INTERVAL_BYTES, ignored -> forces.incrementAndGet())) {
            for (var entry : entries) {
                control.write(entry);
                candidate.write(entry);
            }
            assertThat(candidate.maybeSyncData()).isZero();
        }

        assertThat(forces).hasValue(0);
        assertThat(Files.readAllBytes(candidatePath)).containsExactly(Files.readAllBytes(controlPath));
        try (ParquetFileReader control = ParquetFileReader.open(new LocalInputFile(controlPath));
             ParquetFileReader candidate = ParquetFileReader.open(new LocalInputFile(candidatePath))) {
            assertThat(candidate.getFooter().getBlocks())
                    .extracting(block -> block.getRowCount())
                    .containsExactlyElementsOf(control.getFooter().getBlocks().stream()
                            .map(block -> block.getRowCount()).toList());
        }
    }

    @Test
    void syncEngagesOnlyAfterANaturalRowGroupAndAccountsForEveryPhysicalByte(@TempDir Path dir)
            throws Exception {
        Path path = dir.resolve("part.parquet");
        AtomicInteger forces = new AtomicInteger();
        long syncedBytes = 0L;
        int rows = 0;
        Random random = new Random(0x51A7B00BL);
        PartWriter writer = new PartWriter(path, ParquetSchema.canonical(),
                PeriodicDataSync.MIN_INTERVAL_BYTES, channel -> {
                    assertThat(channel.position()).isEqualTo(Files.size(path));
                    forces.incrementAndGet();
                });

        while (syncedBytes == 0L && rows < 100_000) {
            for (int batchRow = 0; batchRow < 1000; batchRow++) {
                byte[] key = new byte[1024];
                random.nextBytes(key);
                writer.write(new ObjectEntry(KeyBytes.of(key), rows, 1_700_000_000_000_000L + rows,
                        "etag", "STANDARD", null, true, null, null, null, null));
                rows++;
            }
            syncedBytes += writer.maybeSyncData();
        }

        assertThat(syncedBytes).as("a naturally completed 64-MiB row group was emitted").isPositive();
        assertThat(forces).hasValue(1);
        writer.close();

        long residualBytes = writer.periodicSyncResidualBytes().orElseThrow();
        assertThat(syncedBytes + residualBytes).isEqualTo(Files.size(path));
        assertThat(writer.md5()).isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(path)));
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
            assertThat(reader.getFooter().getBlocks()).hasSizeGreaterThanOrEqualTo(2);
            assertThat(reader.getFooter().getBlocks().stream()
                    .mapToLong(block -> block.getRowCount()).sum()).isEqualTo(rows);
        }
    }
}
