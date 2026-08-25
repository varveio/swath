/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SyncableLocalOutputFileTest {

    @Test
    void tracksLogicalPositionAndFlushesTransportBytesBeforeForcing(@TempDir Path dir)
            throws Exception {
        Path path = dir.resolve("part.parquet");
        AtomicInteger forces = new AtomicInteger();
        SyncableLocalOutputFile output = new SyncableLocalOutputFile(path, channel -> {
            assertThat(channel.isOpen()).isTrue();
            assertThat(channel.position()).isEqualTo(6L);
            assertThat(Files.size(path)).isEqualTo(6L);
            forces.incrementAndGet();
        });

        try (PositionOutputStream stream = output.create(0L)) {
            stream.write(1);
            stream.write(new byte[] {2, 3});
            stream.write(new byte[] {9, 4, 5, 6, 9}, 1, 3);
            assertThat(stream.getPos()).isEqualTo(6L);
            assertThat(Files.size(path)).isZero();

            output.syncData();
            assertThat(forces).hasValue(1);
            assertThat(stream.getPos()).isEqualTo(6L);
        }

        assertThat(Files.readAllBytes(path)).containsExactly(1, 2, 3, 4, 5, 6);
    }

    @Test
    void createOrOverwriteTruncatesExistingFile(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("part.parquet");
        Files.write(path, new byte[32]);
        SyncableLocalOutputFile output = new SyncableLocalOutputFile(path);

        try (PositionOutputStream stream = output.createOrOverwrite(0L)) {
            stream.write(new byte[] {1, 2, 3});
        }

        assertThat(Files.readAllBytes(path)).containsExactly(1, 2, 3);
    }

    @Test
    void createPreservesCollisionSemantics(@TempDir Path dir) throws Exception {
        Path path = Files.write(dir.resolve("part.parquet"), new byte[] {7});
        SyncableLocalOutputFile output = new SyncableLocalOutputFile(path);

        assertThatThrownBy(() -> output.create(0L)).isInstanceOf(IOException.class);
        assertThat(Files.readAllBytes(path)).containsExactly(7);
    }

    @Test
    void syncBeforeOpenAndForceFailuresAreReported(@TempDir Path dir) throws Exception {
        SyncableLocalOutputFile unopened = new SyncableLocalOutputFile(dir.resolve("unopened"));
        assertThatThrownBy(unopened::syncData)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not open");

        SyncableLocalOutputFile failing = new SyncableLocalOutputFile(
                dir.resolve("failing"), ignored -> { throw new IOException("force failed"); });
        try (PositionOutputStream stream = failing.create(0L)) {
            stream.write(new byte[] {1, 2, 3});
            assertThatThrownBy(failing::syncData)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("force failed");
        }
    }
}
