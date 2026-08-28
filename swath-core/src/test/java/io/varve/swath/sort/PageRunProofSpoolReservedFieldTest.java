/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageRunProofSpoolReservedFieldTest {

    @Test
    void coordinatorValidatesReservedZeroAndAccountsForAllFixedBytes(@TempDir Path root)
            throws Exception {
        Path path = root.resolve("reserved.tmp");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);
        try (PageRunProofSpool.Writer writer = new PageRunProofSpool.Writer(path, 1, stats)) {
            writer.markOpen(0);
            writer.finish(0, 0, 0, 0, -1, -1, 0, false);
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer nonZero = ByteBuffer.allocate(Integer.BYTES).putInt(1).flip();
            long offset = PageRunProofSpool.reservedFieldOffset(0);
            while (nonZero.hasRemaining()) {
                offset += channel.write(nonZero, offset);
            }
        }
        long operationsBefore = metrics.proofSpoolMappedOperations.sum();
        long bytesBefore = metrics.proofSpoolMappedBytes.sum();

        try (PageRunProofSpool.Reader reader = new PageRunProofSpool.Reader(path, stats)) {
            assertThatThrownBy(() -> reader.read(0, false, false))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("reserved field is non-zero");
        } finally {
            PageRunProofSpool.delete(path, stats);
        }

        assertThat(metrics.proofSpoolMappedOperations.sum() - operationsBefore).isEqualTo(1);
        assertThat(metrics.proofSpoolMappedBytes.sum() - bytesBefore).isEqualTo(56);
    }
}
