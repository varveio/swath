/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class PageRunProofSpoolLargeMapTest {

    @Test
    @Tag("perf")
    @Timeout(30)
    void sparseFfmMappingAboveTwoGiBTouchesBothEndsThenUnmapsAndDeletes(@TempDir Path root)
            throws Exception {
        Path path = root.resolve("large-sparse-proof.tmp");
        long size = (long) Integer.MAX_VALUE + 64 * 1024L;

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
             Arena arena = Arena.ofConfined()) {
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
            mapped.set(ValueLayout.JAVA_BYTE, 0, (byte) 0x35);
            mapped.set(ValueLayout.JAVA_BYTE, size - 1, (byte) 0x7a);
            assertThat(mapped.get(ValueLayout.JAVA_BYTE, 0)).isEqualTo((byte) 0x35);
            assertThat(mapped.get(ValueLayout.JAVA_BYTE, size - 1)).isEqualTo((byte) 0x7a);
        }

        assertThat(Files.size(path)).isEqualTo(size);
        Files.delete(path);
        assertThat(path).doesNotExist();
    }
}
