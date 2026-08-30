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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageRunHeaderTest {

    @Test
    void unknownTlvIsSkippedAndOrderingModeSurvives(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("header.bin");
        byte[] header = headerWithUnknownField(SortMode.VERSIONS);
        Files.write(path, header);

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            PageRunHeader.Header parsed = PageRunHeader.read(
                    channel, path, header.length, SortMetrics.NO_OP);
            assertThat(parsed.orderingMode()).isEqualTo(SortMode.VERSIONS);
            assertThat(parsed.encodedBytes()).isEqualTo(header.length);
        }
    }

    @Test
    void headerBitFlipIsTypedCorruption(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("header.bin");
        byte[] header = headerWithUnknownField(SortMode.OBJECTS);
        header[PageRunHeader.PREFIX_BYTES + 4] ^= 1;
        Files.write(path, header);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> PageRunHeader.read(channel, path, header.length, metrics))
                    .isInstanceOfSatisfying(SegmentCorruptionException.class, failure ->
                            assertThat(failure.errorClass())
                                    .isEqualTo(SegmentCorruptionException.PAGE_RUN_HEADER_CORRUPTION));
        }
        assertThat(metrics.count("SORT.page_run_header_corruption")).isEqualTo(1);
    }

    private static byte[] headerWithUnknownField(SortMode mode) {
        int metadataLength = 5 + 5;
        ByteBuffer header = ByteBuffer.allocate(
                PageRunHeader.PREFIX_BYTES + metadataLength + PageRunHeader.CRC_BYTES)
                .putInt(PageRunSegmentWriter.MAGIC)
                .putShort(PageRunSegmentWriter.FORMAT_VERSION)
                .putShort(PageRunHeader.HEADER_VERSION)
                .putInt(metadataLength)
                .putShort((short) 99).putShort((short) 1).put((byte) 42)
                .putShort((short) 1).putShort((short) 1)
                .put((byte) (mode == SortMode.OBJECTS ? 1 : 2));
        CRC32C crc = new CRC32C();
        crc.update(header.array(), 0, header.position());
        header.putInt((int) crc.getValue());
        return header.array();
    }
}
