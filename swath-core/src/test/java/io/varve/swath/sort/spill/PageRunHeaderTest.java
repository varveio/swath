/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.finalize.SortTestSupport;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
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

    @Test
    void truncatedHeaderIsMeteredBeforeExistingEofFailure(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("truncated.bin");
        Files.write(path, new byte[PageRunHeader.PREFIX_BYTES]);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> PageRunHeader.read(channel, path, Files.size(path), metrics))
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_header_corruption")
                    .hasMessageContaining("truncated page-run header");
        }
        assertThat(metrics.count("SORT.page_run_header_corruption")).isEqualTo(1);
    }

    @Test
    void badMagicAndFormatHardCutUseTheirDistinctClassifications(@TempDir Path dir)
            throws Exception {
        byte[] badMagic = headerWithUnknownField(SortMode.OBJECTS);
        badMagic[0] ^= 1;
        assertTypedHeaderRejectionIsClassified(dir.resolve("bad-magic.bin"), badMagic,
                "bad page-run magic", "page_run_header_corruption");

        byte[] oldFormat = headerWithUnknownField(SortMode.OBJECTS);
        ByteBuffer.wrap(oldFormat).putShort(Integer.BYTES, (short) 1);
        Path oldFormatPath = dir.resolve("old-format.bin");
        Files.write(oldFormatPath, oldFormat);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        try (FileChannel channel = FileChannel.open(oldFormatPath, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> PageRunHeader.read(
                    channel, oldFormatPath, oldFormat.length, metrics))
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_format_mismatch")
                    .hasMessageContaining("unsupported page-run format version 1");
        }
        assertThat(metrics.count("SORT.page_run_format_mismatch")).isEqualTo(1);
    }

    @Test
    void concurrentHeaderTruncationIsMetered(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("concurrent-truncation.bin");
        byte[] header = headerWithUnknownField(SortMode.OBJECTS);
        Files.write(path, Arrays.copyOf(header, PageRunHeader.PREFIX_BYTES));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> PageRunHeader.read(channel, path, header.length, metrics))
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_header_corruption")
                    .hasMessageContaining("unexpected EOF");
        }
        assertThat(metrics.count("SORT.page_run_header_corruption")).isEqualTo(1);
    }

    private static void assertTypedHeaderRejectionIsClassified(
            Path path, byte[] header, String message, String reason) throws Exception {
        Files.write(path, header);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> PageRunHeader.read(channel, path, header.length, metrics))
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=" + reason)
                    .hasMessageContaining(message);
        }
        assertThat(metrics.count("SORT." + reason)).isEqualTo(1);
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
