/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.sorted.SortedDatasetCommitter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.stage.PageRunFixtures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PageBlockUtf8CorruptionTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @ParameterizedTest
    @EnumSource(Utf8Field.class)
    void crcRepairedMalformedUtf8FailsTypedBeforePublication(
            Utf8Field field, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(root.resolve("staging"));
        Path prior = Files.writeString(output.resolve("part-00000.parquet"), "prior-good");
        Path segment = writeSegment(staging.resolve("input.pageseg"), field.rows());
        corruptNeedleAndRepairFrameCrc(segment, field.needle(), field.header());
        SortRun run = new SortRun(SortConfigs.base(), CMP, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);

        assertThatThrownBy(() -> new SortedDatasetCoordinator(run).transform(
                List.of(segment), output, staging, SortedDatasetCommitter.NO_OP,
                ignored -> { }, FinalPassListener.NO_OP))
                .isInstanceOfSatisfying(PageRunCorruptionException.class, failure -> {
                    assertThat(failure.errorClass())
                            .isEqualTo(PageRunCorruptionException.PAGE_RUN_BODY_CORRUPTION);
                    assertThat(failure).hasStackTraceContaining("malformed UTF-8")
                            .hasNoSuppressedExceptions();
                });

        assertThat(Files.readString(prior)).isEqualTo("prior-good");
        assertThat(segment).exists();
        assertThat(output.resolve("part-00001.parquet")).doesNotExist();
        try (var files = Files.list(staging)) {
            assertThat(files.map(path -> path.getFileName().toString()))
                    .containsExactly("input.pageseg");
        }
    }

    private static Path writeSegment(Path path, List<ListEntry> rows) throws IOException {
        return writePages(path, rows);
    }

    @SafeVarargs
    private static Path writePages(Path path, List<ListEntry>... pages) throws IOException {
        PageRunFixtures.Buffer buffer = PageRunFixtures.buffer(
                SortConfigs.base().withSegmentCodec(PageCompression.NONE), CMP);
        long node = 0;
        for (List<ListEntry> page : pages) {
            buffer.admit(node++, page);
        }
        new PageRunWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCompression.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private static void corruptNeedleAndRepairFrameCrc(
            Path path, byte[] needle, boolean header) throws IOException {
        byte[] file = Files.readAllBytes(path);
        int frameStart = PageRunWriter.HEADER_BYTES;
        int bodyLength = ByteBuffer.wrap(file).getInt(frameStart);
        int bodyStart = frameStart + 2 * Integer.BYTES;
        byte[] body = java.util.Arrays.copyOfRange(file, bodyStart, bodyStart + bodyLength);
        int payloadOffset = PageRunRawFixtures.pageHeaderLayout(body).payloadOffset();
        int from = header ? 0 : payloadOffset;
        int to = header ? payloadOffset : body.length;
        int match = uniqueIndexOf(body, needle, from, to);
        body[match] = (byte) 0xC3;
        body[match + 1] = 0x28;
        System.arraycopy(body, 0, file, bodyStart, body.length);
        CRC32C crc = new CRC32C();
        crc.update(body, 0, body.length);
        ByteBuffer.wrap(file).putInt(frameStart + Integer.BYTES, (int) crc.getValue());
        Files.write(path, file);
    }

    private static int uniqueIndexOf(byte[] bytes, byte[] needle, int from, int to) {
        int found = -1;
        for (int i = from; i <= to - needle.length; i++) {
            boolean equal = true;
            for (int j = 0; j < needle.length; j++) {
                equal &= bytes[i + j] == needle[j];
            }
            if (equal) {
                if (found >= 0) {
                    throw new AssertionError("mutation needle is not unique");
                }
                found = i;
            }
        }
        if (found < 0) {
            throw new AssertionError("mutation needle was not found");
        }
        return found;
    }

    private static List<ListEntry> dictionaryHeavyRows(String keyPrefix) {
        return dictionaryHeavyRows(keyPrefix, keyPrefix);
    }

    private static List<ListEntry> dictionaryHeavyRows(
            String firstKeyPrefix, String remainingKeyPrefix) {
        List<ListEntry> rows = new ArrayList<>();
        for (int i = 0; i < PageBlock.DICT_CAP; i++) {
            String suffix = String.format("%02d-", i) + "x".repeat(512);
            String keyPrefix = i == 0 ? firstKeyPrefix : remainingKeyPrefix;
            rows.add(object(keyPrefix + String.format("%03d", i), null,
                    "storage-" + suffix, null, "owner-" + suffix,
                    "display-" + suffix, "checksum-" + suffix, "type-" + suffix));
        }
        return rows;
    }

    private static ObjectEntry object(String key, String etag, String storageClass,
            String versionId, String ownerId, String ownerDisplayName,
            String checksumAlgorithm, String checksumType) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, etag, storageClass, versionId,
                false, ownerId, ownerDisplayName, checksumAlgorithm, checksumType);
    }

    private enum Utf8Field {
        DICTIONARY("D!", true) {
            @Override List<ListEntry> rows() {
                return List.of(object("a", null, "D!", null, null, null, null, null));
            }
        },
        RAW_DICTIONARY("R!", false) {
            @Override List<ListEntry> rows() {
                List<ListEntry> rows = new ArrayList<>();
                rows.add(object("k000", null, null, null, "R!", null, null, null));
                for (int i = 1; i <= PageBlock.DICT_CAP; i++) {
                    rows.add(object(String.format("k%03d", i), null, null, null,
                            "owner-" + i, null, null, null));
                }
                return rows;
            }
        },
        VERSION_ID("V!", false) {
            @Override List<ListEntry> rows() {
                return List.of(object("a", null, null, "V!", null, null, null, null));
            }
        },
        RAW_ETAG("E!", false) {
            @Override List<ListEntry> rows() {
                return List.of(object("a", "E!", null, null, null, null, null, null));
            }
        };

        private final byte[] needle;
        private final boolean header;

        Utf8Field(String needle, boolean header) {
            this.needle = needle.getBytes(StandardCharsets.UTF_8);
            this.header = header;
        }

        byte[] needle() {
            return needle;
        }

        boolean header() {
            return header;
        }

        abstract List<ListEntry> rows();
    }

}
