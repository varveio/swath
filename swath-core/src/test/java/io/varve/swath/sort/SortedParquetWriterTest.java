/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.dataset.PeriodicDataSync;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedParquetWriter} footer stamp (all five keys, round-trip via {@link SortStamp}),
 * {@link SortedFileWriterFactory#DEFAULT} (the unstamped default path) staying unstamped, and the
 * {@code final-row-group-bytes} knob actually producing multiple row groups.
 */
class SortedParquetWriterTest {

    private static SortConfig config(Map<String, String> overrides) {
        return SortConfig.fromProperties(key -> overrides.get(key.substring("swath.sort.".length())));
    }

    @Test
    void durableClosePublishesByteExactImmutableMetadata(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        SortedParquetWriter writer =
                new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1);
        writer.write(object("alpha"));
        writer.write(object("omega"));

        assertThat(writer.finalMetadata())
                .as("open/unfsynced output must never expose publishable metadata")
                .isEmpty();

        writer.markFinal();
        writer.close();
        FinalPartMetadata metadata = writer.finalMetadata().orElseThrow();
        assertThat(metadata.rows()).isEqualTo(2);
        assertThat(metadata.bytes()).isEqualTo(Files.size(path));
        assertThat(metadata.md5()).isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(path)));
        assertThat(metadata.minKey()).isEqualTo("alpha");
        assertThat(metadata.maxKey()).isEqualTo("omega");
        assertThat(metadata.boundsBytes()).isEqualTo("alpha".length() + "omega".length());

        // Idempotent close cannot replace or mutate the already trusted snapshot.
        writer.close();
        assertThat(writer.finalMetadata()).contains(metadata);
    }

    @Test
    void writesAllThreeStampKeysAndSortStampReadsThemBack(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.VERSIONS, 1)) {
            writer.write(object("a"));
            writer.write(object("b"));
        }

        Map<String, String> kv = footerKv(path);
        assertThat(kv).containsEntry(SortedParquetWriter.ORDER_KEY, SortedParquetWriter.ORDER_VALUE);
        assertThat(kv).containsEntry(SortedParquetWriter.MODE_KEY, "versions");
        assertThat(kv).containsEntry(SortedParquetWriter.FORMAT_VERSION_KEY, "1");

        Optional<SortStamp> stamp = SortStamp.read(path);
        assertThat(stamp).isPresent();
        assertThat(stamp.get().order()).isEqualTo(SortedParquetWriter.ORDER_VALUE);
        assertThat(stamp.get().mode()).isEqualTo(SortMode.VERSIONS);
        assertThat(stamp.get().formatVersion()).isEqualTo(1);
        assertThat(stamp.get().isKnownFormatVersion()).isTrue();
    }

    @Test
    void objectsModeStampsTheObjectsValue(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            writer.write(object("a"));
        }

        assertThat(SortStamp.read(path))
                .contains(new SortStamp(SortedParquetWriter.ORDER_VALUE, SortMode.OBJECTS, 1, 1, false));
    }

    /**
     * {@code file_index} is always stamped from construction; {@code file_final} is present (and
     * only ever {@code "true"}) when {@link SortedFileWriter#markFinal()} was called before {@link
     * SortedFileWriter#close()} — never written as {@code "false"}, since its ABSENCE is the
     * negative case that keeps the key genuinely additive.
     */
    @Test
    void fileIndexIsAlwaysStampedAndFileFinalIsPresentOnlyAfterMarkFinal(@TempDir Path dir) throws IOException {
        Path notFinal = dir.resolve("part-00002.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(notFinal, config(Map.of()), SortMode.OBJECTS, 2)) {
            writer.write(object("a"));
            // markFinal() deliberately never called: this file is NOT the last of a multi-file output.
        }
        Map<String, String> notFinalKv = footerKv(notFinal);
        assertThat(notFinalKv).containsEntry(SortedParquetWriter.FILE_INDEX_KEY, "2");
        assertThat(notFinalKv).doesNotContainKey(SortedParquetWriter.FILE_FINAL_KEY);
        assertThat(SortStamp.read(notFinal)).hasValueSatisfying(s -> {
            assertThat(s.fileIndex()).isEqualTo(2);
            assertThat(s.fileFinal()).isFalse();
        });

        Path last = dir.resolve("part-00003.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(last, config(Map.of()), SortMode.OBJECTS, 3)) {
            writer.write(object("b"));
            writer.markFinal();
        }
        Map<String, String> lastKv = footerKv(last);
        assertThat(lastKv).containsEntry(SortedParquetWriter.FILE_INDEX_KEY, "3");
        assertThat(lastKv).containsEntry(SortedParquetWriter.FILE_FINAL_KEY, SortedParquetWriter.FILE_FINAL_VALUE);
        assertThat(SortStamp.read(last)).hasValueSatisfying(s -> {
            assertThat(s.fileIndex()).isEqualTo(3);
            assertThat(s.fileFinal()).isTrue();
        });
    }

    @Test
    void aFileWrittenByTheUnstampedDefaultFactoryHasNoStamp(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part.parquet");
        try (SortedFileWriter writer = SortedFileWriterFactory.DEFAULT.create(path, 1)) {
            writer.write(object("a"));
        }

        assertThat(SortStamp.read(path)).isEmpty();
    }

    @Test
    void aStagingSegmentHasNoStamp(@TempDir Path dir) throws IOException {
        // Segments (SegmentParquetSink, via SegmentWriter) are internal working state, not served —
        // deliberately unstamped: only final files are stamped.
        ListEntryComparator cmp = new ListEntryComparator();
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        Path segment = dir.resolve("seg-0.parquet");
        try (SortedCursor cursor = new InMemoryCursor(List.of(object("a"), object("b")), cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, segment);
        }

        assertThat(SortStamp.read(segment)).isEmpty();
    }

    @Test
    void unrecognizedOrderValueIsRejected() {
        Map<String, String> kv = Map.of(
                SortedParquetWriter.ORDER_KEY, "some_other_order",
                SortedParquetWriter.MODE_KEY, "objects",
                SortedParquetWriter.FORMAT_VERSION_KEY, "1");
        assertThat(SortStamp.fromKeyValueMetaData(kv)).isEmpty();
    }

    @Test
    void missingOrderKeyIsRejected() {
        Map<String, String> kv = Map.of(
                SortedParquetWriter.MODE_KEY, "objects",
                SortedParquetWriter.FORMAT_VERSION_KEY, "1");
        assertThat(SortStamp.fromKeyValueMetaData(kv)).isEmpty();
    }

    @Test
    void unrecognizedModeValueIsRejected() {
        Map<String, String> kv = Map.of(
                SortedParquetWriter.ORDER_KEY, SortedParquetWriter.ORDER_VALUE,
                SortedParquetWriter.MODE_KEY, "not_a_mode",
                SortedParquetWriter.FORMAT_VERSION_KEY, "1");
        assertThat(SortStamp.fromKeyValueMetaData(kv)).isEmpty();
    }

    @Test
    void nonNumericFormatVersionIsRejected() {
        Map<String, String> kv = Map.of(
                SortedParquetWriter.ORDER_KEY, SortedParquetWriter.ORDER_VALUE,
                SortedParquetWriter.MODE_KEY, "objects",
                SortedParquetWriter.FORMAT_VERSION_KEY, "not_a_number");
        assertThat(SortStamp.fromKeyValueMetaData(kv)).isEmpty();
    }

    @Test
    void smallFinalRowGroupBytesProducesMultipleRowGroups(@TempDir Path dir) throws IOException {
        // final-row-group-bytes tiny enough that ~200 x ~200-byte rows must span several row groups.
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (int i = 0; i < 200; i++) {
                writer.write(object(padded(i)));
            }
        }

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
            assertThat(reader.getRowGroups().size()).isGreaterThan(1);
        }
    }

    @Test
    void enabledWritebackIsByteIdenticalWhenNoRowGroupHasEmitted(@TempDir Path dir)
            throws IOException {
        Path controlPath = dir.resolve("control.parquet");
        Path candidatePath = dir.resolve("candidate.parquet");
        AtomicInteger forces = new AtomicInteger();
        SortConfig config = config(Map.of());

        try (SortedFileWriter control =
                        new SortedParquetWriter(controlPath, config, SortMode.OBJECTS, 1);
             SortedFileWriter candidate = new SortedParquetWriter(candidatePath, config,
                     SortMode.OBJECTS, 1, PeriodicDataSync.MIN_INTERVAL_BYTES, null,
                     forces::incrementAndGet)) {
            for (int i = 0; i < 100; i++) {
                ListEntry entry = object(String.format("%08d", i));
                control.write(entry);
                candidate.write(entry);
            }
            control.markFinal();
            candidate.markFinal();
        }

        assertThat(forces).hasValue(0);
        assertThat(Files.readAllBytes(candidatePath)).containsExactly(Files.readAllBytes(controlPath));
    }

    @Test
    void writebackEngagesAfterNaturalRowGroupsAndConservesPhysicalBytes(@TempDir Path dir)
            throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        AtomicInteger forces = new AtomicInteger();
        SortConfig smallGroups = config(Map.of("final-row-group-bytes", "1048576"));
        SortedParquetWriter writer = new SortedParquetWriter(path, smallGroups, SortMode.OBJECTS, 1,
                PeriodicDataSync.MIN_INTERVAL_BYTES, metrics, forces::incrementAndGet);
        Random random = new Random(0x50A7EDL);

        int rows = 0;
        while (forces.get() == 0 && rows < 100_000) {
            writer.write(randomObject(rows++, random));
        }
        assertThat(forces).hasValue(1);
        writer.markFinal();
        writer.close();

        double synced = registry.get("swath.data_sync.bytes")
                .tag("format", "parquet").summary().totalAmount();
        double residual = registry.get("swath.data_sync.residual.bytes")
                .tag("format", "parquet").summary().totalAmount();
        assertThat((long) (synced + residual)).isEqualTo(Files.size(path));
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "OUTPUT", "reason", "data_sync").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "OUTPUT", "reason", "data_sync_sorted_parquet")
                .counter().count()).isEqualTo(1.0);
        assertThat(SortStamp.read(path)).hasValueSatisfying(stamp -> {
            assertThat(stamp.fileIndex()).isEqualTo(1);
            assertThat(stamp.fileFinal()).isTrue();
        });
    }

    @Test
    void failedWritebackPoisonsTheFinalAndPreventsMetadataPublication(@TempDir Path dir)
            throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        SortConfig smallGroups = config(Map.of("final-row-group-bytes", "1048576"));
        SortedParquetWriter writer = new SortedParquetWriter(path, smallGroups, SortMode.OBJECTS, 1,
                PeriodicDataSync.MIN_INTERVAL_BYTES, null,
                () -> { throw new IOException("injected data force failure"); });
        Random random = new Random(0xBADF0ACEL);

        assertThatThrownBy(() -> {
            for (int row = 0; row < 100_000; row++) {
                writer.write(randomObject(row, random));
            }
        }).isInstanceOf(IOException.class).hasMessageContaining("injected data force failure");
        assertThat(writer.finalMetadata()).isEmpty();
        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("refusing to publish after periodic data-sync failure");
        assertThat(writer.finalMetadata()).isEmpty();
    }

    @Test
    void productionFactoryClassifiesWritebackEngagementOnceAcrossFinalFiles(@TempDir Path dir)
            throws IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        SortConfig smallGroups = config(Map.of("final-row-group-bytes", "1048576"));
        SortedParquetWriterFactory factory = new SortedParquetWriterFactory(
                smallGroups, SortMode.OBJECTS, PeriodicDataSync.MIN_INTERVAL_BYTES, metrics);
        Random random = new Random(0xFAC70A1L);

        for (int part = 1; part <= 2; part++) {
            try (SortedFileWriter writer = factory.create(dir.resolve("part-0000" + part + ".parquet"), part)) {
                for (int row = 0; row < 8_000; row++) {
                    writer.write(randomObject(row, random));
                }
            }
        }

        assertThat(registry.get("swath.data_sync.latency").tag("format", "parquet").timer().count())
                .isGreaterThanOrEqualTo(2);
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "OUTPUT", "reason", "data_sync").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "OUTPUT", "reason", "data_sync_sorted_parquet")
                .counter().count()).isEqualTo(1.0);
    }

    private static String padded(int i) {
        // Fixed-width ascending keys, ~200 bytes each, so accumulated bytes cross the tiny row-group
        // threshold well before 200 rows.
        return String.format("%08d", i) + "x".repeat(190);
    }

    private static ListEntry randomObject(int row, Random random) {
        byte[] key = new byte[1024];
        byte[] prefix = String.format("%08d-", row).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] suffix = new byte[key.length - prefix.length];
        random.nextBytes(suffix);
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
        return new ObjectEntry(KeyBytes.of(key), row, 1_700_000_000_000_000L + row,
                "etag", "STANDARD", null, true, null, null, null, null);
    }

    private static Map<String, String> footerKv(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
            return reader.getFooter().getFileMetaData().getKeyValueMetaData();
        }
    }

    private static ListEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }
}
