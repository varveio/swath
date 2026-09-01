/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.ListEntryWriteSupport;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.sorted.SortedParquetIndex.RowGroupKey;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedParquetIndex#firstKeysPerRowGroup} — the index-derive API (§9.1): derived first keys
 * must exactly match the true first row of each row group (never Parquet footer stats), including
 * for adversarial key shapes (1&nbsp;KB, embedded NUL/U+10FFFF) and for a file
 * whose truncated footer stats provably diverge from the true first row.
 */
class SortedParquetIndexTest {

    private static final class StopBoundsScan extends RuntimeException {
    }

    private static SortConfig config(Map<String, String> overrides) {
        SortConfig config = SortConfigs.base();
        String rowGroupBytes = overrides.get("final-row-group-bytes");
        return rowGroupBytes == null ? config : config.withFinalRowGroupBytes(Long.parseLong(rowGroupBytes));
    }

    @Test
    void derivedFirstKeysMatchTheTrueFirstRowOfEachGroup(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(190));   // fixed-width, ascending, ~200 B
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }

        List<RowGroupKey> derived = SortedParquetIndex.firstKeysPerRowGroup(path);
        assertThat(derived.size()).isGreaterThan(1);   // the row-group knob actually took effect

        // The row-group's own reported row count (structural fact, not a stats/min-max quantity)
        // locates the expected first key in the known write order — this pins down exact-match
        // correctness without needing to predict parquet-mr's internal flush boundaries in advance.
        long totalRows = 0;
        int offset = 0;
        for (RowGroupKey rg : derived) {
            assertThat(new String(rg.firstKey(), StandardCharsets.UTF_8)).isEqualTo(keys.get(offset));
            offset += (int) rg.rowCount();
            totalRows += rg.rowCount();
        }
        assertThat(totalRows).isEqualTo(keys.size());
        assertThat(offset).isEqualTo(keys.size());
    }

    @Test
    void exactBoundsScanChecksCancellationWhileReadingRows(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(
                path, SortConfigs.base(), SortMode.OBJECTS, 0)) {
            for (int i = 0; i < 10; i++) {
                writer.write(object(String.format("%04d", i)));
            }
        }
        AtomicInteger checks = new AtomicInteger();

        assertThatThrownBy(() -> SortedParquetIndex.bounds(path, () -> {
            if (checks.incrementAndGet() == 2) {
                throw new StopBoundsScan();
            }
        })).isInstanceOf(StopBoundsScan.class);
        assertThat(checks).hasValue(2);
    }

    @Test
    void derivesTrueFirstKeysFor1KbKeysWithEmbeddedNulAndHighScalar(@TempDir Path dir) throws IOException {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            keys.add(adversarialKey(i));   // 1024 B, ascending, embeds NUL and U+10FFFF
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (byte[] k : keys) {
                writer.write(new ObjectEntry(KeyBytes.of(k), 1L, 0L, null, null, null, false, null, null, null, null));
            }
        }

        List<RowGroupKey> derived = SortedParquetIndex.firstKeysPerRowGroup(path);
        assertThat(derived.size()).isGreaterThan(1);

        int offset = 0;
        for (RowGroupKey rg : derived) {
            assertThat(rg.firstKey()).hasSize(1024);
            assertThat(rg.firstKey()).isEqualTo(keys.get(offset));
            offset += (int) rg.rowCount();
        }
        assertThat(offset).isEqualTo(keys.size());
    }

    @Test
    void derivedFirstKeyDivergesFromTruncatedFooterStats(@TempDir Path dir) throws IOException {
        // A deliberately adversarial fixture: 1 KB keys + a forced small statistics-truncate length,
        // so the footer's column-chunk min (if a caller wrongly consulted it for routing) would be a
        // 16-byte truncated prefix, never the true 1024-byte first row.
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            keys.add(adversarialKey(i));
        }
        Path path = dir.resolve("truncated.parquet");
        writeWithTruncatedStatistics(path, keys, 4096, 16);

        List<RowGroupKey> derived = SortedParquetIndex.firstKeysPerRowGroup(path);
        assertThat(derived).isNotEmpty();
        assertThat(derived.get(0).rowCount()).isGreaterThan(0);
        // The derived first key is the true, full-length first row — always correct by construction.
        assertThat(derived.get(0).firstKey()).isEqualTo(keys.get(0));

        byte[] truncatedMin = footerKeyColumnMinBytes(path, 0);
        assertThat(truncatedMin).hasSizeLessThanOrEqualTo(16);
        // The truncated stat is provably NOT the true first row: different length, and (since every
        // key here shares no long common prefix once truncated to 16 B of a 1024 B key) different
        // content — proving derive must not fall back to stats-based routing.
        assertThat(truncatedMin).isNotEqualTo(derived.get(0).firstKey());
    }

    /**
     * {@link RowGroupKey#pureObjectRowType()} is the eligibility signal
     * {@code io.varve.swath.replay.fixture.SortedFixtures#loadIndex} uses to refuse sorted serving for a
     * file whose {@code mode=objects} stamp doesn't actually guarantee every row is {@code OBJECT}
     * (e.g. a legacy delimiter'd capture re-sorted by {@code sort-fixture}, which never inspects
     * {@code row_type} — only {@code version_id} and adjacent-equal-key). An all-objects file must
     * report every group pure; a file with one {@code COMMON_PREFIX} row inserted must report the
     * containing group(s) as NOT pure, while unaffected groups stay pure (a per-row-group signal, not
     * a whole-file one).
     */
    @Test
    void pureObjectRowTypeIsTrueThroughoutAnAllObjectsFileAndFalseForAGroupHoldingACommonPrefixRow(
            @TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(190));
        }
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));

        Path pureFile = dir.resolve("pure.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(pureFile, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }
        List<RowGroupKey> pureDerived = SortedParquetIndex.firstKeysPerRowGroup(pureFile);
        assertThat(pureDerived.size()).isGreaterThan(1);
        assertThat(pureDerived).allMatch(RowGroupKey::pureObjectRowType);

        Path mixedFile = dir.resolve("mixed.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(mixedFile, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (int i = 0; i < keys.size(); i++) {
                if (i == keys.size() / 2) {
                    writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8(keys.get(i) + "!cp")));
                }
                writer.write(object(keys.get(i)));
            }
        }
        List<RowGroupKey> mixedDerived = SortedParquetIndex.firstKeysPerRowGroup(mixedFile);
        assertThat(mixedDerived.size()).isGreaterThan(1);
        assertThat(mixedDerived).anyMatch(rg -> !rg.pureObjectRowType());
        assertThat(mixedDerived).anyMatch(RowGroupKey::pureObjectRowType);
    }

    /**
     * Records synthesize reference-identity equals/hashCode for array components, so two
     * {@link RowGroupKey} instances built from equal-content-but-distinct {@code firstKey} arrays
     * would never be equal under the default record equals — fails on that synthesized equals.
     */
    @Test
    void rowGroupKeysWithEqualContentButDistinctArraysAreEqualAndHashEqual() {
        byte[] a = "same-key".getBytes(StandardCharsets.UTF_8);
        byte[] b = "same-key".getBytes(StandardCharsets.UTF_8);
        assertThat(a).isNotSameAs(b);   // distinct arrays, equal content

        RowGroupKey one = new RowGroupKey(a, 42L, true);
        RowGroupKey other = new RowGroupKey(b, 42L, true);

        assertThat(one).isEqualTo(other);
        assertThat(one.hashCode()).isEqualTo(other.hashCode());
    }

    @Test
    void emptyFileHasNoRowGroups(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("empty.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            // no writes
        }

        assertThat(SortedParquetIndex.firstKeysPerRowGroup(path)).isEmpty();
    }

    /**
     * Row-group skip: {@link SortedParquetIndex#rowGroupSpans} carries each non-empty group's
     * PHYSICAL block index (the ordinal usable with {@code ParquetFileReader.readRowGroup(int)}) and
     * derives the same true first keys as {@link SortedParquetIndex#firstKeysPerRowGroup}. For an
     * all-non-empty file the block indices are the dense sequence 0,1,2,… — the load-bearing property
     * a range-skip caller relies on.
     */
    @Test
    void rowGroupSpansCarryPhysicalBlockIndicesAndTrueFirstKeys(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(190));
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }

        List<RowGroupKey> byKey = SortedParquetIndex.firstKeysPerRowGroup(path);
        List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(path);
        assertThat(spans).hasSameSizeAs(byKey);
        assertThat(spans.size()).isGreaterThan(1);

        for (int i = 0; i < spans.size(); i++) {
            SortedParquetIndex.RowGroupSpan span = spans.get(i);
            assertThat(span.blockIndex()).isEqualTo(i);   // dense: no empty groups in this file
            assertThat(span.firstKey()).isEqualTo(byKey.get(i).firstKey());
            assertThat(span.rowCount()).isEqualTo(byKey.get(i).rowCount());
        }
    }

    /** {@code i}-th 1024-byte key: ascending, valid UTF-8 with NUL and U+10FFFF. */
    private static byte[] adversarialKey(int i) {
        byte[] key = new byte[1024];
        key[0] = 0x00;
        key[1] = (byte) 0xF4;
        key[2] = (byte) 0x8F;
        key[3] = (byte) 0xBF;
        key[4] = (byte) 0xBF;
        java.util.Arrays.fill(key, 5, key.length - 8, (byte) 'x');
        byte[] suffix = String.format("%08x", i).getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(suffix, 0, key, key.length - suffix.length, suffix.length);
        return key;
    }

    private static byte[] footerKeyColumnMinBytes(Path path, int rowGroupIndex) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
            var columns = reader.getRowGroups().get(rowGroupIndex).getColumns();
            for (var col : columns) {
                if (col.getPath().toDotString().equals("key")) {
                    return col.getStatistics().getMinBytes();
                }
            }
            throw new IllegalStateException("no key column in row group " + rowGroupIndex);
        }
    }

    /** A raw high-level writer with a forced small {@code statisticsTruncateLength} — test-only. */
    private static void writeWithTruncatedStatistics(Path path, List<byte[]> keys, long rowGroupBytes,
                                                      int statisticsTruncateLength) throws IOException {
        Configuration conf = new Configuration(false);
        MessageType schema = ParquetSchema.canonical();
        try (ParquetWriter<ListEntry> writer = new TestBuilder(new LocalOutputFile(path), schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(rowGroupBytes)
                .withPageSize(1024 * 1024)
                .withStatisticsTruncateLength(statisticsTruncateLength)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (byte[] k : keys) {
                writer.write(new ObjectEntry(KeyBytes.of(k), 1L, 0L, null, null, null, false, null, null, null, null));
            }
        }
    }

    private static final class TestBuilder extends ParquetWriter.Builder<ListEntry, TestBuilder> {
        private final MessageType schema;

        TestBuilder(OutputFile file, MessageType schema) {
            super(file);
            this.schema = schema;
        }

        @Override
        protected TestBuilder self() {
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected WriteSupport<ListEntry> getWriteSupport(Configuration conf) {
            return new ListEntryWriteSupport(schema);
        }
    }

    private static ListEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }
}
