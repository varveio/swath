/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.OutputException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.CountingWriter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.dataset.PeriodicDataSync;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class TextWriterPoolTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long WRITEBACK_BYTES = PeriodicDataSync.MIN_INTERVAL_BYTES;

    @Test
    void gzipPartsAreIndependentAndTheirUnionIsExact(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.GZIP, 3);
        List<String> expected = new ArrayList<>();
        for (int node = 0; node < 3; node++) {
            var batch = PageBatches.batch(node, 0, node * 10, node * 10 + 5);
            batch.entries().forEach(entry -> expected.add(entry.key().asString()));
            pool.submit(batch);
        }
        // Duplicate blindness here would hide exactly the kind of retry/lifecycle regression this
        // parallel sink must expose, so deliberately emit a duplicate batch and compare multisets.
        var duplicate = PageBatches.batch(0, 1, 0, 5);
        duplicate.entries().forEach(entry -> expected.add(entry.key().asString()));
        pool.submit(duplicate);
        pool.close();

        List<Path> parts = DatasetLayout.of(dir).dataParts(".jsonl.gz");
        assertThat(parts).hasSize(3).allMatch(path -> path.toString().endsWith(".jsonl.gz"));
        List<String> actual = new ArrayList<>();
        for (Path part : parts) {
            try (InputStream in = new GZIPInputStream(Files.newInputStream(part))) {
                for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList()) {
                    actual.add(JSON.readTree(line).path("key").asText());
                }
            }
        }
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(DatasetLayout.of(dir).success()).exists();
        assertThat(Files.readString(DatasetLayout.of(dir).manifest()))
                .contains("\"fileFormat\": \"JSONL\"")
                .contains(".jsonl.gz");
    }

    @Test
    void zstdPartIsACompleteReadableFrame(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.ZSTD, 2);
        pool.submit(PageBatches.batch(0, 0, 0, 4));
        pool.close();

        Path part = DatasetLayout.of(dir).dataParts(".jsonl.zst").getFirst();
        try (InputStream in = new ZstdInputStream(Files.newInputStream(part))) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8).lines()).hasSize(4);
        }
    }

    @Test
    void streamingDigestMatchesExactPhysicalPartBytesForEveryTextCompression(@TempDir Path dir)
            throws Exception {
        for (TextCompression compression : TextCompression.values()) {
            Path part = dir.resolve("part-" + compression + ".jsonl");
            TextDatasetFormat format = new TextDatasetFormat(OutputFormat.JSONL, compression, true, 0L);
            var writer = format.openPart(part);
            writer.write(PageBatches.batch(0, 0, 0, 1).entries().getFirst());

            assertThatThrownBy(writer::md5).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before durable close");
            writer.close();

            assertThat(writer.md5()).isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(part)));
            assertThat(writer.digestNanos()).isGreaterThanOrEqualTo(0L);
        }
    }

    @Test
    void byteTsvPartsRemainReadableAndDigestExactForEveryCompression(@TempDir Path dir)
            throws Exception {
        for (TextCompression compression : TextCompression.values()) {
            Path part = dir.resolve("part-" + compression + ".tsv");
            TextDatasetFormat format = new TextDatasetFormat(OutputFormat.TSV, compression, true, 0L);
            var writer = format.openPart(part);
            writer.write(PageBatches.batch(0, 0, 0, 1).entries().getFirst());
            writer.close();

            assertThat(writer.md5()).isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(part)));
            try (InputStream input = decoded(part, compression)) {
                List<String> lines = new String(input.readAllBytes(), StandardCharsets.UTF_8)
                        .lines().toList();
                assertThat(lines).hasSize(2);
                assertThat(lines.getFirst())
                        .isEqualTo("key\tsize\tlast_modified\tetag\tstorage_class\trow_type");
                assertThat(lines.get(1).split("\t")).hasSize(6);
            }
        }
    }

    @Test
    void periodicDataSyncIsByteInertForEveryCompression(@TempDir Path dir) throws Exception {
        var entries = incompressibleEntries(14_000, 384);
        for (OutputFormat outputFormat : List.of(OutputFormat.TSV, OutputFormat.JSONL)) {
            for (TextCompression compression : TextCompression.values()) {
                String suffix = outputFormat.name().toLowerCase() + "-" + compression;
                Path control = dir.resolve("control-" + suffix);
                Path candidate = dir.resolve("candidate-" + suffix);
                var controlWriter = new TextDatasetFormat(
                        outputFormat, compression, true, 0L).openPart(control);
                var candidateWriter = new TextDatasetFormat(
                        outputFormat, compression, true, WRITEBACK_BYTES).openPart(candidate);
                for (var entry : entries) {
                    controlWriter.write(entry);
                    candidateWriter.write(entry);
                }

                assertThat(candidateWriter.maybeSyncData()).as(suffix).isPositive();
                assertThatThrownBy(candidateWriter::md5).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("before durable close");
                controlWriter.close();
                candidateWriter.close();

                assertThat(Files.readAllBytes(candidate)).containsExactly(Files.readAllBytes(control));
                assertThat(candidateWriter.md5()).isEqualTo(controlWriter.md5());
                assertThat(candidateWriter.periodicSyncResidualBytes()).isPresent();
            }
        }
    }

    @Test
    void failedPeriodicDataSyncPoisonsThePartAgainstLaterPublication(@TempDir Path dir)
            throws Exception {
        Path part = dir.resolve("part.tsv");
        var writer = new TextDatasetFormat(
                OutputFormat.TSV, TextCompression.NONE, true, WRITEBACK_BYTES)
                .openPartWithForcer(
                        part, ignored -> { throw new IOException("forced sync failure"); });
        for (var entry : incompressibleEntries(6000, 384)) {
            writer.write(entry);
        }

        assertThatThrownBy(writer::maybeSyncData).isInstanceOf(IOException.class)
                .hasMessage("forced sync failure");
        assertThatThrownBy(writer::close).isInstanceOf(IOException.class)
                .hasMessageContaining("refusing to publish")
                .hasRootCauseMessage("forced sync failure");
        assertThatThrownBy(writer::md5).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before durable close");
    }

    @Test
    void textAbortCannotExposeADigest(@TempDir Path dir) throws Exception {
        TextDatasetFormat format = new TextDatasetFormat(
                OutputFormat.JSONL, TextCompression.GZIP, true, 0L);
        var writer = format.openPart(dir.resolve("part.jsonl.gz"));
        writer.write(PageBatches.batch(0, 0, 0, 1).entries().getFirst());

        writer.discard();

        assertThatThrownBy(writer::md5).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before durable close");
    }

    @Test
    void codecConstructionErrorClosesAndDeletesTheCreatedPart(@TempDir Path dir) throws Exception {
        Path part = dir.resolve("part.jsonl");
        TextDatasetFormat format = new TextDatasetFormat(
                OutputFormat.JSONL, TextCompression.GZIP, true, 0L);

        assertThatThrownBy(() -> format.openPartWithEncoder(part, (stream, compression) -> {
            throw new AssertionError("codec construction failed");
        })).isInstanceOf(AssertionError.class).hasMessage("codec construction failed");
        assertThat(part).doesNotExist();
    }

    @Test
    void headerErrorRemainsPrimaryWhileCloseFailureIsSuppressedAndPartIsDeleted(
            @TempDir Path dir) throws Exception {
        Path part = dir.resolve("part.tsv");
        TextDatasetFormat format = new TextDatasetFormat(
                OutputFormat.TSV, TextCompression.NONE, true, 0L);

        assertThatThrownBy(() -> format.openPartWithEncoder(part, (stream, compression) ->
                new CountingWriter(new Writer() {
                    @Override public void write(char[] chars, int offset, int length) {
                        throw new AssertionError("header failed");
                    }

                    @Override public void flush() {
                    }

                    @Override public void close() throws IOException {
                        stream.close();
                        throw new IOException("cleanup close failed");
                    }
                })))
                .isInstanceOf(AssertionError.class)
                .hasMessage("header failed")
                .satisfies(failure -> assertThat(failure.getSuppressed())
                        .extracting(Throwable::getMessage)
                        .contains("cleanup close failed"));
        assertThat(part).doesNotExist();
    }

    @Test
    void createCollisionNeverDeletesAFileTheWriterDidNotCreate(@TempDir Path dir)
            throws Exception {
        Path part = dir.resolve("part.tsv");
        byte[] existing = "belongs-to-another-run".getBytes(StandardCharsets.UTF_8);
        Files.write(part, existing);
        TextDatasetFormat format = new TextDatasetFormat(
                OutputFormat.TSV, TextCompression.NONE, true, 0L);

        assertThatThrownBy(() -> format.openPart(part))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readAllBytes(part)).containsExactly(existing);
    }

    @Test
    void abortPublishesNoSuccessMarker(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.NONE, 2);
        pool.submit(PageBatches.batch(0, 0, 0, 4));
        pool.abort();
        DatasetLayout layout = DatasetLayout.of(dir);
        assertThat(layout.success()).doesNotExist();
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.state()).doesNotExist();
        assertThat(layout.symlink()).doesNotExist();
        assertThat(layout.dataParts(".jsonl")).isEmpty();
    }

    @Test
    void tsvRowRotationRepeatsTheExactHeaderAndPreservesDuplicateRows(@TempDir Path dir)
            throws Exception {
        Files.createDirectories(dir);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 8, 0, 2, 0L, null));
        List<String> expected = new ArrayList<>();
        for (int page = 0; page < 5; page++) {
            var batch = PageBatches.batch(0, page, page % 2, page % 2 + 1);
            expected.add(batch.entries().getFirst().key().asString());
            pool.submit(batch);
        }
        pool.close();

        List<Path> parts = DatasetLayout.of(dir).dataParts(".tsv");
        assertThat(parts).hasSize(3);
        List<String> actual = new ArrayList<>();
        for (Path part : parts) {
            List<String> lines = Files.readAllLines(part, StandardCharsets.UTF_8);
            assertThat(lines.getFirst())
                    .isEqualTo("key\tsize\tlast_modified\tetag\tstorage_class\trow_type");
            lines.stream().skip(1).map(line -> line.substring(0, line.indexOf('\t')))
                    .forEach(actual::add);
        }
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(Files.readString(DatasetLayout.of(dir).manifest()))
                .contains("\"fileSchema\": \"key,size,last_modified,etag,storage_class,row_type\"");
    }

    @Test
    void concurrentCloseHasOneSafeJoinAndPublication(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.NONE, 2);
        pool.submit(PageBatches.batch(0, 0, 0, 4));
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> { start.await(); pool.close(); return null; });
            var second = executor.submit(() -> { start.await(); pool.close(); return null; });
            start.countDown();
            first.get();
            second.get();
        }
        assertThat(DatasetLayout.of(dir).success()).exists();
    }

    @Test
    void closeAfterAbortCannotPublishMetadata(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.NONE, 2);
        pool.submit(PageBatches.batch(0, 0, 0, 4));
        pool.abort();
        assertThatThrownBy(pool::close).hasMessageContaining("aborted");
        assertThat(DatasetLayout.of(dir).success()).doesNotExist();
    }

    @Test
    void reportsLaneRotationFinalizePartOutcomeAndLiveness(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.JSONL, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 8, 0, 1, 0L, metrics));
        long progressBefore = metrics.progressSignal();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        pool.close();

        assertThat(registry.counter("swath.text_dataset.rotation", "trigger", "rows").count())
                .isEqualTo(1);
        assertThat(registry.counter("swath.text_dataset.parts", "outcome", "finalized").count())
                .isEqualTo(1);
        assertThat(registry.timer("swath.text_dataset.finalize.latency").count()).isEqualTo(1);
        assertThat(registry.timer("swath.text_dataset.write.latency").count()).isGreaterThanOrEqualTo(1);
        assertThat(metrics.progressSignal()).isGreaterThan(progressBefore);
        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "work_stealing", 0L, 0L);
        assertThat(summary.datasetWriter()).isNotNull();
        assertThat(summary.datasetWriter().format()).isEqualTo("jsonl");
        assertThat(summary.datasetWriter().writerCount()).isEqualTo(2);
        assertThat(summary.datasetWriter().totalQueueCapacity()).isEqualTo(16L);
        assertThat(summary.datasetWriter().jvmMaxHeapBytes()).isPositive();
        assertThat(summary.datasetWriter().rowGroupTargetBytesPerWriter()).isNull();
        assertThat(summary.datasetWriter().rowGroupAllowanceMultiplier()).isNull();
        assertThat(summary.datasetWriter().plannedHeapBytes()).isNull();
        assertThat(summary.datasetWriter().heapAdmissionApplied()).isNull();
        assertThat(summary.datasetWriter().partDigestCount()).isEqualTo(1L);
        assertThat(summary.datasetWriter().manifestWriteCount()).isEqualTo(1L);
        assertThat(summary.datasetWriter().lanes()).extracting(RunSummary.DatasetWriterLane::rowsWritten)
                .containsExactlyInAnyOrder(1L, 0L);
    }

    @Test
    void tsvBytePathReportsItsEngagementAndEscapeMode(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 8, 0, 0, 0L, metrics));

        pool.close();

        assertThat(registry.counter("swath.steal_reason", "outcome", "OUTPUT", "reason",
                "tsv_byte_encoder").count()).isEqualTo(1.0);
        assertThat(registry.counter("swath.steal_reason", "outcome", "OUTPUT", "reason",
                "tsv_escape_on").count()).isEqualTo(1.0);
    }

    @Test
    void periodicSyncReportsEngagementBytesLatencyAndFinalizeResidual(@TempDir Path dir)
            throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 8, 0, 0, WRITEBACK_BYTES, metrics));

        pool.submit(new PageBatch(0, 0, incompressibleEntries(6000, 384)));
        pool.close();

        assertThat(registry.counter("swath.steal_reason", "outcome", "OUTPUT", "reason",
                "data_sync").count()).isEqualTo(1.0);
        assertThat(registry.counter("swath.steal_reason", "outcome", "OUTPUT", "reason",
                "data_sync_text_uncompressed").count()).isEqualTo(1.0);
        assertThat(registry.timer("swath.data_sync.latency", "format", "tsv").count()).isPositive();
        assertThat(registry.summary("swath.data_sync.bytes", "format", "tsv").totalAmount()).isPositive();
        assertThat(registry.summary("swath.data_sync.residual.bytes", "format", "tsv").count())
                .isEqualTo(1L);
        assertThat(registry.summary("swath.data_sync.residual.bytes", "format", "tsv").max())
                .isLessThan(WRITEBACK_BYTES);
    }

    @Test
    void configuredWritebackWithoutAnActualSyncReportsNoEngagement(@TempDir Path dir)
            throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 8, 0, 0, WRITEBACK_BYTES, metrics));

        pool.submit(PageBatches.batch(0, 0, 0, 10));
        pool.close();

        assertThat(registry.find("swath.steal_reason")
                .tags("outcome", "OUTPUT", "reason", "data_sync").counter()).isNull();
        assertThat(registry.find("swath.data_sync.latency").timer()).isNull();
        assertThat(registry.summary("swath.data_sync.residual.bytes", "format", "tsv").count())
                .isEqualTo(1L);
    }

    @Test
    void concurrentLanesRetainEveryWritebackObservation(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 1, 0, 0, WRITEBACK_BYTES, metrics));

        pool.submit(new PageBatch(0, 0, incompressibleEntries(6000, 384)));
        pool.submit(new PageBatch(1, 0, incompressibleEntries(6000, 384)));
        pool.close();

        assertThat(registry.timer("swath.data_sync.latency", "format", "tsv").count())
                .isEqualTo(2L);
        assertThat(registry.summary("swath.data_sync.bytes", "format", "tsv").count())
                .isEqualTo(2L);
        assertThat(registry.summary("swath.data_sync.residual.bytes", "format", "tsv").count())
                .isEqualTo(2L);
        assertThat(registry.counter("swath.steal_reason", "outcome", "OUTPUT", "reason",
                "data_sync").count()).isEqualTo(1.0);
    }

    @Test
    void oneLongPartRetainsRepeatedWritebackCadenceAndOneFinalResidual(@TempDir Path dir)
            throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 1, 0, 0, WRITEBACK_BYTES, metrics));
        List<ListEntry> entries = incompressibleEntries(6000, 384);

        for (int page = 0; page < 4; page++) {
            pool.submit(new PageBatch(0, page, entries));
        }
        pool.close();

        assertThat(registry.timer("swath.data_sync.latency", "format", "tsv").count())
                .isEqualTo(4L);
        assertThat(registry.summary("swath.data_sync.residual.bytes", "format", "tsv").count())
                .isEqualTo(1L);
        assertThat(registry.summary("swath.data_sync.residual.bytes", "format", "tsv").max())
                .isLessThan(WRITEBACK_BYTES);
        assertThat(DatasetLayout.of(dir).dataParts(".tsv")).hasSize(1);
    }

    @Test
    void poolDeletesAPartAndPublishesNothingAfterWritebackFailure(@TempDir Path dir)
            throws Exception {
        Files.createDirectories(dir);
        TextWriterPoolConfig config = new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, Long.MAX_VALUE, 1, 0, 0, WRITEBACK_BYTES, null);
        TextWriterPool pool = TextWriterPool.withDataForcer(
                config, ignored -> { throw new IOException("forced pool sync failure"); });
        pool.submit(new PageBatch(0, 0, incompressibleEntries(6000, 384)));

        assertThatThrownBy(pool::close).isInstanceOf(OutputException.class)
                .hasMessageContaining("writer");

        DatasetLayout layout = DatasetLayout.of(dir);
        assertThat(layout.dataParts(".tsv")).isEmpty();
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.success()).doesNotExist();
    }

    @Test
    void writebackIsByteInertThroughThePoolAndManifest(@TempDir Path dir) throws Exception {
        Path controlDir = Files.createDirectories(dir.resolve("control"));
        Path candidateDir = Files.createDirectories(dir.resolve("candidate"));
        TextWriterPool control = new TextWriterPool(new TextWriterPoolConfig(
                controlDir, OutputFormat.TSV, TextCompression.GZIP, true, "hash", "bucket",
                2, Long.MAX_VALUE, 1, 0, 0, 0L, null));
        TextWriterPool candidate = new TextWriterPool(new TextWriterPoolConfig(
                candidateDir, OutputFormat.TSV, TextCompression.GZIP, true, "hash", "bucket",
                2, Long.MAX_VALUE, 1, 0, 0, WRITEBACK_BYTES, null));
        PageBatch input = new PageBatch(0, 0, incompressibleEntries(14_000, 384));

        control.submit(input);
        candidate.submit(input);
        control.close();
        candidate.close();

        Path controlPart = DatasetLayout.of(controlDir).dataParts(".tsv.gz").getFirst();
        Path candidatePart = DatasetLayout.of(candidateDir).dataParts(".tsv.gz").getFirst();
        assertThat(Files.readAllBytes(candidatePart)).containsExactly(Files.readAllBytes(controlPart));
        assertThat(JSON.readTree(Files.readString(DatasetLayout.of(candidateDir).manifest())).path("files"))
                .isEqualTo(JSON.readTree(Files.readString(DatasetLayout.of(controlDir).manifest())).path("files"));
    }

    @Test
    void sizeRotationSkipsRedundantPeriodicSyncAtThePartBoundary(@TempDir Path dir)
            throws Exception {
        Files.createDirectories(dir);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        TextWriterPool pool = new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.TSV, TextCompression.NONE, true, "hash", "bucket",
                2, 1L, 8, 0, 0, WRITEBACK_BYTES, metrics));

        pool.submit(PageBatches.batch(0, 0, 0, 100));
        pool.close();

        assertThat(registry.counter("swath.text_dataset.rotation", "trigger", "size").count())
                .isEqualTo(1.0);
        assertThat(registry.find("swath.data_sync.latency").timer()).isNull();
    }

    private static InputStream decoded(Path part, TextCompression compression) throws IOException {
        InputStream input = Files.newInputStream(part);
        return switch (compression) {
            case NONE -> input;
            case GZIP -> new GZIPInputStream(input);
            case ZSTD -> new ZstdInputStream(input);
        };
    }

    private static List<ListEntry> incompressibleEntries(int count, int randomBytesPerKey) {
        Random random = new Random(0x5A17C0DEL);
        List<ListEntry> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            byte[] keyBytes = new byte[randomBytesPerKey];
            random.nextBytes(keyBytes);
            entries.add(new ObjectEntry(
                    KeyBytes.ofUtf8(index + "-" + HexFormat.of().formatHex(keyBytes)),
                    index,
                    1_700_000_000_000_000L + index,
                    "etag-" + index,
                    "STANDARD",
                    null,
                    true,
                    null,
                    null,
                    null,
                    null));
        }
        return entries;
    }

    private static TextWriterPool pool(Path dir, TextCompression compression, int writers)
            throws IOException {
        Files.createDirectories(dir);
        return new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.JSONL, compression, true, "hash", "bucket",
                writers, Long.MAX_VALUE, 8, 0, 0, 0L, null));
    }
}
