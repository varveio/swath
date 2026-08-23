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
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.CountingWriter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class TextWriterPoolTest {
    private static final ObjectMapper JSON = new ObjectMapper();

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
    void codecConstructionErrorClosesAndDeletesTheCreatedPart(@TempDir Path dir) throws Exception {
        Path part = dir.resolve("part.jsonl");
        TextDatasetFormat format = new TextDatasetFormat(
                OutputFormat.JSONL, TextCompression.GZIP, true);

        assertThatThrownBy(() -> format.openPart(part, (stream, compression) -> {
            throw new AssertionError("codec construction failed");
        })).isInstanceOf(AssertionError.class).hasMessage("codec construction failed");
        assertThat(part).doesNotExist();
    }

    @Test
    void headerErrorRemainsPrimaryWhileCloseFailureIsSuppressedAndPartIsDeleted(
            @TempDir Path dir) throws Exception {
        Path part = dir.resolve("part.tsv");
        TextDatasetFormat format = new TextDatasetFormat(
                OutputFormat.TSV, TextCompression.NONE, true);

        assertThatThrownBy(() -> format.openPart(part, (stream, compression) ->
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
                2, Long.MAX_VALUE, 8, 0, 2));
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
                2, Long.MAX_VALUE, 8, 0, 1, metrics));
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
        assertThat(summary.datasetWriter().manifestWriteCount()).isEqualTo(2L);
        assertThat(summary.datasetWriter().lanes()).extracting(RunSummary.DatasetWriterLane::rowsWritten)
                .containsExactlyInAnyOrder(1L, 0L);
    }

    private static TextWriterPool pool(Path dir, TextCompression compression, int writers)
            throws IOException {
        Files.createDirectories(dir);
        return new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.JSONL, compression, true, "hash", "bucket",
                writers, Long.MAX_VALUE, 8, 0, 0));
    }
}
