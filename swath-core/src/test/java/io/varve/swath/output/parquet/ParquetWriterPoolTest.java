/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static io.varve.swath.output.parquet.ParquetPoolTestSupport.incompressibleRowGroupBatch;
import static io.varve.swath.output.parquet.ParquetPoolTestSupport.parts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.error.OutputException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.LastModifiedParseException;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.dataset.PeriodicDataSync;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import io.varve.swath.testkit.ParquetReads;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Writer-pool unit checks: sticky routing, size rotation, manifest, exact-once parts. */
class ParquetWriterPoolTest {

    @Test
    void invalidTimestampAbortsWithoutPublishingAPart(@TempDir Path dir) throws Exception {
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1,
                Long.MAX_VALUE, 8);
        ObjectEntry entry = new ObjectEntry(KeyBytes.ofUtf8("bad-time"), 1L, "not-a-timestamp",
                "etag", "STANDARD", null, true, null, null, null, null);
        pool.submit(new PageBatch(0L, 0L, List.of(entry)));

        assertThatThrownBy(pool::close)
                .isInstanceOf(OutputException.class)
                .hasCauseInstanceOf(LastModifiedParseException.class);
        assertThat(parts(dir)).isEmpty();
        assertThat(DatasetLayout.of(dir).manifest()).doesNotExist();
    }

    @Test
    void periodicSyncFailureDeletesTheOpenPartAndPublishesNothing(@TempDir Path dir)
            throws Exception {
        var config = ParquetWriterPoolConfig.DEFAULT
                .withWritebackBytes(PeriodicDataSync.MIN_INTERVAL_BYTES);
        var pool = ParquetWriterPool.withDataForcer(
                dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 1, config,
                ignored -> { throw new java.io.IOException("forced parquet sync failure"); });
        pool.submit(incompressibleRowGroupBatch(0, 0));

        assertThatThrownBy(pool::close)
                .isInstanceOf(OutputException.class)
                .hasMessageContaining("writer");

        DatasetLayout layout = DatasetLayout.of(dir);
        assertThat(layout.dataParts()).isEmpty();
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.success()).doesNotExist();
    }

    @Test
    void successfulPeriodicSyncIsNeitherAbortNorCrashResumeAuthority(@TempDir Path dir)
            throws Exception {
        Path liveDir = dir.resolve("live");
        AtomicInteger successfulSyncs = new AtomicInteger();
        AtomicInteger finalizedEvents = new AtomicInteger();
        CountDownLatch syncCompleted = new CountDownLatch(1);
        var config = ParquetWriterPoolConfig.DEFAULT
                .withWritebackBytes(PeriodicDataSync.MIN_INTERVAL_BYTES)
                .withPartListener(ignored -> finalizedEvents.incrementAndGet());
        var pool = ParquetWriterPool.withDataForcer(
                liveDir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 1, config,
                channel -> {
                    channel.force(false);
                    successfulSyncs.incrementAndGet();
                    syncCompleted.countDown();
                });

        pool.submit(incompressibleRowGroupBatch(0, 0));
        assertThat(syncCompleted.await(20, TimeUnit.SECONDS))
                .as("the open part completed a real data-only force")
                .isTrue();

        DatasetLayout liveLayout = DatasetLayout.of(liveDir);
        Path openPart = liveLayout.dataFile("part-w0-00000.parquet");
        assertThat(openPart).exists();
        assertThat(successfulSyncs).hasValue(1);
        assertThat(finalizedEvents)
                .as("periodic sync must not invoke the checkpoint/finalization listener")
                .hasValue(0);
        assertThat(pool.committedPartCount()).isZero();
        assertThat(liveLayout.manifest()).doesNotExist();
        assertThat(liveLayout.success()).doesNotExist();

        // Preserve the exact open-file image as a stand-in for bytes left by SIGKILL. The live
        // pool then exercises normal abort cleanup, while the copied image independently exercises
        // resume's authority rule: no listener/checkpoint record means the file is discarded even
        // though a successful periodic force made its emitted prefix reach storage.
        Path crashedDir = dir.resolve("crashed");
        DatasetLayout crashedLayout = DatasetLayout.of(crashedDir);
        Path crashedPart = crashedLayout.dataFile(openPart.getFileName().toString());
        Files.createDirectories(crashedPart.getParent());
        Files.copy(openPart, crashedPart);
        assertThat(crashedPart).exists();

        pool.abort();

        assertThat(finalizedEvents).hasValue(0);
        assertThat(liveLayout.dataParts()).isEmpty();
        assertThat(liveLayout.manifest()).doesNotExist();
        assertThat(liveLayout.success()).doesNotExist();

        ParquetResume.discardNonFinalized(crashedDir, Set.of());
        assertThat(crashedLayout.dataParts()).isEmpty();
        assertThat(crashedLayout.manifest()).doesNotExist();
        assertThat(crashedLayout.success()).doesNotExist();
    }

    @Test
    void eightLanePoolPublishesEveryRowAndUniquePart(@TempDir Path dir) throws Exception {
        int writers = 8;
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", writers,
                Long.MAX_VALUE, SharedDatasetWriterPool.defaultQueueCapacityPerLane(writers));
        List<String> expected = new ArrayList<>();
        for (int lane = 0; lane < writers; lane++) {
            PageBatch submitted = batch(lane, 0, lane * 10, lane * 10 + 10);
            submitted.entries().forEach(entry -> expected.add(entry.key().asString()));
            pool.submit(submitted);
        }
        pool.close();

        List<Path> dataParts = parts(dir);
        assertThat(dataParts).hasSize(writers);
        assertThat(dataParts.stream().map(path -> path.getFileName().toString()))
                .doesNotHaveDuplicates()
                .allMatch(name -> name.matches("part-w[0-7]-00000\\.parquet"));

        List<String> actual = new ArrayList<>();
        for (Path part : dataParts) {
            actual.addAll(ParquetReads.keys(part));
        }
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        JsonNode files = new ObjectMapper().readTree(DatasetLayout.of(dir).manifest().toFile())
                .path("files");
        assertThat(files).hasSize(writers);
    }

    @Test
    void resumeWithFewerWritersContinuesLiveLaneSequencesWithoutOverwritingCarriedParts(
            @TempDir Path dir) throws Exception {
        int initialWriters = 8;
        var rotateEveryBatch = ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(1);
        try (var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash",
                initialWriters, Long.MAX_VALUE,
                SharedDatasetWriterPool.defaultQueueCapacityPerLane(initialWriters),
                rotateEveryBatch)) {
            for (int lane = 0; lane < initialWriters; lane++) {
                pool.submit(batch(lane, 0, lane, lane + 1));
            }
        }

        List<PartInfo> carried = parts(dir).stream().map(path -> {
            String name = path.getFileName().toString();
            int writerId = Integer.parseInt(name.substring("part-w".length(), name.indexOf('-', 6)));
            try {
                return new PartInfo(DatasetLayout.key(name), writerId, 1L, Files.size(path), "");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }).toList();
        assertThat(carried).hasSize(initialWriters);

        int resumedWriters = 3;
        var resumedConfig = rotateEveryBatch.withExistingParts(carried);
        try (var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash",
                resumedWriters, Long.MAX_VALUE,
                SharedDatasetWriterPool.defaultQueueCapacityPerLane(resumedWriters), resumedConfig)) {
            for (int lane = 0; lane < resumedWriters; lane++) {
                pool.submit(batch(lane, 1, 100 + lane, 101 + lane));
            }
        }

        List<String> names = parts(dir).stream().map(path -> path.getFileName().toString()).toList();
        assertThat(names).hasSize(initialWriters + resumedWriters);
        assertThat(names).doesNotHaveDuplicates();
        for (int lane = 0; lane < resumedWriters; lane++) {
            assertThat(names).contains("part-w" + lane + "-00000.parquet",
                    "part-w" + lane + "-00001.parquet");
        }
        for (int lane = resumedWriters; lane < initialWriters; lane++) {
            assertThat(names).contains("part-w" + lane + "-00000.parquet");
        }
    }

    @Test
    void rejectsQueueCapacityThatExceedsWholePoolBudget(@TempDir Path dir) {
        assertThatThrownBy(() -> new ParquetWriterPool(
                dir, ParquetSchema.canonical(), "hash", 64, Long.MAX_VALUE, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aggregate writer queue capacity <= 256")
                .hasMessageContaining("aggregate=320");
    }

    @Test
    void rotatesBySizeAndUnionEqualsInput(@TempDir Path dir) throws Exception {
        // Small rotation target forces multiple parts.
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, 64 * 1024, 8);
        TreeSet<String> expected = new TreeSet<>();
        for (int p = 0; p < 20; p++) {
            PageBatch b = batch(0, p, p * 1000, p * 1000 + 1000);
            b.entries().forEach(e -> expected.add(e.key().asString()));
            pool.submit(b);
        }
        pool.close();

        List<Path> parts = parts(dir);
        assertThat(parts.size()).isGreaterThan(1);             // rotation happened
        assertThat(DatasetLayout.of(dir).manifest()).exists();

        TreeSet<String> got = new TreeSet<>();
        for (Path part : parts) {
            got.addAll(ParquetReads.keys(part));
        }
        assertThat(got).isEqualTo(expected);                   // union == input, no loss/dup
    }

    @Test
    void stickyRoutingSendsANodeToOneWriter(@TempDir Path dir) throws Exception {
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 3, Long.MAX_VALUE, 8);
        // node 0 → writer 0, node 1 → writer 1, node 4 → writer 1 (4 % 3).
        pool.submit(batch(0, 0, 0, 10));
        pool.submit(batch(1, 0, 0, 10));
        pool.submit(batch(4, 0, 0, 10));
        pool.close();

        List<String> names = parts(dir).stream().map(p -> p.getFileName().toString()).toList();
        assertThat(names).anyMatch(n -> n.startsWith("part-w0-"));   // node 0
        assertThat(names).anyMatch(n -> n.startsWith("part-w1-"));   // nodes 1 and 4
        assertThat(names).noneMatch(n -> n.startsWith("part-w2-"));  // node 2 unused
    }

    @Test
    void abortDiscardsOpenNonFinalizedPartAndWritesNoManifest(@TempDir Path dir) throws Exception {
        // No rotation (huge target) → everything sits in open parts. abort() must delete them
        // (non-durable) and leave no manifest blessing a failed run.
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 2, Long.MAX_VALUE, 8);
        pool.submit(batch(0, 0, 0, 500));
        pool.submit(batch(1, 0, 0, 500));
        pool.abort();   // must complete (no deadlock) and discard

        assertThat(parts(dir)).isEmpty();                          // open parts deleted
        assertThat(DatasetLayout.of(dir).manifest()).doesNotExist();
    }

    @Test
    void abortKeepsRotatedPartsButDropsTheOpenTailWithoutPublishingManifest(@TempDir Path dir)
            throws Exception {
        // Make the durable part and open tail explicit so abort cannot race the lane before the
        // first rotation or after every submitted row has already rotated.
        var config = ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(1000);
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1,
                Long.MAX_VALUE, 8, config);
        pool.submit(batch(0, 0, 0, 1000));
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);

        pool.submit(batch(0, 1, 1000, 1001));
        await().atMost(Duration.ofSeconds(5)).until(() ->
                Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00001.parquet")));
        pool.abort();

        assertThat(parts(dir)).isNotEmpty();                      // rotated parts survive
        assertThat(DatasetLayout.of(dir).manifest())
                .as("a failed dataset retains resume-durable parts but publishes no consumer snapshot")
                .doesNotExist();
        long durableRows = 0;
        for (Path part : parts(dir)) {
            durableRows += ParquetReads.keys(part).size();
        }
        // Only the 1,000-row finalized part is durable; the open tail was discarded.
        assertThat(durableRows).isEqualTo(1000);
    }

    @Test
    void manifestIsConsumerSchemaAndArgsHashInStateFile(@TempDir Path dir) throws Exception {
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "deadbeef", 2, Long.MAX_VALUE, 8);
        pool.submit(batch(0, 0, 0, 500));
        pool.submit(batch(1, 0, 0, 500));
        pool.close();

        DatasetLayout layout = DatasetLayout.of(dir);
        // Consumer manifest: S3-Inventory schema, data/-prefixed keys, size + MD5, no identity.
        String manifest = Files.readString(layout.manifest());
        assertThat(manifest).contains("\"fileFormat\": \"Parquet\"");
        assertThat(manifest).contains("\"key\": \"data/part-w");
        assertThat(manifest).contains("\"MD5checksum\":");
        assertThat(manifest).doesNotContain("args_hash");
        assertThat(manifest).doesNotContain("total_rows");
        // Resume identity lives in the internal state file.
        String state = Files.readString(layout.state());
        assertThat(state).contains("\"args_hash\": \"deadbeef\"");
        // _SUCCESS marker written last on a successful close.
        assertThat(layout.success()).exists();
    }

    @Test
    void manifestParsesAndEachFileMatchesRealPartSizeAndMd5_dataDirIsPureParquet(@TempDir Path dir)
            throws Exception {
        // Small rotation target ⇒ several real parts to check across.
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, 64 * 1024, 8);
        for (int p = 0; p < 12; p++) {
            pool.submit(batch(0, p, p * 1000, p * 1000 + 1000));
        }
        pool.close();

        DatasetLayout layout = DatasetLayout.of(dir);
        JsonNode manifest = new ObjectMapper().readTree(layout.manifest().toFile());
        assertThat(manifest.path("fileFormat").asText()).isEqualTo("Parquet");
        assertThat(manifest.path("sourceBucket").isMissingNode()).isFalse();

        JsonNode files = manifest.path("files");
        assertThat(files.isArray()).isTrue();
        assertThat(files.size()).isGreaterThan(1);   // rotation produced several parts

        // Every manifest file entry names a real data/<name> part; size + MD5 are computed
        // INDEPENDENTLY here and must match the manifest exactly.
        List<String> partNames = parts(dir).stream().map(p -> p.getFileName().toString()).toList();
        for (JsonNode f : files) {
            String key = f.path("key").asText();
            assertThat(key).startsWith("data/");
            String name = key.substring("data/".length());
            assertThat(partNames).contains(name);   // key EQUALS an actual filename under data/
            Path part = layout.dataFile(name);
            assertThat(f.path("size").asLong()).isEqualTo(Files.size(part));
            assertThat(f.path("MD5checksum").asText()).isEqualTo(md5Hex(Files.readAllBytes(part)));
        }

        // data/ is PURE parquet — no .tmp, no markers, no manifest, nothing but *.parquet.
        try (var entries = Files.list(layout.dataDir())) {
            assertThat(entries.map(p -> p.getFileName().toString()))
                    .allMatch(n -> n.endsWith(".parquet"));
        }
        // _SUCCESS exists and is empty (an empty completion marker).
        assertThat(layout.success()).exists();
        assertThat(Files.size(layout.success())).isZero();
    }

    private static String md5Hex(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("MD5").digest(bytes);
        return String.format("%032x", new BigInteger(1, digest));
    }
}
