/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static io.varve.swath.output.parquet.ParquetPoolTestSupport.parts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.model.PageBatch;
import io.varve.swath.testkit.ParquetReads;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Writer-pool unit checks: sticky routing, size rotation, manifest, exact-once parts. */
class ParquetWriterPoolTest {

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
    void abortKeepsRotatedPartsButDropsTheOpenTail(@TempDir Path dir) throws Exception {
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
        assertThat(DatasetLayout.of(dir).manifest()).exists();
        long durableRows = 0;
        for (Path part : parts(dir)) {
            durableRows += ParquetReads.keys(part).size();
        }
        // Only finalized parts are durable; the open tail was discarded ⇒ strictly fewer rows.
        assertThat(durableRows).isLessThan(1001).isGreaterThan(0);
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
