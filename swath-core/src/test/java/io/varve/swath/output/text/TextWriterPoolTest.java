/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class TextWriterPoolTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void gzipPartsAreIndependentAndTheirUnionIsExact(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.GZIP, 3);
        Set<String> expected = new HashSet<>();
        for (int node = 0; node < 3; node++) {
            var batch = PageBatches.batch(node, 0, node * 10, node * 10 + 5);
            batch.entries().forEach(entry -> expected.add(entry.key().asString()));
            pool.submit(batch);
        }
        pool.close();

        List<Path> parts = DatasetLayout.of(dir).dataParts(".jsonl.gz");
        assertThat(parts).hasSize(3).allMatch(path -> path.toString().endsWith(".jsonl.gz"));
        Set<String> actual = new HashSet<>();
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
    void abortPublishesNoSuccessMarker(@TempDir Path dir) throws Exception {
        TextWriterPool pool = pool(dir, TextCompression.NONE, 2);
        pool.submit(PageBatches.batch(0, 0, 0, 4));
        pool.abort();
        assertThat(DatasetLayout.of(dir).success()).doesNotExist();
    }

    private static TextWriterPool pool(Path dir, TextCompression compression, int writers)
            throws IOException {
        Files.createDirectories(dir);
        return new TextWriterPool(new TextWriterPoolConfig(
                dir, OutputFormat.JSONL, compression, true, "hash", "bucket",
                writers, Long.MAX_VALUE, 8, 0, 0));
    }
}
