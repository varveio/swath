/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.testkit.ParquetReads;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The single-sink {@link ParquetFormatter} (contract §4): rows written through the sealed
 * {@code EntryFormatter} SPI land in a canonical-schema part under {@code data/} that round-trips,
 * and {@code close()} commits the consumer {@code manifest.json} + internal {@code .swath-state.json}
 * + {@code _SUCCESS} (I6). Guards against the formatter regressing to the throw-everything stub.
 */
class ParquetFormatterTest {

    @Test
    void writesCanonicalPartAndCommitsManifest(@TempDir Path dir) throws Exception {
        List<ListEntry> entries = List.of(
                ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("a/one.txt"), 10L, 1_700_000_000_000_000L,
                        "etag1", "STANDARD", null, true, null, null),
                ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("a/two.txt"), 20L, 1_700_000_000_000_000L,
                        "etag2", "GLACIER", null, true, null, null),
                new CommonPrefixEntry(KeyBytes.ofUtf8("a/")));

        try (ParquetFormatter f = new ParquetFormatter(dir, "my-bucket", "argshashXYZ")) {
            f.writeHeader();
            for (ListEntry e : entries) {
                f.write(e);
            }
        }

        DatasetLayout layout = DatasetLayout.of(dir);
        Path part = layout.dataFile("part-00000.parquet");
        assertThat(part).exists();

        // Canonical schema, exactly (physical + logical types, nullability, field order).
        assertThat(ParquetReads.schema(part)).isEqualTo(ParquetSchema.canonical());

        // The part's rows == what we wrote, in order, exactly once.
        assertThat(ParquetReads.keys(part)).containsExactly("a/one.txt", "a/two.txt", "a/");

        // Consumer manifest: S3-Inventory schema — parsed and checked field-by-field, size + MD5
        // computed INDEPENDENTLY here and matched exactly.
        JsonNode manifest = new ObjectMapper().readTree(layout.manifest().toFile());
        assertThat(manifest.path("sourceBucket").asText()).isEqualTo("my-bucket");
        assertThat(manifest.path("fileFormat").asText()).isEqualTo("Parquet");
        assertThat(manifest.path("creationTimestamp").isMissingNode()).isFalse();
        JsonNode files = manifest.path("files");
        assertThat(files.size()).isEqualTo(1);
        JsonNode file = files.get(0);
        assertThat(file.path("key").asText()).isEqualTo("data/part-00000.parquet");
        assertThat(file.path("size").asLong()).isEqualTo(Files.size(part));
        assertThat(file.path("MD5checksum").asText()).isEqualTo(md5Hex(Files.readAllBytes(part)));
        // Identity lives in the internal state file, never the consumer manifest.
        assertThat(Files.readString(layout.manifest())).doesNotContain("args_hash");
        String state = Files.readString(layout.state());
        assertThat(state).contains("\"args_hash\": \"argshashXYZ\"");
        // data/ is PURE parquet.
        try (var dataEntries = Files.list(layout.dataDir())) {
            assertThat(dataEntries.map(p -> p.getFileName().toString()))
                    .allMatch(n -> n.endsWith(".parquet"));
        }
        // _SUCCESS marker written on success, and empty.
        assertThat(layout.success()).exists();
        assertThat(Files.size(layout.success())).isZero();
    }

    private static String md5Hex(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("MD5").digest(bytes);
        return String.format("%032x", new BigInteger(1, digest));
    }
}
