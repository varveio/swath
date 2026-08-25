/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.sort.FinalPart;
import io.varve.swath.sort.FinalPartMetadata;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Guards the close-gated metadata fast path and the validating fallback for carried parts. */
class SortedManifestMetadataHandoffTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void trustedFreshMetadataPublishesWithoutOpeningTheFinalPart(@TempDir Path root) throws Exception {
        DatasetLayout layout = DatasetLayout.of(root);
        Files.createDirectories(layout.dataDir());
        Path absent = layout.dataDir().resolve("part-00000.parquet");
        FinalPartMetadata metadata =
                new FinalPartMetadata(2, 123, "0123456789abcdef0123456789abcdef",
                        "alpha", "omega", 23, 17, 10);

        // The path deliberately does not exist. Publication can succeed only if the new-part path
        // performs neither Files.size/MD5 nor SortedFileIndex.bounds after durable close.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ListRunner.writeSortedManifest(root, "bucket", "args", 7,
                List.of(new FinalPart(absent, Optional.of(metadata))),
                new RunMetrics(registry));

        JsonNode file = MAPPER.readTree(layout.manifest().toFile()).path("files").get(0);
        assertThat(file.path("key").asText()).isEqualTo("data/part-00000.parquet");
        assertThat(file.path("size").asLong()).isEqualTo(123);
        assertThat(file.path("MD5checksum").asText()).isEqualTo(metadata.md5());
        assertThat(file.path("rowCount").asLong()).isEqualTo(2);
        assertThat(file.path("minKey").asText()).isEqualTo("alpha");
        assertThat(file.path("maxKey").asText()).isEqualTo("omega");
        assertThat(Files.exists(layout.success())).isTrue();
        assertThat(registry.counter("swath.steal_reason", "outcome", "SORT", "reason",
                "manifest_metadata_trusted").count()).isEqualTo(1);
    }

    @Test
    void metadataLessCarriedPartIsStillReadAndRejectedWhenTruncated(@TempDir Path root) throws Exception {
        DatasetLayout layout = DatasetLayout.of(root);
        Files.createDirectories(layout.dataDir());
        Path truncated = layout.dataDir().resolve("part-00000.parquet");
        Files.write(truncated, new byte[] {1, 2, 3, 4});

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        assertThatThrownBy(() -> ListRunner.writeSortedManifest(root, "bucket", "args", 7,
                List.of(new FinalPart(truncated, Optional.empty())),
                new RunMetrics(registry)))
                .isInstanceOfAny(IOException.class, RuntimeException.class);
        assertThat(Files.exists(layout.success())).isFalse();
        assertThat(registry.counter("swath.steal_reason", "outcome", "SORT", "reason",
                "manifest_metadata_fallback_scan").count()).isEqualTo(1);
    }

    @Test
    void metadataBoundsPresenceMustMatchRowCount() {
        assertThatThrownBy(() -> new FinalPartMetadata(0, 1, "md5", "a", "z", 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("empty parts must have no bounds");
        assertThatThrownBy(() -> new FinalPartMetadata(1, 1, "md5", null, null, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("non-empty parts must have bounds");
    }
}
