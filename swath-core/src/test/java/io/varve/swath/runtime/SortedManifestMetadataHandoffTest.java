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
        Path absent = layout.dataDir().resolve("part-00001.parquet");
        FinalPartMetadata metadata =
                new FinalPartMetadata(2, 123, "0123456789abcdef0123456789abcdef",
                        "alpha", "omega", 23, 17, 10);

        // The path deliberately does not exist. Publication can succeed only if the new-part path
        // performs neither Files.size/MD5 nor SortedFileIndex.bounds after durable close.
        ListRunner.writeSortedManifest(root, "bucket", "args", 7,
                List.of(new FinalPart(absent, Optional.of(metadata))),
                new RunMetrics(new SimpleMeterRegistry()));

        JsonNode file = MAPPER.readTree(layout.manifest().toFile()).path("files").get(0);
        assertThat(file.path("key").asText()).isEqualTo("data/part-00001.parquet");
        assertThat(file.path("size").asLong()).isEqualTo(123);
        assertThat(file.path("MD5checksum").asText()).isEqualTo(metadata.md5());
        assertThat(file.path("rowCount").asLong()).isEqualTo(2);
        assertThat(file.path("minKey").asText()).isEqualTo("alpha");
        assertThat(file.path("maxKey").asText()).isEqualTo("omega");
        assertThat(Files.exists(layout.success())).isTrue();
    }

    @Test
    void metadataLessCarriedPartIsStillReadAndRejectedWhenTruncated(@TempDir Path root) throws Exception {
        DatasetLayout layout = DatasetLayout.of(root);
        Files.createDirectories(layout.dataDir());
        Path truncated = layout.dataDir().resolve("part-00001.parquet");
        Files.write(truncated, new byte[] {1, 2, 3, 4});

        assertThatThrownBy(() -> ListRunner.writeSortedManifest(root, "bucket", "args", 7,
                List.of(new FinalPart(truncated, Optional.empty())),
                new RunMetrics(new SimpleMeterRegistry())))
                .isInstanceOfAny(IOException.class, RuntimeException.class);
        assertThat(Files.exists(layout.success())).isFalse();
    }
}
