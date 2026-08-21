/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.s3.S3ClientFactory;
import io.varve.swath.store.s3.S3Config;
import io.varve.swath.store.s3.S3PageFetcher;
import io.varve.swath.store.s3.S3PageFetcherConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class SwathRoundTripIT {

    @Test
    void swathFetcherReadsSwathParquetThroughReplayServer(@TempDir Path dir) throws Exception {
        Path parquet = dir.resolve("part-00000.parquet");
        try (var writer = ParquetFixtures.open(parquet)) {
            writer.write(object("a/1.txt", 10));
            writer.write(object("a/2.txt", 20));
            writer.write(object("b/1.txt", 30));
        }

        try (ListObjectsV2Pager direct = pager(parquet)) {
            assertThat(direct.list(new S3ListRequest(
                    "bucket", null, null, null, null, 2, true, false)).entries())
                    .hasSize(2);
        }

        try (ListObjectsV2Pager fixture = pager(parquet);
             ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", fixture)) {
            server.start();
            S3Config config = new S3Config(
                    Region.US_EAST_1,
                    URI.create("http://127.0.0.1:" + server.port()),
                    true,
                    4,
                    S3Config.DEFAULT_MAX_ATTEMPTS,
                    Duration.ofSeconds(5),
                    S3Config.DEFAULT_API_CALL_TIMEOUT,
                    AnonymousCredentialsProvider.create(),
                    S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
            try (var client = S3ClientFactory.create(config)) {
                S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

                var first = fetcher.fetchPage(PageRequest.objects(null, null, 2));
                assertThat(first.entries()).extracting(e -> e.key().asString())
                        .containsExactly("a/1.txt", "a/2.txt");
                assertThat(first.truncated()).isTrue();

                var second = fetcher.fetchPage(PageRequest.objects(null,
                        first.entries().getLast().key().rawUnsafe(), 2));
                assertThat(second.entries()).extracting(e -> e.key().asString())
                        .containsExactly("b/1.txt");
                assertThat(second.truncated()).isFalse();
            }
        }
    }

    @Test
    void swathDelimitedProbeReceivesCommonPrefixes(@TempDir Path dir) throws Exception {
        Path parquet = dir.resolve("part-00000.parquet");
        try (var writer = ParquetFixtures.open(parquet)) {
            writer.write(object("a/0.txt", 10));
            writer.write(object("a/dir/1.txt", 20));
            writer.write(object("a/dir/2.txt", 30));
            writer.write(object("a/z.txt", 40));
        }

        try (ListObjectsV2Pager fixture = pager(parquet);
             ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", fixture)) {
            server.start();
            S3Config config = new S3Config(
                    Region.US_EAST_1,
                    URI.create("http://127.0.0.1:" + server.port()),
                    true,
                    4,
                    S3Config.DEFAULT_MAX_ATTEMPTS,
                    Duration.ofSeconds(5),
                    S3Config.DEFAULT_API_CALL_TIMEOUT,
                    AnonymousCredentialsProvider.create(),
                    S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
            try (var client = S3ClientFactory.create(config)) {
                S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

                var page = fetcher.fetchPage(PageRequest.objectsDelimited(
                        bytes("a/"), bytes("/"), null, 1000));

                assertThat(page.entries()).extracting(e -> e.key().asString())
                        .containsExactly("a/0.txt", "a/z.txt");
                assertThat(page.commonPrefixes()).extracting(KeyBytes::asString)
                        .containsExactly("a/dir/");
            }
        }
    }

    @Test
    void swathFetcherReadsOwnerDisplayNameAndChecksumTypeThroughReplayServer(@TempDir Path dir) throws Exception {
        Path parquet = dir.resolve("part-00000.parquet");
        try (var writer = ParquetFixtures.open(parquet)) {
            writer.write(ObjectEntries.key("a/1.txt").size(10).lastModifiedEpochMicros(1_767_225_600_000_000L)
                    .etag("etag-10").storageClass("STANDARD").isLatest(true)
                    .owner("owner-1", "Alice").checksum("SHA256", "FULL_OBJECT").build());
        }

        try (ListObjectsV2Pager fixture = pager(parquet);
             ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", fixture)) {
            server.start();
            S3Config config = new S3Config(
                    Region.US_EAST_1,
                    URI.create("http://127.0.0.1:" + server.port()),
                    true,
                    4,
                    S3Config.DEFAULT_MAX_ATTEMPTS,
                    Duration.ofSeconds(5),
                    S3Config.DEFAULT_API_CALL_TIMEOUT,
                    AnonymousCredentialsProvider.create(),
                    S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
            try (var client = S3ClientFactory.create(config)) {
                S3PageFetcher fetcher = new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withFetchOwner(true));

                var page = fetcher.fetchPage(PageRequest.objects(null, null, 1000));

                ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
                assertThat(entry.ownerId()).isEqualTo("owner-1");
                assertThat(entry.ownerDisplayName()).isEqualTo("Alice");
                assertThat(entry.checksumAlgorithm()).isEqualTo("SHA256");
                assertThat(entry.checksumType()).isEqualTo("FULL_OBJECT");
            }
        }
    }

    @Test
    void duckDbStoreAcceptsSwathParquetDirectory(@TempDir Path dir) throws Exception {
        Path parquet = dir.resolve("part-00000.parquet");
        try (var writer = ParquetFixtures.open(parquet)) {
            writer.write(object("a/1.txt", 10));
        }

        try (ListObjectsV2Pager fixture = pager(dir)) {
            S3ListResult page = fixture.list(new S3ListRequest(
                    "bucket", null, null, null, null, 1000, true, false));

            assertThat(page.entries()).extracting(e -> ByteKeys.utf8(e.key()))
                    .containsExactly("a/1.txt");
        }
    }

    @Test
    void sortedModeServesIdenticallyToDuckDbThroughTheRealSdk(@TempDir Path dir) throws Exception {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        List<String> expectedKeys = new ArrayList<>();
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            // Unsorted arrival order across several delimiter'd prefixes — the sort must recover order.
            for (int d = 0; d < 4; d++) {
                for (int i = 0; i < 15; i++) {
                    String key = String.format("dir%d/obj-%03d.txt", d, i);
                    expectedKeys.add(key);
                    writer.write(object(key, 10L + i));
                }
            }
            expectedKeys.add("root-b");
            writer.write(object("root-b", 1));
            expectedKeys.add("root-a");
            writer.write(object("root-a", 2));
        }
        expectedKeys.sort((left, right) -> Arrays.compareUnsigned(
                left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8)));

        Path sorted = Files.createDirectories(dir.resolve("sorted"));
        new CaptureSorter(SortConfig.fromSystemProperties()).sort(capture, sorted);

        // The real AWS SDK walks the whole listing against a --serving-mode sorted server and against
        // a DuckDB-mode server over the same fixture; both must return the identical key sequence.
        List<String> sortedKeys = walkViaSdk(sorted, ServingMode.SORTED, ServingMode.SORTED, 7);
        List<String> duckKeys = walkViaSdk(sorted, ServingMode.DUCKDB, ServingMode.DUCKDB, 7);

        assertThat(sortedKeys).containsExactlyElementsOf(expectedKeys);
        assertThat(duckKeys).containsExactlyElementsOf(expectedKeys);
        assertThat(sortedKeys).isEqualTo(duckKeys);
    }

    private static List<String> walkViaSdk(Path fixture, ServingMode mode, ServingMode expected, int maxKeys)
            throws Exception {
        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", fixture, 4, mode)) {
            server.start();
            assertThat(server.resolvedServingMode()).isEqualTo(expected);
            S3Config config = new S3Config(
                    Region.US_EAST_1,
                    URI.create("http://127.0.0.1:" + server.port()),
                    true,
                    4,
                    S3Config.DEFAULT_MAX_ATTEMPTS,
                    Duration.ofSeconds(5),
                    S3Config.DEFAULT_API_CALL_TIMEOUT,
                    AnonymousCredentialsProvider.create(),
                    S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
            try (var client = S3ClientFactory.create(config)) {
                S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");
                List<String> keys = new ArrayList<>();
                byte[] startAfter = null;
                while (true) {
                    var page = fetcher.fetchPage(PageRequest.objects(null, startAfter, maxKeys));
                    page.entries().forEach(e -> keys.add(e.key().asString()));
                    if (!page.truncated()) {
                        return keys;
                    }
                    startAfter = page.entries().getLast().key().rawUnsafe();
                }
            }
        }
    }

    private static ListObjectsV2Pager pager(Path parquet) {
        ReplayMetrics metrics = new ReplayMetrics();
        return new ListObjectsV2Pager(
                new DuckDbListingStore(parquet, metrics, DuckDbListingStore.defaultConnectionCount()), metrics);
    }

    private static ObjectEntry object(String key, long size) {
        return ObjectEntries.key(key).size(size).lastModifiedEpochMicros(1_767_225_600_000_000L)
                .etag("etag-" + size).storageClass("STANDARD").isLatest(true).build();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
