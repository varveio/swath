/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.error.OutputException;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.PartInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrency guards for the one owner of consumer-facing dataset publication. These deliberately
 * exercise the coordinator directly, apart from lane scheduling, because a lost manifest update
 * would silently make finalized data unreachable to a consumer.
 */
class DatasetPublicationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DatasetFormat FORMAT = new DatasetFormat() {
        @Override public String partSuffix() { return ".test"; }
        @Override public String manifestFormat() { return "test"; }
        @Override public String manifestSchema() { return "test-schema"; }
        @Override public DatasetPartWriter openPart(Path path) {
            throw new AssertionError("DatasetPublication must not open part writers");
        }
    };

    @Test
    void concurrentFinalizesNeverLoseOrRegressPublishedParts(@TempDir Path dir) throws Exception {
        int partCount = 24;
        DatasetPublication publication = publication(dir, List.of());
        publication.publishFinalizedPart(part(0));

        CountDownLatch startingLine = new CountDownLatch(1);
        AtomicBoolean watch = new AtomicBoolean(true);
        AtomicReference<Throwable> watcherFailure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(partCount)) {
            Future<?> watcher = executor.submit(() -> {
                Set<String> lastSeen = Set.of();
                try {
                    while (watch.get()) {
                        Set<String> current = new LinkedHashSet<>(manifestPaths(dir));
                        if (!current.containsAll(lastSeen)) {
                            throw new AssertionError("manifest regressed from " + lastSeen + " to " + current);
                        }
                        lastSeen = current;
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                } catch (Throwable failure) {
                    watcherFailure.compareAndSet(null, failure);
                }
            });

            List<Future<?>> finalizers = new ArrayList<>();
            for (int i = 1; i < partCount; i++) {
                int part = i;
                finalizers.add(executor.submit(() -> {
                    startingLine.await();
                    publication.publishFinalizedPart(part(part));
                    return null;
                }));
            }
            try {
                startingLine.countDown();
                for (Future<?> finalizer : finalizers) {
                    finalizer.get(10, TimeUnit.SECONDS);
                }
            } finally {
                watch.set(false);
            }
            watcher.get(10, TimeUnit.SECONDS);
        }

        assertThat(watcherFailure.get()).isNull();
        assertThat(manifestPaths(dir)).containsExactlyInAnyOrderElementsOf(partPaths(partCount));
        assertThat(publication.committedPartCount()).isEqualTo(partCount);
        assertThat(publication.committedBytes()).isEqualTo(bytesFor(partCount));
        assertThat(publication.manifestWriteCount()).isEqualTo(partCount);
    }

    @Test
    void finalizedPartIsRejectedAfterSuccessWithoutMutatingPublishedArtifacts(@TempDir Path dir)
            throws Exception {
        DatasetPublication publication = publication(dir, List.of());
        publication.publishFinalizedPart(part(0));
        publication.publishSuccess();
        Map<String, byte[]> before = artifacts(dir);

        assertThatThrownBy(() -> publication.publishFinalizedPart(part(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("after _SUCCESS");

        assertThat(artifacts(dir)).satisfies(after -> before.forEach((name, bytes) ->
                assertThat(after.get(name)).as(name).isEqualTo(bytes)));
        assertThat(manifestPaths(dir)).containsExactly(part(0).path());
        assertThat(publication.committedPartCount()).isEqualTo(1);
    }

    @Test
    void existingPartsSeedCountersAndRemainUniqueInFinalManifest(@TempDir Path dir) throws Exception {
        List<PartInfo> existing = List.of(part(2), part(5));
        DatasetPublication publication = publication(dir, existing);

        assertThat(publication.committedPartCount()).isEqualTo(existing.size());
        assertThat(publication.committedBytes()).isEqualTo(existing.stream().mapToLong(PartInfo::bytes).sum());

        publication.publishFinalizedPart(part(9));
        publication.publishSuccess();

        List<String> paths = manifestPaths(dir);
        assertThat(paths).containsExactlyInAnyOrder(part(2).path(), part(5).path(), part(9).path());
        assertThat(new LinkedHashSet<>(paths)).hasSameSizeAs(paths);
        assertThat(Files.readAllLines(dir.resolve(Manifest.SYMLINK_FILE_NAME)))
                .containsExactlyInAnyOrderElementsOf(paths);
    }

    @Test
    void finalizedPartIsRejectedAfterTerminalPublicationFailure(@TempDir Path dir) throws Exception {
        Path notADirectory = dir.resolve("not-a-directory");
        Files.writeString(notADirectory, "not a directory");
        DatasetPublication publication = publication(notADirectory, List.of());

        assertThatThrownBy(publication::publishSuccess).isInstanceOf(OutputException.class);
        assertThatThrownBy(() -> publication.publishFinalizedPart(part(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publication failed");
    }

    @Test
    void failedPartManifestWriteRetainsThePartAndAllowsALaterMonotoneRewrite(@TempDir Path dir)
            throws Exception {
        DatasetPublication publication = publication(dir, List.of());
        Path blockedTempFile = dir.resolve(Manifest.FILE_NAME + ".tmp");
        Files.createDirectory(blockedTempFile);

        assertThatThrownBy(() -> publication.publishFinalizedPart(part(0)))
                .isInstanceOf(IOException.class);
        assertThat(publication.committedPartCount()).isEqualTo(1);
        assertThat(publication.committedBytes()).isEqualTo(part(0).bytes());

        Files.delete(blockedTempFile);
        publication.publishFinalizedPart(part(1));

        assertThat(manifestPaths(dir)).containsExactly(part(0).path(), part(1).path());
        assertThat(publication.manifestWriteCount())
                .as("the failed replacement and its successful retry are both measured")
                .isEqualTo(2);
    }

    @Test
    void concurrentSuccessPublishesOneTerminalSnapshot(@TempDir Path dir) throws Exception {
        DatasetPublication publication = publication(dir, List.of());
        publication.publishFinalizedPart(part(0));
        CountDownLatch startingLine = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> first = executor.submit(() -> {
                startingLine.await();
                publication.publishSuccess();
                return null;
            });
            Future<?> second = executor.submit(() -> {
                startingLine.await();
                publication.publishSuccess();
                return null;
            });
            startingLine.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        }

        assertThat(dir.resolve(Manifest.SUCCESS_FILE_NAME)).exists();
        assertThat(manifestPaths(dir)).containsExactly(part(0).path());
        assertThat(publication.manifestWriteCount())
                .as("one finalize and exactly one terminal snapshot write")
                .isEqualTo(2);
    }

    private static DatasetPublication publication(Path dir, List<PartInfo> existing) {
        AtomicLong ticks = new AtomicLong();
        return new DatasetPublication(dir, "bucket", FORMAT, "args-hash", existing,
                ticks::incrementAndGet);
    }

    private static PartInfo part(int number) {
        return new PartInfo("data/part-" + number + ".test", number % 3, number + 1L,
                100L + number, String.format("%032x", number));
    }

    private static List<String> partPaths(int count) {
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            paths.add(part(i).path());
        }
        return paths;
    }

    private static long bytesFor(int count) {
        long bytes = 0;
        for (int i = 0; i < count; i++) {
            bytes += part(i).bytes();
        }
        return bytes;
    }

    private static List<String> manifestPaths(Path dir) throws IOException {
        JsonNode files = MAPPER.readTree(Files.readString(dir.resolve(Manifest.FILE_NAME))).get("files");
        List<String> paths = new ArrayList<>();
        for (JsonNode file : files) {
            paths.add(file.get("key").textValue());
        }
        return paths;
    }

    private static Map<String, byte[]> artifacts(Path dir) throws IOException {
        Map<String, byte[]> artifacts = new LinkedHashMap<>();
        for (String name : List.of(Manifest.FILE_NAME, Manifest.STATE_FILE_NAME,
                Manifest.SYMLINK_FILE_NAME, Manifest.SUCCESS_FILE_NAME)) {
            artifacts.put(name, Files.readAllBytes(dir.resolve(name)));
        }
        return artifacts;
    }
}
