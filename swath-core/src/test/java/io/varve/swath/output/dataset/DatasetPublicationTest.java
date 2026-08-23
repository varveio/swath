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
import io.varve.swath.error.PublicationPendingException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrency guards for the one owner of consumer-facing dataset publication. These deliberately
 * exercise the coordinator directly, apart from lane scheduling, because the completion manifest
 * must include every concurrently finalized part while no incomplete manifest leaks mid-run.
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
    void concurrentFinalizesAreCountedWithoutPublishingAnIncompleteManifest(@TempDir Path dir)
            throws Exception {
        int partCount = 24;
        DatasetPublication publication = publication(dir, List.of());

        CountDownLatch startingLine = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(partCount)) {
            List<Future<?>> finalizers = new ArrayList<>();
            for (int i = 0; i < partCount; i++) {
                int part = i;
                finalizers.add(executor.submit(() -> {
                    startingLine.await();
                    publication.recordFinalizedPart(part(part));
                    return null;
                }));
            }
            startingLine.countDown();
            for (Future<?> finalizer : finalizers) {
                finalizer.get(10, TimeUnit.SECONDS);
            }
        }

        assertThat(dir.resolve(Manifest.FILE_NAME)).doesNotExist();
        assertThat(publication.committedPartCount()).isEqualTo(partCount);
        assertThat(publication.committedBytes()).isEqualTo(bytesFor(partCount));
        assertThat(publication.manifestWriteCount()).isZero();

        publication.publishSuccess();

        assertThat(manifestPaths(dir)).containsExactlyInAnyOrderElementsOf(partPaths(partCount));
        assertThat(publication.manifestWriteCount()).isEqualTo(1L);
    }

    @Test
    void finalizedPartIsRejectedAfterSuccessWithoutMutatingPublishedArtifacts(@TempDir Path dir)
            throws Exception {
        DatasetPublication publication = publication(dir, List.of());
        publication.recordFinalizedPart(part(0));
        publication.publishSuccess();
        Map<String, byte[]> before = artifacts(dir);

        assertThatThrownBy(() -> publication.recordFinalizedPart(part(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("after _SUCCESS");

        assertThat(artifacts(dir)).satisfies(after -> before.forEach((name, bytes) ->
                assertThat(after.get(name)).as(name).isEqualTo(bytes)));
        assertThat(manifestPaths(dir)).containsExactly(part(0).path());
        assertThat(publication.committedPartCount()).isEqualTo(1);
    }

    @Test
    void existingPartsSeedCountersAndReplaceLegacyPartialManifestAtSuccess(@TempDir Path dir)
            throws Exception {
        List<PartInfo> existing = List.of(part(2), part(5));
        Manifest.write(dir, "bucket", FORMAT.manifestFormat(), FORMAT.manifestSchema(),
                List.of(part(2)), false, null);
        DatasetPublication publication = publication(dir, existing);

        assertThat(publication.committedPartCount()).isEqualTo(existing.size());
        assertThat(publication.committedBytes()).isEqualTo(existing.stream().mapToLong(PartInfo::bytes).sum());

        publication.recordFinalizedPart(part(9));
        assertThat(manifestPaths(dir))
                .as("a legacy incomplete manifest is non-authoritative and is not refreshed mid-run")
                .containsExactly(part(2).path());
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
        publication.recordFinalizedPart(part(0));

        assertThatThrownBy(publication::publishSuccess).isInstanceOf(PublicationPendingException.class);
        assertThat(publication.committedPartCount()).isEqualTo(1L);
        assertThatThrownBy(() -> publication.recordFinalizedPart(part(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publication failed");
    }

    @Test
    void failedCompletionManifestRetainsPartsForANewResumePublication(@TempDir Path dir)
            throws Exception {
        DatasetPublication publication = publication(dir, List.of());
        publication.recordFinalizedPart(part(0));
        publication.recordFinalizedPart(part(1));
        Path blockedTempFile = dir.resolve(Manifest.FILE_NAME + ".tmp");
        Files.createDirectory(blockedTempFile);

        assertThatThrownBy(publication::publishSuccess).isInstanceOf(PublicationPendingException.class);
        assertThat(publication.committedPartCount()).isEqualTo(2L);
        assertThat(publication.committedBytes()).isEqualTo(part(0).bytes() + part(1).bytes());
        assertThat(publication.manifestWriteCount()).isEqualTo(1L);
        assertThat(dir.resolve(Manifest.FILE_NAME)).doesNotExist();

        Files.delete(blockedTempFile);
        DatasetPublication resumed = publication(dir, List.of(part(0), part(1)));
        resumed.publishSuccess();

        assertThat(manifestPaths(dir)).containsExactly(part(0).path(), part(1).path());
        assertThat(resumed.manifestWriteCount()).isEqualTo(1L);
    }

    @Test
    void concurrentSuccessPublishesOneTerminalSnapshot(@TempDir Path dir) throws Exception {
        DatasetPublication publication = publication(dir, List.of());
        publication.recordFinalizedPart(part(0));
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
                .as("concurrent success calls still emit exactly one terminal snapshot")
                .isEqualTo(1L);
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
