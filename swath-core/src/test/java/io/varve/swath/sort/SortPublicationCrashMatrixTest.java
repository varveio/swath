/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.Manifest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Adversarial replacement-publication crash/re-entry matrix for {@link SortTransform}. */
final class SortPublicationCrashMatrixTest {

    private final ListEntryComparator comparator = new ListEntryComparator();

    @ParameterizedTest(name = "{0}, prior={1}, retain={2}")
    @MethodSource("publicationMatrix")
    void everyPublicationBoundaryConvergesToOneExactDebrisFreeSet(
            MergeShape shape, boolean priorOutput, boolean retain, @TempDir Path root) throws Exception {
        int attempt = 0;
        for (CrashPoint crash : shape.crashPoints()) {
            Path scenario = root.resolve("attempt-" + attempt++);
            Path output = Files.createDirectories(scenario.resolve("data"));
            Path staging = Files.createDirectories(scenario.resolve("_staging"));
            List<Path> originals = stage(staging, shape.segmentRows());
            List<String> originalNames = fileNames(originals);
            Files.writeString(staging.resolve("merge-abandoned.pageseg"), "debris");
            Files.writeString(staging.resolve("pipeline-00099.parquet.tmp"), "debris");
            if (priorOutput) {
                Files.writeString(output.resolve("part-00000.parquet"), "prior-zero");
                Files.writeString(output.resolve("part-99999.parquet"), "prior-extra");
            }
            AtomicInteger listenerCalls = new AtomicInteger();
            PublicationStepHook crashingHook = (step, ordinal) -> {
                if (crash.matches(step, ordinal)) {
                    throw new IOException("injected publication crash at " + crash);
                }
            };

            var failure = assertThatThrownBy(() -> transform(shape.config(retain), SortedFileWriterFactory.DEFAULT,
                    crashingHook).transform(originals, output, staging,
                            (parts, rows) -> listenerCalls.incrementAndGet(),
                            units -> { }, FinalPassListener.NO_OP))
                    .isInstanceOf(IOException.class);
            if (crash.afterListener()) {
                failure.isInstanceOf(CommittedPublicationCleanupException.class)
                        .hasRootCauseMessage("injected publication crash at " + crash);
            } else {
                failure.isNotInstanceOf(CommittedPublicationCleanupException.class)
                        .hasMessageContaining("injected publication crash");
            }

            assertThat(listenerCalls.get()).isEqualTo(crash.afterListener() ? 1 : 0);
            assertThat(scenario.resolve(Manifest.FILE_NAME)).doesNotExist();
            assertThat(scenario.resolve(Manifest.SUCCESS_FILE_NAME)).doesNotExist();
            if (priorOutput && crash.priorFinalsMustRemain()) {
                assertThat(Files.readString(output.resolve("part-00000.parquet"))).isEqualTo("prior-zero");
                assertThat(Files.readString(output.resolve("part-99999.parquet"))).isEqualTo("prior-extra");
            }

            // All failures before staging completion remain merge-reachable from the same durable
            // originals. With retention enabled, even a synthetic throw after completion can replay;
            // with retention off that step has already completed the direct transform in full.
            if (crash.step() != PublicationStep.AFTER_STAGING_COMPLETION || retain) {
                assertThat(originals).allMatch(Files::exists);
                transform(shape.config(retain), SortedFileWriterFactory.DEFAULT,
                        PublicationStepHook.NO_OP).transform(originals, output, staging,
                                (parts, rows) -> listenerCalls.incrementAndGet(),
                                units -> { }, FinalPassListener.NO_OP);
            }

            assertPublishedSet(output, shape.expectedKeys(), shape.expectedParts());
            assertNoWorkingDebris(scenario);
            if (retain) {
                assertThat(Files.isDirectory(staging)).isTrue();
                assertThat(immediateNames(staging)).containsExactlyInAnyOrderElementsOf(originalNames);
            } else {
                assertThat(staging).doesNotExist();
            }
        }
    }

    @Test
    void nthCloseFailureLeavesPriorFinalsAndOriginalsReachableThenRepairsPipelineShapes(
            @TempDir Path root) throws Exception {
        for (MergeShape shape : List.of(
                MergeShape.ONE_ENCODER_ROLLED, MergeShape.PIPELINE)) {
            Path scenario = root.resolve(shape.name().toLowerCase());
            Path output = Files.createDirectories(scenario.resolve("data"));
            Path staging = Files.createDirectories(scenario.resolve("_staging"));
            List<Path> originals = stage(staging, shape.segmentRows());
            Files.writeString(output.resolve("part-00000.parquet"), "prior-zero");
            Files.writeString(output.resolve("part-99999.parquet"), "prior-extra");
            CloseTrackingFactory writers = new CloseTrackingFactory(
                    SortedFileWriterFactory.DEFAULT, shape == MergeShape.PIPELINE ? 0 : 1);
            List<PublicationStep> steps = new ArrayList<>();

            assertThatThrownBy(() -> transform(shape.config(false), writers,
                    (step, ordinal) -> steps.add(step)).transform(
                            originals, output, staging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("injected close failure");

            assertThat(writers.failureInjected()).isTrue();
            assertThat(writers.openNow()).as("all partially-closed writers are quiescent").isZero();
            assertThat(steps).containsExactly(PublicationStep.AFTER_WORKING_SWEEP);
            assertThat(Files.readString(output.resolve("part-00000.parquet"))).isEqualTo("prior-zero");
            assertThat(Files.readString(output.resolve("part-99999.parquet"))).isEqualTo("prior-extra");
            assertThat(originals).allMatch(Files::exists);

            transform(shape.config(false), SortedFileWriterFactory.DEFAULT,
                    PublicationStepHook.NO_OP).transform(originals, output, staging,
                            PublishListener.NO_OP, units -> { }, FinalPassListener.NO_OP);
            assertPublishedSet(output, shape.expectedKeys(), shape.expectedParts());
            assertNoWorkingDebris(scenario);
            assertThat(staging).doesNotExist();
        }
    }

    @Test
    void onlyThePublishListenerCreatesAuthorityAndStagingCompletesAfterIt(@TempDir Path root)
            throws Exception {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> originals = stage(staging, MergeShape.SERIAL.segmentRows());
        AtomicInteger listenerCalls = new AtomicInteger();
        PublishListener authorityListener = (parts, rows) -> {
            listenerCalls.incrementAndGet();
            Files.writeString(root.resolve(Manifest.FILE_NAME), "listener-owned manifest");
            Files.writeString(root.resolve(Manifest.SUCCESS_FILE_NAME), "");
        };

        assertThatThrownBy(() -> transform(MergeShape.SERIAL.config(false),
                SortedFileWriterFactory.DEFAULT,
                crashAt(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC)).transform(
                        originals, output, staging, authorityListener,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class);
        assertThat(listenerCalls).hasValue(0);
        assertThat(root.resolve(Manifest.FILE_NAME)).doesNotExist();
        assertThat(root.resolve(Manifest.SUCCESS_FILE_NAME)).doesNotExist();
        assertThat(originals).allMatch(Files::exists);

        assertThatThrownBy(() -> transform(MergeShape.SERIAL.config(false),
                SortedFileWriterFactory.DEFAULT,
                crashAt(PublicationStep.AFTER_PUBLISH_LISTENER)).transform(
                        originals, output, staging, authorityListener,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class);
        assertThat(listenerCalls).hasValue(1);
        assertThat(root.resolve(Manifest.FILE_NAME)).exists();
        assertThat(root.resolve(Manifest.SUCCESS_FILE_NAME)).exists();
        assertThat(originals).as("listener precedes staging completion").allMatch(Files::exists);
    }

    @Test
    void ordinaryStagingDeletionFailureAfterAuthorityIsTypedAndObservable(@TempDir Path root)
            throws Exception {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> originals = stage(staging, MergeShape.SERIAL.segmentRows());
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PublishListener authorityListener = (parts, rows) -> {
            Files.writeString(root.resolve(Manifest.FILE_NAME), "listener-owned manifest");
            Files.writeString(root.resolve(Manifest.SUCCESS_FILE_NAME), "");
            // The merge has already consumed this original. Replacing it with a non-empty directory
            // makes the ordinary Files.deleteIfExists staging completion fail deterministically,
            // without relying on platform-specific permission behavior.
            Path blocked = originals.getFirst();
            Files.delete(blocked);
            Files.createDirectory(blocked);
            Files.writeString(blocked.resolve("still-present"), "cleanup blocker");
        };
        SortRun run = new SortRun(MergeShape.SERIAL.config(false), comparator,
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, metrics,
                SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);

        assertThatThrownBy(() -> new SortTransform(run).transform(
                originals, output, staging, authorityListener,
                units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(CommittedPublicationCleanupException.class)
                .satisfies(failure -> assertThat(
                        ((CommittedPublicationCleanupException) failure).stage())
                        .isEqualTo(CommittedPublicationCleanupException.Stage.ORIGINAL_STAGING_COMPLETION))
                .hasRootCauseInstanceOf(java.nio.file.DirectoryNotEmptyException.class);

        assertThat(root.resolve(Manifest.FILE_NAME)).hasContent("listener-owned manifest");
        assertThat(root.resolve(Manifest.SUCCESS_FILE_NAME)).exists();
        assertPublishedSet(output, MergeShape.SERIAL.expectedKeys(),
                MergeShape.SERIAL.expectedParts());
        assertThat(metrics.count("SORT.post_publish_cleanup_pending")).isEqualTo(1);
    }

    private static Stream<Arguments> publicationMatrix() {
        return Stream.of(MergeShape.SERIAL, MergeShape.PIPELINE,
                        MergeShape.EMPTY_PIPELINE_REQUEST)
                .flatMap(shape -> Stream.of(false, true)
                        .flatMap(prior -> Stream.of(false, true)
                                .map(retain -> Arguments.of(shape, prior, retain))));
    }

    private SortTransform transform(SortConfig config, SortedFileWriterFactory writers,
            PublicationStepHook hook) {
        return new SortTransform(run(config, writers), hook);
    }

    private SortRun run(SortConfig config, SortedFileWriterFactory writers) {
        return new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, writers, MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, SortRun.PROCESS_SOFT_FD_LIMIT,
                StaleFinalSweep.OWN_PARTS_ONLY);
    }

    private List<Path> stage(Path staging, List<List<ListEntry>> rowsBySegment) throws IOException {
        List<Path> paths = new ArrayList<>();
        for (int i = 0; i < rowsBySegment.size(); i++) {
            paths.add(SortTestSupport.writePageRun(
                    staging.resolve("seg-" + i + StagingNames.PAGE_RUN_SUFFIX),
                    rowsBySegment.get(i), comparator));
        }
        return List.copyOf(paths);
    }

    private static void assertPublishedSet(Path output, List<String> expectedKeys, int expectedParts)
            throws IOException {
        List<Path> parts;
        try (var listed = Files.newDirectoryStream(output, "part-*.parquet")) {
            parts = new ArrayList<>();
            listed.forEach(parts::add);
        }
        parts.sort(Comparator.comparing(path -> path.getFileName().toString()));
        assertThat(parts).hasSize(expectedParts);
        assertThat(fileNames(parts)).containsExactlyElementsOf(
                Stream.iterate(0, n -> n + 1).limit(expectedParts)
                        .map(StagingNames::finalPart).toList());
        assertThat(keys(parts)).containsExactlyElementsOf(expectedKeys);
    }

    private static List<String> keys(List<Path> parts) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : parts) {
            try (SegmentReader reader = new SegmentReader(part)) {
                while (reader.hasNext()) {
                    keys.add(reader.next().key().asString());
                }
            }
        }
        return keys;
    }

    private static void assertNoWorkingDebris(Path root) throws IOException {
        try (var paths = Files.walk(root)) {
            assertThat(paths.filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".tmp")
                            || name.startsWith("merge-")
                            || name.startsWith("prange-")))
                    .isEmpty();
        }
    }

    private static List<String> immediateNames(Path dir) throws IOException {
        try (var entries = Files.list(dir)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    private static List<String> fileNames(List<Path> paths) {
        return paths.stream().map(path -> path.getFileName().toString()).sorted().toList();
    }

    private static PublicationStepHook crashAt(PublicationStep target) {
        return (step, ordinal) -> {
            if (step == target) {
                throw new IOException("injected publication crash at " + target);
            }
        };
    }

    private static List<ListEntry> objects(String... keys) {
        List<ListEntry> rows = new ArrayList<>();
        for (String key : keys) {
            rows.add(new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null,
                    false, null, null, null, null));
        }
        return rows;
    }

    private enum MergeShape {
        SERIAL(1, Long.MAX_VALUE,
                List.of(objects("a", "d", "g"), objects("b", "e", "h"), objects("c", "f", "i")),
                1),
        ONE_ENCODER_ROLLED(1, 1L,
                List.of(objects("a", "b", "c"), objects("d", "e", "f"), objects("g", "h", "i")),
                3),
        PIPELINE(3, Long.MAX_VALUE,
                List.of(objects("a", "d", "g"), objects("b", "e", "h"), objects("c", "f", "i")),
                1),
        EMPTY_PIPELINE_REQUEST(3, Long.MAX_VALUE, List.of(), 1);

        private final int parallelism;
        private final long rollBytes;
        private final List<List<ListEntry>> segmentRows;
        private final int expectedParts;

        MergeShape(int parallelism, long rollBytes, List<List<ListEntry>> segmentRows,
                int expectedParts) {
            this.parallelism = parallelism;
            this.rollBytes = rollBytes;
            this.segmentRows = segmentRows;
            this.expectedParts = expectedParts;
        }

        SortConfig config(boolean retain) {
            return SortConfigs.base()
                    .withMergeParallelism(parallelism)
                    .withMergeBudgetBytes(64L << 20)
                    .withFinalFileBytes(rollBytes)
                    .withFinalization(SortFinalization.PIPELINE)
                    .withStagingRetention(retain
                            ? StagingRetention.RETAIN_ORIGINALS
                            : StagingRetention.DELETE_AFTER_PUBLISH);
        }

        List<List<ListEntry>> segmentRows() {
            return segmentRows;
        }

        List<String> expectedKeys() {
            return segmentRows.stream().flatMap(List::stream)
                    .map(row -> row.key().asString()).sorted().toList();
        }

        int expectedParts() {
            return expectedParts;
        }

        List<CrashPoint> crashPoints() {
            List<CrashPoint> points = new ArrayList<>();
            points.add(CrashPoint.simple(PublicationStep.AFTER_WORKING_SWEEP));
            points.add(CrashPoint.simple(PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE));
            points.add(CrashPoint.simple(PublicationStep.AFTER_STALE_FINAL_SWEEP));
            int renamedParts = expectedParts;
            for (int ordinal = 0; ordinal < renamedParts; ordinal++) {
                points.add(new CrashPoint(PublicationStep.AFTER_PART_RENAME, ordinal));
            }
            points.add(CrashPoint.simple(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC));
            points.add(CrashPoint.simple(PublicationStep.AFTER_PUBLISH_LISTENER));
            points.add(CrashPoint.simple(PublicationStep.AFTER_STAGING_COMPLETION));
            return List.copyOf(points);
        }
    }

    private record CrashPoint(PublicationStep step, int ordinal) {

        static CrashPoint simple(PublicationStep step) {
            return new CrashPoint(step, -1);
        }

        boolean matches(PublicationStep reached, int reachedOrdinal) {
            return step == reached && ordinal == reachedOrdinal;
        }

        boolean afterListener() {
            return step == PublicationStep.AFTER_PUBLISH_LISTENER
                    || step == PublicationStep.AFTER_STAGING_COMPLETION;
        }

        boolean priorFinalsMustRemain() {
            return step == PublicationStep.AFTER_WORKING_SWEEP
                    || step == PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE;
        }
    }

    /** Real writer decorator with one deterministic, retryable close failure and live-handle count. */
    private static final class CloseTrackingFactory implements SortedFileWriterFactory {
        private final SortedFileWriterFactory delegate;
        private final int failCloseIndex;
        private final AtomicInteger nextCloseIndex = new AtomicInteger();
        private final AtomicInteger openNow = new AtomicInteger();
        private final AtomicBoolean failureInjected = new AtomicBoolean();

        private CloseTrackingFactory(SortedFileWriterFactory delegate, int failCloseIndex) {
            this.delegate = delegate;
            this.failCloseIndex = failCloseIndex;
        }

        @Override
        public SortedFileWriterFactory forOutputSequence() {
            return new Sequence(delegate.forOutputSequence());
        }

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            return createTracked(delegate.create(path, fileIndex));
        }

        int openNow() {
            return openNow.get();
        }

        boolean failureInjected() {
            return failureInjected.get();
        }

        private SortedFileWriter createTracked(SortedFileWriter writer) {
            openNow.incrementAndGet();
            return new SortedFileWriter() {
                private final AtomicBoolean closed = new AtomicBoolean();

                @Override
                public void write(ListEntry entry) throws IOException {
                    writer.write(entry);
                }

                @Override
                public long rows() {
                    return writer.rows();
                }

                @Override
                public long dataSize() {
                    return writer.dataSize();
                }

                @Override
                public void markFinal() {
                    writer.markFinal();
                }

                @Override
                public Optional<FinalPartMetadata> finalMetadata() {
                    return writer.finalMetadata();
                }

                @Override
                public void close() throws IOException {
                    if (closed.get()) {
                        return;
                    }
                    int closeIndex = nextCloseIndex.getAndIncrement();
                    if (closeIndex == failCloseIndex && failureInjected.compareAndSet(false, true)) {
                        throw new IOException("injected close failure at index " + closeIndex);
                    }
                    writer.close();
                    if (closed.compareAndSet(false, true)) {
                        openNow.decrementAndGet();
                    }
                }
            };
        }

        private final class Sequence implements SortedFileWriterFactory {
            private final SortedFileWriterFactory sequenceDelegate;

            private Sequence(SortedFileWriterFactory sequenceDelegate) {
                this.sequenceDelegate = sequenceDelegate;
            }

            @Override
            public SortedFileWriter create(Path path, int fileIndex) throws IOException {
                return createTracked(sequenceDelegate.create(path, fileIndex));
            }
        }
    }
}
