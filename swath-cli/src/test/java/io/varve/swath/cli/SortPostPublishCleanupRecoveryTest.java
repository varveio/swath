/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.OutputException;
import io.varve.swath.error.PublicationPendingException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.sorted.CommittedPublicationCleanupException;
import io.varve.swath.output.sorted.PublicationStep;
import io.varve.swath.output.sorted.StagingReconciliation;
import io.varve.swath.output.sorted.StagingRetention;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortTransform;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

/** Managed WP8 post-commit cleanup failure and PUBLISHED re-entry contract. */
final class SortPostPublishCleanupRecoveryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";

    @Test
    void preListenerFailureRemainsFatalAndResumeRefuses(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = root.resolve("checkpoint.sqlite");
        ListCommand initial = command(outputDir, checkpoint,
                fetcher(List.of("data/a", "data/b")), false);
        initial.listRunnerOverride = new ListRunner((step, ordinal) -> {
            if (step == PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC) {
                throw new java.io.IOException("injected pre-listener publication failure");
            }
        });

        Throwable failure = catchThrowable(initial::call);

        assertThat(failure)
                .isInstanceOf(OutputException.class)
                .isNotInstanceOf(PublicationPendingException.class)
                .hasMessageContaining("sort merge/publish failed");
        DatasetLayout layout = DatasetLayout.of(outputDir);
        assertThat(layout.success()).doesNotExist();
        assertThat(CheckpointDbProbe.runStatus(checkpoint)).isEqualTo("FAILED");
        assertThat(CheckpointDbProbe.fatalError(checkpoint)).isTrue();

        MockPageFetcher forbidden = MockPageFetcher.builder().keys(List.of()).build();
        ListCommand resume = command(outputDir, checkpoint, forbidden, false);
        resume.checkpoint.resume = true;
        assertThat(catchThrowable(resume::call))
                .isInstanceOf(io.varve.swath.error.InvalidArgsException.class)
                .hasMessageContaining("FAILED");
        assertThat(forbidden.apiCalls()).isZero();
    }

    @ParameterizedTest(name = "keep staging={0}")
    @ValueSource(booleans = {false, true})
    void committedCleanupFailureNeverPoisonsAndResumeOnlyCleans(
            boolean retainStaging, @TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = root.resolve("checkpoint.sqlite");
        List<String> expectedKeys = Stream.iterate(0, n -> n + 1).limit(50)
                .map(n -> String.format("data/key-%05d", n)).toList();
        MockPageFetcher initialFetcher = fetcher(expectedKeys);
        ListCommand initial = command(outputDir, checkpoint, initialFetcher, retainStaging);
        initial.listRunnerOverride = new ListRunner((step, ordinal) -> {
            if (step == PublicationStep.AFTER_PUBLISH_LISTENER) {
                throw new java.io.IOException("injected managed post-publish cleanup failure");
            }
        });

        Logger logger = (Logger) LoggerFactory.getLogger(SortTransform.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);
        Throwable failure;
        try {
            failure = catchThrowable(initial::call);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        assertThat(failure)
                .isInstanceOf(PublicationPendingException.class)
                .hasMessageContaining("publication committed; cleanup pending")
                .hasCauseInstanceOf(CommittedPublicationCleanupException.class);
        CommittedPublicationCleanupException committed =
                (CommittedPublicationCleanupException) failure.getCause();
        assertThat(committed.stage())
                .isEqualTo(CommittedPublicationCleanupException.Stage.AFTER_PUBLISH_LISTENER_HOOK);
        assertThat(initialFetcher.apiCalls()).isPositive();

        DatasetLayout layout = DatasetLayout.of(outputDir);
        Manifest.Identity identity = Manifest.readIdentity(outputDir).orElseThrow();
        long runId = identity.runId();
        assertThat(layout.manifest()).exists();
        assertThat(layout.state()).exists();
        assertThat(layout.symlink()).exists();
        assertThat(layout.success()).exists();
        assertThat(outputKeys(outputDir)).containsExactlyElementsOf(expectedKeys);
        DatasetSnapshot published = snapshot(layout);

        assertThat(checkpoint).as("the failed invocation keeps its resume ledger").exists();
        assertThat(CheckpointDbProbe.runStatusEnum(checkpoint, runId)).isEqualTo(RunStatus.RUNNING);
        assertThat(CheckpointDbProbe.fatalError(checkpoint)).isFalse();
        Set<String> finalizedNames;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            assertThat(store.sortPhase(runId))
                    .as("the caught post-commit failure is a PUBLISHED cleanup state")
                    .isEqualTo(SortPhase.PUBLISHED);
            finalizedNames = store.finalizedParts(runId).stream()
                    .filter(part -> ListRunner.SORT_SEGMENT_FORMAT.equals(part.format()))
                    .map(PartRef::path)
                    .collect(Collectors.toCollection(TreeSet::new));
        }
        assertThat(finalizedNames).isNotEmpty();
        assertThat(stagingNames(outputDir.resolve(ListCommand.SORT_STAGING_DIR)))
                .containsExactlyInAnyOrderElementsOf(finalizedNames);

        String marker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("sort_post_publish_cleanup_pending "))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no post-publish cleanup marker emitted"));
        assertThat(marker)
                .contains("publication_committed=true")
                .contains("cleanup_pending=true")
                .contains("stage=after_publish_listener_hook");
        assertThat(reasonCount(outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME),
                "post_publish_cleanup_pending")).isEqualTo(1);

        AtomicInteger cleanupAttempts = new AtomicInteger();
        ListCommand.PublishedSortCleanup flakyCleanup =
                (out, staging, sortConfig, segments) -> {
                    assertThat(Manifest.readIdentity(out)).contains(identity);
                    assertThat(DatasetLayout.of(out).success()).exists();
                    int attempt = cleanupAttempts.incrementAndGet();
                    if (attempt == 1) {
                        throw new java.io.IOException("injected repeatable PUBLISHED cleanup failure");
                    }
                    if (attempt == 2) {
                        throw new IllegalStateException(
                                "injected runtime PUBLISHED reconciliation failure");
                    }
                    return ListCommand.cleanSortStagingAndStaleTmp(
                            out, staging, sortConfig, segments);
                };

        for (int attempt = 1; attempt <= 2; attempt++) {
            MockPageFetcher forbidden = failOnAnyList();
            ListCommand resume = command(outputDir, checkpoint, forbidden, retainStaging);
            resume.checkpoint.resume = true;
            resume.publishedSortCleanupOverride = flakyCleanup;

            assertThat(catchThrowable(resume::call))
                    .as("PUBLISHED cleanup attempt %s", attempt)
                    .isInstanceOf(PublicationPendingException.class)
                    .hasMessageContaining("PUBLISHED cleanup pending")
                    .hasCauseInstanceOf(CommittedPublicationCleanupException.class);
            assertThat(forbidden.apiCalls()).isZero();
            assertThat(snapshot(layout)).isEqualTo(published);
            assertThat(CheckpointDbProbe.runStatusEnum(checkpoint, runId)).isEqualTo(RunStatus.RUNNING);
            assertThat(CheckpointDbProbe.fatalError(checkpoint)).isFalse();
            try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
                assertThat(store.sortPhase(runId)).isEqualTo(SortPhase.PUBLISHED);
            }
        }

        MockPageFetcher forbidden = failOnAnyList();
        ListCommand resume = command(outputDir, checkpoint, forbidden, retainStaging);
        resume.checkpoint.resume = true;
        resume.publishedSortCleanupOverride = flakyCleanup;
        assertThat(resume.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(cleanupAttempts).hasValue(3);
        assertThat(forbidden.apiCalls()).isZero();
        assertThat(snapshot(layout)).isEqualTo(published);
        assertThat(outputKeys(outputDir)).containsExactlyElementsOf(expectedKeys);
        assertThat(checkpoint).exists();
        assertThat(CheckpointDbProbe.runStatusEnum(checkpoint, runId)).isEqualTo(RunStatus.COMPLETED);
        assertThat(CheckpointDbProbe.fatalError(checkpoint)).isFalse();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            assertThat(store.sortPhase(runId)).isEqualTo(SortPhase.PUBLISHED);
        }
        Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        if (retainStaging) {
            assertThat(stagingNames(staging)).containsExactlyInAnyOrderElementsOf(finalizedNames);
        } else {
            assertThat(staging).doesNotExist();
        }
        JsonNode completed = MAPPER.readTree(
                outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME).toFile());
        assertThat(completed.path("completed").asBoolean()).isTrue();
        assertThat(completed.path("sort").path("arm").asText()).isEqualTo("PUBLISHED_REENTRY");
    }

    @Test
    void completedKeepOffRunCanReenterRepeatedlyWithKeepOnAfterStagingWasRemoved(
            @TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = root.resolve("checkpoint.sqlite");
        List<String> expectedKeys = List.of("data/a", "data/b", "data/c");
        ListCommand initial = command(
                outputDir, checkpoint, fetcher(expectedKeys), false);

        assertThat(initial.call()).isEqualTo(ExitCodes.SUCCESS);

        DatasetLayout layout = DatasetLayout.of(outputDir);
        Manifest.Identity identity = Manifest.readIdentity(outputDir).orElseThrow();
        DatasetSnapshot published = snapshot(layout);
        Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        assertThat(checkpoint).as("an explicit checkpoint survives successful publication").exists();
        assertThat(staging).as("keep-staging=off removed the staging directory").doesNotExist();

        for (int attempt = 1; attempt <= 2; attempt++) {
            MockPageFetcher forbidden = failOnAnyList();
            ListCommand resume = command(outputDir, checkpoint, forbidden, true);
            resume.checkpoint.resume = true;

            assertThat(resume.call())
                    .as("PUBLISHED keep-on re-entry %s is an idempotent no-op", attempt)
                    .isEqualTo(ExitCodes.SUCCESS);
            assertThat(forbidden.apiCalls()).isZero();
            assertThat(snapshot(layout)).isEqualTo(published);
            assertThat(staging).doesNotExist();
            assertThat(CheckpointDbProbe.runStatusEnum(checkpoint, identity.runId()))
                    .isEqualTo(RunStatus.COMPLETED);
            assertThat(reasonCount(
                    outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME),
                    "post_publish_cleanup_pending"))
                    .isZero();
        }
    }

    @Test
    void retainedCleanupHelperTreatsAbsentStagingAsEmptyAndStillSweepsFinalTmp(
            @TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("out"));
        Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        Path staleTmp = Files.createDirectories(DatasetLayout.of(outputDir).dataDir())
                .resolve("part-00000.parquet.tmp");
        Files.writeString(staleTmp, "stale");
        SortConfig keepOn = SortConfig.fromSystemProperties()
                .withStagingRetention(StagingRetention.RETAIN_ORIGINALS);

        StagingReconciliation.Result result = ListCommand.cleanSortStagingAndStaleTmp(
                outputDir, staging, keepOn, List.of("segment-00000.pageseg"));

        assertThat(result).isEqualTo(new StagingReconciliation.Result(0, 0));
        assertThat(staging).doesNotExist();
        assertThat(staleTmp).doesNotExist();
    }

    @Test
    void pipelineCommittedFailureSummaryRetainsExactPublishedFacts(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = root.resolve("checkpoint.sqlite");
        List<String> expectedKeys = Stream.iterate(0, n -> n + 1).limit(3_000)
                .map(n -> String.format("data/key-%05d", n)).toList();
        ListCommand initial = command(outputDir, checkpoint, fetcher(expectedKeys), false);
        initial.tune.entries = List.of(
                SortConfig.KEEP_STAGING_TUNE_KEY + "=off",
                "sort.merge-parallelism=3");
        initial.listRunnerOverride = new ListRunner((step, ordinal) -> {
            if (step == PublicationStep.AFTER_PUBLISH_LISTENER) {
                throw new java.io.IOException("injected pipeline post-publish cleanup failure");
            }
        });

            Throwable failure = catchThrowable(initial::call);

            assertThat(failure).isInstanceOf(PublicationPendingException.class)
                    .hasCauseInstanceOf(CommittedPublicationCleanupException.class);
            CommittedPublicationCleanupException committed =
                    (CommittedPublicationCleanupException) failure.getCause();
        assertThat(committed.stage()).isEqualTo(
                CommittedPublicationCleanupException.Stage.AFTER_PUBLISH_LISTENER_HOOK);
            var result = committed.publishedResult();
            assertThat(result.finalizationParallelism()).isEqualTo(3);
            assertThat(result.mergePasses()).isPositive();
        assertThat(result.finalFiles()).hasSize(1);
            long publishedBytes = result.finalFiles().stream().mapToLong(path -> {
                try {
                    return Files.size(path);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }).sum();
            assertThat(result.outputBytes()).isEqualTo(publishedBytes);

            Path summaryPath = outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
            JsonNode summary = MAPPER.readTree(summaryPath.toFile());
            assertThat(summary.path("completed").asBoolean()).isFalse();
            assertThat(summary.path("output").path("files").asLong())
                    .isEqualTo(result.finalFiles().size());
            assertThat(summary.path("output").path("compressed_size_bytes").asLong())
                    .isEqualTo(publishedBytes);
            assertThat(summary.path("sort").path("passes").asLong())
                    .isEqualTo(result.mergePasses());
            assertThat(summary.path("sort").path("finalize_parallelism").asLong())
                    .isEqualTo(result.finalizationParallelism());
            assertThat(summary.path("sort").path("merge_ms").asLong()).isPositive();
            assertThat(meterValue(summaryPath, "swath.output.files",
                    Map.of("format", "parquet", "outcome", "written")))
                    .isEqualTo(result.finalFiles().size());
            assertThat(meterValue(summaryPath, "swath.output.bytes",
                    Map.of("format", "parquet"))).isEqualTo(publishedBytes);
            assertThat(outputKeys(outputDir)).containsExactlyElementsOf(expectedKeys);
            assertThat(CheckpointDbProbe.runStatus(checkpoint)).isEqualTo("RUNNING");
            assertThat(CheckpointDbProbe.fatalError(checkpoint)).isFalse();
            Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
            assertThat(staging).isDirectory();

            MockPageFetcher forbidden = failOnAnyList();
            ListCommand resume = command(outputDir, checkpoint, forbidden, false);
            resume.checkpoint.resume = true;
            assertThat(resume.call()).isEqualTo(ExitCodes.SUCCESS);
            assertThat(forbidden.apiCalls()).isZero();
            assertThat(staging).doesNotExist();
            assertThat(outputKeys(outputDir)).containsExactlyElementsOf(expectedKeys);
        assertThat(CheckpointDbProbe.runStatus(checkpoint)).isEqualTo("COMPLETED");
    }

    private static ListCommand command(Path outputDir, Path checkpoint, MockPageFetcher fetcher,
            boolean retainStaging) {
        ListCommand command = new ListCommand();
        command.uri = "s3://" + BUCKET + "/" + PREFIX;
        command.connection.region = "us-east-1";
        command.connection.noSignRequest = true;
        command.output.format = OutputFormat.PARQUET;
        command.output.destination = outputDir.toString();
        command.checkpoint.location = checkpoint.toString();
        command.sorting.sort = true;
        command.tune.entries = List.of(SortConfig.KEEP_STAGING_TUNE_KEY
                + (retainStaging ? "=on" : "=off"));
        command.fetcherOverride = fetcher;
        return command;
    }

    private static MockPageFetcher fetcher(List<String> keys) {
        return MockPageFetcher.builder().keys(keys.stream()
                .map(key -> key.getBytes(StandardCharsets.UTF_8)).toList()).build();
    }

    private static MockPageFetcher failOnAnyList() {
        return MockPageFetcher.builder()
                .keys(List.of())
                .interceptor((request, call, page) -> {
                    throw new ListingException("unexpected LIST during PUBLISHED cleanup re-entry");
                })
                .build();
    }

    private static List<String> outputKeys(Path outputDir) throws Exception {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(outputDir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static Set<String> stagingNames(Path staging) throws Exception {
        if (!Files.isDirectory(staging)) {
            return Set.of();
        }
        try (Stream<Path> entries = Files.list(staging)) {
            return entries.map(path -> path.getFileName().toString())
                    .collect(Collectors.toCollection(TreeSet::new));
        }
    }

    private static DatasetSnapshot snapshot(DatasetLayout layout) throws Exception {
        List<String> partBytes = new ArrayList<>();
        for (Path part : layout.dataParts()) {
            partBytes.add(Base64.getEncoder().encodeToString(Files.readAllBytes(part)));
        }
        return new DatasetSnapshot(partBytes,
                Base64.getEncoder().encodeToString(Files.readAllBytes(layout.manifest())),
                Base64.getEncoder().encodeToString(Files.readAllBytes(layout.state())),
                Base64.getEncoder().encodeToString(Files.readAllBytes(layout.symlink())),
                Base64.getEncoder().encodeToString(Files.readAllBytes(layout.success())));
    }

    private static long reasonCount(Path summary, String reason) throws Exception {
        long count = 0;
        for (JsonNode meter : MAPPER.readTree(summary.toFile()).path("meters")) {
            if ("swath.steal_reason".equals(meter.path("name").asText())
                    && "SORT".equals(meter.path("tags").path("outcome").asText())
                    && reason.equals(meter.path("tags").path("reason").asText())) {
                count += meter.path("value").asLong();
            }
        }
        return count;
    }

    private static long meterValue(Path summary, String name, Map<String, String> tags)
            throws Exception {
        for (JsonNode meter : MAPPER.readTree(summary.toFile()).path("meters")) {
            if (!name.equals(meter.path("name").asText())) {
                continue;
            }
            boolean matches = tags.entrySet().stream().allMatch(entry -> entry.getValue().equals(
                    meter.path("tags").path(entry.getKey()).asText()));
            if (matches) {
                return meter.path("value").asLong();
            }
        }
        throw new AssertionError("missing meter " + name + " tags=" + tags);
    }

    private record DatasetSnapshot(List<String> parts, String manifest, String state,
                                   String symlink, String success) {
    }
}
