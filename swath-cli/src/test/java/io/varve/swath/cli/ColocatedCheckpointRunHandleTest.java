/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.ListingException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.sort.BenchmarkCheckpointCatalog;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * The directory-as-run-handle behaviour end to end: with the default {@code --checkpoint auto}, a
 * {@code list -o <dir>} co-locates its checkpoint at {@code <dir>/.swath/checkpoint.sqlite}, deletes
 * it on clean completion, and thereafter protects the finished dataset using the on-disk completion
 * markers alone — proving the refusal no longer depends on a surviving checkpoint.
 */
final class ColocatedCheckpointRunHandleTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ListCommand autoCommand(Path outputDir, MockPageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        // checkpoint.location left at its "auto" field default — the co-located run handle.
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.fetcherOverride = fetcher;
        return cmd;
    }

    private static MockPageFetcher fetcher(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format("data/key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        return MockPageFetcher.builder().keys(keys).build();
    }

    private static ListCommand retainedSortCommand(Path outputDir, MockPageFetcher fetcher) {
        ListCommand cmd = autoCommand(outputDir, fetcher);
        cmd.sorting.sort = true;
        cmd.tune.entries = List.of(SortConfig.KEEP_STAGING_TUNE_KEY + "=on");
        return cmd;
    }

    private static MockPageFetcher failOnAnyList() {
        return MockPageFetcher.builder()
                .keys(List.of())
                .interceptor((request, call, page) -> {
                    throw new ListingException("unexpected LIST during merge-only replay");
                })
                .build();
    }

    private static int resumeWithRetention(Path outputDir, MockPageFetcher fetcher) throws Exception {
        ResumeCommand resume = new ResumeCommand();
        new CommandLine(resume).parseArgs(outputDir.toString(),
                "--tune", SortConfig.KEEP_STAGING_TUNE_KEY + "=on");
        resume.fetcherOverride = fetcher;
        return resume.call();
    }

    private static Set<String> checkpointSegmentNames(Path checkpoint, long runId) throws Exception {
        return checkpointSegments(checkpoint, runId).stream()
                .map(PartRef::path)
                .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
    }

    private static List<PartRef> checkpointSegments(Path checkpoint, long runId) throws Exception {
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            return store.finalizedParts(runId).stream()
                    .filter(part -> ListRunner.SORT_SEGMENT_FORMAT.equals(part.format()))
                    .toList();
        }
    }

    private static Set<String> stagingNames(Path staging) throws Exception {
        if (!Files.isDirectory(staging)) {
            return Set.of();
        }
        try (Stream<Path> entries = Files.list(staging)) {
            Set<String> names = new TreeSet<>();
            entries.forEach(path -> names.add(path.getFileName().toString()));
            return names;
        }
    }

    private static List<String> outputKeys(Path outputDir) throws Exception {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(outputDir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static List<byte[]> outputPartBytes(Path outputDir) throws Exception {
        List<byte[]> bytes = new ArrayList<>();
        for (Path part : DatasetLayout.of(outputDir).dataParts()) {
            bytes.add(Files.readAllBytes(part));
        }
        return bytes;
    }

    private static List<ManifestPart> manifestParts(Path manifest) throws Exception {
        List<ManifestPart> parts = new ArrayList<>();
        for (JsonNode file : MAPPER.readTree(manifest.toFile()).path("files")) {
            parts.add(new ManifestPart(file.path("key").asText(), file.path("rowCount").asLong(),
                    file.path("size").asLong()));
        }
        return parts.stream().sorted(java.util.Comparator.comparing(ManifestPart::key)).toList();
    }

    @Test
    void retainedSortedRunKeepsCheckpointTrackedInputsForMergeOnlyDiagnostics(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);

        assertThat(retainedSortCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);

        Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        assertThat(checkpoint).exists();
        assertThat(staging).isDirectory();
        long runId = Manifest.readIdentity(outputDir).orElseThrow().runId();
        Set<String> finalizedNames = checkpointSegmentNames(checkpoint, runId);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            assertThat(store.sortPhase(runId)).isEqualTo(SortPhase.PUBLISHED);
            assertThat(store.finalizedParts(runId))
                    .isNotEmpty()
                    .allSatisfy(part -> {
                        assertThat(part.format()).isEqualTo("page-run");
                        assertThat(staging.resolve(part.path())).exists();
                    });
        }
        assertThat(stagingNames(staging)).containsExactlyInAnyOrderElementsOf(finalizedNames);
    }

    @Test
    void retainedSortedRunIsOrganicBenchmarkCatalogAuthority(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);

        assertThat(retainedSortCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);

        Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        Manifest.Identity identity = Manifest.readIdentity(outputDir).orElseThrow();
        BenchmarkCheckpointCatalog.Authority authority =
                BenchmarkCheckpointCatalog.read(outputDir, staging, identity);

        assertThat(checkpoint).exists();
        assertThat(DatasetLayout.of(outputDir).success()).exists();
        assertThat(authority.runId()).isEqualTo(identity.runId());
        assertThat(authority.argsHash()).isEqualTo(identity.argsHash());
        assertThat(authority.segments()).isNotEmpty()
                .allSatisfy(segment -> assertThat(segment.path().getParent()).isEqualTo(staging));
        assertThat(authority.segments().stream().mapToLong(BenchmarkCheckpointCatalog.TrackedSegment::rows).sum())
                .isEqualTo(50);
    }

    @Test
    void publishedResumeWithRetentionKeepsOnlyCheckpointFinalizedOriginals(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        assertThat(retainedSortCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);
        long runId = Manifest.readIdentity(outputDir).orElseThrow().runId();
        Set<String> finalizedNames = checkpointSegmentNames(checkpoint, runId);
        Path staging = outputDir.resolve(ListCommand.SORT_STAGING_DIR);

        Files.createFile(staging.resolve("merge-99.pageseg"));
        Files.createFile(staging.resolve("unfinalized.pageseg"));
        Files.createFile(staging.resolve("prange-0-99.parquet.tmp"));
        Files.createFile(staging.resolve("part-99.parquet.tmp"));
        Path dataTmp = Files.createFile(DatasetLayout.of(outputDir).dataDir()
                .resolve("part-99999.parquet.tmp"));
        MockPageFetcher forbidden = failOnAnyList();

        Logger logger = (Logger) LoggerFactory.getLogger(ListCommand.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        try {
            assertThat(resumeWithRetention(outputDir, forbidden)).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        assertThat(forbidden.apiCalls()).isZero();
        assertThat(checkpoint).exists();
        assertThat(stagingNames(staging)).containsExactlyInAnyOrderElementsOf(finalizedNames);
        assertThat(dataTmp).doesNotExist();
        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("sort_staging_retained "))
                .toList())
                .singleElement()
                .asString()
                .contains("source=published_reentry")
                .contains("retained_segments=" + finalizedNames.size())
                .contains("removed_entries=4");
        JsonNode summary = MAPPER.readTree(
                outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME).toFile());
        List<JsonNode> retainedMeters = new ArrayList<>();
        summary.path("meters").forEach(meter -> {
            if ("swath.steal_reason".equals(meter.path("name").asText())
                    && "SORT".equals(meter.path("tags").path("outcome").asText())
                    && "staging_retained".equals(
                            meter.path("tags").path("reason").asText())) {
                retainedMeters.add(meter);
            }
        });
        assertThat(retainedMeters).singleElement()
                .satisfies(meter -> assertThat(meter.path("value").asDouble()).isEqualTo(1.0));
    }

    @Test
    void mergeOnlyResumeUsesNoListAndRetainsIdenticalOutputAndArtifacts(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        assertThat(retainedSortCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Manifest.Identity expectedIdentity = Manifest.readIdentity(outputDir).orElseThrow();
        long runId = expectedIdentity.runId();
        Set<String> finalizedNames = checkpointSegmentNames(checkpoint, runId);
        List<PartRef> expectedFinalized = checkpointSegments(checkpoint, runId);
        List<ManifestPart> expectedManifestParts = manifestParts(layout.manifest());
        String expectedSymlink = Files.readString(layout.symlink());
        List<String> expected = outputKeys(outputDir);
        List<byte[]> expectedPartBytes = outputPartBytes(outputDir);
        Path orphan = Files.createFile(outputDir.resolve(ListCommand.SORT_STAGING_DIR)
                .resolve("orphan.pageseg"));
        Files.delete(layout.success());
        MockPageFetcher forbidden = failOnAnyList();

        assertThat(resumeWithRetention(outputDir, forbidden)).isEqualTo(ExitCodes.SUCCESS);

        assertThat(forbidden.apiCalls()).isZero();
        assertThat(layout.success()).exists();
        assertThat(outputKeys(outputDir)).containsExactlyElementsOf(expected);
        List<byte[]> actualPartBytes = outputPartBytes(outputDir);
        assertThat(actualPartBytes).hasSameSizeAs(expectedPartBytes);
        for (int i = 0; i < actualPartBytes.size(); i++) {
            assertThat(actualPartBytes.get(i)).isEqualTo(expectedPartBytes.get(i));
        }
        assertThat(checkpoint).exists();
        assertThat(manifestParts(layout.manifest())).containsExactlyElementsOf(expectedManifestParts);
        assertThat(Manifest.readIdentity(outputDir)).contains(expectedIdentity);
        assertThat(expectedIdentity.runId()).isEqualTo(runId);
        assertThat(expectedIdentity.argsHash()).isNotBlank();
        assertThat(Files.readString(layout.symlink())).isEqualTo(expectedSymlink);
        assertThat(checkpointSegments(checkpoint, runId)).containsExactlyElementsOf(expectedFinalized);
        assertThat(stagingNames(outputDir.resolve(ListCommand.SORT_STAGING_DIR)))
                .containsExactlyInAnyOrderElementsOf(finalizedNames);
        assertThat(orphan).doesNotExist();
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void retentionPropertyIsSnapshottedBeforeListingAndCannotSplitCompletionCleanup(
            @TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            keys.add(String.format("data/key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        MockPageFetcher mutatingFetcher = MockPageFetcher.builder()
                .keys(keys)
                .interceptor((request, call, page) -> {
                    System.setProperty(SortConfig.KEEP_STAGING_PROPERTY, "off");
                    return page;
                })
                .build();
        String previous = System.getProperty(SortConfig.KEEP_STAGING_PROPERTY);
        try {
            System.setProperty(SortConfig.KEEP_STAGING_PROPERTY, "on");
            ListCommand command = autoCommand(outputDir, mutatingFetcher);
            command.sorting.sort = true;

            assertThat(command.call()).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            if (previous == null) {
                System.clearProperty(SortConfig.KEEP_STAGING_PROPERTY);
            } else {
                System.setProperty(SortConfig.KEEP_STAGING_PROPERTY, previous);
            }
        }

        long runId = Manifest.readIdentity(outputDir).orElseThrow().runId();
        assertThat(checkpoint).exists();
        assertThat(stagingNames(outputDir.resolve(ListCommand.SORT_STAGING_DIR)))
                .containsExactlyInAnyOrderElementsOf(checkpointSegmentNames(checkpoint, runId));
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void resumeTuneOffOverridesKeepStagingJvmPropertyThroughDelegatedCommand(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        assertThat(retainedSortCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);
        String previous = System.getProperty(SortConfig.KEEP_STAGING_PROPERTY);
        try {
            System.setProperty(SortConfig.KEEP_STAGING_PROPERTY, "on");
            ResumeCommand resume = new ResumeCommand();
            new CommandLine(resume).parseArgs(outputDir.toString(),
                    "--tune", SortConfig.KEEP_STAGING_TUNE_KEY + "=off");
            resume.fetcherOverride = failOnAnyList();

            assertThat(resume.call()).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            if (previous == null) {
                System.clearProperty(SortConfig.KEEP_STAGING_PROPERTY);
            } else {
                System.setProperty(SortConfig.KEEP_STAGING_PROPERTY, previous);
            }
        }

        assertThat(outputDir.resolve(ListCommand.SORT_STAGING_DIR)).doesNotExist();
        assertThat(checkpoint).doesNotExist();
    }

    /**
     * A completed {@code auto} run deletes its co-located checkpoint, and a second fresh run over the
     * same dir is then refused by the on-disk markers alone (no checkpoint left to consult), leaving
     * the finished dataset byte-for-byte intact until {@code --overwrite} re-lists it.
     */
    @Test
    void completedRunDeletesItsCheckpointAndTheMarkersAloneRefuseAReRun(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        Set<String> cwdLitterBefore = cwdCheckpointDirSnapshot();

        // Drive a real listing all the way to a COMPLETED dataset via the co-located auto checkpoint.
        assertThat(autoCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);

        DatasetLayout layout = DatasetLayout.of(outputDir);
        assertThat(layout.success()).exists();
        assertThat(layout.manifest()).exists();
        assertThat(layout.dataParts()).isNotEmpty();
        // The run handle is complete, so the checkpoint (and the emptied .swath/ dir) are gone.
        assertThat(checkpoint).doesNotExist();
        assertThat(outputDir.resolve(".swath")).doesNotExist();
        // The co-located checkpoint was isolated under the output dir: the run added nothing to the
        // old ./.swath-checkpoint/ default location.
        assertThat(cwdCheckpointDirSnapshot()).isEqualTo(cwdLitterBefore);

        String manifestBefore = Files.readString(layout.manifest());
        List<Path> partsBefore = layout.dataParts();

        // With NO checkpoint on disk, a second fresh run must still refuse — the completed-dataset
        // guard reads _SUCCESS + a valid manifest, not any checkpoint state — and steer to --overwrite.
        assertThatThrownBy(() -> autoCommand(outputDir, fetcher(50)).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("completed")
                .hasMessageContaining("--overwrite");
        assertThat(checkpoint).as("a refused re-run never recreates the checkpoint").doesNotExist();

        // The finished dataset is preserved exactly.
        assertThat(layout.success()).exists();
        assertThat(Files.readString(layout.manifest())).isEqualTo(manifestBefore);
        assertThat(partsBefore).allMatch(Files::exists);

        // --overwrite discards the completed run and re-lists cleanly, again deleting its checkpoint.
        ListCommand overwrite = autoCommand(outputDir, fetcher(50));
        overwrite.checkpoint.overwrite = true;
        assertThat(overwrite.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(DatasetLayout.of(outputDir).dataParts()).isNotEmpty();
        assertThat(checkpoint).doesNotExist();
    }

    /** A plain {@code auto} stdout run keeps nothing on disk — no {@code ./.swath-checkpoint/} litter. */
    @Test
    void plainStdoutRunDropsNoCheckpointLitter() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        // checkpoint.location "auto" + stdout (no -o) = ephemeral.
        cmd.output.format = OutputFormat.JSONL;
        cmd.fetcherOverride = fetcher;

        Set<String> cwdLitterBefore = cwdCheckpointDirSnapshot();
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        // The ephemeral run wrote no checkpoint file at all — the old ./.swath-checkpoint/ default is
        // untouched (compared as a snapshot so this holds regardless of unrelated pre-existing files).
        assertThat(cwdCheckpointDirSnapshot()).isEqualTo(cwdLitterBefore);
    }

    private record ManifestPart(String key, long rows, long bytes) {
    }

    /** Snapshot of the file names in the working-directory {@code .swath-checkpoint} dir (empty if absent). */
    private static Set<String> cwdCheckpointDirSnapshot() throws Exception {
        Path dir = Path.of(".swath-checkpoint");
        if (!Files.isDirectory(dir)) {
            return Set.of();
        }
        try (Stream<Path> entries = Files.list(dir)) {
            Set<String> names = new TreeSet<>();
            entries.forEach(p -> names.add(p.getFileName().toString()));
            return names;
        }
    }
}
