/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.sort.PageRunFormat;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * (RES-class guard) A {@code --sort} run whose checkpoint carries staging segments tagged
 * with the legacy staging format ({@code "parquet-segment"}) must REFUSE cleanly on {@code swath resume} —
 * NOT silently sweep the un-recognized (old-format) finalized segments as "non-finalized" and re-list
 * their data. The reattach path selects staging by {@link ListRunner#SORT_SEGMENT_FORMAT}
 * ("page-run"), so an old-format row would be invisible to it and the finalized segment re-listed
 * (dup/loss). Refused exactly like the {@code --sort/--no-sort} mismatch (InvalidArgsException, exit
 * 2). A page-run→page-run resume is NOT refused for the format.
 */
final class SortResumeStaleStagingFormatTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String OLD_STAGING_FORMAT = "parquet-segment";

    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    /** Seed a resumable --sort run and record ONE staging {@code part_file} row with {@code segFormat}. */
    private static long seedSortRunWithSegmentFormat(Path db, String segFormat) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            PartFinalize finalized = PageRunFormat.NAME.equals(segFormat)
                    ? new PartFinalize(run.id(), 0, "seg-page-run.pageseg",
                            PageRunFormat.currentListing(), 2L, 128L, List.of())
                    : new PartFinalize(run.id(), 0, "seg-" + segFormat + ".pageseg",
                            segFormat, 2L, 128L, List.of());
            store.partFinalized(finalized);
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
            return run.id();
        }
    }

    /** Simulate metadata already present in an old, future, or damaged SQLite checkpoint. */
    private static void rewritePageRunMetadata(Path db, long runId,
            Object formatVersion, Object extensionType) throws Exception {
        try (var c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             var ps = c.prepareStatement(
                     "UPDATE part_file SET format_version=?, extension_type=? WHERE run_id=?")) {
            ps.setObject(1, formatVersion);
            ps.setObject(2, extensionType);
            ps.setLong(3, runId);
            ps.executeUpdate();
        }
    }

    private static ListCommand resumeSortCommand(Path db) {
        return resumeSortCommand(db, null);
    }

    private static ListCommand resumeSortCommand(Path db, Path outputDir) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir == null ? "out" : outputDir.toString();
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = true;
        return cmd;
    }

    @Test
    void oldParquetSegmentStagingResume_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedSortRunWithSegmentFormat(db, OLD_STAGING_FORMAT);
        assertThatThrownBy(() -> resumeSortCommand(db).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("sort staging format")
                .hasMessageContaining(OLD_STAGING_FORMAT)
                .hasMessageContaining(ListRunner.SORT_SEGMENT_FORMAT)
                .hasMessageContaining("--restart");
    }

    @Test
    void preColumnPageRunStagingResume_isNotRefusedForFormat(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        long runId = seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        rewritePageRunMetadata(db, runId, null, null);
        // A matching page-run staging format must PASS the format guard. The resume may still fail
        // later for unrelated reasons (no S3 / missing staging files), but never with THIS refusal.
        Throwable t = catchThrowable(() -> resumeSortCommand(db).call());
        if (t instanceof InvalidArgsException) {
            assertThat(t).hasMessageNotContaining("sort staging format")
                    .hasMessageNotContaining("page-run staging metadata");
        }
    }

    @Test
    void currentPageRunMetadataReachesMergeOnlyReentry(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);

        Throwable t = catchThrowable(() -> resumeSortCommand(db).call());
        assertThat(t).isNotInstanceOf(InvalidArgsException.class);
    }

    @Test
    void legacyMinimaPageRunMetadataRemainsReadable(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        long runId = seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        rewritePageRunMetadata(db, runId,
                PageRunFormat.CURRENT_FORMAT_VERSION, PageRunFormat.LEGACY_MINIMA_EXTENSION);

        Throwable t = catchThrowable(() -> resumeSortCommand(db).call());
        assertThat(t).isNotInstanceOf(InvalidArgsException.class);
    }

    @Test
    void unknownPageRunFormatVersionIsRefusedBeforeMerge(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        long runId = seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        rewritePageRunMetadata(db, runId,
                PageRunFormat.CURRENT_FORMAT_VERSION + 1,
                PageRunFormat.PAGE_INDEX_EXTENSION);

        assertThatThrownBy(() -> resumeSortCommand(db).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("page-run staging metadata")
                .hasMessageContaining("format_version="
                        + (PageRunFormat.CURRENT_FORMAT_VERSION + 1))
                .hasMessageContaining("UNKNOWN_FORMAT_VERSION")
                .hasMessageContaining("--restart");
    }

    @Test
    void unknownPageRunExtensionTypeIsRefusedBeforeMerge(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        int unknownType = PageRunFormat.PAGE_INDEX_EXTENSION + 99;
        long runId = seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        rewritePageRunMetadata(db, runId,
                PageRunFormat.CURRENT_FORMAT_VERSION, unknownType);

        assertThatThrownBy(() -> resumeSortCommand(db).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("page-run staging metadata")
                .hasMessageContaining("extension_type=" + unknownType)
                .hasMessageContaining("UNKNOWN_EXTENSION_TYPE")
                .hasMessageContaining("--restart");
    }

    @Test
    void missingExtensionTypeIsRefusedBeforeStagingMutation(@TempDir Path dir) throws Exception {
        assertIncompletePairRefusedBeforeStagingMutation(dir, "extension_type");
    }

    @Test
    void missingFormatVersionIsRefusedBeforeStagingMutation(@TempDir Path dir) throws Exception {
        assertIncompletePairRefusedBeforeStagingMutation(dir, "format_version");
    }

    @Test
    void unsigned32BitWraparoundCannotMasqueradeAsCurrentMetadata(@TempDir Path dir) throws Exception {
        assertCorruptIntegerRefusedBeforeStagingMutation(
                dir, 4_294_967_297L, 4_294_967_298L, "format_version", "4294967297");
    }

    @Test
    void negativeMetadataIsCheckpointCorruptionBeforeStagingMutation(@TempDir Path dir) throws Exception {
        assertCorruptIntegerRefusedBeforeStagingMutation(dir, -1L,
                (long) PageRunFormat.PAGE_INDEX_EXTENSION, "format_version", "-1");
    }

    @Test
    void positiveIntOverflowIsCheckpointCorruptionBeforeStagingMutation(@TempDir Path dir)
            throws Exception {
        assertCorruptIntegerRefusedBeforeStagingMutation(dir,
                (long) PageRunFormat.CURRENT_FORMAT_VERSION, 2_147_483_648L,
                "extension_type", "2147483648");
    }

    private static void assertIncompletePairRefusedBeforeStagingMutation(Path dir, String nullColumn)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        PageRunFormat current = PageRunFormat.currentListing();
        long runId = seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        rewritePageRunMetadata(db, runId,
                "format_version".equals(nullColumn) ? null : current.formatVersion(),
                "extension_type".equals(nullColumn) ? null : current.extensionType());
        Path staging = Files.createDirectories(dir.resolve("out/_staging"));
        Path sentinel = Files.writeString(staging.resolve("must-survive"), "sentinel");

        assertThatThrownBy(() -> resumeSortCommand(db, staging.getParent()).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("page-run staging metadata")
                .hasMessageContaining("INCOMPLETE");
        assertThat(sentinel).exists();
    }

    private static void assertCorruptIntegerRefusedBeforeStagingMutation(Path dir,
            long formatVersion, long extensionType, String column, String value) throws Exception {
        Path db = dir.resolve("c.sqlite");
        long runId = seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        rewritePageRunMetadata(db, runId, formatVersion, extensionType);
        Path staging = Files.createDirectories(dir.resolve("out/_staging"));
        Path sentinel = Files.writeString(staging.resolve("must-survive"), "sentinel");

        assertThatThrownBy(() -> resumeSortCommand(db, staging.getParent()).call())
                .isInstanceOf(CheckpointException.class)
                .hasMessageContaining("part_file." + column)
                .hasMessageContaining(value)
                .hasMessageContaining("non-negative 32-bit integer");
        assertThat(sentinel).exists();
    }
}
