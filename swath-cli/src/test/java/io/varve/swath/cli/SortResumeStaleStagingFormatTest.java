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
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.runtime.ListRunner;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
    private static void seedSortRunWithSegmentFormat(Path db, String segFormat) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.partFinalized(new PartFinalize(run.id(), 0, "seg-" + segFormat + ".pageseg",
                    segFormat, 2L, 128L, List.of()));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    private static ListCommand resumeSortCommand(Path db) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = "out";
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
    void currentPageRunStagingResume_isNotRefusedForFormat(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedSortRunWithSegmentFormat(db, ListRunner.SORT_SEGMENT_FORMAT);
        // A matching page-run staging format must PASS the format guard. The resume may still fail
        // later for unrelated reasons (no S3 / missing staging files), but never with THIS refusal.
        Throwable t = catchThrowable(() -> resumeSortCommand(db).call());
        if (t instanceof InvalidArgsException) {
            assertThat(t).hasMessageNotContaining("sort staging format");
        }
    }
}
