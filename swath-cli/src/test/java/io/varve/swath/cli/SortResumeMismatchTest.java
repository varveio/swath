/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A {@code --sort}/{@code --no-sort} mismatch on {@code swath resume} is refused exactly like a changed
 * filter/format: {@code args_hash} excludes output flags, so without the
 * {@code sort_enabled} check a mismatched resume would interleave unsorted parts with orphaned
 * staging (or vice versa). Mirrors {@code ListCommandResumeArgsTest}: the prior run is seeded
 * COMPLETE so the refusal fires before any S3 work.
 */
final class SortResumeMismatchTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";

    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    private static void seedCompletedRun(Path db, boolean sortEnabled) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null),
                sortEnabled, storedIdentitySpec(sortEnabled));
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    /**
     * The {@code run_meta.identity_spec} a real creating run would persist: the registry's IDENTITY
     * fingerprint over a {@link ListCommand} mirroring the seeded run's fully-resolved state. Reusing
     * {@link ResumeRegistry#identitySpec} (not a hand-written string) keeps the fixture in lockstep
     * with the single source of truth the drift guard protects.
     */
    private static String storedIdentitySpec(boolean sortEnabled) throws Exception {
        ListCommand original = new ListCommand();
        original.uri = "s3://" + BUCKET + "/" + PREFIX;
        original.connection.endpointUrl = ENDPOINT;
        original.output.destination = "out";
        original.output.format = OutputFormat.PARQUET;
        original.sorting.sort = sortEnabled;
        FilterSpecCodec.Decoded filters = FilterSpecCodec.decode(NO_FILTER_SPEC);
        original.filters.include = filters.include();
        original.filters.exclude = filters.exclude();
        original.filters.minSize = filters.minSize();
        original.filters.maxSize = filters.maxSize();
        original.filters.modifiedAfter = filters.modifiedAfter();
        original.filters.modifiedBefore = filters.modifiedBefore();
        original.filters.storageClasses = filters.storageClasses();
        original.output.resolveOutput(false);   // resolves format + destination kind, as creation did
        return ResumeRegistry.identitySpec(original);
    }

    private static ListCommand resumeCommand(Path db, boolean sort) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = "out";
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = sort;
        return cmd;
    }

    @Test
    void sortRunResumedWithNoSort_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, true);
        assertThatThrownBy(() -> resumeCommand(db, false).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("sort_enabled changed since the checkpointed run");
    }

    @Test
    void plainRunResumedWithSort_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, false);
        assertThatThrownBy(() -> resumeCommand(db, true).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("sort_enabled changed since the checkpointed run");
    }
}
