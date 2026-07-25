/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
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
 * {@code swath resume} eligibility through the CLI.
 * Same listing args resume; a changed prefix (⇒ {@code args_hash}) or a changed
 * filter/format is refused (exit 2, suggest {@code --restart}); a changed
 * concurrency knob still resumes (not part of {@code args_hash}).
 *
 * <p>The prior run is seeded COMPLETE, so the <i>allowed</i> cases return cleanly
 * without touching S3 (resume of a finished run is a no-op once eligibility passes).
 */
final class ListCommandResumeArgsTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";

    /** The canonical no-filter spec, exactly as {@link ListCommand#buildFilterSpec()} computes it. */
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    /** Seed a COMPLETED run with the given filter/format, matching what {@link ListCommand} computes. */
    private static void seedCompletedRun(Path db, String filterSpec, OutputFormat format) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, filterSpec, format.name(),
                SoftRestoreContext.NONE, false, storedIdentitySpec(filterSpec, format));
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
    private static String storedIdentitySpec(String filterSpec, OutputFormat format) throws Exception {
        ListCommand original = new ListCommand();
        original.uri = "s3://" + BUCKET + "/" + PREFIX;
        original.connection.endpointUrl = ENDPOINT;
        original.output.format = format;
        FilterSpecCodec.Decoded filters = FilterSpecCodec.decode(filterSpec);
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

    private static ListCommand resumeCommand(Path db, OutputFormat format) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = format;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        return cmd;
    }

    @Test
    void sameArgs_resumeAllowed(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, NO_FILTER_SPEC, OutputFormat.JSONL);
        // No filter/format change ⇒ eligible; the run is complete ⇒ a clean no-op resume.
        assertThat(resumeCommand(db, OutputFormat.JSONL).call()).isEqualTo(ExitCodes.SUCCESS);
    }

    @Test
    void changedConcurrency_stillResumes(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, NO_FILTER_SPEC, OutputFormat.JSONL);
        ListCommand cmd = resumeCommand(db, OutputFormat.JSONL);
        cmd.connection.maxParallelListings = 7;   // not part of args_hash ⇒ resume still allowed
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    @Test
    void changedFilter_resumeRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, NO_FILTER_SPEC, OutputFormat.JSONL);
        ListCommand cmd = resumeCommand(db, OutputFormat.JSONL);
        cmd.filters.include = "\\.parquet$";   // changes the filter spec
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("filter_spec changed since the checkpointed run");
    }

    @Test
    void changedFormat_resumeRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, NO_FILTER_SPEC, OutputFormat.JSONL);
        // Same args/filter but TSV instead of the stored JSONL ⇒ refused.
        assertThatThrownBy(() -> resumeCommand(db, OutputFormat.TSV).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("output_format changed since the checkpointed run");
    }

    @Test
    void changedPrefix_resumeRefused(@TempDir Path dir) throws Exception {
        // A different prefix is a different listing identity (and a different args_hash):
        // there is no checkpointed run for it, so swath resume is refused (exit 2). The
        // same-identity-but-mismatched-args_hash branch is covered at the store level.
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, NO_FILTER_SPEC, OutputFormat.JSONL);
        ListCommand cmd = resumeCommand(db, OutputFormat.JSONL);
        cmd.uri = "s3://" + BUCKET + "/other/";   // different prefix ⇒ no matching run
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("nothing to resume");
    }

    @Test
    void checkpointNoneWithResume_refused(@TempDir Path dir) throws Exception {
        ListCommand cmd = resumeCommand(dir.resolve("unused.sqlite"), OutputFormat.JSONL);
        cmd.checkpoint.location = "none";
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("durable state");
    }

    @Test
    void resumeAndRestart_mutuallyExclusive(@TempDir Path dir) throws Exception {
        ListCommand cmd = resumeCommand(dir.resolve("c.sqlite"), OutputFormat.JSONL);
        cmd.checkpoint.restart = true;
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("mutually exclusive");
    }
}
