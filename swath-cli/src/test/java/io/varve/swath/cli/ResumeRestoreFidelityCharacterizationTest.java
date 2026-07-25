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
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins per-setting resume-restore fidelity — which of three dispositions each setting gets when it
 * differs between the checkpointed run and the resuming invocation:
 * <ul>
 *   <li><b>refused</b> — {@code swath resume} throws {@link InvalidArgsException} (exit 2);</li>
 *   <li><b>silently restored</b> — the checkpointed value wins when the caller does not re-supply it,
 *       with no signal to the caller beyond a {@code log.warn} on an actual mismatch;</li>
 *   <li><b>silently honoured</b> — an explicitly differing invocation value is simply used, again with
 *       nothing beyond a {@code log.warn}, never a refusal.</li>
 * </ul>
 *
 * <p><b>The disposition table this file pins</b> (every option is classified identity/sticky/free):
 * <pre>
 * setting            | class    | disposition
 * -------------------|----------|--------------------------------------------------------------
 * filters            | identity | refused (filterSpec mismatch, ListCommand#call)
 * --format           | identity | refused (storedOutputFormat mismatch, ListCommand#call)
 * --sort/--no-sort   | identity | refused (run.sortEnabled() mismatch, ListCommand#call)
 * --endpoint-url     | identity | refused ("nothing to resume" -- a different endpoint simply
 *                     |          | finds no matching run_meta row; SqliteCheckpointStore#openRun)
 * --fetch-owner      | identity | refused via run_meta.identity_spec: an explicit value that
 *                     |          | disagrees with the checkpoint survives the cli-wins restore and
 *                     |          | mismatches the stored fetch_owner column.
 * destination path   | identity | refused via run_meta.identity_spec: an explicit -o retargeting a
 *                     |          | resumed directory dataset to a different path mismatches the
 *                     |          | stored output column.
 * --region/--profile | sticky   | silently restored when absent, silently honoured (cli wins) when
 *                     |          | explicitly differing -- never refused.
 * --concurrency       | free     | never stored, never restored; the newly passed value is simply
 *                     |          | used every time.
 * </pre>
 *
 * <p>Every row above is pinned on the ALREADY-COMPLETED resume path (the checkpointed run seeded via
 * {@code seedCompletedRun} is a clean no-op); the one exception is {@link
 * #destinationPathRetargetedOnUnfinishedRun_isRefused}
 * below, which drives a genuine UNFINISHED run through the real engine to observe the destination-path
 * cell's refusal against real committed state.
 */
final class ResumeRestoreFidelityCharacterizationTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String OTHER_ENDPOINT = "http://localhost:9999";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    private static void seedCompletedRun(Path db, String endpoint, String filterSpec,
                                          OutputFormat format, SoftRestoreContext context,
                                          boolean sortEnabled) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", endpoint == null ? "" : endpoint, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", endpoint, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, filterSpec, format.name(), context, sortEnabled,
                storedIdentitySpec(endpoint, filterSpec, format, context, sortEnabled));
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            if (format == OutputFormat.PARQUET) {
                store.markOutputComplete(run.id());
            }
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    /**
     * The {@code run_meta.identity_spec} a real creating run would persist: the registry's
     * IDENTITY fingerprint over a {@link ListCommand} mirroring the seeded run's fully-resolved state.
     * Reusing {@link ResumeRegistry#identitySpec} (not a hand-written string) keeps the fixture in
     * lockstep with the single source of truth the drift guard protects.
     */
    private static String storedIdentitySpec(String endpoint, String filterSpec, OutputFormat format,
                                             SoftRestoreContext context, boolean sortEnabled) throws Exception {
        ListCommand original = new ListCommand();
        original.uri = "s3://" + BUCKET + "/" + PREFIX;
        original.connection.endpointUrl = endpoint;
        original.connection.fetchOwner = context.fetchOwner();
        original.output.rawOutput = context.rawOutput();
        original.output.outputType = context.outputType();
        original.output.destination = context.outputPath();
        original.output.format = format;
        original.sorting.sort = sortEnabled;
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

    private static ListCommand bareResumeCommand(Path db, String endpoint) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = endpoint;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        return cmd;
    }

    // ---- identity: filters / format / sort — all refused today (matches the spec) ---------------

    @Test
    void filters_changedOnResume_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL, SoftRestoreContext.NONE, false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.filters.include = "\\.parquet$";

        // Still refused, but the registry-driven check now names the changed column
        // (filter_spec) instead of the old catch-all "filter, output format, or --sort/--no-sort" text.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("filter_spec changed since the checkpointed run");
    }

    @Test
    void format_changedOnResume_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL, SoftRestoreContext.NONE, false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.output.format = OutputFormat.TSV;

        // Still refused; the check now names output_format as the changed column.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("output_format changed since the checkpointed run");
    }

    @Test
    void sort_changedOnResume_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = Files.createDirectories(dir.resolve("sorted-out"));
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.PARQUET,
                new SoftRestoreContext(true, null, null, false, false, outDir.toString(), false, null, null), true);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.output.format = OutputFormat.PARQUET;   // keep format constant: isolate the --sort mismatch
        cmd.sorting.sort = false;                   // stored run was sorted; this invocation is not

        // Still refused; the check now names sort_enabled as the changed column.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("sort_enabled changed since the checkpointed run");
    }

    // ---- identity: --endpoint-url — refused, but via a different mechanism (no matching row) ------

    @Test
    void endpointUrl_changedOnResume_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL, SoftRestoreContext.NONE, false);

        ListCommand cmd = bareResumeCommand(db, OTHER_ENDPOINT);   // different store identity

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("nothing to resume");
    }

    // ---- THE KEY DIVERGENCE: --fetch-owner is spec-classified identity but is NEVER refused --------

    @Test
    void fetchOwner_notPassed_isSilentlyRestoredFromCheckpoint(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL,
                new SoftRestoreContext(false, null, null, true, false, null, false, null, null), false);   // stored: fetchOwner=true

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);   // --fetch-owner not passed (defaults false)

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.connection.fetchOwner).as("fetch_owner silently restored from the checkpoint").isTrue();
    }

    /**
     * {@code --fetch-owner} is classified <i>identity</i> ("change ⇒ refuse resume") because it
     * changes emitted data (owner columns in the Parquet schema): an explicit value that disagrees
     * with the checkpoint survives the cli-wins restore, mismatches the stored {@code fetch_owner}
     * column, and is refused through {@code run_meta.identity_spec}.
     */
    @Test
    void fetchOwner_explicitlyDiffering_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL,
                new SoftRestoreContext(false, null, null, false, false, null, false, null, null), false);   // stored: fetchOwner=false

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.connection.fetchOwner = true;   // explicit --fetch-owner, disagrees with the checkpoint

        assertThatThrownBy(cmd::call)
                .as("--fetch-owner is §4.4 identity, now enforced via identity_spec")
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("fetch_owner changed since the checkpointed run");
    }

    // ---- A second identity-classified field with the same gap: destination path retargeting -------

    /**
     * "Destination kind + path" is classified <i>identity</i>: the destination path is an IDENTITY
     * option ({@code output} column) enforced via {@code identity_spec}. An explicit {@code -o}
     * that disagrees with the checkpoint survives the cli-wins restore, mismatches the stored
     * {@code output} column, and is refused.
     */
    @Test
    void destinationPath_explicitlyRetargeted_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path originalDataset = Files.createDirectories(dir.resolve("original-dataset"));
        Path retargetedDataset = Files.createDirectories(dir.resolve("retargeted-dataset"));
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.PARQUET,
                new SoftRestoreContext(true, null, "us-west-2", false, false, originalDataset.toString(), false, null, null),
                false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.output.destination = retargetedDataset.toString();   // explicit -o, a DIFFERENT directory
        cmd.output.format = OutputFormat.PARQUET;

        assertThatThrownBy(cmd::call)
                .as("retargeting a directory-dataset -o is §4.4 identity, now enforced via identity_spec")
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("output changed since the checkpointed run");
    }

    /**
     * The hazardous case identity classification of the destination path exists to prevent: retargeting
     * an UNFINISHED run (a page already durably committed) to a brand-new, empty directory. Drives the
     * REAL engine (via {@link MockPageFetcher}, no S3) through a genuine mid-run crash (an {@code
     * InterruptedException} — the same un-marked-fatal disposition a SIGKILL leaves, per {@link
     * ListCommand#runEngineGuarded}) so the checkpoint/dataset state is exactly what a real interrupted
     * run leaves — the run's {@code identity_spec} is persisted at creation, before the crash — then
     * resumes into a new directory.
     *
     * <p>Without this check, the retarget would be silently honoured and the
     * resumed run would redundantly RE-LIST the whole keyspace into the new directory (orphaning the
     * original's committed page). Because the destination path is an IDENTITY option enforced via
     * {@code identity_spec}, retargeting the unfinished run is refused before any relist instead.
     */
    @Test
    void destinationPathRetargetedOnUnfinishedRun_isRefused(
            @TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            keys.add(String.format(PREFIX + "key-%02d", i).getBytes(StandardCharsets.UTF_8));
        }
        Path db = dir.resolve("c.sqlite");
        Path originalDir = dir.resolve("original-dataset");

        // First run: pages of 2 keys; crash after callIndex 1 (a leading seed probe is callIndex 0,
        // so this lets at least the first REAL page durably commit before the injected crash).
        MockPageFetcher faulty = MockPageFetcher.builder()
                .keys(keys)
                .maxKeysCap(2)
                .interceptor((req, idx, page) -> {
                    if (idx == 2) {
                        // An InterruptedException (unlike a fatal ListingException) passes through
                        // ListCommand#runEngineGuarded UN-MARKED (no fatal_error flag set) -- the
                        // same disposition a real SIGKILL mid-run leaves: RUNNING, resumable.
                        throw new InterruptedException("injected crash after the first page committed");
                    }
                    return page;
                })
                .build();
        ListCommand crashRun = new ListCommand();
        crashRun.uri = "s3://" + BUCKET + "/" + PREFIX;
        crashRun.connection.region = "us-east-1";
        crashRun.connection.noSignRequest = true;
        crashRun.checkpoint.location = db.toString();
        crashRun.output.format = OutputFormat.PARQUET;
        crashRun.output.destination = originalDir.toString();
        crashRun.fetcherOverride = faulty;

        assertThatThrownBy(crashRun::call).isInstanceOf(InterruptedException.class);
        // The run is left UNFINISHED (RUNNING, one page durably committed) — exactly the hazardous
        // precondition, produced by the real engine/checkpoint, not hand-seeded.

        // Resume: retarget -o to a DIFFERENT, empty, valid directory.
        Path retargetedDir = Files.createDirectories(dir.resolve("retargeted-dataset"));
        MockPageFetcher continuation = MockPageFetcher.builder().keys(keys).maxKeysCap(2).build();
        ListCommand resumeRun = new ListCommand();
        resumeRun.uri = "s3://" + BUCKET + "/" + PREFIX;
        resumeRun.connection.region = "us-east-1";
        resumeRun.connection.noSignRequest = true;
        resumeRun.checkpoint.location = db.toString();
        resumeRun.checkpoint.resume = true;
        resumeRun.output.format = OutputFormat.PARQUET;
        resumeRun.output.destination = retargetedDir.toString();
        resumeRun.fetcherOverride = continuation;

        // The retargeted -o mismatches the stored `output` column: refused before any relist, so the
        // retargeted directory is never written and the original's committed page is not orphaned.
        assertThatThrownBy(resumeRun::call)
                .as("retargeting an UNFINISHED run's -o is §4.4 identity, now enforced via identity_spec")
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("output changed since the checkpointed run");
    }

    // ---- identity_spec must be collision-free — delimiter-bearing values cannot hide a change -----

    /**
     * A directory-dataset {@code -o} path is a valid POSIX path that may
     * contain {@code ;} and {@code =}. The old naive {@code column=value;...} identity_spec join
     * fragmented such a value — the suffix after the first {@code ;} split off, and a fragment lacking
     * {@code =} was silently dropped — so a genuine retarget that differs ONLY in that suffix was NOT
     * detected and the resume silently re-listed into the new directory (orphaning committed pages).
     * With the length-prefixed encoding the whole path is one opaque value, so the retarget is REFUSED
     * naming {@code output}. Mutation-verified: under the old {@code ;}-grammar this test goes RED
     * (no refusal — the differing suffix is dropped, the {@code output} fragment matches).
     */
    @Test
    void destinationPathWithDelimiters_retargeted_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        // Same pre-';' prefix, differing suffix with no '=' — the exact shape the naive grammar drops.
        Path originalDataset = Files.createDirectories(dir.resolve("ds;v1"));
        Path retargetedDataset = Files.createDirectories(dir.resolve("ds;v2"));
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.PARQUET,
                new SoftRestoreContext(true, null, "us-west-2", false, false, originalDataset.toString(), false, null, null),
                false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.output.destination = retargetedDataset.toString();   // explicit -o, differs only after the ';'
        cmd.output.format = OutputFormat.PARQUET;

        assertThatThrownBy(cmd::call)
                .as("a ';'-bearing -o retarget is §4.4 identity; the length prefix keeps it detectable")
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("output changed since the checkpointed run");
    }

    @Test
    void bareResumeWithDelimiterPath_isNotSpuriouslyRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path dataset = Files.createDirectories(dir.resolve("ds;v1"));
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.PARQUET,
                new SoftRestoreContext(true, null, "us-west-2", false, false, dataset.toString(), false, null, null),
                false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);   // -o not re-passed: restored from checkpoint
        cmd.output.format = OutputFormat.PARQUET;

        // The ';'-bearing path round-trips through the encoding unchanged, so a bare resume matches.
        assertThat(cmd.call())
                .as("a ';'-bearing path is not misparsed into a spurious mismatch on a bare resume")
                .isEqualTo(ExitCodes.SUCCESS);
    }

    /**
     * The same root cause, on {@code filter_spec}: its value is itself a {@link FilterSpecCodec} blob
     * full of {@code ;}/{@code =}/{@code :}. Under the naive grammar a change to a non-first filter
     * field (e.g. {@code --exclude}) fragmented into a PHANTOM column name ({@code exclude}) in the
     * refusal — misnaming the change. Carried as one opaque length-prefixed value, the refusal now
     * correctly names {@code filter_spec}. Mutation-verified: under the old {@code ;}-grammar this test
     * goes RED (the message names {@code exclude}, not {@code filter_spec}).
     */
    @Test
    void filterExcludeChangedOnResume_refusesNamingFilterSpec(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL, SoftRestoreContext.NONE, false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.filters.exclude = "\\.tmp$";   // a non-first filter field: the naive grammar would name it directly

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("filter_spec changed since the checkpointed run");
    }

    // ---- --raw is identity, enforced via identity_spec ---------------------------------------------

    /**
     * {@code raw_output} is an IDENTITY option (it changes what is emitted), enforced through
     * {@code identity_spec}. An explicit {@code --raw}
     * that disagrees with the checkpoint survives the cli-wins restore ({@link ListCommand#restoreBoolean})
     * and is now REFUSED, symmetric with {@code --fetch-owner}.
     */
    @Test
    void rawOutput_explicitlyDiffering_isRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL,
                new SoftRestoreContext(false, null, null, false, false, null, false, null, null), false);   // stored: rawOutput=false

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.output.rawOutput = true;   // explicit --raw, disagrees with the checkpoint

        assertThatThrownBy(cmd::call)
                .as("--raw is §4.4 identity, now enforced via identity_spec")
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("raw_output changed since the checkpointed run");
    }

    // ---- sticky: --region/--profile — matches the spec (silently restored, or cli wins) -----------

    @Test
    void regionAndProfile_notPassed_areSilentlyRestoredFromCheckpoint(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL,
                new SoftRestoreContext(true, "stored-profile", "us-west-2", false, false, null, false, null, null), false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);   // no --region/--profile re-passed

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.connection.region).as("region restored").isEqualTo("us-west-2");
        assertThat(cmd.connection.profile).as("profile restored").isEqualTo("stored-profile");
    }

    @Test
    void regionAndProfile_explicitlyDiffering_areSilentlyHonouredNotRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL,
                new SoftRestoreContext(true, "stored-profile", "us-west-2", false, false, null, false, null, null), false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.connection.region = "eu-west-1";     // explicit, disagrees with the checkpointed us-west-2
        cmd.connection.profile = "cli-profile";  // explicit, disagrees with the checkpointed profile

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.connection.region).as("cli region wins, never refused").isEqualTo("eu-west-1");
        assertThat(cmd.connection.profile).as("cli profile wins, never refused").isEqualTo("cli-profile");
    }

    // ---- free: --concurrency — never stored, never restored, never refused (matches the spec) -----

    @Test
    void concurrency_isFreePerInvocation_neverStoredNeverRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, ENDPOINT, NO_FILTER_SPEC, OutputFormat.JSONL, SoftRestoreContext.NONE, false);

        ListCommand cmd = bareResumeCommand(db, ENDPOINT);
        cmd.connection.maxParallelListings = 7;   // arbitrary per-invocation value; nothing stores it

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.connection.maxParallelListings)
                .as("--concurrency is free: the newly passed value is simply used, untouched by restore")
                .isEqualTo(7);
    }
}
