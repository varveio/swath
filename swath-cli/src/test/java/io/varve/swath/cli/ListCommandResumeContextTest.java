/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * RES: {@code swath resume} must restore the checkpointed run's non-{@code args_hash}
 * <b>context</b> — {@code --no-sign-request}/{@code --profile}/{@code --region}, the
 * fetch-owner/raw-output compatibility fields, and {@code -o} — when the caller does not re-supply
 * them, so a bare {@code swath resume} reconstructs the original run's auth/output
 * context instead of silently defaulting (losing {@code --no-sign-request} → auth failure,
 * losing {@code --region} → wrong endpoint).
 *
 * <p>The prior run is seeded COMPLETE so the resume is a clean no-op that never touches S3 —
 * restoring {@code --no-sign-request}/{@code --region} into the effective config makes {@code
 * buildConfig()} succeed without any AWS credentials/region in the test environment; a bug that
 * dropped the restore would either need real AWS env, or (region) throw {@code
 * InvalidConfigException}.
 */
final class ListCommandResumeContextTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    /** Seed a COMPLETED run carrying the full run-context (defaults {@code --requester-pays} off). */
    private static void seedCompletedRun(Path db, boolean noSignRequest, String profile, String region,
                                         boolean fetchOwner, boolean rawOutput, String outputPath) throws Exception {
        seedCompletedRun(db, noSignRequest, profile, region, fetchOwner, rawOutput, outputPath, false);
    }

    /** Seed a COMPLETED run carrying the full run-context. */
    private static void seedCompletedRun(Path db, boolean noSignRequest, String profile, String region,
                                         boolean fetchOwner, boolean rawOutput, String outputPath,
                                         boolean requestPayer) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        OutputFormat format = outputPath == null ? OutputFormat.JSONL : OutputFormat.PARQUET;
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, format.name(),
                new SoftRestoreContext(noSignRequest, profile, region, fetchOwner, rawOutput, outputPath, requestPayer, null, null),
                false);
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

    private static ListCommand bareResumeCommand(Path db) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        return cmd;
    }

    @Test
    void bareResume_restoresFullRunContext(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = Files.createDirectories(dir.resolve("out-dataset"));
        seedCompletedRun(db, true, "my-profile", "us-west-2", true, true, outDir.toString());

        ListCommand cmd = bareResumeCommand(db);   // no --no-sign-request/--profile/--region/... re-passed
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(cmd.connection.noSignRequest).as("no_sign_request restored").isTrue();
        assertThat(cmd.connection.profile).as("profile restored").isEqualTo("my-profile");
        assertThat(cmd.connection.region).as("region restored").isEqualTo("us-west-2");
        assertThat(cmd.connection.fetchOwner).as("fetch_owner restored").isTrue();
        assertThat(cmd.output.rawOutput).as("raw_output restored").isTrue();
        assertThat(cmd.output.destination).as("output path restored").isEqualTo(outDir.toString());
    }

    /**
     * {@code --requester-pays requester} restores through {@code swath resume} exactly
     * like {@code --no-sign-request} (RES coverage mirroring {@code bareResume_restoresFullRunContext}).
     */
    @Test
    void bareResume_restoresRequestPayer(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, false, null, null, false, false, null, true);

        ListCommand cmd = bareResumeCommand(db);   // no --requester-pays re-passed
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(cmd.connection.requestPayerEnabled).as("request_payer restored").isTrue();
    }

    /** An explicit {@code --requester-pays requester} on the resuming CLI wins over the checkpoint. */
    @Test
    void explicitRequestPayer_winsOverCheckpointedContext(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedCompletedRun(db, false, null, null, false, false, null, false);   // checkpointed: off

        ListCommand cmd = bareResumeCommand(db);
        cmd.connection.requestPayer = "requester";   // explicit, conflicts with checkpointed off

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.connection.requestPayerEnabled).isTrue();
    }

    @Test
    void explicitCliValue_winsOverCheckpointedContext(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path checkpointOut = Files.createDirectories(dir.resolve("checkpoint-out"));
        Path cliOut = Files.createDirectories(dir.resolve("cli-out"));
        seedCompletedRun(db, true, "checkpoint-profile", "us-west-2", true, true,
                checkpointOut.toString());

        ListCommand cmd = bareResumeCommand(db);
        cmd.connection.region = "eu-west-1";                 // explicit, conflicts with checkpointed us-west-2
        cmd.connection.profile = "cli-profile";               // explicit, conflicts with checkpointed profile
        cmd.output.destination = cliOut.toString();          // explicit, conflicts with checkpointed path
        cmd.output.format = OutputFormat.PARQUET;             // directory path cannot infer its format

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(cmd.connection.region).isEqualTo("eu-west-1");
        assertThat(cmd.connection.profile).isEqualTo("cli-profile");
        assertThat(cmd.output.destination).isEqualTo(cliOut.toString());
        // Fields the caller did NOT set are still restored.
        assertThat(cmd.connection.noSignRequest).isTrue();
        assertThat(cmd.connection.fetchOwner).isTrue();
        assertThat(cmd.output.rawOutput).isTrue();
    }

    /**
     * A bare resume from a DB with a stored {@code region} and <b>NO</b> {@code endpointUrl}
     * must open the checkpoint and restore the run-context (region) BEFORE building {@code S3Config}
     * — so region resolution succeeds from the checkpoint even when the caller passes no
     * {@code --region}/{@code --endpoint-url} and the environment resolves none. The other tests here always set {@code endpointUrl}, which makes {@code resolveRegion}
     * short-circuit to US_EAST_1 and MASKS this bug; dropping it exercises the real default-chain
     * path. Pre-fix, {@code buildConfig()} ran at the top of {@code call()} before the DB was opened,
     * so region resolution failed with {@code InvalidConfigException} (exit 2) before the stored
     * region could be read.
     */
    @Test
    void bareResume_noEndpoint_restoresStoredRegionBeforeConfig(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        // Seed a COMPLETED run with NO endpoint (args_hash + RunKey use endpoint=null) carrying a
        // stored region. no_sign_request keeps credential resolution env-free (anonymous).
        String argsHash = ArgsHashFields.forListing("s3", "", BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", null, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.JSONL.name(),
                new SoftRestoreContext(true, null, "ap-southeast-2", false, false, null, false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        // deliberately NO endpointUrl and NO --region: resolution must come from the checkpoint.
        cmd.output.format = OutputFormat.JSONL;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;

        assertThat(cmd.call()).as("bare resume resolves region from the checkpoint, not a failure")
                .isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.connection.region).as("stored region restored").isEqualTo("ap-southeast-2");
        assertThat(cmd.connection.noSignRequest).as("no_sign_request restored").isTrue();
    }

    @Test
    void bareResume_withoutFormatFlag_restoresStoredFormat_notRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        // Original run wrote parquet; a bare resume with no --format must not default (TTY-based)
        // to jsonl/aligned and trip the "filter or output format changed" refusal.
        Path outDir = dir.resolve("out-dir");
        Files.createDirectories(outDir);   // writeEarlyExitSummary needs it to exist (summary sidecar)
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(true, null, "us-west-2", false, false, outDir.toString(), false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            // Parquet (file-sink) output-complete needs durable_cursor latched too (I6) — else
            // loadResumable(fileSink=true) reopens the node and the test would try a real S3 call.
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        // format left null (not passed) — must restore PARQUET from the checkpoint, not refuse.

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    @Test
    @ResourceLock("SYSTEM_ERR")
    void bareResumeEchoesTheRestoredDirectoryDestination(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = Files.createDirectories(dir.resolve("restored-dataset"));
        seedCompletedRun(db, true, null, "us-west-2", false, false, outDir.toString());
        ListCommand cmd = bareResumeCommand(db);

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream previous = System.err;
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            System.setErr(previous);
        }

        assertThat(captured.toString(StandardCharsets.UTF_8))
                .containsOnlyOnce("writing parquet dataset to " + outDir);
    }

    @Test
    @ResourceLock("SYSTEM_ERR")
    void quietSuppressesThePostRestoreDestinationEcho(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = Files.createDirectories(dir.resolve("restored-dataset"));
        seedCompletedRun(db, true, null, "us-west-2", false, false, outDir.toString());
        ListCommand cmd = bareResumeCommand(db);
        cmd.global.quiet = new boolean[] {true};

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream previous = System.err;
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            System.setErr(previous);
        }

        assertThat(captured.toString(StandardCharsets.UTF_8))
                .doesNotContain("writing parquet dataset to " + outDir);
    }
}
