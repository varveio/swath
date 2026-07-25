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
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

/**
 * End-to-end (through {@link ListCommand#call()}) pin for {@code --format auto}:
 * {@code table} on a terminal stdout, {@code tsv} when piped, driven by {@link
 * TerminalCapabilities} via the {@code terminalOverride} test seam -- no real controlling
 * terminal exists under a build, so this is the only way to exercise both branches
 * deterministically. Uses {@code --sort} with no explicit {@code --format} as the observation
 * point: {@link ListCommand#validateSortFlags} fails fast (before any checkpoint/network I/O)
 * and its message names the resolved format, so the auto-detected value is directly visible.
 */
class FormatAutoDetectionTest {

    private static final String ENDPOINT = "http://localhost:4566";

    private static ListCommand sortCommand(boolean stdoutIsTerminal) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/prefix";
        cmd.sorting.sort = true;
        cmd.terminalOverride = new TerminalCapabilities(fd -> stdoutIsTerminal && fd == TerminalCapabilities.STDOUT_FD);
        return cmd;
    }

    @Test
    void terminalStdoutAutoDetectsTable() {
        assertThatThrownBy(sortCommand(true)::call)
                .isInstanceOf(InvalidArgsException.class)
                .as("a terminal stdout with no explicit --format auto-detects table (not the old aligned/jsonl split)")
                .hasMessageContaining("resolved format is table");
    }

    @Test
    void pipedStdoutAutoDetectsTsv() {
        assertThatThrownBy(sortCommand(false)::call)
                .isInstanceOf(InvalidArgsException.class)
                .as("piped stdout with no explicit --format auto-detects tsv")
                .hasMessageContaining("resolved format is tsv");
    }

    /** {@code --format auto} must be an ACCEPTED EXPLICIT spelling
     * (the spec advertises it), not just the behavior of omitting {@code --format} -- must route
     * to the exact same detection. Parsed through picocli (not direct field assignment) since the
     * {@code auto} spelling only exists via {@link OutputOptions.FormatConverter}. */
    @Test
    void explicitFormatAutoSpellingRoutesToDetectionJustLikeOmittingTheFlag() {
        ListCommand explicitAuto = new ListCommand();
        new CommandLine(explicitAuto).parseArgs("s3://bucket/prefix", "--sort", "--format", "auto");
        Assertions.assertThat(explicitAuto.output.formatWasExplicitlySet()).isTrue();
        explicitAuto.terminalOverride = new TerminalCapabilities(fd -> fd == TerminalCapabilities.STDOUT_FD);
        assertThatThrownBy(explicitAuto::call)
                .isInstanceOf(InvalidArgsException.class)
                .as("--format auto on a terminal stdout resolves to table, exactly like omitting --format")
                .hasMessageContaining("got --format table");

        ListCommand explicitAutoPiped = new ListCommand();
        new CommandLine(explicitAutoPiped).parseArgs("s3://bucket/prefix", "--sort", "--format", "AUTO");
        explicitAutoPiped.terminalOverride = new TerminalCapabilities(fd -> false);
        assertThatThrownBy(explicitAutoPiped::call)
                .isInstanceOf(InvalidArgsException.class)
                .as("--format AUTO (case-insensitive) piped resolves to tsv")
                .hasMessageContaining("got --format tsv");
    }

    @Test
    void explicitAutoOnResumeUsesTheCurrentTerminalAndParticipatesInFormatMismatch(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = Files.createDirectories(dir.resolve("parquet-dataset"));
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, "bucket", "prefix").hash();
        String noFilters = FilterSpecCodec.encode(null, null, null, null, null, null, null);
        RunKey key = new RunKey("s3", ENDPOINT, "bucket", "prefix".getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, noFilters, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, outDir.toString(), false, null, null),
                false, storedIdentitySpec(outDir, noFilters));
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "prefix/z".getBytes(StandardCharsets.UTF_8), true));
        }

        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint",
                db.toString(), "--format", "auto");
        cmd.checkpoint.resume = true; // the resume verb sets this internal field before direct dispatch
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.terminalOverride = new TerminalCapabilities(fd -> fd == TerminalCapabilities.STDOUT_FD);

        // The terminal auto-detects to a text format, which mismatches the stored parquet identity:
        // the registry refusal names output_format as the changed column.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("output_format")
                .hasMessageContaining("changed since the checkpointed run")
                .hasMessageNotContaining("FILE kind");
    }

    /**
     * The {@code run_meta.identity_spec} a real creating run would persist: the registry's IDENTITY
     * fingerprint over a {@link ListCommand} mirroring the seeded parquet run's fully-resolved state.
     */
    private static String storedIdentitySpec(Path outDir, String filterSpec) throws Exception {
        ListCommand original = new ListCommand();
        original.uri = "s3://bucket/prefix";
        original.connection.endpointUrl = ENDPOINT;
        original.output.destination = outDir.toString();
        original.output.format = OutputFormat.PARQUET;
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

    @Test
    void unknownFormatValueIsRejectedWithAllFiveTokensListed() {
        ListCommand cmd = new ListCommand();
        CommandLine picocliCmd = new CommandLine(cmd);
        Assertions.assertThatThrownBy(
                        () -> picocliCmd.parseArgs("s3://bucket/prefix", "--format", "xml"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("table, tsv, jsonl, parquet, auto");
    }
}
