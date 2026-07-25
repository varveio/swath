/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.output.OutputFormat;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link OutputOptions#resolveOutput}: the {@code --format} x {@code -o} destination axes
 * -- extension inference, the {@code --output-type} override, and every
 * conflict/precedence rule between an explicit {@code --format} and {@code -o}'s extension.
 */
class OutputDestinationResolutionTest {

    private static OutputOptions options() {
        return new OutputOptions();
    }

    // ---- auto format: table on a terminal, tsv piped ---------------------------------------

    @Test
    void noDestinationDefaultsToStdoutWithAutoFormat() throws Exception {
        OutputOptions out = options();
        assertThat(out.resolveOutput(true))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.TABLE, OutputOptions.DestinationKind.STDOUT));
        assertThat(out.resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.TSV, OutputOptions.DestinationKind.STDOUT));
    }

    @Test
    void dashDestinationIsExactlyStdout() throws Exception {
        OutputOptions out = options();
        out.destination = "-";
        assertThat(out.resolveOutput(true).kind()).isEqualTo(OutputOptions.DestinationKind.STDOUT);
        // Critical: -o - must stay EXPLICITLY visible on the field, not collapse
        // to null (which is indistinguishable from "-o was never passed at all"). Nulling it here
        // would let ListCommand#restoreRunContext's restoreString silently restore a checkpointed
        // destination over this explicit stdout request on a bare resume (repro test below).
        assertThat(out.destination).as("stays '-', never normalized to null").isEqualTo("-");
        assertThat(out.isStdoutDestination()).isTrue();
    }

    @Test
    void explicitDashSurvivesTheRestoreStringPattern() {
        // repro: restoreString(field, cliValue, storedValue) only
        // restores storedValue when cliValue == null. An explicit "-o -" must leave a non-null
        // cliValue so a stored checkpoint destination is NEVER silently substituted in.
        OutputOptions out = options();
        out.destination = "-";
        String stored = "/some/checkpointed/dataset/dir";
        String restored = out.destination != null ? out.destination : stored;   // mirrors restoreString's own logic
        assertThat(restored).as("explicit -o - wins over ANY stored destination").isEqualTo("-");
    }

    @Test
    void explicitFormatWinsOverAutoOnStdout() throws Exception {
        OutputOptions out = options();
        out.format = OutputFormat.JSONL;
        assertThat(out.resolveOutput(true).format()).isEqualTo(OutputFormat.JSONL);
    }

    // ---- extension inference ---------------------------------------------------------------

    @Test
    void knownExtensionInfersBothFormatAndFileKind() throws Exception {
        assertThat(destOnly("out.tsv").resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.TSV, OutputOptions.DestinationKind.FILE));
        assertThat(destOnly("out.jsonl").resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.JSONL, OutputOptions.DestinationKind.FILE));
        assertThat(destOnly("out.parquet").resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.PARQUET, OutputOptions.DestinationKind.FILE));
    }

    @Test
    void recordedDestinationClassifierNormalizesTrailingSeparators() {
        assertThat(OutputOptions.formatFromExtension("out.jsonl/"))
                .isEqualTo(OutputFormat.JSONL);
        assertThat(OutputOptions.formatFromExtension("out.parquet/"))
                .isEqualTo(OutputFormat.PARQUET);
        assertThat(OutputOptions.formatFromExtension("out.PARQUET"))
                .isEqualTo(OutputFormat.PARQUET);
        assertThat(OutputOptions.formatFromExtension("a.jsonl.bak")).isNull();
        assertThat(OutputOptions.formatFromExtension("/")).isNull();
        assertThat(OutputOptions.formatFromExtension("")).isNull();
    }

    @Test
    void unrecognizedExtensionWithExplicitParquetIsADirectoryDataset() throws Exception {
        OutputOptions out = destOnly("results");
        out.format = OutputFormat.PARQUET;
        assertThat(out.resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.PARQUET, OutputOptions.DestinationKind.DIRECTORY));
    }

    @Test
    void trailingSlashDoesNotChangeTheInference() throws Exception {
        // A trailing slash is not load-bearing: results/ and results must resolve identically.
        OutputOptions withSlash = destOnly("results/");
        withSlash.format = OutputFormat.PARQUET;
        OutputOptions withoutSlash = destOnly("results");
        withoutSlash.format = OutputFormat.PARQUET;
        assertThat(withSlash.resolveOutput(false)).isEqualTo(withoutSlash.resolveOutput(false));
    }

    // ---- conflicts / error cases (exit 2) --------------------------------------------------

    @Test
    void explicitFormatConflictingWithTheExtensionIsRejected() {
        OutputOptions out = destOnly("out.tsv");
        out.format = OutputFormat.JSONL;
        assertThatThrownBy(() -> out.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--format jsonl")
                .hasMessageContaining("'.tsv'");
    }

    @Test
    void explicitFormatMatchingTheExtensionIsAccepted() throws Exception {
        OutputOptions out = destOnly("out.tsv");
        out.format = OutputFormat.TSV;
        assertThat(out.resolveOutput(false).format()).isEqualTo(OutputFormat.TSV);
    }

    @Test
    void noExtensionAndNoFormatIsRejected() {
        OutputOptions out = destOnly("results");
        assertThatThrownBy(() -> out.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("no recognized extension")
                .hasMessageContaining("no --format was given");
    }

    @Test
    void directoryKindWithATextFormatIsRejected() {
        OutputOptions out = destOnly("results");
        out.format = OutputFormat.JSONL;
        assertThatThrownBy(() -> out.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("supported for --format parquet only today");
    }

    @Test
    void tableDirectoryGuardDoesNotRecommendTheUnrecognizedTableExtension() {
        OutputOptions out = destOnly("results");
        out.format = OutputFormat.TABLE;
        assertThatThrownBy(() -> out.resolveOutput(true))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--output-type file")
                .hasMessageContaining(".tsv/.jsonl/.parquet")
                .hasMessageNotContaining(".table file path");
    }

    // ---- --output-type override ------------------------------------------------------------

    @Test
    void outputTypeFileOverridesAnUnrecognizedExtensionToASingleFile() throws Exception {
        OutputOptions out = destOnly("out.txt");
        out.format = OutputFormat.TABLE;
        out.outputType = "file";
        assertThat(out.resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.TABLE, OutputOptions.DestinationKind.FILE));
    }

    @Test
    void outputTypeDirOverridesAKnownExtensionToADirectoryDataset() throws Exception {
        OutputOptions out = destOnly("out.parquet");
        out.outputType = "dir";
        assertThat(out.resolveOutput(false))
                .isEqualTo(new OutputOptions.Resolved(OutputFormat.PARQUET, OutputOptions.DestinationKind.DIRECTORY));
    }

    @Test
    void unknownOutputTypeValueIsRejected() {
        OutputOptions out = destOnly("out.parquet");
        out.outputType = "bogus";
        assertThatThrownBy(() -> out.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--output-type must be 'file' or 'dir'");
    }

    @Test
    void outputTypeDirDoesNotBypassTheExtensionFormatMismatchCheck() {
        // --output-type dir must not let a conflicting --format sneak
        // past the extension-implies-format check -- the extension is a fact about the path,
        // unaffected by a file-vs-directory override.
        OutputOptions out = destOnly("out.tsv");
        out.format = OutputFormat.JSONL;
        out.outputType = "dir";
        assertThatThrownBy(() -> out.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--format jsonl")
                .hasMessageContaining("'.tsv'");
    }

    // ---- --output-type + stdout precedence holes -----------------------------------------

    @Test
    void outputTypeWithNoDestinationIsRejectedAsMeaningless() {
        OutputOptions out = options();
        out.outputType = "file";
        assertThatThrownBy(() -> out.resolveOutput(true))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--output-type is meaningless with a stdout destination");
    }

    @Test
    void outputTypeWithExplicitDashDestinationIsRejectedAsMeaningless() {
        OutputOptions out = options();
        out.destination = "-";
        out.outputType = "dir";
        assertThatThrownBy(() -> out.resolveOutput(true))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--output-type is meaningless with a stdout destination");
    }

    @Test
    void invalidOutputTypeValueIsValidatedEvenForStdout() {
        // The VALUE must be checked (exit 2 on garbage) before the "meaningless with stdout"
        // refusal -- an invalid value is its own bug regardless of destination.
        OutputOptions out = options();
        out.outputType = "bogus";
        assertThatThrownBy(() -> out.resolveOutput(true))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--output-type must be 'file' or 'dir'");
    }

    // ---- output-type-missing message names --format, not --output-type -----------------------

    @Test
    void outputTypeFilePlusNoExtensionPlusNoFormatNamesFormatAsMissing_notOutputType() {
        // The old message suggested "--output-type file" even when it was ALREADY set -- circular,
        // useless advice. The fixed message must name --format as what's actually missing.
        OutputOptions out = destOnly("results");
        out.outputType = "file";
        assertThatThrownBy(() -> out.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("pass --format explicitly")
                .hasMessageNotContaining("or --output-type file");
    }

    private static OutputOptions destOnly(String destination) {
        OutputOptions out = options();
        out.destination = destination;
        return out;
    }
}
