/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * CLI acceptance: {@code swath --version} and {@code swath help list} work.
 */
class AppCliTest {

    private static String run(int[] exit, String... args) {
        StringWriter out = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.setOut(new PrintWriter(out));
        cmd.setErr(new PrintWriter(out));
        exit[0] = cmd.execute(args);
        return out.toString();
    }

    @Test
    void versionFlagPrintsVersionAndExitsZero() {
        int[] exit = new int[1];
        String text = run(exit, "--version");
        assertThat(exit[0]).isZero();
        assertThat(text).isEqualTo("swath development" + System.lineSeparator());
    }

    @Test
    void helpListShowsTheListUsage() {
        int[] exit = new int[1];
        String text = run(exit, "help", "list");
        assertThat(exit[0]).isZero();
        assertThat(text).contains("List a bucket or prefix");
        assertThat(text).contains("<s3-uri>");
    }

    @Test
    void listParsesRegionFlag() {
        ListCommand list = new ListCommand();
        new CommandLine(list).parseArgs("s3://bucket/prefix", "--region", "ap-northeast-1",
                "--checkpoint", "none");

        assertThat(list.connection.region).isEqualTo("ap-northeast-1");
    }

    @Test
    void listParsesProgressIntervalFlag() throws Exception {
        ListCommand list = new ListCommand();
        new CommandLine(list).parseArgs("s3://bucket/prefix", "--progress-interval", "500ms",
                "--checkpoint", "none");
        list.syncArgGroups();

        assertThat(list.liveness.resolveProgressInterval()).isEqualTo(Duration.ofMillis(500));
        assertThat(DurationParser.progressInterval("2s")).isEqualTo(Duration.ofSeconds(2));
        assertThat(DurationParser.progressInterval("PT2S")).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void invalidProgressIntervalIsRejected() {
        // garbage, non-positive, and empty must be rejected (InvalidConfigException -> exit 2)
        for (String bad : new String[] {"foo", "0s", "-5s", "", "10x"}) {
            assertThatThrownBy(() -> DurationParser.progressInterval(bad))
                    .as("reject --progress-interval=%s", bad)
                    .isInstanceOf(InvalidConfigException.class);
        }
    }

    @Test
    void noSubcommandIsAUsageErrorExitTwo() {
        int[] exit = new int[1];
        run(exit);   // no args
        assertThat(exit[0]).isEqualTo(2);   // incomplete invocation
    }

    @Test
    void topLevelHelpListsBothVerbs() {
        int[] exit = new int[1];
        String text = run(exit, "--help");
        assertThat(exit[0]).isZero();
        assertThat(text).contains("list").contains("resume");
        assertThat(text).doesNotContain("completion");
    }

    @Test
    void hiddenCompletionSubcommandIsRegisteredAndGeneratesAScript() {
        CommandLine cli = App.commandLine();
        assertThat(cli.getSubcommands()).containsKey("completion");
        assertThat(cli.getSubcommands().get("completion").getCommandSpec()
                .usageMessage().hidden()).isTrue();
        StringWriter out = new StringWriter();
        cli.setOut(new PrintWriter(out));

        assertThat(cli.execute("completion")).isZero();
        assertThat(out.toString()).contains("function _complete_swath()")
                .contains("complete -F _complete_swath -o default swath");
    }

    @Test
    void listHelpFlagPrintsUsageAndExitsZero() {
        // `list` mixes in the standard help options directly (not just the top-level `help
        // list`/`--help` on App), so `swath list --help` works too.
        int[] exit = new int[1];
        String text = run(exit, "list", "--help");
        assertThat(exit[0]).isZero();
        assertThat(text).contains("List a bucket or prefix");
        assertThat(text).contains("<s3-uri>");
    }

    @Test
    void resumeHelpFlagPrintsUsageAndExitsZero() {
        int[] exit = new int[1];
        String text = run(exit, "resume", "--help");
        assertThat(exit[0]).isZero();
        assertThat(text).contains("Resume a run by its output directory");
    }

    @Test
    void bareUriFirstTokenHintsAtTheListVerb() {
        // D-B: explicit verbs, no default command — a bare `swath s3://…` is a usage error
        // (exit 2) with a "did you mean: swath list …" recovery hint ON STDERR, not a listing.
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.setOut(new PrintWriter(out));
        cmd.setErr(new PrintWriter(err));
        int exit = cmd.execute("s3://bucket/prefix");
        assertThat(exit).isEqualTo(2);
        assertThat(err.toString()).contains("did you mean: swath list s3://bucket/prefix");
        assertThat(out.toString()).isEmpty();
    }

    @Test
    void bareUriHintEscapesControlCharactersInsteadOfInjectingAnErrorLine() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.setErr(new PrintWriter(err));

        assertThat(cmd.execute("s3://bucket\nforged-marker")).isEqualTo(2);
        assertThat(err.toString()).contains("s3://bucket\\x0aforged-marker")
                .doesNotContain("\nforged-marker");
    }

    @Test
    void unknownVerbWithoutUriShapeKeepsPicocliDefaultSuggestion() {
        // Only a URI-shaped first token gets the "did you mean: swath list" recovery hint;
        // an ordinary typo'd/unknown verb keeps picocli's own unmatched-argument handling.
        int[] exit = new int[1];
        String text = run(exit, "frobnicate");
        assertThat(exit[0]).isEqualTo(2);
        assertThat(text).doesNotContain("did you mean: swath list");
    }

    @Test
    void malformedEndpointUrlMapsToExitTwo() {
        int[] exit = new int[1];
        String text = run(exit, "list", "s3://bucket", "--checkpoint", "none",
                "--region", "us-east-1", "--endpoint-url", "http://[bad");
        assertThat(exit[0]).isEqualTo(2);
        assertThat(text).contains("invalid --endpoint-url");
    }
}
