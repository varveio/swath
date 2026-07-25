/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * The end-of-run block as an operator actually meets it — end-to-end through {@link
 * ListCommand#call()} with a mock fetcher, reading the real stderr the process would write.
 *
 * <p>The load-bearing case is a <b>redirected</b> stderr: {@code isatty} decides whether swath may
 * colorize, never whether the operator is told what happened, so a captured log gets the same
 * content a terminal does. That is the case a naive terminal gate silently breaks, and for an
 * overnight or fleet run it is the only artifact there is.
 */
final class RunSummaryBlockTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENDPOINT = "http://localhost:4566";

    private final ch.qos.logback.classic.Logger swathLogger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("io.varve.swath");
    private Level originalLevel;

    /**
     * A directly-constructed {@link ListCommand} never runs picocli's execution strategy, so pin
     * the default verbosity the real CLI installs there — these tests are about what the default
     * tier writes to stderr.
     */
    @BeforeEach
    void defaultVerbosity() {
        originalLevel = swathLogger.getLevel();
        CliLogging.configure(0, 0);
    }

    @AfterEach
    void restoreVerbosity() {
        swathLogger.setLevel(originalLevel);
    }

    /** Long enough to clear {@link SummaryRenderer#AUTO_MIN_ELAPSED} without a flag forcing it. */
    private static final long OVER_THRESHOLD_MS = 1_800L;

    @Test
    void everyFdCombinationGetsIdenticalContent(@TempDir Path dir) throws Exception {
        List<String> rendered = new ArrayList<>();
        for (boolean stdoutTty : List.of(true, false)) {
            for (boolean stderrTty : List.of(true, false)) {
                ListCommand cmd = listCommand(dir.resolve("out-" + stdoutTty + stderrTty + ".jsonl"));
                cmd.output.stats = true;
                cmd.terminalOverride = new TerminalCapabilities(fd ->
                        fd == TerminalCapabilities.STDOUT_FD ? stdoutTty : stderrTty);
                // Figures differ run to run (rate, RSS, and whether a rate crosses into
                // thousands); the CONTENT — which fields, in which shape — must not.
                rendered.add(block(runCapturingStderr(cmd)).replaceAll("[0-9][0-9,.]*", "#"));
            }
        }

        assertThat(rendered).as("the fd probe selects form, never whether: all four cells "
                        + "carry the same summary content")
                .containsOnly(rendered.getFirst());
        assertThat(rendered.getFirst()).contains("objects in").contains("API calls");
        assertThat(rendered.getFirst()).doesNotContain("\u001B[");
    }

    @Test
    void aRunLongEnoughToWaitOnWritesTheBlockToARedirectedStderrAsPlainText(@TempDir Path dir)
            throws Exception {
        ListCommand cmd = listCommand(dir.resolve("out.jsonl"), OVER_THRESHOLD_MS);
        cmd.terminalOverride = new TerminalCapabilities(fd -> false);   // stderr is a file

        String stderr = runCapturingStderr(cmd);

        assertThat(block(stderr))
                .as("no flag was passed: the run earned its summary by taking long enough to wait on")
                .contains("objects in").contains("API calls");
        assertThat(stderr).as("plain text, no ANSI, into a captured stderr")
                .doesNotContain("\u001B[");
    }

    @Test
    void theBlockAgreesWithTheReportJsonFieldForField(@TempDir Path dir) throws Exception {
        Path report = dir.resolve("report.json");
        ListCommand cmd = listCommand(dir.resolve("out.jsonl"));
        cmd.output.stats = true;
        cmd.output.summaryJson = report.toString();

        String stderr = runCapturingStderr(cmd);

        JsonNode root = MAPPER.readTree(report.toFile());
        long objects = root.get("objects").asLong();
        long apiCalls = root.get("cost").get("api_calls").asLong();
        assertThat(block(stderr))
                .as("the block and the report render one source, so their figures cannot disagree")
                .contains(objects + " objects in")
                .contains(apiCalls + " API calls")
                .contains("peak " + root.get("engine").get("peak_in_flight").asLong());
        assertThat(root.get("cost").get("basis").get("rate_per_1k_usd").asDouble())
                .isEqualTo(RunMetrics.LIST_COST_PER_1K_USD);
        assertThat(root.get("cost").get("basis").get("source").asText())
                .isEqualTo(RunMetrics.LIST_COST_SOURCE);
    }

    /**
     * The W1 silence invariant, as amended by the summary landing: a healthy run's stderr carries
     * the destination echo and the block and <b>nothing log-shaped</b> — no timestamped level
     * lines, no warnings. {@code --no-stats} restores literal silence for scripts that want it.
     */
    @Test
    void healthyRunCarriesNoLogShapedLinesAndNoStatsRestoresSilence(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("out.jsonl");
        String withSummary = runCapturingStderr(listCommand(out));

        assertThat(withSummary).startsWith("→ writing jsonl to " + out);
        assertThat(withSummary.lines()).as("a healthy run logs nothing: no WARN, no timestamped line")
                .noneMatch(line -> line.matches("\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d .*"));
        assertThat(block(withSummary)).contains("objects in");

        ListCommand silent = listCommand(dir.resolve("silent.jsonl"));
        silent.output.stats = false;

        assertThat(runCapturingStderr(silent).lines())
                .as("--no-stats leaves exactly the destination echo")
                .containsExactly("→ writing jsonl to " + dir.resolve("silent.jsonl"));
    }

    @Test
    void unknownProviderWithholdsTheDollarButStillTagsTheReportBasis(@TempDir Path dir) throws Exception {
        Path report = dir.resolve("report.json");
        ListCommand cmd = listCommand(dir.resolve("out.jsonl"));
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.stats = true;
        cmd.output.summaryJson = report.toString();

        String stderr = runCapturingStderr(cmd);

        assertThat(block(stderr)).as("--endpoint-url: unknown provider, so no dollar figure at all")
                .doesNotContain("$");
        assertThat(MAPPER.readTree(report.toFile()).get("cost").get("basis").get("source").asText())
                .as("the report still names the rate it derived cost_usd from")
                .isEqualTo(RunMetrics.LIST_COST_SOURCE);
    }

    /** Everything the summary block writes, i.e. stderr minus the destination echo. */
    private static String block(String stderr) {
        return stderr.lines().filter(line -> !line.startsWith("→ writing"))
                .reduce("", (a, b) -> a + b + "\n");
    }

    private static String runCapturingStderr(ListCommand cmd) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            System.setErr(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    private static ListCommand listCommand(Path out) {
        return listCommand(out, 0L);
    }

    /** A checkpoint-free run over a mock keyspace, optionally slowed to earn an auto summary. */
    private static ListCommand listCommand(Path out, long firstPageDelayMs) {
        MockPageFetcher.Builder fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)));
        if (firstPageDelayMs > 0) {
            fetcher.interceptor((req, idx, page) -> {
                TimeUnit.MILLISECONDS.sleep(firstPageDelayMs);
                return page;
            });
        }
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = "none";
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = out.toString();
        cmd.fetcherOverride = fetcher.build();
        return cmd;
    }
}
