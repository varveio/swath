/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The live progress record as an operator actually meets it — end-to-end through {@link
 * ListCommand#call()} with a mock fetcher, reading the real stderr the process would write.
 *
 * <p>The load-bearing case is the ORDER: progress and the end-of-run block share one file
 * descriptor, and a frame that lands after the summary is the classic failure of a live display.
 */
final class LiveProgressRecordTest {

    /** The supported floor, so a frame lands inside a run short enough for a test. */
    private static final String FASTEST_CADENCE = "1s";

    @Test
    @Timeout(60)
    void forcedProgressReachesARedirectedStderrAndEveryFramePrecedesTheSummary(@TempDir Path dir)
            throws Exception {
        ListCommand cmd = slowListCommand(dir.resolve("out.jsonl"));
        cmd.output.progress = true;   // forced on: stderr is a file here, not a terminal
        cmd.output.stats = true;
        cmd.liveness.progressInterval = FASTEST_CADENCE;
        cmd.terminalOverride = new TerminalCapabilities(fd -> false);
        cmd.envOverride = key -> null;

        List<String> stderr = runCapturingStderr(cmd);

        List<String> frames = stderr.stream().filter(LiveProgressRecordTest::isFrame).toList();
        assertThat(frames).as("--progress emits plain records off a terminal too").isNotEmpty();
        assertThat(frames).allSatisfy(frame -> assertThat(frame)
                .as("a redirected stderr receives no carriage returns and no escapes")
                .doesNotContain("\r").doesNotContain("\u001B"));
        int summaryAt = indexOfSummary(stderr);
        assertThat(summaryAt).isGreaterThanOrEqualTo(0);
        assertThat(stderr.subList(summaryAt, stderr.size()))
                .as("the block is the run's last word: no frame lands after it")
                .noneMatch(LiveProgressRecordTest::isFrame);
    }

    @Test
    @Timeout(60)
    void noProgressStaysSilentEvenOnATerminalAndEvenWithAnExplicitCadence(@TempDir Path dir)
            throws Exception {
        ListCommand cmd = slowListCommand(dir.resolve("out.jsonl"));
        cmd.output.progress = false;
        cmd.output.stats = true;
        cmd.liveness.progressInterval = FASTEST_CADENCE;   // an opt-in --no-progress still overrides
        cmd.terminalOverride = new TerminalCapabilities(fd -> true);
        cmd.envOverride = key -> null;

        List<String> stderr = runCapturingStderr(cmd);

        assertThat(stderr).as("--no-progress suppresses the display everywhere")
                .noneMatch(LiveProgressRecordTest::isFrame);
        assertThat(indexOfSummary(stderr)).as("the end-of-run block is a separate decision")
                .isGreaterThanOrEqualTo(0);
    }

    @Test
    @Timeout(60)
    void verboseRunsPublishTheStructuredRecordAndNoProgressSilencesThatToo(@TempDir Path dir)
            throws Exception {
        List<String> withProgress = runVerbose(dir.resolve("with.jsonl"), null);
        assertThat(withProgress).as("-v makes the structured record this run's progress surface")
                .anyMatch(line -> line.contains("progress run_id="));

        List<String> suppressed = runVerbose(dir.resolve("without.jsonl"), false);
        assertThat(suppressed)
                .as("--no-progress suppresses progress, not merely the display: no record either")
                .noneMatch(line -> line.contains("progress run_id="));
        assertThat(suppressed).as("the rest of the -v log is untouched")
                .anyMatch(line -> line.contains("list_run_start"));
    }

    /**
     * One {@code -v} run at INFO, with the log level restored so no other test inherits it. The
     * cadence is left at its default: an explicit {@code --progress-interval} is itself an opt-in to
     * the display, and this is the case where the structured record is the surface. The default
     * activation delay still puts a tick inside this run.
     */
    private static List<String> runVerbose(Path out, Boolean progress) throws Exception {
        ListCommand cmd = slowListCommand(out);
        cmd.global.verbosity = new boolean[] {true};
        cmd.output.progress = progress;
        cmd.terminalOverride = new TerminalCapabilities(fd -> false);
        cmd.envOverride = key -> null;

        CliLogging.configure(1, 0);
        try {
            return runCapturingStderr(cmd);
        } finally {
            CliLogging.configure(0, 0);
        }
    }

    /** A progress frame: the indented phase-led record, as distinct from the summary block. */
    private static boolean isFrame(String line) {
        return line.startsWith("  seeding · ") || line.startsWith("  listing · ")
                || line.startsWith("  merging · ") || line.startsWith("  writing · ");
    }

    private static int indexOfSummary(List<String> stderr) {
        for (int i = 0; i < stderr.size(); i++) {
            if (stderr.get(i).contains(" objects in ")) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> runCapturingStderr(ListCommand cmd) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        } finally {
            System.setErr(original);
        }
        return captured.toString(StandardCharsets.UTF_8).lines().toList();
    }

    /**
     * How long the run is held open, on its FIRST fetch only. What earns a frame is the run's
     * elapsed wall clock, not the number of slow pages: the reporter's first frame lands at
     * {@code min(activation delay, cadence)} — 1s under the explicit {@link #FASTEST_CADENCE}, 2s
     * for {@link #runVerbose}, which deliberately keeps the default cadence — so one hold past the
     * later of those two is what every case here needs. Sleeping on every page instead multiplies
     * that by the fetch count (~10 for this keyspace, seed probes included) and buys nothing.
     */
    private static final long FIRST_PAGE_HOLD_MS = 3_500L;

    /** A checkpoint-free run over a mock keyspace, slowed so at least one frame is due. */
    private static ListCommand slowListCommand(Path out) {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)))
                .interceptor((req, idx, page) -> {
                    if (idx == 0) {
                        TimeUnit.MILLISECONDS.sleep(FIRST_PAGE_HOLD_MS);
                    }
                    return page;
                })
                .build();
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = "none";
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = out.toString();
        cmd.fetcherOverride = fetcher;
        return cmd;
    }
}
