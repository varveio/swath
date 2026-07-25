/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.observability.JsonRunSummaryWriter.TerminalStatus;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.StopReason;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The end-of-run block's gate and content, exercised directly on real {@link RunSummary}/{@code
 * RunDiagnostics} objects built by {@link RunMetrics} — the same objects the run hands the sink.
 */
final class SummaryRendererTest {

    private static final String WORK_STEALING = "WORK_STEALING";
    private static final Duration SHORT = Duration.ofMillis(200);
    private static final Duration LONG = Duration.ofSeconds(30);

    private static final TerminalStatus COMPLETED = new TerminalStatus(StopReason.COMPLETED);
    private static final TerminalStatus INTERRUPTED = new TerminalStatus(StopReason.SIGNAL);
    private static final TerminalStatus BROKEN_PIPE = new TerminalStatus(null);

    /** Auto (flag unset), not quiet, stdout destination, AWS pricing, nothing resumable. */
    private static final SummaryRenderer.Preferences AUTO =
            new SummaryRenderer.Preferences(null, false, false, true, null);

    // ---- the gate ----------------------------------------------------

    @Test
    void subThresholdRunStaysSilent() {
        assertThat(SummaryRenderer.shouldRender(AUTO, summary(SHORT), COMPLETED)).isFalse();
    }

    @Test
    void runLongEnoughToWaitOnEarnsASummary() {
        assertThat(SummaryRenderer.shouldRender(AUTO, summary(LONG), COMPLETED)).isTrue();
    }

    @Test
    void durableOutputEarnsASummaryRegardlessOfDuration() {
        SummaryRenderer.Preferences toDisk =
                new SummaryRenderer.Preferences(null, false, true, true, null);
        assertThat(SummaryRenderer.shouldRender(toDisk, summary(SHORT, 3L, 4096L), COMPLETED)).isTrue();
    }

    @Test
    void interruptedRunEarnsASummaryRegardlessOfDuration() {
        assertThat(SummaryRenderer.shouldRender(AUTO, summary(SHORT), INTERRUPTED)).isTrue();
    }

    @Test
    void quietSuppressesTheAutoSummaryButNotAnExplicitOne() {
        SummaryRenderer.Preferences quiet =
                new SummaryRenderer.Preferences(null, true, false, true, null);
        SummaryRenderer.Preferences quietWithStats =
                new SummaryRenderer.Preferences(true, true, false, true, null);
        assertThat(SummaryRenderer.shouldRender(quiet, summary(LONG), COMPLETED)).isFalse();
        assertThat(SummaryRenderer.shouldRender(quietWithStats, summary(LONG), COMPLETED)).isTrue();
    }

    @Test
    void statsForcesASummaryOnASubThresholdRun() {
        SummaryRenderer.Preferences forced =
                new SummaryRenderer.Preferences(true, false, false, true, null);
        assertThat(SummaryRenderer.shouldRender(forced, summary(SHORT), COMPLETED)).isTrue();
    }

    @Test
    void noStatsSuppressesEverywhere() {
        SummaryRenderer.Preferences never =
                new SummaryRenderer.Preferences(false, false, true, true, "./out");
        assertThat(SummaryRenderer.shouldRender(never, summary(LONG, 3L, 4096L), COMPLETED)).isFalse();
        assertThat(SummaryRenderer.shouldRender(never, summary(LONG), INTERRUPTED)).isFalse();
    }

    @Test
    void brokenPipeSuppressesTheAutoSummaryEvenOnALongRun() {
        assertThat(SummaryRenderer.shouldRender(AUTO, summary(LONG), BROKEN_PIPE)).isFalse();
    }

    @Test
    void statsBypassesEvenTheBrokenPipeGate() {
        SummaryRenderer.Preferences forced =
                new SummaryRenderer.Preferences(true, false, false, true, null);
        assertThat(SummaryRenderer.shouldRender(forced, summary(LONG), BROKEN_PIPE))
                .as("an explicit --stats bypasses EVERY gate, the broken-pipe one included: the "
                        + "flag check must stay ahead of all of them")
                .isTrue();
    }

    @Test
    void theAutoThresholdIsExclusiveSoAnExactlyOnTheBoundaryRunStaysSilent() {
        assertThat(SummaryRenderer.shouldRender(AUTO, summary(SummaryRenderer.AUTO_MIN_ELAPSED), COMPLETED))
                .isFalse();
        assertThat(SummaryRenderer.shouldRender(
                AUTO, summary(SummaryRenderer.AUTO_MIN_ELAPSED.plusMillis(1)), COMPLETED)).isTrue();
    }

    @Test
    void aDurableDestinationThatProducedNothingDoesNotEarnASummary() {
        SummaryRenderer.Preferences toDisk =
                new SummaryRenderer.Preferences(null, false, true, true, null);
        assertThat(SummaryRenderer.shouldRender(toDisk, summary(SHORT), COMPLETED))
                .as("the reason to report is output that exists, not a destination that could "
                        + "have held some")
                .isFalse();
    }

    // ---- content -----------------------------------------------------

    @Test
    void cleanRunRendersHeadlineCostAndNoFaultsLine() {
        List<String> lines = render(AUTO, LONG, metrics -> metrics.recordEntriesEmitted(1_500L), COMPLETED);

        assertThat(lines).noneMatch(line -> line.startsWith("INCOMPLETE"));
        assertThat(lines.get(0)).contains("1,500 objects in 30.0s").contains("keys/s");
        assertThat(lines.get(1)).contains("API calls").contains("per 1k objects")
                .contains("in flight avg").contains("peak");
        assertThat(lines).as("a clean run's faults line is absent, so its presence is the signal")
                .noneMatch(line -> line.startsWith("throttled"));
    }

    @Test
    void faultsLineRendersEveryCounterOnceAnyIsNonZero() {
        List<String> lines = render(AUTO, LONG, metrics -> {
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
            metrics.recordThrottleEvent(ThrottleType.SERVER_5XX);
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }, COMPLETED);

        assertThat(lines).as("real 503/5xx backpressure and retried client-side transients are "
                        + "deliberately distinct counts and must not be folded together")
                .anyMatch(line -> line.equals("throttled 2 · retried 1 · errors 0"));
    }

    @Test
    void costStatesTheRateItAssumed() {
        List<String> lines = render(AUTO, LONG, metrics -> {
            metrics.recordEntriesEmitted(1_000L);
            metrics.recordApiCall();
        }, COMPLETED);

        assertThat(lines).anyMatch(line -> line.contains("(est. @ $0.005/1k LIST)"));
    }

    @Test
    void unknownProviderWithholdsTheDollarFigureEntirely() {
        SummaryRenderer.Preferences selfHosted =
                new SummaryRenderer.Preferences(null, false, false, false, null);

        List<String> lines = render(selfHosted, LONG, metrics -> {
            metrics.recordEntriesEmitted(1_000L);
            metrics.recordApiCall();
        }, COMPLETED);

        assertThat(lines).as("--endpoint-url means the provider's LIST pricing is unknown; any $ "
                        + "would be a guess").noneMatch(line -> line.contains("$"));
    }

    @Test
    void incompleteRunIsMarkedAndOffersTheResumeThatWillWork() {
        SummaryRenderer.Preferences resumable =
                new SummaryRenderer.Preferences(null, false, true, true, "./out");

        List<String> lines = render(resumable, LONG, metrics -> { }, INTERRUPTED);

        assertThat(lines.getFirst()).isEqualTo("INCOMPLETE (signal) — resume: swath resume ./out");
    }

    @Test
    void incompleteRunWithNothingResumableOffersNoResume() {
        List<String> lines = render(AUTO, LONG, metrics -> { }, INTERRUPTED);

        assertThat(lines.getFirst()).isEqualTo("INCOMPLETE (signal)");
    }

    @Test
    void forcedBrokenPipeSummaryIsWordedNeutrallyAndCarriesNoMarker() {
        SummaryRenderer.Preferences forced =
                new SummaryRenderer.Preferences(true, false, false, true, "./out");

        List<String> lines = render(forced, LONG, metrics -> { }, BROKEN_PIPE);

        assertThat(lines.getFirst()).isEqualTo("stopped early — downstream closed");
        assertThat(lines).noneMatch(line -> line.contains("INCOMPLETE"));
    }

    @Test
    void aCrashIsMarkedButNeverInvitesAResumeThatWouldFailAgain() {
        SummaryRenderer.Preferences resumable =
                new SummaryRenderer.Preferences(null, false, true, true, "./out");

        List<String> lines = render(resumable, LONG, metrics -> { }, new TerminalStatus(StopReason.CRASH));

        assertThat(lines.getFirst())
                .as("a crash is deterministic: a resume would hit the same failure again")
                .isEqualTo("INCOMPLETE (crash)");
    }

    @Test
    void aRunThatIssuedNoListCallsShowsNoCostLineAtAll() {
        List<String> lines = render(AUTO, LONG, metrics -> { }, COMPLETED);

        assertThat(lines).as("a merge-only resume fetches nothing, and an exact zero rendered as "
                        + "an estimate is noise")
                .noneMatch(line -> line.contains("$"));
    }

    @Test
    void printedBlockIsIndentedPlainTextAndNothingReachesStdout() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.markRunStarted();
        metrics.recordEntriesEmitted(10L);

        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
        try {
            new SummaryRenderer(new PrintStream(captured, true, StandardCharsets.UTF_8), () -> AUTO)
                    .accept(metrics.summary(LONG, WORK_STEALING, 0L, 0L), metrics.diagnostics(LONG), COMPLETED);
        } finally {
            System.setOut(originalOut);
        }

        assertThat(captured.toString(StandardCharsets.UTF_8))
                .startsWith("  10 objects in 30.0s").doesNotContain("[");
        assertThat(stdout.toString(StandardCharsets.UTF_8))
                .as("stdout is data, always: the block never goes near it").isEmpty();
    }

    private static List<String> render(SummaryRenderer.Preferences prefs, Duration duration,
            java.util.function.Consumer<RunMetrics> arrange, TerminalStatus status) {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.markRunStarted();
        arrange.accept(metrics);
        return SummaryRenderer.lines(prefs, metrics.summary(duration, WORK_STEALING, 0L, 0L),
                metrics.diagnostics(duration), status);
    }

    private static RunSummary summary(Duration duration) {
        return summary(duration, 0L, 0L);
    }

    private static RunSummary summary(Duration duration, long outputFiles, long bytes) {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.markRunStarted();
        return metrics.summary(duration, WORK_STEALING, outputFiles, bytes);
    }
}
