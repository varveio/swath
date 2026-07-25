/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.JsonRunSummaryWriter.TerminalStatus;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The live progress record an operator meets on stderr: one plain line per tick, shaped by the
 * phase, and the gate that decides whether it appears at all.
 *
 * <p>The frames are driven straight into the sink from constructed {@link ProgressEvent}s — the
 * same objects the run's reporter hands it — so every assertion here is deterministic: no clock,
 * no scheduler, and an output sink the test owns.
 */
final class ProgressDisplayTest {

    private static final String RATE_LABEL = " (est. @ $" + RunMetrics.LIST_COST_PER_1K_USD + "/1k LIST)";

    // ---- the shape of each phase -------------------------------------

    @Test
    void seedingRendersProbesAgainstTheBudgetAndTheAgeOfTheLastProbe() {
        String line = ProgressDisplay.line(seeding(12L, 64L, Duration.ofMillis(3_100)), true);

        assertThat(line).isEqualTo("seeding · 12/64 probes (19%) · last probe 3.1s ago"
                + " · 21.7s elapsed · 64 API calls · <$0.001" + RATE_LABEL);
    }

    @Test
    void listingRendersCountersAndRatesWithNoBarPercentageOrEta() {
        String line = ProgressDisplay.line(listing(4_231_000L, 0L), true);

        assertThat(line).isEqualTo("listing · 4,231,000 objects · 128,000 keys/s (avg 96,000)"
                + " · 4,231 pages · 512 in flight · 1m12s elapsed · 8,900 API calls · ~$0.045"
                + RATE_LABEL);
        assertThat(line).as("an unsorted scan has no honest denominator: no bar, no %, no ETA")
                .doesNotContain("%").doesNotContain("eta");
    }

    @Test
    void listingKeepsRecoveredRowsApartFromTheWorkThisSessionDid() {
        String line = ProgressDisplay.line(listing(1_000L, 4_000_000_000L), true);

        assertThat(line).as("a resumed run neither shows a zero it did not earn nor jumps by billions")
                .contains("1,000 objects (+4,000,000,000 recovered)");
    }

    @Test
    void mergingRendersRowsMergedAgainstTheStagedTotal() {
        String line = ProgressDisplay.line(merging(3_100_000L, 12_000_000L), true);

        assertThat(line).isEqualTo("merging · 3,100,000/12,000,000 rows (26%) · 24 segments"
                + " · 2m05s elapsed · 8,900 API calls · ~$0.045" + RATE_LABEL);
    }

    @Test
    void everyFrameIsOnePlainLineWithNoControlCharacters() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        StderrCoordinator coordinator = coordinator(captured);
        try (ProgressDisplay display = new ProgressDisplay(coordinator, true)) {
            display.accept(seeding(1L, 64L, Duration.ofMillis(200)));
            display.accept(listing(10L, 0L));
            display.accept(merging(1L, 2L));
        }

        String output = captured.toString(StandardCharsets.UTF_8);
        assertThat(output.lines()).hasSize(3).allMatch(line -> line.startsWith("  "));
        assertThat(output).as("no redraw, no ANSI, no key bytes: a redirected stderr gets plain text")
                .doesNotContain("\r").doesNotContain("\u001B")
                .matches("[^\\p{Cntrl}\\n]*(\\n[^\\p{Cntrl}\\n]*)*");
    }

    // ---- cost honesty ------------------------------------------------

    @Test
    void anUnknownProviderWithholdsTheDollarExactlyAsTheSummaryDoes() {
        String line = ProgressDisplay.line(listing(10L, 0L), false);

        assertThat(line).as("--endpoint-url: the LIST price is a guess, so the live line withholds it")
                .doesNotContain("$");
    }

    @Test
    void aRunThatHasIssuedNoListCallShowsNoCost() {
        ProgressEvent noCalls = new ProgressEvent(Phase.MERGING, 7L, "WORK_STEALING",
                Duration.ofSeconds(20), Duration.ofSeconds(20), 0L, 0.0, 0L,
                new ProgressEvent.Completion(1L, 2L, ProgressEvent.Unit.ROWS), null, null,
                new ProgressEvent.Merging(1L, 2L, 3L, 0L));

        assertThat(ProgressDisplay.line(noCalls, true))
                .as("a merge-only resume fetches nothing: '~$0.000' would be noise dressed as an estimate")
                .doesNotContain("$");
    }

    // ---- the gate ----------------------------------------------------

    @Test
    void autoShowsProgressOnATerminalOnly() {
        assertThat(ProgressDisplay.shouldDisplay(auto(true))).isTrue();
        assertThat(ProgressDisplay.shouldDisplay(auto(false)))
                .as("progress repeats, so off a terminal it appears only when asked for")
                .isFalse();
    }

    @Test
    void autoStandsDownUnderQuietAndUnderVerbose() {
        assertThat(ProgressDisplay.shouldDisplay(
                new ProgressDisplay.Preferences(null, true, false, false, true))).isFalse();
        assertThat(ProgressDisplay.shouldDisplay(
                new ProgressDisplay.Preferences(null, false, true, false, true)))
                .as("-v makes the structured progress log record the run's progress surface")
                .isFalse();
    }

    @Test
    void anExplicitProgressFlagBeatsQuietAndANonTerminalStderr() {
        assertThat(ProgressDisplay.shouldDisplay(
                new ProgressDisplay.Preferences(true, true, true, false, false))).isTrue();
    }

    @Test
    void noProgressSuppressesItEverywhere() {
        assertThat(ProgressDisplay.shouldDisplay(
                new ProgressDisplay.Preferences(false, false, false, true, true))).isFalse();
    }

    @Test
    void anExplicitIntervalIsItselfAnOptIn() {
        assertThat(ProgressDisplay.shouldDisplay(
                new ProgressDisplay.Preferences(null, false, false, true, false)))
                .as("asking for a cadence asks for the thing that has a cadence")
                .isTrue();
    }

    // ---- lifecycle ---------------------------------------------------

    @Test
    void nothingIsWrittenAfterTheDisplayIsClosedAndClosingIsIdempotent() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        ProgressDisplay display = new ProgressDisplay(coordinator(captured), true);
        display.accept(listing(10L, 0L));

        display.close();
        display.close();
        display.accept(listing(20L, 0L));

        assertThat(display.isEnabled()).isFalse();
        assertThat(captured.toString(StandardCharsets.UTF_8).lines())
                .as("a tick that was still formatting when the run ended is dropped")
                .hasSize(1);
    }

    @Test
    void theSummaryBlockPermanentlyEndsProgressBeforeItWritesALine() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        StderrCoordinator coordinator = coordinator(captured);
        ProgressDisplay display = new ProgressDisplay(coordinator, true);
        display.accept(listing(10L, 0L));
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.markRunStarted();
        metrics.recordEntriesEmitted(10L);
        Duration ran = Duration.ofSeconds(30);

        new SummaryRenderer(coordinator, () -> new SummaryRenderer.Preferences(
                true, false, false, true, null, false))
                .accept(metrics.summary(ran, "WORK_STEALING", 0L, 0L), metrics.diagnostics(ran),
                        new TerminalStatus(StopReason.COMPLETED));
        display.accept(listing(20L, 0L));

        List<String> lines = captured.toString(StandardCharsets.UTF_8).lines().toList();
        assertThat(lines.getFirst()).startsWith("  listing · 10 objects");
        assertThat(lines.get(1)).as("the block is the run's last word on this fd")
                .startsWith("  10 objects in 30.0s");
        assertThat(lines).noneMatch(line -> line.contains("20 objects"));
    }

    @Test
    void aBrokenStderrSilencesProgressWithoutThrowing() {
        PrintStream broken = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("Broken pipe");
            }
        }, true, StandardCharsets.UTF_8);
        ProgressDisplay display = new ProgressDisplay(new StderrCoordinator(() -> broken), true);

        display.accept(listing(10L, 0L));   // must not throw: the run's disposition is not stderr's

        assertThat(display.isEnabled())
                .as("EPIPE disables further progress and changes nothing else about the run")
                .isFalse();
    }

    // ---- fixtures ----------------------------------------------------

    private static ProgressDisplay.Preferences auto(boolean stderrIsTerminal) {
        return new ProgressDisplay.Preferences(null, false, false, false, stderrIsTerminal);
    }

    private static StderrCoordinator coordinator(ByteArrayOutputStream captured) {
        PrintStream stream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        return new StderrCoordinator(() -> stream);
    }

    private static ProgressEvent seeding(long probes, long budget, Duration sinceLastProbe) {
        return new ProgressEvent(Phase.SEEDING, 7L, "WORK_STEALING",
                Duration.ofMillis(21_700), Duration.ofMillis(21_700), 64L,
                RunMetrics.LIST_COST_PER_1K_USD * 64 / 1_000.0, 0L,
                new ProgressEvent.Completion(probes, budget, ProgressEvent.Unit.PROBES),
                new ProgressEvent.Seeding(probes, budget, sinceLastProbe), null, null);
    }

    private static ProgressEvent listing(long sessionObjects, long recoveredObjects) {
        return new ProgressEvent(Phase.LISTING, 7L, "WORK_STEALING",
                Duration.ofSeconds(72), Duration.ofSeconds(50), 8_900L, 0.0445, 3L, null, null,
                new ProgressEvent.Listing(sessionObjects, recoveredObjects, 128_000.0, 96_000.0,
                        4_231L, 512L, 64L, 900L, 30L),
                null);
    }

    private static ProgressEvent merging(long rowsMerged, long stagedRows) {
        return new ProgressEvent(Phase.MERGING, 7L, "WORK_STEALING",
                Duration.ofSeconds(125), Duration.ofSeconds(20), 8_900L, 0.0445, 3L,
                new ProgressEvent.Completion(rowsMerged, stagedRows, ProgressEvent.Unit.ROWS), null,
                null, new ProgressEvent.Merging(rowsMerged, stagedRows, 24L, 0L));
    }
}
