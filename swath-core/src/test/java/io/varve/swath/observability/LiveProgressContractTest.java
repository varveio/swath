/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The live-progress contract: ONE lifecycle spanning every phase, a phase-shaped event, a neutral
 * sink the presentation layer owns, an activation delay that is not the cadence, and no work at all
 * when nothing is rendering.
 */
final class LiveProgressContractTest {

    /** Collects events; {@code enabled} flips exactly as a real display's would. */
    private static final class RecordingSink implements ProgressSink {

        private final ConcurrentLinkedQueue<ProgressEvent> events = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean enabled = new AtomicBoolean(true);

        @Override
        public void accept(ProgressEvent event) {
            events.add(event);
        }

        @Override
        public boolean isEnabled() {
            return enabled.get();
        }
    }

    @Test
    void seedingHasAWireCodeOfItsOwnAndRenumbersNothing() {
        assertThat(Phase.LISTING.code()).isZero();
        assertThat(Phase.MERGING.code()).isEqualTo(1);
        assertThat(Phase.WRITING.code()).isEqualTo(2);
        assertThat(Phase.COMPLETE.code()).isEqualTo(3);
        assertThat(Phase.SEEDING.code()).isEqualTo(4);
        assertThat(Phase.STARTING.code()).isEqualTo(5);
        assertThat(List.of(Phase.values()).stream().map(Phase::code).distinct()).hasSize(Phase.values().length);
    }

    @Test
    void seedingEventCarriesProbesAgainstTheirBudgetAndTheAgeOfTheLastOne() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordSeedProbeBudget(64L);
        metrics.setPhase(Phase.SEEDING);
        metrics.recordSeedProbe();
        metrics.recordSeedProbe();
        Thread.sleep(5);

        ProgressEvent event = metrics.progressEvent(Duration.ofSeconds(3));

        assertThat(event.phase()).isEqualTo(Phase.SEEDING);
        assertThat(event.seeding().probesCompleted()).isEqualTo(2L);
        assertThat(event.seeding().probeBudget()).isEqualTo(64L);
        assertThat(event.seeding().sinceLastProbe()).isPositive();
        // A bounded probe budget IS an exact denominator, so seeding gets a completion figure even
        // though listing never does.
        assertThat(event.completion())
                .isEqualTo(new ProgressEvent.Completion(2L, 64L, ProgressEvent.Unit.PROBES));
        assertThat(event.completion().fraction()).isCloseTo(2 / 64.0, within(1e-9));
        assertThat(event.listing()).isNull();
    }

    @Test
    void mergingEventCountsTheRowsTheMergeMovedAgainstTheRowsItWasHanded() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        metrics.recordEntriesEmitted(1_000);   // listing's progress units must not count as merged rows
        metrics.recordSortStaged(4L, 1_000L);
        metrics.setPhase(Phase.MERGING);
        metrics.recordProgress(250);
        metrics.startFinalMergePass(true);   // the final pass: the one with an exact denominator
        metrics.recordProgress(250);

        ProgressEvent event = metrics.progressEvent(Duration.ofSeconds(9));

        assertThat(event.merging().sessionRowsMerged()).isEqualTo(500L);
        assertThat(event.merging().stagedRows()).isEqualTo(1_000L);
        assertThat(event.merging().segments()).isEqualTo(4L);
        assertThat(event.completion())
                .isEqualTo(new ProgressEvent.Completion(250L, 1_000L, ProgressEvent.Unit.ROWS));
        assertThat(event.listing()).isNull();
    }

    @Test
    void aCascadingMergeReportsWorkAndNoPercentageUntilTheFinalPass() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordSortStaged(5L, 5L);
        metrics.setPhase(Phase.MERGING);
        metrics.recordProgress(5);   // cascade pass 1 rewrites every staged row
        metrics.recordProgress(5);   // cascade pass 2 rewrites them again

        ProgressEvent midCascade = metrics.progressEvent(Duration.ofSeconds(3));
        assertThat(midCascade.merging().sessionRowsMerged())
                .as("cumulative merge work legitimately exceeds the staged rows")
                .isEqualTo(10L);
        assertThat(midCascade.completion())
                .as("a cascade cannot report a fraction: 10/5 would show a finished merge")
                .isNull();

        metrics.startFinalMergePass(true);
        metrics.recordProgress(2);
        assertThat(metrics.progressEvent(Duration.ofSeconds(4)).completion())
                .isEqualTo(new ProgressEvent.Completion(2L, 5L, ProgressEvent.Unit.ROWS));
    }

    @Test
    void aRunThatHasSetNoPhaseYetSaysSoInsteadOfFabricatingListing() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        ProgressEvent event = metrics.progressEvent(Duration.ofSeconds(6));

        assertThat(event.phase()).as("checkpoint recovery is not listing").isEqualTo(Phase.STARTING);
        assertThat(event.listing()).isNull();
        assertThat(event.phaseElapsed())
                .as("the phase clock starts with the run, not at the nanoTime origin")
                .isLessThan(Duration.ofMinutes(1));
    }

    @Test
    void anUnknownProviderHasNoCostFigureForAnySinkToRender() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordApiCall();
        metrics.setListCostKnown(false);

        assertThat(metrics.progressEvent(Duration.ofSeconds(2)).estimatedCostUsd())
                .as("--endpoint-url: an AWS-priced figure is a guess, so no figure exists")
                .isNull();

        metrics.setListCostKnown(true);
        assertThat(metrics.progressEvent(Duration.ofSeconds(2)).estimatedCostUsd()).isNotNull();
    }

    @Test
    void theTerminalSummaryEndsProgressForWhicheverSinkIsInstalled() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        RecordingSink sink = new RecordingSink();
        metrics.setProgressSink(sink);
        metrics.emitProgress(Duration.ofSeconds(1));

        Duration ran = Duration.ofSeconds(30);
        metrics.emitSummary(metrics.summary(ran, "WORK_STEALING", 0L, 0L), metrics.diagnostics(ran),
                new JsonRunSummaryWriter.TerminalStatus(StopReason.COMPLETED));
        metrics.emitProgress(Duration.ofSeconds(31));
        metrics.setProgressSink(sink);   // even a re-install cannot revive it
        metrics.emitProgress(Duration.ofSeconds(32));

        assertThat(sink.events).as("the summary is the run's last word, whichever sink renders progress")
                .hasSize(1);
    }

    @Test
    void resumedRunSeparatesSessionWorkFromRecoveredWork() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        metrics.recordEntriesEmitted(120);
        metrics.recordRecoveredObjects(5_000_000);   // the pre-crash backfill, landing in one lump

        ProgressEvent.Listing listing = metrics.progressEvent(Duration.ofSeconds(4)).listing();

        assertThat(listing.sessionObjects()).isEqualTo(120L);
        assertThat(listing.recoveredObjects()).isEqualTo(5_000_000L);
        // The summary's objects field still counts the whole run — the split is a display concern.
        assertThat(metrics.objectsEmitted()).isEqualTo(5_000_120L);
    }

    /**
     * The counts and the rates must split the same way. Session elapsed starts with THIS process, so
     * a rate whose numerator still carried the recovered rows reported a resumed run's whole
     * previous attempt as this session's throughput — 4 billion objects over the first few seconds.
     */
    @Test
    void aResumedRunsRatesMeasureSessionWorkOverSessionTime() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        metrics.recordRecoveredObjects(4_000_000_000L);   // the pre-crash rows, known before listing
        metrics.recordEntriesEmitted(200);

        ProgressEvent.Listing first = metrics.progressEvent(Duration.ofSeconds(10)).listing();

        assertThat(first.averageObjectsPerSecond())
                .as("200 objects in 10s, not 4,000,000,200")
                .isCloseTo(20.0, within(1e-9));
        assertThat(first.liveObjectsPerSecond())
                .as("the first tick has no window yet and falls back to the same session average")
                .isCloseTo(20.0, within(1e-9));

        metrics.recordEntriesEmitted(300);
        ProgressEvent.Listing second = metrics.progressEvent(Duration.ofSeconds(20)).listing();

        assertThat(second.averageObjectsPerSecond()).isCloseTo(25.0, within(1e-9));
        assertThat(second.liveObjectsPerSecond()).isCloseTo(30.0, within(1e-9));
    }

    /**
     * The reattach backfill lands in one lump mid-run, so the windowed rate must not see it as a
     * window's worth of work either — the same numerator, sampled a tick apart.
     */
    @Test
    void aBackfillLandingBetweenTicksIsNotAWindowOfThroughput() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        metrics.recordEntriesEmitted(100);
        metrics.progressEvent(Duration.ofSeconds(10));   // the tick that sets the window baseline

        metrics.recordRecoveredObjects(4_000_000_000L);
        metrics.recordEntriesEmitted(100);
        ProgressEvent.Listing afterBackfill = metrics.progressEvent(Duration.ofSeconds(20)).listing();

        assertThat(afterBackfill.liveObjectsPerSecond()).isCloseTo(10.0, within(1e-9));
        assertThat(afterBackfill.averageObjectsPerSecond()).isCloseTo(10.0, within(1e-9));
        assertThat(afterBackfill.recoveredObjects()).isEqualTo(4_000_000_000L);
    }

    @Test
    void phaseElapsedRestartsPerPhaseWhileSessionElapsedNeverDoes() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.SEEDING);
        Thread.sleep(20);
        metrics.setPhase(Phase.LISTING);

        ProgressEvent event = metrics.progressEvent(Duration.ofMinutes(7));

        assertThat(event.sessionElapsed()).isEqualTo(Duration.ofMinutes(7));
        assertThat(event.phaseElapsed()).isLessThan(Duration.ofMinutes(7));
    }

    @Test
    @Timeout(30)
    void aDisabledSinkCostsNothing_noEventIsEverBuilt() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        RecordingSink sink = new RecordingSink();
        sink.enabled.set(false);
        metrics.setProgressSink(sink);
        metrics.recordEntriesEmitted(100);

        try (RunProgressReporter ignored = RunProgressReporter.start(metrics, Duration.ofMillis(5))) {
            Thread.sleep(60);
        }

        assertThat(sink.events).isEmpty();
        // The proof that no snapshot was built behind the sink's back: the windowed rate's baseline
        // is still unset, so the first event ever built falls back to the cumulative average
        // (100 objects / 10s) instead of reporting a window against a tick nobody rendered.
        assertThat(metrics.progressEvent(Duration.ofSeconds(10)).listing().liveObjectsPerSecond())
                .isCloseTo(10.0, within(1e-9));
    }

    @Test
    @Timeout(30)
    void aNestedStartJoinsTheSessionReporterInsteadOfRunningASecondOne() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        RecordingSink sink = new RecordingSink();
        metrics.setProgressSink(sink);

        try (RunProgressReporter session = RunProgressReporter.start(metrics, Duration.ofMillis(5))) {
            RunProgressReporter nested = RunProgressReporter.start(metrics, Duration.ofMillis(5));
            assertThat(executorOf(nested)).as("a nested start must not schedule a ticker").isNull();
            nested.close();   // the inner scope ending must NOT silence the session
            int before = sink.events.size();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (sink.events.size() <= before && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(sink.events.size()).isGreaterThan(before);
        }

        int afterClose = sink.events.size();
        Thread.sleep(40);
        assertThat(sink.events.size())
                .as("the owning close() stops the ticker")
                .isEqualTo(afterClose);
    }

    @Test
    @Timeout(30)
    void theFirstFrameArrivesAfterTheActivationDelayNotAfterAWholeCadence() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setPhase(Phase.LISTING);
        RecordingSink sink = new RecordingSink();
        metrics.setProgressSink(sink);

        // The motivating failure: a 22-second run at the 30s default cadence showed nothing at all,
        // because the first tick was scheduled one whole period out.
        long startedNs = System.nanoTime();
        try (RunProgressReporter ignored = RunProgressReporter.start(metrics, Duration.ofSeconds(30))) {
            long deadline = startedNs + TimeUnit.SECONDS.toNanos(20);
            while (sink.events.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
        }

        assertThat(sink.events).isNotEmpty();
        assertThat(Duration.ofNanos(System.nanoTime() - startedNs)).isLessThan(Duration.ofSeconds(20));
    }

    private static java.util.concurrent.ScheduledExecutorService executorOf(RunProgressReporter reporter)
            throws Exception {
        Field f = RunProgressReporter.class.getDeclaredField("executor");
        f.setAccessible(true);
        return (java.util.concurrent.ScheduledExecutorService) f.get(reporter);
    }
}
