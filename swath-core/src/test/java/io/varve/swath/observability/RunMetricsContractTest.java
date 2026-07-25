/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

final class RunMetricsContractTest {

    @Test
    void apiLatencyTimerPublishesAP90PercentileSnapshot() {
        // §3.7: publishPercentiles(0.5, 0.90, 0.99) — a p90 series alongside the existing p50/p99.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        for (int i = 0; i < 50; i++) {
            metrics.recordS3Latency(metrics.startS3PageTimer());
        }

        var snapshot = registry.find("swath.api.latency").timer().takeSnapshot();
        boolean hasP90 = false;
        for (var vp : snapshot.percentileValues()) {
            if (Math.abs(vp.percentile() - 0.90) < 1e-9) {
                hasP90 = true;
            }
        }
        assertThat(hasP90).isTrue();
    }

    @Test
    void nonTtyProgressIntervalIsThirtySecondsAndSnapshotCarriesContractFields() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setRunId(42L);
        metrics.setStrategy("WORK_STEALING");
        metrics.setPrefix("p/".getBytes(StandardCharsets.UTF_8));
        metrics.setCursor("p/k1".getBytes(StandardCharsets.UTF_8));
        metrics.setConcurrencyTarget(7);
        metrics.incrementInFlight();
        metrics.recordApiCall();
        metrics.recordPage();
        metrics.recordEntriesEmitted(3);

        RunMetrics.ProgressSnapshot snapshot = metrics.snapshot(Duration.ofSeconds(2));

        assertThat(RunProgressReporter.nonTtyInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(snapshot.strategy()).isEqualTo("WORK_STEALING");
        assertThat(snapshot.inFlight()).isEqualTo(1L);
        assertThat(snapshot.concurrencyTarget()).isEqualTo(7L);
        assertThat(snapshot.liveKeysPerSecond()).isGreaterThan(0.0);
        assertThat(snapshot.keys()).isEqualTo(3L);
        assertThat(snapshot.apiCalls()).isEqualTo(1L);
        assertThat(snapshot.estimatedCostUsd()).isCloseTo(0.000005, within(0.000000001));
        assertThat(snapshot.oldestPendingRange()).contains("p/k1");
        assertThat(snapshot.eta()).isEqualTo("unknown");
    }

    @Test
    void summaryCarriesCostAndOutputFields() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setRunId(99L);
        metrics.setStrategy("SEQUENTIAL");
        metrics.recordApiCall();
        metrics.recordApiCall();
        metrics.recordPage();
        metrics.recordEntriesEmitted(11);

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "SEQUENTIAL", 2L, 1234L);

        assertThat(summary.objects()).isEqualTo(11L);
        assertThat(summary.duration()).isEqualTo(Duration.ofMillis(500));
        assertThat(summary.strategy()).isEqualTo("SEQUENTIAL");
        assertThat(summary.apiCalls()).isEqualTo(2L);
        assertThat(summary.costUsd()).isCloseTo(0.00001, within(0.000000001));
        assertThat(summary.outputFiles()).isEqualTo(2L);
        assertThat(summary.compressedBytes()).isEqualTo(1234L);
    }

    @Test
    void summaryCarriesEfficiencyAndResourceFields() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        for (int i = 0; i < 4; i++) {
            metrics.recordApiCall();
        }
        metrics.recordEntriesEmitted(2000);

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        // api_calls * 1000 / objects = 4 * 1000 / 2000 = 2.0
        assertThat(summary.apiCallsPer1kObjects()).isCloseTo(2.0, within(1e-9));

        // Resource probes: -1 (unavailable) or a non-negative real reading.
        assertThat(summary.peakRssBytes()).satisfiesAnyOf(
                v -> assertThat(v).isEqualTo(-1L),
                v -> assertThat(v).isPositive());
        assertThat(summary.peakHeapBytes()).satisfiesAnyOf(
                v -> assertThat(v).isEqualTo(-1L),
                v -> assertThat(v).isPositive());
        assertThat(summary.cpuSeconds()).satisfiesAnyOf(
                v -> assertThat(v).isEqualTo(-1.0),
                v -> assertThat(v).isGreaterThanOrEqualTo(0.0));
        // When CPU time is available, efficiency is cpu/wall and must be non-negative.
        if (summary.cpuSeconds() >= 0.0) {
            assertThat(summary.cpuEfficiency()).isGreaterThanOrEqualTo(0.0);
        } else {
            assertThat(summary.cpuEfficiency()).isEqualTo(-1.0);
        }
    }

    @Test
    void overfetchRatioIsFetchedKeysOverEmittedEntries() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        // Two fetched pages of 1000 raw keys each (2000 fetched) against 1000 emitted downstream
        // (e.g. half filtered out) -> overfetch_ratio == 2.0.
        metrics.recordListingPageShape(1000, true, 1000);
        metrics.recordListingPageShape(1000, false, 1000);
        metrics.recordEntriesEmitted(1000);

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        assertThat(summary.overfetchRatio()).isCloseTo(2.0, within(1e-9));
    }

    @Test
    void pageFillRatioIsMeanKeysPerPageOverConfiguredMaxKeys() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        // Two pages averaging 500 keys against a configured max-keys of 1000 -> 0.5 fill.
        metrics.recordListingPageShape(600, true, 1000);
        metrics.recordListingPageShape(400, false, 1000);

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        assertThat(summary.pageFillRatio()).isCloseTo(0.5, within(1e-9));
    }

    @Test
    void emptySplitRatioIsUnsplittableOutcomesOverTotalSteals() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordStealReason("CHILD_CREATED", "split_committed");
        metrics.recordSteal("CHILD_CREATED");
        metrics.recordStealReason("UNSPLITTABLE", "no_pivot");
        metrics.recordSteal("UNSPLITTABLE");
        metrics.recordStealReason("UNSPLITTABLE", "no_pivot");
        metrics.recordSteal("UNSPLITTABLE");
        metrics.recordStealReason("RETRY", "bound_moved");
        metrics.recordSteal("RETRY");

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        // 2 UNSPLITTABLE / 4 total steal attempts == 0.5.
        assertThat(summary.emptySplitRatio()).isCloseTo(0.5, within(1e-9));
    }

    @Test
    void wastedProbeRatioIsEmptyUpperBisectionsOverProbeFetches() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordProbeFetch();
        metrics.recordProbeFetch();
        metrics.recordProbeFetch();
        metrics.recordProbeFetch();
        metrics.recordEmptyUpperBisection();

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        assertThat(summary.wastedProbeRatio()).isCloseTo(0.25, within(1e-9));
    }

    @Test
    void wastedProbeRatioFoldsInStructureProbeFetches() {
        // swath.probe.structure_fetches must be folded into the denominator alongside
        // swath.probe.fetches — structure-probe LIST fetches are probes too, and excluding them
        // undercounts waste. 1 empty-upper bisection / (2 probe fetches + 2 structure-probe fetches)
        // == 0.25; excluding structure fetches from the denominator would read 0.5 instead.
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordProbeFetch();
        metrics.recordProbeFetch();
        metrics.recordStructureProbeFetch();
        metrics.recordStructureProbeFetch();
        metrics.recordEmptyUpperBisection();

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        assertThat(summary.wastedProbeRatio()).isCloseTo(0.25, within(1e-9));
    }

    @Test
    void stealSuccessRateIsChildCreatedOverTotalSteals() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordSteal("CHILD_CREATED");
        metrics.recordSteal("CHILD_CREATED");
        metrics.recordSteal("CHILD_CREATED");
        metrics.recordSteal("RETRY");

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        assertThat(summary.stealSuccessRate()).isCloseTo(0.75, within(1e-9));
    }

    @Test
    void efficiencyRatiosAreZeroNotDivideByZeroOnAFreshRun() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");

        RunSummary summary = metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 1L, 0L);

        assertThat(summary.overfetchRatio()).isEqualTo(0.0);
        assertThat(summary.pageFillRatio()).isEqualTo(0.0);
        assertThat(summary.emptySplitRatio()).isEqualTo(0.0);
        assertThat(summary.wastedProbeRatio()).isEqualTo(0.0);
        assertThat(summary.stealSuccessRate()).isEqualTo(0.0);
    }

    @Test
    void s3PoolGaugesAreUnobservedNaNBeforeAnyUpdate() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);

        assertThat(registry.find("swath.s3.pool.leased").gauge().value()).isNaN();
        assertThat(registry.find("swath.s3.pool.idle_available").gauge().value()).isNaN();
        assertThat(registry.find("swath.s3.pool.pending_acquisition").gauge().value()).isNaN();
        assertThat(registry.find("swath.s3.pool.max").gauge().value()).isNaN();
    }

    @Test
    void updateS3PoolSetsAllFourGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.updateS3Pool(12, 68, 3, 80);

        assertThat(registry.find("swath.s3.pool.leased").gauge().value()).isEqualTo(12.0);
        assertThat(registry.find("swath.s3.pool.idle_available").gauge().value()).isEqualTo(68.0);
        assertThat(registry.find("swath.s3.pool.pending_acquisition").gauge().value()).isEqualTo(3.0);
        assertThat(registry.find("swath.s3.pool.max").gauge().value()).isEqualTo(80.0);
    }

    @Test
    void updateS3PoolWithPartialArgumentsLeavesOthersUnobserved() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.updateS3Pool(null, null, 5, null);

        assertThat(registry.find("swath.s3.pool.pending_acquisition").gauge().value()).isEqualTo(5.0);
        assertThat(registry.find("swath.s3.pool.leased").gauge().value()).isNaN();
        assertThat(registry.find("swath.s3.pool.idle_available").gauge().value()).isNaN();
        assertThat(registry.find("swath.s3.pool.max").gauge().value()).isNaN();
    }

    @Test
    void updateS3PoolLaterCallsOnlyOverwriteSuppliedFields() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.updateS3Pool(12, 68, 0, 80);
        metrics.updateS3Pool(20, null, null, null);   // a later attempt only reports leased

        assertThat(registry.find("swath.s3.pool.leased").gauge().value()).isEqualTo(20.0);
        assertThat(registry.find("swath.s3.pool.idle_available").gauge().value()).isEqualTo(68.0);
        assertThat(registry.find("swath.s3.pool.pending_acquisition").gauge().value()).isEqualTo(0.0);
        assertThat(registry.find("swath.s3.pool.max").gauge().value()).isEqualTo(80.0);
    }

    // ---- connection-churn counters -----------------------------------------------

    @Test
    void recordConnectionAbortedIncrementsConnectionAbortedCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordConnectionAborted();
        metrics.recordConnectionAborted();

        assertThat(registry.find("swath.s3.pool.connection_aborted").counter().count()).isEqualTo(2.0);
    }

    @Test
    void recordConnectionHandshakeIncrementsHandshakesCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordConnectionHandshake();

        assertThat(registry.find("swath.s3.pool.handshakes").counter().count()).isEqualTo(1.0);
    }

    // ---- §3.2: swath.progress.units / swath.phase ------------------------------

    @Test
    void recordProgressAdvancesProgressUnitsCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordProgress(250);
        metrics.recordProgress(50);

        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(300.0);
    }

    @Test
    void recordProgressIgnoresNonPositiveUnits() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordProgress(0);
        metrics.recordProgress(-5);

        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(0.0);
    }

    @Test
    void recordEntriesEmittedAdvancesBothEntriesEmittedAndProgressUnits() {
        // §3.2: listing/writing progress is entries emitted, by construction — the same call that
        // advances swath.entries.emitted must ALSO advance the universal swath.progress.units
        // counter, so rate(progress.units)==0 is a valid stuck signal during those phases too.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordEntriesEmitted(120);

        assertThat(registry.find("swath.entries.emitted").counter().count()).isEqualTo(120.0);
        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(120.0);
    }

    @Test
    void progressUnitsAccumulatesAcrossEntriesEmittedAndDirectMergeProgress() {
        // A sorted run's total progress is listing (recordEntriesEmitted) PLUS merge rows
        // (recordProgress) — the two must add, never overwrite or double-count each other.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordEntriesEmitted(100);   // listing phase
        metrics.recordProgress(40);          // merge phase

        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(140.0);
        assertThat(registry.find("swath.entries.emitted").counter().count()).isEqualTo(100.0);
    }

    // ---- recovery backfills touch ONLY their target counter, never progress.units ---------------

    @Test
    void recordRecoveredObjects_backfillsEntriesEmittedOnly_neverProgressUnits() {
        // The reattach-resume backfill must bump swath.entries.emitted (so the summary's
        // `objects` field reflects the true pre-crash + tail total) but must NOT route through
        // recordEntriesEmitted, which would also bump swath.progress.units and double-count the
        // pre-crash rows against progress already fed by the freshly-relisted tail's own
        // recordEntriesEmitted calls in this same process.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordRecoveredObjects(348);

        assertThat(registry.find("swath.entries.emitted").counter().count()).isEqualTo(348.0);
        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(0.0);

        // The rows <= 0 guard is a no-op: neither call moves entries.emitted off 348.
        metrics.recordRecoveredObjects(0);
        metrics.recordRecoveredObjects(-5);

        assertThat(registry.find("swath.entries.emitted").counter().count()).isEqualTo(348.0);
        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(0.0);
    }

    @Test
    void recordRecoveredSortSegments_backfillsSegmentsWrittenOnly_neverEntriesOrProgressUnits() {
        // The merge-only-resume backfill must bump swath.sort.segments.written (so the
        // summary's `sort.segments` field reflects the true recovered segment count) but must NOT
        // touch entries.emitted or progress.units -- the merge already fed progress.units correctly
        // via recordProgress during the k-way merge itself.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordRecoveredSortSegments(7);

        assertThat(registry.find("swath.sort.segments.written").counter().count()).isEqualTo(7.0);
        assertThat(registry.find("swath.entries.emitted").counter().count()).isEqualTo(0.0);
        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(0.0);

        // The count <= 0 guard is a no-op: neither call moves segments.written off 7.
        metrics.recordRecoveredSortSegments(0);
        metrics.recordRecoveredSortSegments(-3);

        assertThat(registry.find("swath.sort.segments.written").counter().count()).isEqualTo(7.0);
    }

    @Test
    void recordEntriesEmitted_bumpsBothEntriesAndProgressUnits_theContrastRecoveryBackfillsAvoid() {
        // The trap the two tests above guard against: a NORMAL recordEntriesEmitted call (the one a
        // freshly-relisted tail uses) DOES advance both counters -- exactly why the recovery
        // backfills above must NOT route through it for pre-crash/pre-existing counts.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordEntriesEmitted(42);

        assertThat(registry.find("swath.entries.emitted").counter().count()).isEqualTo(42.0);
        assertThat(registry.find("swath.progress.units").counter().count()).isEqualTo(42.0);
    }

    @Test
    void phaseGaugeIsUnobservedBeforeAnySetPhaseCall() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);

        assertThat(registry.find("swath.phase").gauge().value()).isNaN();
    }

    @Test
    void setPhaseUpdatesThePhaseGaugeToTheCorrectCode() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.setPhase(Phase.LISTING);
        assertThat(registry.find("swath.phase").gauge().value()).isEqualTo(0.0);

        metrics.setPhase(Phase.MERGING);
        assertThat(registry.find("swath.phase").gauge().value()).isEqualTo(1.0);

        metrics.setPhase(Phase.WRITING);
        assertThat(registry.find("swath.phase").gauge().value()).isEqualTo(2.0);

        metrics.setPhase(Phase.COMPLETE);
        assertThat(registry.find("swath.phase").gauge().value()).isEqualTo(3.0);
    }

    @Test
    void apiCallsPer1kObjectsIsZeroWhenNoObjects() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordApiCall();

        RunSummary summary = metrics.summary(Duration.ofMillis(100), "WORK_STEALING", 0L, 0L);

        assertThat(summary.objects()).isZero();
        assertThat(summary.apiCallsPer1kObjects()).isEqualTo(0.0);
    }

    @Test
    void liveRateIsWindowedAcrossSuccessiveSnapshots() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");

        metrics.recordEntriesEmitted(100);
        // First snapshot: no prior sample -> live rate == cumulative average.
        RunMetrics.ProgressSnapshot first = metrics.snapshot(Duration.ofSeconds(10));
        assertThat(first.liveKeysPerSecond()).isCloseTo(10.0, within(1e-6));

        // 50 more keys over the next 1s window -> live rate reflects the window (50/s),
        // distinct from the cumulative average (150/11s ~= 13.6/s).
        metrics.recordEntriesEmitted(50);
        RunMetrics.ProgressSnapshot second = metrics.snapshot(Duration.ofSeconds(11));
        assertThat(second.liveKeysPerSecond()).isCloseTo(50.0, within(1e-6));
        assertThat(second.keysPerSecond()).isCloseTo(150.0 / 11.0, within(1e-6));
    }

    @Test
    void customProgressIntervalEmitsInFlightField() throws Exception {
        Logger logger =
                (Logger) LoggerFactory.getLogger(RunProgressReporter.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        try {
            RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
            metrics.setRunId(7L);
            metrics.setStrategy("WORK_STEALING");
            metrics.setConcurrencyTarget(4L);
            metrics.incrementInFlight();
            try (RunProgressReporter ignored = RunProgressReporter.start(metrics, Duration.ofMillis(5))) {
                Thread.sleep(30);
            }
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("progress "))
                .findFirst())
                .hasValueSatisfying(line -> assertThat(line).contains("in_flight=1"));
    }

    @Test
    void summaryFieldNamesAreNotMicrometerMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        metrics.setStrategy("WORK_STEALING");
        metrics.recordEntriesEmitted(10);
        metrics.recordApiCall();

        metrics.summary(Duration.ofMillis(100), "WORK_STEALING", 1L, 0L);

        // Derived summary/log fields must NOT leak into the Micrometer meter set:
        // cost_usd/cpu_efficiency/api_calls_per_1k_objects remain summary/JSON-only ratios,
        // never registered as their own meter.
        assertThat(registry.find("swath.cpu.seconds").meter()).isNull();
        assertThat(registry.find("swath.cpu.efficiency").meter()).isNull();
        assertThat(registry.find("swath.peak.rss").meter()).isNull();
        assertThat(registry.find("swath.peak.heap").meter()).isNull();
        assertThat(registry.find("swath.api.efficiency").meter()).isNull();
        assertThat(registry.find("swath.cost_usd").meter()).isNull();
        assertThat(registry.find("swath.api_calls_per_1k_objects").meter()).isNull();

        // Unlike the above, swath.run.duration/swath.run.throughput (the JSON summary's
        // keys_per_sec) ARE real end-of-run meters, registered up front alongside every other
        // meter in the constructor — confirm they don't silently regress into a non-meter, and
        // that recordRunCompletion is what feeds their live values.
        assertThat(registry.find("swath.run.duration").timer()).isNotNull();
        assertThat(registry.find("swath.run.throughput").gauge()).isNotNull();
    }

    @Test
    void micrometerMetersStayWithinCliContractNames() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        metrics.setStrategy("WORK_STEALING");
        metrics.recordApiCall();
        metrics.recordPage();
        metrics.recordEntriesEmitted(1);
        metrics.recordSplit();
        metrics.recordSteal("CHILD_CREATED");
        metrics.recordS3Throttle();
        metrics.recordEstimatedBytes(10);
        // Exercise the diagnostic-fingerprint counters too: these DO back real Micrometer
        // counters (swath.steal_reason{outcome,reason} + sibling swath.probe.*/swath.throttle.*/
        // swath.aimd.* meters), but under names distinct from the never-a-meter list below.
        metrics.recordListingPageShape(1000, true, 1000);
        metrics.recordStealReason("CHILD_CREATED", "split_committed");
        metrics.recordProbeFetch();
        metrics.recordEmptyUpperBisection();
        metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        metrics.recordAimdVote();
        metrics.recordAimdTargetReduction();

        assertThat(registry.find("swath.api.calls").counter()).isNotNull();
        assertThat(registry.find("swath.api.latency").timer()).isNotNull();
        assertThat(registry.find("swath.entries.emitted").counter()).isNotNull();
        assertThat(registry.find("swath.bytes.estimated").counter()).isNotNull();
        assertThat(registry.find("swath.workers.active").gauge()).isNotNull();
        assertThat(registry.find("swath.steals").counter()).isNotNull();
        assertThat(registry.find("swath.errors").counter()).isNotNull();
        assertThat(registry.find("swath.queue.wait").timer()).isNotNull();
        assertThat(registry.find("swath.rate_limit.wait").timer()).isNotNull();
        assertThat(registry.find("swath.rate_limit.api_wait").timer()).isNotNull();

        assertThat(registry.find("swath.splits").meter()).isNull();
        assertThat(registry.find("swath.pivots.unsplittable").meter()).isNull();
        assertThat(registry.find("swath.pages").meter()).isNull();
        assertThat(registry.find("swath.keys.per.page").meter()).isNull();
        assertThat(registry.find("swath.worker.keys").meter()).isNull();
        assertThat(registry.find("swath.inflight").meter()).isNull();
        assertThat(registry.find("swath.outstanding").meter()).isNull();
    }

    @Test
    void stealReasonBacksATaggedMicrometerCounterReadBackIntoDiagnostics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordStealReason("CHILD_CREATED", "split_committed");
        metrics.recordStealReason("CHILD_CREATED", "split_committed");
        metrics.recordSeedBands(3);

        // recordStealReason/recordSeedBands back a single real swath.steal_reason{outcome,reason}
        // Micrometer counter (the same computeIfAbsent idiom as apiCalls/errors/steals) instead of a
        // parallel hand-rolled ConcurrentHashMap<String,LongAdder>.
        assertThat(registry.find("swath.steal_reason")
                .tags("outcome", "CHILD_CREATED", "reason", "split_committed")
                .counter().count()).isEqualTo(2.0);
        assertThat(registry.find("swath.steal_reason")
                .tags("outcome", "SEED", "reason", "radix_bands")
                .counter().count()).isEqualTo(3.0);

        var reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.get("CHILD_CREATED.split_committed")).isEqualTo(2L);
        assertThat(reasons.get("SEED.radix_bands")).isEqualTo(3L);
    }

    @Test
    void diagnosticsStealReasonsContainsExactlyTheRecordedKeysNoDroppedOrLeakedEntries() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordStealReason("CHILD_CREATED", "split_committed");
        metrics.recordStealReason("RETRY", "bound_moved");
        metrics.recordStealReason("RETRY", "bound_moved");
        metrics.recordStealReason("UNSPLITTABLE", "no_pivot");
        metrics.recordSeedBands(4);
        // Sibling scalar counters (probe/throttle/aimd/page-shape) must NOT leak into stealReasons.
        metrics.recordProbeFetch();
        metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();

        assertThat(reasons.keySet()).containsExactlyInAnyOrder(
                "CHILD_CREATED.split_committed",
                "RETRY.bound_moved",
                "UNSPLITTABLE.no_pivot",
                "SEED.radix_bands");
        assertThat(reasons.get("CHILD_CREATED.split_committed")).isEqualTo(1L);
        assertThat(reasons.get("RETRY.bound_moved")).isEqualTo(2L);
        assertThat(reasons.get("UNSPLITTABLE.no_pivot")).isEqualTo(1L);
        assertThat(reasons.get("SEED.radix_bands")).isEqualTo(4L);
    }

    // ---- typed swath.throttle.events{type} --------------------------------------------------

    @Test
    void recordThrottleEventIncrementsOnlyTheMatchingTypeSeries() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);

        assertThat(registry.find("swath.throttle.events").tags("type", "attempt_timeout").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.find("swath.throttle.events").tags("type", "slowdown").counter().count())
                .isEqualTo(0.0);
        assertThat(registry.find("swath.throttle.events").tags("type", "server5xx").counter().count())
                .isEqualTo(0.0);
        assertThat(registry.find("swath.throttle.events").tags("type", "network").counter().count())
                .isEqualTo(0.0);
    }

    @Test
    void throttleEventsTotalSumsAMixOfTypesCorrectly() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        metrics.recordThrottleEvent(ThrottleType.SERVER_5XX);
        metrics.recordThrottleEvent(ThrottleType.NETWORK);
        metrics.recordThrottleEvent(ThrottleType.NETWORK);
        metrics.recordThrottleEvent(ThrottleType.NETWORK);

        // The raw total is every type; the diagnostics split honestly partitions that same
        // unified series by AIMD-voting class — voting (slowdown+server5xx)=3, transient
        // (attempt_timeout+network)=4 — so throttleEvents no longer silently includes transients.
        assertThat(metrics.throttleEventsTotal()).isEqualTo(7.0);
        RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);
        assertThat(diag.throttleEvents()).isEqualTo(3L);
        assertThat(diag.transientEvents()).isEqualTo(4L);
    }

    @Test
    void concurrentRecordStealReasonAndRecordSeedBandsCallsLoseNoUpdates() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        int threads = 16;
        int perThread = 4_000;
        long bandsPerCall = 3L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> futures = IntStream.range(0, threads)
                    .<Future<?>>mapToObj(i -> pool.submit(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        for (int j = 0; j < perThread; j++) {
                            // All threads hammer the SAME (outcome, reason) pair: proves the
                            // computeIfAbsent-then-increment path on a single shared Counter has no
                            // lost updates under real contention (the whole point — many worker
                            // threads record concurrently in production).
                            metrics.recordStealReason("CHILD_CREATED", "split_committed");
                            metrics.recordSeedBands(bandsPerCall);
                        }
                    }))
                    .collect(Collectors.toList());
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.get("CHILD_CREATED.split_committed")).isEqualTo((long) threads * perThread);
        assertThat(reasons.get("SEED.radix_bands")).isEqualTo((long) threads * perThread * bandsPerCall);
    }

    @Test
    void stealAttemptCounterRecordsTheAttemptDenominator() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordStealAttempt();
        metrics.recordStealAttempt();
        metrics.recordStealAttempt();

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.get("STEAL.attempted")).isEqualTo(3L);
    }

    // ---- output-completeness + run-level aggregate meters -----------------------

    @Test
    void recordOutputAccumulatesFilesAndBytesPerFormatOutcomeSeries() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordOutput("jsonl", "written", 1, 123);

        assertThat(registry.get("swath.output.files")
                .tags("format", "jsonl", "outcome", "written").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("swath.output.bytes")
                .tags("format", "jsonl").counter().count()).isEqualTo(123.0);

        // A second call to the SAME (format, outcome) accumulates -- these are run totals, not
        // last-write-wins snapshots.
        metrics.recordOutput("jsonl", "written", 1, 77);

        assertThat(registry.get("swath.output.files")
                .tags("format", "jsonl", "outcome", "written").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("swath.output.bytes")
                .tags("format", "jsonl").counter().count()).isEqualTo(200.0);

        // A different format lands on its own, separate series.
        metrics.recordOutput("parquet", "written", 3, 999);

        assertThat(registry.get("swath.output.files")
                .tags("format", "parquet", "outcome", "written").counter().count()).isEqualTo(3.0);
        assertThat(registry.get("swath.output.bytes")
                .tags("format", "parquet").counter().count()).isEqualTo(999.0);
        // The parquet write must not have leaked into the jsonl series.
        assertThat(registry.get("swath.output.files")
                .tags("format", "jsonl", "outcome", "written").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("swath.output.bytes")
                .tags("format", "jsonl").counter().count()).isEqualTo(200.0);
    }

    @Test
    void recordOutputBytesIsTaggedByFormatOnlyWhileFilesIsTaggedByFormatAndOutcome() {
        // Tag-cardinality contract: swath.output.bytes carries no `outcome` tag dimension (there is
        // only ever one bytes series per format), while swath.output.files is tagged by both.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordOutput("tsv", "written", 1, 50);

        assertThat(registry.get("swath.output.bytes").counter().getId().getTags())
                .extracting("key").containsExactly("format");
        assertThat(registry.get("swath.output.files").counter().getId().getTags())
                .extracting("key").containsExactlyInAnyOrder("format", "outcome");
    }

    @Test
    void recordOutputBrokenPipeIncrementsTheBrokenPipeCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordOutputBrokenPipe();
        metrics.recordOutputBrokenPipe();

        assertThat(registry.get("swath.output.broken_pipe").counter().count()).isEqualTo(2.0);
    }

    @Test
    void recordRunCompletionRecordsDurationTimerAndThroughputGauge() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.recordRunCompletion(Duration.ofSeconds(5), 1000.0);

        assertThat(registry.get("swath.run.duration").timer().count()).isEqualTo(1L);
        assertThat(registry.get("swath.run.duration").timer().totalTime(TimeUnit.SECONDS))
                .isCloseTo(5.0, within(0.01));
        assertThat(registry.get("swath.run.throughput").gauge().value()).isEqualTo(1000.0);
    }

    @Test
    void nullOrBlankOutcomeNormalizesToUnknownAndADottedReasonRoundTrips() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordStealReason(null, "some_reason");
        metrics.recordStealReason("", "another_reason");
        metrics.recordStealReason("OUTCOME", "reason.with.dots");

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();

        assertThat(reasons.keySet()).containsExactlyInAnyOrder(
                "unknown.some_reason", "unknown.another_reason", "OUTCOME.reason.with.dots");
        assertThat(reasons.get("unknown.some_reason")).isEqualTo(1L);
        assertThat(reasons.get("unknown.another_reason")).isEqualTo(1L);
        assertThat(reasons.get("OUTCOME.reason.with.dots")).isEqualTo(1L);
    }

    // ---- concurrency-target low-water mark ---------------------------------------

    @Test
    void concurrencyTargetLowWaterTracksTheMinimumTargetReached() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        metrics.setConcurrencyTarget(64);
        metrics.setConcurrencyTarget(8);    // shed
        metrics.setConcurrencyTarget(16);   // partial regrow — must NOT raise the low-water
        assertThat(registry.find("swath.aimd.target_low_water").gauge().value()).isEqualTo(8.0);
        // The live target gauge still reflects the CURRENT value, unaffected by the low-water mirror.
        assertThat(registry.find("swath.workers.active").gauge().value()).isEqualTo(16.0);
    }
}
