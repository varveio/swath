/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization: the {@link RunMetrics#summary} / {@link RunMetrics#diagnostics} ASSEMBLY —
 * which fields exist, in what order, which optional blocks are present vs {@code null}/empty, and
 * which derived ratios are computed from which counters.
 *
 * <p>Complements (does NOT duplicate) {@code JsonRunSummaryWriterTest}, which guards the rendered
 * summary-JSON's byte identity from an already-built {@link RunSummary}. This one guards the step
 * BEFORE that: {@code RunMetrics} turning its ~50 fields into that record. A decomposition that
 * re-homes an accumulator can leave the writer's byte identity intact while silently feeding it a
 * zero, a {@code null} block, or a ratio with the wrong denominator.
 *
 * <p>The field-name list and the derived values are FROZEN in the same sense as the meter-series
 * snapshot: updated deliberately alongside an intended change, never re-captured to go green.
 */
final class RunMetricsSummaryAssemblyCharacterizationTest {

    /**
     * The time-weighted in-flight average for the shared workload, computed over its FAKE clock
     * (1ms per read) so it is deterministic and machine-independent. FROZEN as the observed value.
     */
    private static final double AVG_IN_FLIGHT = 1.0;

    /**
     * {@link RunSummary}'s record components, in declaration order. FROZEN. {@code sessionDuration}
     * was ADDED right after {@code duration} — the resolution for the elapsed-scope inconsistency
     * between the live progress line (session-scoped, seeding included) and this record's
     * `duration` (listing-scoped, a fresh run's clock starts AFTER seeding): moved deliberately here
     * alongside that change, not re-captured.
     */
    private static final List<String> EXPECTED_SUMMARY_FIELDS = List.of(
            "runId", "objects", "duration", "sessionDuration", "strategy", "apiCalls", "costUsd",
            "outputFiles", "compressedBytes", "keys", "pages", "peakInFlight", "avgInFlight", "timeToFirstStealMs",
            "timeToPeakInFlightMs", "steals", "splits",
            "errors", "keysPerSecond", "apiCallsPer1kObjects", "peakRssBytes", "peakHeapBytes",
            "cpuSeconds", "cpuEfficiency", "overfetchRatio", "pageFillRatio", "emptySplitRatio",
            "wastedProbeRatio", "stealSuccessRate", "compressionRatio", "seed", "shape",
            "trajectory", "slowRanges", "callClassLatency", "clientCost", "demandGate");

    /** {@link RunMetrics.RunDiagnostics}'s record components, in declaration order. FROZEN. */
    private static final List<String> EXPECTED_DIAGNOSTICS_FIELDS = List.of(
            "runId", "durationMs", "strategy", "strategyWhy", "stealReasons", "probeFetches",
            "emptyUpperBisections", "splitsCommitted", "unsplittableVictims", "splitGuardAborts",
            "peakInFlight", "timeToFirstStealMs", "timeToPeakInFlightMs", "pages", "totalKeys",
            "meanKeysPerPage", "shortTruncatedPages", "throttleEvents", "transientEvents",
            "aimdVotes", "aimdTargetReductions");

    @Test
    void summaryAndDiagnosticsFieldSetsAreFrozen() {
        assertThat(components(RunSummary.class)).containsExactlyElementsOf(EXPECTED_SUMMARY_FIELDS);
        assertThat(components(RunMetrics.RunDiagnostics.class))
                .containsExactlyElementsOf(EXPECTED_DIAGNOSTICS_FIELDS);
    }

    @Test
    void summaryDerivesEveryScalarFromTheSameCountersItDoesToday(@TempDir Path scratchDir) {
        RunMetrics m = RunMetricsCharacterizationWorkload.drive(new SimpleMeterRegistry(), scratchDir);

        RunSummary s = m.summary(Duration.ofSeconds(5), "WORK_STEALING", 2L, 4_096L);

        assertThat(s.runId()).isEqualTo(7L);
        assertThat(s.objects()).isEqualTo(910L);          // entries.emitted, incl. recordRecoveredObjects
        assertThat(s.keys()).isEqualTo(910L);             // keys mirrors objects
        assertThat(s.duration()).isEqualTo(Duration.ofSeconds(5));   // passed through verbatim
        // No session-wide RunProgressReporter ever claimed this RunMetrics (the workload drives
        // counters directly), so sessionDuration falls back to the listing duration verbatim rather
        // than reading a garbage delta off an unset clock — see RunMetrics#sessionDuration(Duration).
        assertThat(s.sessionDuration()).isEqualTo(Duration.ofSeconds(5));
        assertThat(s.strategy()).isEqualTo("WORK_STEALING");         // passed through verbatim
        assertThat(s.apiCalls()).isEqualTo(3L);           // the single WORK_STEALING series (production ordering)
        assertThat(s.costUsd()).isCloseTo(3 * 0.005 / 1_000.0, within(1e-12));
        assertThat(s.pages()).isEqualTo(1L);
        assertThat(s.peakInFlight()).isEqualTo(2L);
        // avgInFlight is time-weighted over the FAKE clock (1ms/read), so it is fully deterministic
        // and machine-independent — pinned here as the observed value for this exact workload.
        assertThat(s.avgInFlight()).isCloseTo(AVG_IN_FLIGHT, within(1e-9));
        assertThat(s.steals()).isEqualTo(2L);
        assertThat(s.splits()).isEqualTo(1L);
        assertThat(s.errors()).isEqualTo(3L);
        assertThat(s.outputFiles()).isEqualTo(2L);
        assertThat(s.compressedBytes()).isEqualTo(4_096L);
        // Session work over session time / session API calls: the 10 recovered rows are part of the
        // dataset `objects` reports, but this process neither listed them nor paid a call for them.
        assertThat(s.keysPerSecond()).isCloseTo(900 / 5.0, within(1e-9));
        assertThat(s.apiCallsPer1kObjects()).isCloseTo(3 * 1_000.0 / 900, within(1e-9));

        // The resource/CPU scalars read the live JVM/OS, so their exact bytes aren't value-pinnable.
        // But their AVAILABILITY is deterministic on any given platform: probe ResourceMetrics
        // directly here, then require the summary field to match that availability class exactly.
        // This makes swapping a live probe for the -1 sentinel go RED wherever the probe works —
        // the weaker "sentinel OR positive" form would have accepted that swap silently.
        long probedRss = ResourceMetrics.peakRssBytes();
        long probedHeap = ResourceMetrics.peakHeapBytes();
        boolean cpuAvailable = ResourceMetrics.processCpuTimeNanos() >= 0;

        if (probedRss >= 0) {
            assertThat(s.peakRssBytes()).isGreaterThan(0L);
            // /proc/self/status reports VmHWM in kB, so the byte value is always a whole KiB.
            assertThat(s.peakRssBytes() % 1024)
                    .as("peak RSS is a whole number of 1024-byte (kB) units")
                    .isZero();
        } else {
            assertThat(s.peakRssBytes()).isEqualTo(-1L);
        }
        if (probedHeap >= 0) {
            assertThat(s.peakHeapBytes()).isGreaterThanOrEqualTo(0L);   // a zero heap total is valid
        } else {
            assertThat(s.peakHeapBytes()).isEqualTo(-1L);
        }
        // cpuSeconds and cpuEfficiency are COUPLED in production, from a single CPU sample: both are
        // -1 together when unavailable, and when available cpuEfficiency == cpuSeconds / wallSeconds
        // (5.0s here). Assert the coupling, not two independent broad domains.
        if (cpuAvailable) {
            assertThat(s.cpuSeconds()).isGreaterThanOrEqualTo(0.0);
            assertThat(s.cpuEfficiency()).isCloseTo(s.cpuSeconds() / 5.0, within(1e-12));
        } else {
            assertThat(s.cpuSeconds()).isEqualTo(-1.0);
            assertThat(s.cpuEfficiency()).isEqualTo(-1.0);
        }

        // Derived ratios — numerator/denominator pairing is exactly what a decomposition can
        // silently re-home (raw_keys/entries, mean-keys-per-page/max-keys, unsplittable/steals,
        // empty-upper/(probe+structure probes), child-created/steals, raw-bytes/compressed).
        assertThat(s.overfetchRatio()).isCloseTo(900 / 900.0, within(1e-9));   // raw keys / SESSION keys
        assertThat(s.pageFillRatio()).isCloseTo(900 / 1_000.0, within(1e-9));
        assertThat(s.emptySplitRatio()).isCloseTo(1 / 2.0, within(1e-9));
        assertThat(s.wastedProbeRatio()).isCloseTo(1 / 2.0, within(1e-9));
        assertThat(s.stealSuccessRate()).isCloseTo(1 / 2.0, within(1e-9));
        assertThat(s.compressionRatio()).isCloseTo(90_000 / 4_096.0, within(1e-9));

        // Optional blocks, all present on a run that fetched a page.
        assertThat(s.seed()).isNotNull();
        assertThat(s.seed().mode()).isEqualTo("shallow");
        assertThat(s.shape()).isNotNull();
        assertThat(s.trajectory()).isNotNull();
        assertThat(s.demandGate()).isNotNull();
        assertThat(s.demandGate().events()).isEqualTo(1L);
        assertThat(s.demandGate().lastT()).isEqualTo(4);
        assertThat(s.demandGate().minT()).isEqualTo(4);
        assertThat(s.demandGate().tMax()).isEqualTo(16);
        assertThat(s.slowRanges()).hasSize(1);
        assertThat(s.callClassLatency()).hasSize(15);
        assertThat(s.clientCost()).hasSize(5);
    }

    @Test
    void shapeBlockIsAssembledFromTheShapeAccumulators(@TempDir Path scratchDir) {
        RunMetrics m = RunMetricsCharacterizationWorkload.drive(new SimpleMeterRegistry(), scratchDir);

        RunSummary.ShapeSummary shape = m.summary(Duration.ofSeconds(5), "WORK_STEALING", 2L, 4_096L).shape();

        // maskWords {0b1011, 0, 0b11, 0} -> popcount 3 at position 0, 2 at position 1, 0 elsewhere.
        assertThat(shape.alphabetCardinality()).containsExactly(3, 2, 0, 0, 0, 0, 0, 0);
        assertThat(shape.alphabetPositionsObserved()).isEqualTo(2);
        // All four recorded pivots diverge from their reference at byte index 1.
        long[] expectedDepths = new long[RunMetrics.DIVERGENCE_DEPTH_BUCKETS];
        expectedDepths[1] = 4L;
        assertThat(shape.divergenceDepthHistogram()).containsExactly(expectedDepths);
        assertThat(shape.delimiterFanoutMax()).isEqualTo(6L);
        assertThat(shape.delimiterFanoutTotal()).isEqualTo(6L);
        assertThat(shape.delimiterProbes()).isEqualTo(1L);
        // CHILD_MASS {empty,tiny,small,large} = 1 each -> the fixed 4-group Gini.
        assertThat(shape.massSkewGini()).isCloseTo(
                RunMetrics.giniFromGroups(new long[] {1, 1, 1, 1}, new double[] {0.0, 10.0, 1005.0, 100_000.0}),
                within(1e-12));
        assertThat(shape.apiLatencyP50Ms()).isNotNull();
        assertThat(shape.apiLatencyP99Ms()).isNotNull();
    }

    @Test
    void preRunEarlyExitOmitsEveryOptionalBlockAndNeverNullsTheListBlocks() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());

        RunSummary s = m.summary(Duration.ofSeconds(1), null, 0L, 0L);

        // No page was ever fetched -> shape/trajectory are deliberately null rather than all-zero.
        assertThat(s.shape()).isNull();
        assertThat(s.trajectory()).isNull();
        assertThat(s.seed()).isNull();          // never seeded
        assertThat(s.demandGate()).isNull();    // the demand gate never fired
        // The three LIST blocks are empty, never null.
        assertThat(s.slowRanges()).isEmpty();
        assertThat(s.callClassLatency()).isEmpty();
        assertThat(s.clientCost()).isEmpty();
        // A null strategy is passed straight through to the record (the normalize-to-"unknown"
        // pass applies to the TAG, not to this field).
        assertThat(s.strategy()).isNull();
        // Every zero-denominator ratio guards to 0.0, never NaN and never the -1 unavailable sentinel.
        assertThat(s.overfetchRatio()).isEqualTo(0.0);
        assertThat(s.pageFillRatio()).isEqualTo(0.0);
        assertThat(s.emptySplitRatio()).isEqualTo(0.0);
        assertThat(s.wastedProbeRatio()).isEqualTo(0.0);
        assertThat(s.stealSuccessRate()).isEqualTo(0.0);
        assertThat(s.compressionRatio()).isEqualTo(0.0);
        assertThat(s.apiCallsPer1kObjects()).isEqualTo(0.0);
    }

    @Test
    void trajectoryIsGatedOnAFetchedPageNotOnInFlightTransitions() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        m.markRunStarted();
        m.incrementInFlight();
        m.decrementInFlight();

        // In-flight transitions alone do NOT produce a trajectory block — the same rawPageCount>0
        // guard that gates `shape` gates it, so a pre-run early exit never emits either.
        assertThat(m.summary(Duration.ofSeconds(1), "s", 0L, 0L).trajectory()).isNull();

        m.recordListingPageShape(10L, false, 1_000);
        assertThat(m.summary(Duration.ofSeconds(1), "s", 0L, 0L).trajectory()).isNotNull();
    }

    @Test
    void slowRangesDegradeToEmptyWhenTheSourceIsAbsentNullOrThrowing() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        assertThat(m.summary(Duration.ofSeconds(1), "s", 0L, 0L).slowRanges()).isEmpty();

        m.registerRangeSnapshotSource(() -> null);
        assertThat(m.summary(Duration.ofSeconds(1), "s", 0L, 0L).slowRanges()).isEmpty();

        m.registerRangeSnapshotSource(() -> {
            throw new IllegalStateException("worklist mutated under us");
        });
        assertThat(m.summary(Duration.ofSeconds(1), "s", 0L, 0L).slowRanges()).isEmpty();
    }

    /**
     * The elapsed-scope split, on the exact figures the confirmed inconsistency was reported with: a
     * live run's final progress frame read {@code 2m22s elapsed} while the summary line beneath it
     * read {@code 3,270,132 objects in 1m43s} — the 39 s delta was the seed phase, which the
     * session-wide {@link RunProgressReporter} covers but the listing clock ({@code duration},
     * {@code RunMetrics#markRunStarted()}'s zero point) does not. {@code keysPerSecond} must stay
     * keyed to {@code duration} (the listing clock) even though {@code sessionDuration} is now also
     * on the record — a future edit that silently re-keys it to the larger, session figure would
     * under-report throughput without any test noticing except this one.
     */
    @Test
    void keysPerSecondStaysKeyedToTheListingClockNotTheSessionOne() throws Exception {
        AtomicLong clock = new AtomicLong(0L);
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry(), clock::get);

        try (RunProgressReporter reporter = RunProgressReporter.start(m, Duration.ofSeconds(30))) {
            // Session start is captured HERE, at the CLI's pre-seed instant -- clock=0.
            clock.addAndGet(Duration.ofSeconds(39).toNanos());   // the seed phase
            m.markRunStarted();                                  // the listing clock's zero point
            m.recordEntriesEmitted(3_270_132L);
            clock.addAndGet(Duration.ofSeconds(103).toNanos());  // the listing phase

            RunSummary s = m.summary(Duration.ofSeconds(103), "WORK_STEALING", 0L, 0L);

            assertThat(s.duration()).isEqualTo(Duration.ofSeconds(103));
            assertThat(s.sessionDuration())
                    .as("39s of seeding folded back in -- the SAME 2m22s the live progress line reported")
                    .isEqualTo(Duration.ofSeconds(142));
            assertThat(s.keysPerSecond())
                    .as("keyed to the 103s listing clock, never the 142s session one")
                    .isCloseTo(3_270_132 / 103.0, within(1e-6));
        }
    }

    @Test
    void objectsOverrideReplacesTheCounterDerivedObjectCountWithoutTouchingProgressUnits() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        m.recordEntriesEmitted(5L);

        RunSummary overridden = m.summary(Duration.ofSeconds(1), "s", 0L, 0L, 4_242L);

        assertThat(overridden.objects()).isEqualTo(4_242L);
        assertThat(overridden.keys()).isEqualTo(4_242L);
        assertThat(m.progressUnits()).isEqualTo(5L);
        assertThat(m.summary(Duration.ofSeconds(1), "s", 0L, 0L).objects()).isEqualTo(5L);
    }

    @Test
    void diagnosticsRollUpTheStealReasonNamespaceAndSplitThrottleByVotingClass(@TempDir Path scratchDir) {
        RunMetrics m = RunMetricsCharacterizationWorkload.drive(new SimpleMeterRegistry(), scratchDir);

        RunMetrics.RunDiagnostics d = m.diagnostics(Duration.ofSeconds(5));

        assertThat(d.runId()).isEqualTo(7L);
        assertThat(d.durationMs()).isEqualTo(5_000L);
        assertThat(d.strategy()).isEqualTo("WORK_STEALING");
        assertThat(d.strategyWhy()).isEqualTo("checkpointed_resumable_nodes");
        // steal_reasons is rebuilt off the swath.steal_reason meters, keyed "outcome.reason".
        assertThat(d.stealReasons()).isEqualTo(Map.ofEntries(
                Map.entry("CHILD_CREATED.split_committed", 1L),
                Map.entry("CHILD_MASS.empty", 1L),
                Map.entry("CHILD_MASS.large", 1L),
                Map.entry("CHILD_MASS.small", 1L),
                Map.entry("CHILD_MASS.tiny", 1L),
                Map.entry("NO_VICTIM.no_splittable_victim", 1L),
                Map.entry("OWNER_SPLIT.self_published", 1L),
                Map.entry("PIVOT_BYTE.dead_zone", 1L),
                Map.entry("PIVOT_BYTE.hex_alpha", 1L),
                Map.entry("PIVOT_BYTE.hex_digit", 1L),
                Map.entry("PIVOT_BYTE.other", 1L),
                Map.entry("RESUME.args_hash_refused", 1L),
                Map.entry("RESUME.durable_cursor_lag", 1L),
                Map.entry("RESUME.nodes_reopened", 2L),
                Map.entry("RETRY.split_aborted", 1L),
                Map.entry("SEED.radix_bands", 4L),
                Map.entry("SHED.timeout_storm_probe_fed", 1L),
                Map.entry("SHED.timeout_storm_worker_fed", 2L),
                Map.entry("STEAL.attempted", 1L),
                Map.entry("UNSPLITTABLE.no_pivot", 1L)));
        assertThat(d.probeFetches()).isEqualTo(1L);
        assertThat(d.emptyUpperBisections()).isEqualTo(1L);
        assertThat(d.splitsCommitted()).isEqualTo(1L);
        assertThat(d.unsplittableVictims()).isEqualTo(1L);
        assertThat(d.splitGuardAborts()).isEqualTo(1L);
        assertThat(d.peakInFlight()).isEqualTo(2L);
        // The shared fake clock (1ms per read) makes both event timings EXACT for this workload —
        // without these, a decomposition that lost the firstSteal/peakInFlight timestamps and
        // always emitted the -1 never-observed sentinel would still pass.
        assertThat(d.timeToFirstStealMs()).isEqualTo(1L);
        assertThat(d.timeToPeakInFlightMs()).isEqualTo(5L);
        assertThat(d.pages()).isEqualTo(1L);
        assertThat(d.totalKeys()).isEqualTo(900L);   // raw page keys, NOT entries.emitted
        assertThat(d.meanKeysPerPage()).isCloseTo(900.0, within(1e-9));
        assertThat(d.shortTruncatedPages()).isEqualTo(1L);
        // The voting split: slowdown+server5xx vote, attempt_timeout+network do not.
        assertThat(d.throttleEvents()).isEqualTo(2L);
        assertThat(d.transientEvents()).isEqualTo(2L);
        assertThat(d.aimdVotes()).isEqualTo(1L);
        assertThat(d.aimdTargetReductions()).isEqualTo(1L);
    }

    @Test
    void diagnosticsFallBackToPageCountersWhenNoListingPageShapeWasEverRecorded() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        m.recordPage();
        m.recordPage();
        m.recordEntriesEmitted(50L);

        RunMetrics.RunDiagnostics d = m.diagnostics(Duration.ofSeconds(1));

        assertThat(d.pages()).isEqualTo(2L);
        assertThat(d.totalKeys()).isEqualTo(50L);
        assertThat(d.meanKeysPerPage()).isCloseTo(25.0, within(1e-9));
        // Never-observed event timings render as the -1 unavailable sentinel, not 0.
        assertThat(d.timeToFirstStealMs()).isEqualTo(-1L);
        assertThat(d.timeToPeakInFlightMs()).isEqualTo(-1L);
    }

    private static List<String> components(Class<?> record) {
        return Arrays.stream(record.getRecordComponents()).map(RecordComponent::getName).toList();
    }
}
