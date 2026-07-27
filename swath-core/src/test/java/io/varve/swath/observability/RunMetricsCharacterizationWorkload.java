/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.varve.swath.error.ThrottleType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The ONE representative {@link RunMetrics} workload shared by every characterization guard —
 * the Simple-registry series snapshot, the OTLP wire-identity snapshot, and the summary/diagnostics
 * assembly assertions. Kept in one place so those guards can never drift onto different inputs and
 * silently stop describing the same run.
 *
 * <p><b>Production fidelity is the point, and it is enforced by PROVENANCE.</b> Every tag value and
 * every {@code (category, reason)} pair below is traceable to the production call site that emits
 * that exact literal:
 * <pre>
 *   strategy=WORK_STEALING            ListCommand:354 / ListRunner:469 / WorkStealingScan:319
 *   strategy_why                      ListRunner:470 "checkpointed_resumable_nodes"
 *   errors{type=throttle}             RunMetrics:617 recordS3Throttle, from S3FaultClassifier:112,145,166,180
 *   errors{type=accessdenied}         S3FaultClassifier:205 recordS3Error(code="AccessDenied")
 *   errors{type=nosuchbucket}         S3FaultClassifier:219 recordS3Error(code="NoSuchBucket")
 *   steals{result=CHILD_CREATED|...}  WorkStealingScan:612 recordSteal(outcome.name()), Thief.Outcome
 *   CHILD_CREATED.split_committed     Thief:668
 *   NO_VICTIM.no_splittable_victim    Thief:276
 *   UNSPLITTABLE.no_pivot             Thief:358
 *   RETRY.split_aborted               Thief:631
 *   OWNER_SPLIT.self_published        WorkStealingScan (OWNER_SPLIT category)
 *   STEAL.attempted                   RunMetrics:1078, from WorkStealingScan:610
 *   SHED.timeout_storm_{worker,probe}_fed  RunMetrics:966,969, from ConcurrencyGauge:703
 *   SEED.radix_bands                  RunMetrics:1090, from SeedStep:234
 *   RESUME.{nodes_reopened,durable_cursor_lag,args_hash_refused}
 *                                     RunMetrics:1454,1466,1472, from SqliteCheckpointStore:426,428,184
 *   PIVOT_BYTE.{hex_digit,dead_zone,hex_alpha,other}
 *                                     RunMetrics:1011, from WorkStealingScan:1027
 *   CHILD_MASS.{empty,tiny,small,large}    RunMetrics:1033, from WorkStealingScan:821
 *   throttle.events{type=...}         the four {@link ThrottleType} tags, S3FaultClassifier/S3PageFetcher
 *   parquet.rotation{trigger=size|rows|time}   ParquetWriterPool:314 RotationReason.tag()
 *   parquet.parts{outcome=finalized|discarded|finalize_failed}  ParquetWriterPool:363,384,337
 *   output{format=parquet}            ListRunner:393,633,848,904,1285
 *   output{format=jsonl}              ListRunner:219,295,499 textFormatTag(OutputFormat.JSONL)
 *   output{outcome=written}           ListRunner:220 (the sibling value is "truncated")
 *   call_class, phase                 the RunMetrics CALL_CLASS_ and LATENCY_PHASE_ constants,
 *                                     passed through by S3PageFetcher:234-237,285-289
 *   seed.mode=shallow                 SeedStep:194,261 (the only other value is "none")
 * </pre>
 * <b>Do NOT introduce a tag value no production source emits.</b> The snapshots these feed are the
 * frozen Varve-facing surface; a fabricated value there promotes fixture input into the
 * contract and makes the guard look authoritative while encoding fiction. If you add a value here,
 * add its provenance row above; if you cannot find an emitter, the value does not belong.
 *
 * <p><b>Call ORDER is part of that fidelity.</b> {@code setStrategy} runs before the first {@code
 * recordApiCall}, exactly as {@code ListCommand.runWithCheckpoint} does (it sets the strategy ahead
 * of {@code seedFreshRun} precisely so seed probes do not land on a separate {@code
 * strategy="unknown"} series). A valid production run therefore emits exactly ONE {@code
 * swath.api.calls} series. The hazard — that {@code recordApiCall} reads the MUTABLE {@code
 * strategy} field, so the wrong order fragments the series — is already characterized end-to-end,
 * on the real OTLP wire, by {@link ApiCallsOtlpStrategyTagOrderingTest}; it is deliberately not
 * duplicated here.
 */
final class RunMetricsCharacterizationWorkload {

    private RunMetricsCharacterizationWorkload() {
    }

    /**
     * Drives a representative workload across every {@link RunMetrics} subsystem that can register
     * or touch a meter: api/error/steal/steal-reason tagging, the typed throttle series, the AIMD
     * rungs, idle backoff, checkpoint, Parquet, output completeness, the {@code --sort} lane, the S3
     * pool + connection churn, per-call-class latency phases, the in-flight gauge, progress/phase,
     * the shape accumulators and the demand gate.
     *
     * <p>Driven off a monotonic FAKE clock (1ms per read) so nothing observable depends on wall
     * time — including {@code diagnostics()}'s {@code timeToFirstStealMs}/{@code
     * timeToPeakInFlightMs}, which are exact for this workload.
     */
    static RunMetrics drive(MeterRegistry registry, Path scratchDir) {
        AtomicLong clock = new AtomicLong();
        RunMetrics m = new RunMetrics(registry, () -> clock.addAndGet(1_000_000L));

        m.registerDiskFreeGauge(scratchDir);
        m.registerCheckpointQueueDepthGauge(() -> 3);
        m.registerRangeSnapshotSource(() -> List.of(new RunMetrics.RangeSnapshot(
                "a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8),
                "ab".getBytes(StandardCharsets.UTF_8), 12.0, 3.0, 1L, 2L, 3L, 4L)));
        m.setRunId(7L);
        m.markRunStarted();
        m.setPhase(Phase.LISTING);

        // Production ordering: the strategy tag is set BEFORE any api call for the run (including
        // seed probes), so all three land on the one canonical series. See the class javadoc.
        m.setStrategy("WORK_STEALING");
        m.setStrategyWhy("checkpointed_resumable_nodes");
        m.recordApiCall();
        m.recordApiCall();
        m.recordApiCall();

        // Errors. `recordS3Throttle` is THE emitter of errors{type=throttle}
        // (S3FaultClassifier routes SlowDown/5xx here); every other errors{type} value is a real S3
        // error code lowercased by `recordError`. There is no errors{type=slowdown} or {type=timeout}
        // in production -- SlowDown lands on {type=throttle}, and timeouts are recorded only on the
        // typed swath.throttle.events / TRANSIENT steal-reason series.
        m.recordS3Throttle();
        m.recordS3Error("AccessDenied");
        m.recordS3Error("NoSuchBucket");

        // Steal outcomes + the steal_reason engagement namespace.
        m.recordSteal("CHILD_CREATED");
        m.recordSteal("NO_VICTIM");
        m.recordStealAttempt();
        m.recordStealReason("CHILD_CREATED", "split_committed");
        m.recordStealReason("UNSPLITTABLE", "no_pivot");
        m.recordStealReason("OWNER_SPLIT", "self_published");
        m.recordStealReason("RETRY", "split_aborted");
        m.recordStealReason("NO_VICTIM", "no_splittable_victim");
        m.recordShedCallClassMix(2, 1);
        m.recordSeedBands(4);
        m.recordResumeNodesReopened(2);
        m.recordResumeDurableCursorLag(1);
        m.recordResumeArgsHashRefused();
        m.recordPivotByteRegion("a5".getBytes(StandardCharsets.UTF_8), "a1".getBytes(StandardCharsets.UTF_8));
        m.recordPivotByteRegion("aZ".getBytes(StandardCharsets.UTF_8), "a1".getBytes(StandardCharsets.UTF_8));
        m.recordPivotByteRegion("ab".getBytes(StandardCharsets.UTF_8), "a1".getBytes(StandardCharsets.UTF_8));
        m.recordPivotByteRegion("a~".getBytes(StandardCharsets.UTF_8), "a1".getBytes(StandardCharsets.UTF_8));
        m.recordChildMass(0L);
        m.recordChildMass(10L);
        m.recordChildMass(1_000L);
        m.recordChildMass(1_000_000L);

        // Split/probe engagement.
        m.recordSplit();
        m.recordProbeFetch();
        m.recordStructureProbeFetch();
        m.recordEmptyUpperBisection();
        m.recordDelimiterFanout(6);
        m.recordAlphabetObservation(new long[] {0b1011L, 0L, 0b11L, 0L});

        // Typed throttle series — every ThrottleType, so all 4 tag values are present.
        for (ThrottleType type : ThrottleType.values()) {
            m.recordThrottleEvent(type);
        }

        // AIMD rungs.
        m.recordAimdVote();
        m.recordAimdTargetReduction();
        m.recordTimeoutShed();
        m.recordLatencyFreeze();
        m.recordGrowthFreeze();
        m.setLatencyBaselineMillis(42L);
        m.setConcurrencyTarget(16L);
        m.setConcurrencyTarget(4L);

        // Latency timers.
        m.recordS3Latency(m.startS3PageTimer());
        m.recordQueueWait(m.startQueueWaitTimer());
        m.recordRateLimitWait(m.startRateLimitWaitTimer());
        m.recordApiRateLimitWait(m.startApiRateLimitWaitTimer());

        // Idle backoff.
        m.setIdleBackoffLevel(2L);
        m.recordIdleBackoffSlotDenied();
        m.recordIdleBackoffReset();
        m.recordIdleBackoffPark(m.startIdleBackoffParkTimer());

        // Checkpoint/resume.
        m.recordCheckpointCommit(m.startCheckpointCommitTimer(), 64);
        m.recordCheckpointQueueWait(1_000L);
        m.recordCheckpointCommitWait(3_000L);

        // Per-page emit span.
        m.recordEmit(2_000L);

        // Parquet writer pool — every rotation trigger and part outcome.
        m.recordParquetRotation("size");
        m.recordParquetRotation("rows");
        m.recordParquetRotation("time");
        m.recordParquetPart("finalized");
        m.recordParquetPart("discarded");
        m.recordParquetPart("finalize_failed");
        m.recordParquetFinalizeLatency(m.startParquetFinalizeTimer());
        m.recordParquetWrite(3_000L);

        // Output completeness.
        m.recordOutput("parquet", "written", 2L, 4_096L);
        m.recordOutput("jsonl", "written", 1L, 512L);
        m.recordOutputBrokenPipe();
        m.recordRunCompletion(Duration.ofSeconds(5), 1_234.5);

        // --sort lane.
        m.recordSortEntries(500L);
        m.recordSortSegment(2_048L, 3);
        m.recordRecoveredSortSegments(2L);
        m.recordRecoveredObjects(10L);
        m.recordSortBackpressureWait(5_000L);
        m.recordSortStagingBytesLive(9_999L);
        m.recordSortHandoffQueueDepth(4);
        m.recordSortOffThreadBuffersLive(2);
        m.recordSortMergePasses(3L);
        m.recordSortMerge(m.startSortMergeTimer());
        m.recordSortMergeRange(7_000L);

        // S3 pool + connection churn.
        m.updateS3Pool(5, 3, 1, 32);
        m.recordConnectionAborted();
        m.recordConnectionHandshake();
        m.recordSocketClosureRecovered();

        // Per-call-class latency phases — all 3 x 5 series (a negative sample is the
        // SDK-didn't-report sentinel and must NOT register a series).
        for (String callClass : List.of(RunMetrics.CALL_CLASS_WORKER_PAGE, RunMetrics.CALL_CLASS_PIVOT_PROBE,
                RunMetrics.CALL_CLASS_STRUCTURE_PROBE)) {
            for (String phase : List.of(RunMetrics.LATENCY_PHASE_CONNECT_ACQUIRE, RunMetrics.LATENCY_PHASE_TTFB,
                    RunMetrics.LATENCY_PHASE_SDK_UNMARSHAL, RunMetrics.LATENCY_PHASE_TOTAL,
                    RunMetrics.LATENCY_PHASE_RESPONSE_PARSE)) {
                m.recordCallClassLatency(callClass, phase, 1_500_000L);
            }
        }

        // In-flight gauge + trajectory rollup (sampled on every transition).
        m.incrementInFlight();
        m.incrementInFlight();
        m.decrementInFlight();

        // Progress / listing shape.
        m.recordListingPageShape(900L, true, 1_000);
        m.recordPage();
        m.recordEntriesEmitted(900L);
        m.recordEstimatedBytes(90_000L);
        m.recordProgress(50L);
        m.markProgress();
        m.setPrefix("p/".getBytes(StandardCharsets.UTF_8));
        m.setCursor("p/k1".getBytes(StandardCharsets.UTF_8));

        // Demand gate + seed block.
        m.recordDemandGatedConcurrency(4, 16);
        m.recordSeedSummary("shallow", 8L, 12L, 2L, 6L);

        m.setPhase(Phase.COMPLETE);
        return m;
    }
}
