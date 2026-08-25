/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A run summary's {@code max} and percentiles cover the WHOLE RUN, exactly like its {@code count}
 * and {@code total} — never a rolling sub-window.
 *
 * <p>REGRESSION: Micrometer's default {@code DistributionStatisticConfig} is a rolling window
 * ({@code expiry=2m}, {@code bufferLength=3}), so {@code max()} and every published percentile DECAY
 * while {@code count()}/{@code totalTime()} stay cumulative. The JSON run summary is explicitly a
 * post-hoc forensics artifact, so that silently mixed two time bases in a single row. One field run
 * reported {@code swath.rate_limit.wait} as {@code count=6819, total_ms=143045, max_ms=0.001117} —
 * 21 ms of average slot wait against a sub-microsecond max — because slot contention stopped partway
 * through and the rolling max had decayed by the time the summary was written. Every {@code
 * probe_latency[]} percentile and {@code shape.regime.api_latency_p*} shared the defect, describing
 * only the run's last ~2 minutes while being read as run-level facts. See
 * {@code docs/ops/dev/field-investigations.md}.
 *
 * <p>Against Micrometer's defaults both tests below fail (max and p99 read 0.0 after the window
 * rotates). See {@code RunMetrics#DISTRIBUTION_WINDOW}.
 */
class RunMetricsDistributionWindowTest {

    /** Comfortably past Micrometer's 2-minute default expiry. */
    private static final Duration PAST_DEFAULT_EXPIRY = Duration.ofMinutes(10);

    private static RunMetrics metricsOn(MockClock clock) {
        return new RunMetrics(new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock));
    }

    private static Timer timer(RunMetrics metrics, String name) {
        Timer t = metrics.registry().find(name).timer();
        assertThat(t).as("timer %s is registered", name).isNotNull();
        return t;
    }

    /**
     * The exact shape that misled: a burst of slot waits early in a long run, then quiet. The max
     * must still describe the burst when the summary is written minutes later.
     */
    @Test
    void slotWaitMaxSurvivesAQuietTailOfTheRun() {
        MockClock clock = new MockClock();
        RunMetrics metrics = metricsOn(clock);

        Timer.Sample sample = metrics.startRateLimitWaitTimer();
        clock.add(Duration.ofMillis(250));
        metrics.recordRateLimitWait(sample);

        // ... then the run goes quiet (concurrency collapsed; nothing waits for a slot again).
        clock.add(PAST_DEFAULT_EXPIRY);

        Timer t = timer(metrics, "swath.rate_limit.wait");
        assertThat(t.count()).as("count is cumulative -- it always was").isEqualTo(1L);
        assertThat(t.max(TimeUnit.MILLISECONDS))
                .as("max must cover the whole run, matching count/total -- not a rolling 2min window")
                .isEqualTo(250.0);
        assertThat(t.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(250.0);
    }

    /** The published percentiles the summary's probe_latency[] and api_latency_p* rows read. */
    @Test
    void publishedPercentilesSurviveAQuietTailOfTheRun() {
        MockClock clock = new MockClock();
        RunMetrics metrics = metricsOn(clock);

        metrics.recordCallClassLatency(
                RunMetrics.CALL_CLASS_STRUCTURE_PROBE, RunMetrics.LATENCY_PHASE_TOTAL,
                Duration.ofSeconds(20).toNanos());
        clock.add(PAST_DEFAULT_EXPIRY);

        Timer t = metrics.registry().find("swath.fetch.latency.phase")
                .tag("call_class", RunMetrics.CALL_CLASS_STRUCTURE_PROBE)
                .tag("phase", RunMetrics.LATENCY_PHASE_TOTAL)
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.max(TimeUnit.MILLISECONDS))
                .as("a 20s structure probe is still the run's max minutes later")
                .isEqualTo(20_000.0);
        assertThat(t.takeSnapshot().percentileValues())
                .as("published percentiles describe the run, not its last 2 minutes")
                .anySatisfy(p -> assertThat(p.value(TimeUnit.MILLISECONDS)).isGreaterThan(0.0));
    }

    @Test
    void dataSyncByteMaximaSurviveAQuietTailOfTheRun() {
        MockClock clock = new MockClock();
        RunMetrics metrics = metricsOn(clock);

        metrics.recordDatasetDataSync("tsv", Duration.ofMillis(20).toNanos(), 32L * 1024 * 1024);
        metrics.recordDatasetDataSyncResidual("tsv", 3L * 1024 * 1024);
        clock.add(PAST_DEFAULT_EXPIRY);

        DistributionSummary synced = metrics.registry().find("swath.data_sync.bytes")
                .tag("format", "tsv").summary();
        DistributionSummary residual = metrics.registry().find("swath.data_sync.residual.bytes")
                .tag("format", "tsv").summary();
        assertThat(synced).isNotNull();
        assertThat(residual).isNotNull();
        assertThat(synced.max()).isEqualTo(32L * 1024 * 1024);
        assertThat(residual.max()).isEqualTo(3L * 1024 * 1024);
    }
}
