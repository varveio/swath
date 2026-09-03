/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * The per-page CLIENT-SERVICE-COST decomposition read back as {@code RunSummary#clientCost()} (the
 * JSON summary's {@code client_cost[]}): what a page costs the client once the store has answered,
 * split into the spans that can contend independently — the fetch worker's durable-commit wait, the
 * checkpoint writer's own queue wait and batch commit, the consumer stage's sink write, the
 * worker's blocked-on-a-full-channel handoff, and — off the page's critical path — a Parquet writer
 * lane's own encode/write stretch.
 *
 * <p>Each row is a DISTRIBUTION, not a total: a per-page cost read as a mean cannot distinguish an
 * iid per-page cost from a queue behind a shared single writer (whose tail grows with worker count),
 * which is the whole question the decomposition exists to answer. So every span publishes
 * percentiles and every row carries them.
 */
class RunMetricsClientCostSpanTest {

    private static Map<String, RunSummary.ClientCostSpan> spansOf(RunMetrics metrics) {
        return metrics.summary(Duration.ofSeconds(1), "work_stealing", 0L, 0L).clientCost().stream()
                .collect(Collectors.toMap(RunSummary.ClientCostSpan::span, Function.identity()));
    }

    @Test
    void everySpanIsReadBackUnderItsOwnNameWithItsOwnObservationCount() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordCheckpointCommitWait(1_000_000L);
        metrics.recordCheckpointCommitWait(3_000_000L);
        metrics.recordCheckpointQueueWait(2_000_000L);
        metrics.recordCheckpointCommit(1L, 8);
        metrics.recordEmit(4_000_000L);
        metrics.recordChannelReceive(6_000_000L);
        metrics.recordQueueWait(metrics.startQueueWaitTimer());
        metrics.recordParquetWrite(5_000_000L);

        Map<String, RunSummary.ClientCostSpan> spans = spansOf(metrics);

        assertThat(spans).containsOnlyKeys(
                RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_COMMIT_WAIT,
                RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_QUEUE_WAIT,
                RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_COMMIT,
                RunMetrics.CLIENT_COST_SPAN_EMIT,
                RunMetrics.CLIENT_COST_SPAN_CHANNEL_RECEIVE,
                RunMetrics.CLIENT_COST_SPAN_WRITER_BACKPRESSURE,
                RunMetrics.CLIENT_COST_SPAN_PARQUET_WRITE);
        assertThat(spans.get(RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_COMMIT_WAIT).count())
                .as("one observation per committed page, not per batch")
                .isEqualTo(2L);
        assertThat(spans.values()).allSatisfy(s -> assertThat(s.count()).isPositive());
    }

    /** A span nothing ever observed contributes no row at all — never a fabricated all-zero one. */
    @Test
    void aNeverObservedSpanIsOmittedRatherThanReportedAsZero() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordEmit(5_000_000L);

        assertThat(spansOf(metrics)).containsOnlyKeys(RunMetrics.CLIENT_COST_SPAN_EMIT);
    }

    /**
     * Every span is percentile-capable, and (like every other run-summary distribution) its
     * percentiles cover the WHOLE run rather than Micrometer's rolling 2-minute default — see
     * {@link RunMetricsDistributionWindowTest} for the defect that pins.
     */
    @Test
    void everySpanPublishesRunScopedPercentilesNotJustATotal() {
        MockClock clock = new MockClock();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock));

        metrics.recordCheckpointCommitWait(Duration.ofMillis(40).toNanos());
        metrics.recordCheckpointQueueWait(Duration.ofMillis(40).toNanos());
        metrics.recordCheckpointCommit(1L, 1);
        metrics.recordEmit(Duration.ofMillis(40).toNanos());
        metrics.recordQueueWait(metrics.startQueueWaitTimer());
        metrics.recordParquetWrite(Duration.ofMillis(40).toNanos());
        clock.add(Duration.ofMinutes(10));   // comfortably past Micrometer's default 2m expiry

        assertThat(spansOf(metrics).values()).allSatisfy(s -> {
            assertThat(s.p50Ms()).as("%s p50", s.span()).isNotNull();
            assertThat(s.p90Ms()).as("%s p90", s.span()).isNotNull();
            assertThat(s.p99Ms()).as("%s p99", s.span()).isNotNull();
        });
    }

    /**
     * Each span reads its own timer — no two rows are wired to the same meter. Every span is given a
     * DISTINCT observation count precisely so two spans silently sharing one backing timer would be
     * caught here: a shared timer's count would be the SUM of the two spans' counts, not either
     * span's own distinct count, and the touched-timer-id set would also collapse below six.
     */
    @Test
    void eachSpanIsBackedByItsOwnMeter() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordCheckpointCommitWait(1_000_000L);
        for (int i = 0; i < 2; i++) {
            metrics.recordCheckpointQueueWait(1_000_000L);
        }
        for (int i = 0; i < 3; i++) {
            metrics.recordCheckpointCommit(1L, 1);
        }
        for (int i = 0; i < 4; i++) {
            metrics.recordEmit(1_000_000L);
        }
        for (int i = 0; i < 5; i++) {
            metrics.recordQueueWait(metrics.startQueueWaitTimer());
        }
        for (int i = 0; i < 6; i++) {
            metrics.recordParquetWrite(1_000_000L);
        }

        Map<String, Long> touchedCountsByName = metrics.registry().getMeters().stream()
                .filter(Timer.class::isInstance)
                .map(Timer.class::cast)
                .filter(t -> t.count() > 0L)
                .collect(Collectors.toMap(t -> t.getId().getName(), Timer::count));

        assertThat(touchedCountsByName).containsOnly(
                Map.entry("swath.checkpoint.commit.wait", 1L),
                Map.entry("swath.checkpoint.queue.wait", 2L),
                Map.entry("swath.checkpoint.commit.latency", 3L),
                Map.entry("swath.emit.latency", 4L),
                Map.entry("swath.queue.wait", 5L),
                Map.entry("swath.parquet.write.latency", 6L));
    }
}
