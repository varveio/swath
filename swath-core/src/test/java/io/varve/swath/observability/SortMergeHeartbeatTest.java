/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link SortMergeHeartbeat} wraps a scheduled executor with a direct {@link RunMetrics} read on
 * every tick, inside a try-with-resources around the whole merge. Covers its lifecycle: the final
 * synchronous {@code close()} line + executor termination, idempotent {@code close()}, a
 * metrics-read failure inside a scheduled tick being swallowed (never propagating out of the tick
 * or out of {@code close()}, and never silently killing the recurring schedule — see {@code
 * reportSafely}'s javadoc), and the heartbeat thread being a daemon.
 */
final class SortMergeHeartbeatTest {

    /**
     * A {@link SimpleMeterRegistry} whose {@code swath.sort.merge.passes} counter throws on every
     * {@code count()} read — simulates {@code RunMetrics#sortMergePassesCount()} failing mid-tick,
     * the exact failure {@code reportSafely} must swallow. Every other counter behaves normally
     * (delegates to the real {@link SimpleMeterRegistry}), so {@code RunMetrics}'s own constructor
     * (which registers a whole set of other meters) is unaffected.
     */
    private static final class ThrowingPassesCounterRegistry extends SimpleMeterRegistry {
        @Override
        protected Counter newCounter(Meter.Id id) {
            if ("swath.sort.merge.passes".equals(id.getName())) {
                return new Counter() {
                    @Override
                    public void increment(double amount) {
                        // no-op: only the read side (count()) needs to fail for this test
                    }

                    @Override
                    public double count() {
                        throw new IllegalStateException("boom: swath.sort.merge.passes read failed");
                    }

                    @Override
                    public Meter.Id getId() {
                        return id;
                    }
                };
            }
            return super.newCounter(id);
        }
    }

    private static ScheduledExecutorService executorOf(SortMergeHeartbeat heartbeat) throws Exception {
        Field f = SortMergeHeartbeat.class.getDeclaredField("executor");
        f.setAccessible(true);
        return (ScheduledExecutorService) f.get(heartbeat);
    }

    @Test
    void closeEmitsAFinalLineAndTerminatesTheExecutor() throws Exception {
        Logger logger =
                (Logger) LoggerFactory.getLogger(SortMergeHeartbeat.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordProgress(500L);
        // A long interval so no periodic tick fires on its own during the test — only close()'s
        // own synchronous final report should produce a line.
        SortMergeHeartbeat heartbeat = SortMergeHeartbeat.start(metrics, Duration.ofDays(1));
        ScheduledExecutorService executor = executorOf(heartbeat);
        try {
            heartbeat.close();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        assertThat(executor.isShutdown())
                .as("close() must shut down the heartbeat's scheduler").isTrue();
        assertThat(executor.isTerminated())
                .as("close() awaits termination before returning — no lingering thread").isTrue();

        String finalLine = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("sort_merge_progress"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no sort_merge_progress line emitted by close()"));
        assertThat(finalLine).contains("progress_units=500");
    }

    @Test
    void closeIsIdempotent() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SortMergeHeartbeat heartbeat = SortMergeHeartbeat.start(metrics, Duration.ofDays(1));
        heartbeat.close();

        assertThatCode(heartbeat::close)
                .as("a second close() must not throw").doesNotThrowAnyException();
    }

    @Test
    void heartbeatThreadIsADaemon() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SortMergeHeartbeat heartbeat = SortMergeHeartbeat.start(metrics, Duration.ofDays(1));
        try {
            ScheduledExecutorService executor = executorOf(heartbeat);
            AtomicReference<Thread> threadRef = new AtomicReference<>();
            executor.submit(() -> threadRef.set(Thread.currentThread())).get(5, TimeUnit.SECONDS);

            assertThat(threadRef.get()).isNotNull();
            assertThat(threadRef.get().isDaemon())
                    .as("the heartbeat scheduler thread must be a daemon, like every other swath "
                            + "background reporter (RunProgressReporter/JsonRunSummaryWriter)")
                    .isTrue();
            assertThat(threadRef.get().getName()).isEqualTo("swath-sort-merge-heartbeat");
        } finally {
            heartbeat.close();
        }
    }

    /**
     * {@code reportSafely}'s contract: a {@code RunMetrics} read failing mid-tick must never
     * propagate — out of the scheduled tick (which would otherwise silently kill ALL future ticks:
     * {@code ScheduledThreadPoolExecutor} de-schedules a periodic task after an uncaught exception)
     * or out of {@code close()}. Drives a tiny interval against a registry whose
     * {@code swath.sort.merge.passes} counter always throws, and asserts (a) the schedule survives
     * — at least two failures are individually logged and swallowed, proving ticks kept recurring
     * — and (b) {@code start(...)}/{@code close()} both complete cleanly despite every tick failing.
     */
    @Test
    void reportingFailureIsSwallowedAndTheScheduleSurvivesRepeatedFailures() {
        Logger logger =
                (Logger) LoggerFactory.getLogger(SortMergeHeartbeat.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);

        RunMetrics metrics = new RunMetrics(new ThrowingPassesCounterRegistry());
        SortMergeHeartbeat heartbeat = SortMergeHeartbeat.start(metrics, Duration.ofMillis(20));
        try {
            assertThatCode(() -> await().atMost(5, TimeUnit.SECONDS).until(() ->
                    failureMarkerCount(appender) >= 2))
                    .as("the recurring schedule must survive repeated tick failures — a bare "
                            + "uncaught exception from a scheduleAtFixedRate task would silently "
                            + "de-schedule it after the FIRST failure")
                    .doesNotThrowAnyException();
        } finally {
            assertThatCode(heartbeat::close)
                    .as("close() must complete cleanly even though every tick (including its own "
                            + "final synchronous report) fails to read metrics")
                    .doesNotThrowAnyException();
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        assertThat(failureMarkerCount(appender)).isGreaterThanOrEqualTo(2);
    }

    private static long failureMarkerCount(ListAppender<ILoggingEvent> appender) {
        List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        return messages.stream().filter(m -> m.startsWith("sort_merge_heartbeat_report_failed")).count();
    }
}
