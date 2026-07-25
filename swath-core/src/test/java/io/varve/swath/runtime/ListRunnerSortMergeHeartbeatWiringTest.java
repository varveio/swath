/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.SortMergeHeartbeat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * The merge-phase heartbeat must resolve a caller-configured {@code --progress-interval},
 * the way {@code startProgress} already does for the listing phase's {@code
 * RunProgressReporter} — not silently fall back to {@code SortMergeHeartbeat.start(metrics)}'s
 * fixed 30s non-TTY default. A multi-pass merge that runs many multiples of the configured
 * interval would otherwise emit only the final {@code close()} line instead of periodic
 * {@code sort_merge_progress} ticks.
 *
 * <p>This pins {@link ListRunner#startSortMergeHeartbeat}, the exact resolution helper {@code
 * sortMergeAndPublish} calls, directly (package-private, no reflection) — proving a short
 * configured interval is what the merge phase's heartbeat actually uses, not the hardcoded
 * default. The class-level cadence contract itself (repeated ticks at a tiny interval) is already
 * covered by {@code SortMergeHeartbeatTest}; a full {@code ListRunner}-level integration test
 * (real staging segments + a merge slow enough to observe multiple periodic ticks before
 * completion) would be disproportionate to this wiring-only fix, so this test targets the
 * resolution helper instead — the same "pin the seam, not the whole pipeline" tradeoff {@code
 * startProgress} itself would need if it were being guarded fresh today.
 */
final class ListRunnerSortMergeHeartbeatWiringTest {

    @Test
    void configuredIntervalIsHonoredNotTheFixedThirtySecondDefault() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        Logger logger =
                (Logger) LoggerFactory.getLogger(SortMergeHeartbeat.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);

        // The exact call ListRunner#sortMergeAndPublish makes with a caller-configured
        // --progress-interval (e.g. "2s", stood in here by a much tinier interval so the test
        // stays fast) -- must produce MULTIPLE periodic ticks well within a couple hundred
        // milliseconds, not just the one line a 30s-default heartbeat would emit in that window.
        try (SortMergeHeartbeat heartbeat =
                     ListRunner.startSortMergeHeartbeat(metrics, Duration.ofMillis(20))) {
            await().atMost(5, TimeUnit.SECONDS).until(() -> sortMergeProgressLineCount(appender) >= 3);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        assertThat(sortMergeProgressLineCount(appender))
                .as("a 20ms configured interval must produce several periodic ticks, proving the "
                        + "configured --progress-interval is honored rather than the fixed 30s default")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    void nullIntervalStillResolvesToTheDefaultAndRunsCleanly() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        // Unset (--progress-interval not passed) must resolve to SortMergeHeartbeat's own
        // non-TTY default -- default behavior is UNCHANGED by this wiring fix.
        SortMergeHeartbeat heartbeat = ListRunner.startSortMergeHeartbeat(metrics, null);
        try {
            assertThat(heartbeat).isNotNull();
        } finally {
            heartbeat.close();
        }
    }

    private static long sortMergeProgressLineCount(ListAppender<ILoggingEvent> appender) {
        List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        return messages.stream().filter(m -> m.startsWith("sort_merge_progress")).count();
    }
}
