/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleType;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Characterization: the stuck/fatal ERROR-CLASSIFICATION mapping.
 *
 * <p>These tokens ({@code stuck_api_timeouts}/{@code stuck_throttle}/{@code stuck_unknown}) are read
 * by external classifiers off the stderr terminal marker AND by the JSON run-summary sidecar, so the
 * mapping is part of the same frozen surface the meter series is. It is pinned here — a
 * decomposition that moves the classifier out of {@code RunMetrics} must reproduce this table
 * exactly, including the noise floor, the dominance tie-break, the since-last-real-progress WINDOW,
 * and the {@code stop_source} routing.
 *
 * <p>Scope note: the sibling CRASH-terminal {@code fatalErrorClass} accessor (default-null,
 * null-ignored, first-writer-wins, and its concurrent-unwind behaviour) is already owned by
 * {@link RunMetricsFatalErrorClassTest} and is deliberately not duplicated here.
 */
final class RunMetricsStuckClassificationCharacterizationTest {

    /**
     * The shared dominance rule, reached via the retry-cap path (which classifies the caller's own
     * local fault counts, so the rule is observable without driving the windowed run-wide signal).
     * Noise floor is 8 events; on a tie the VOTING class wins.
     */
    @ParameterizedTest
    @CsvSource({
            "0, 0, stuck_unknown",
            "7, 0, stuck_unknown",
            "0, 7, stuck_unknown",
            "7, 7, stuck_unknown",
            "8, 0, stuck_api_timeouts",
            "0, 8, stuck_throttle",
            "9, 8, stuck_api_timeouts",
            "8, 8, stuck_throttle",
            "8, 9, stuck_throttle",
            "100, 99, stuck_api_timeouts",
    })
    void dominanceRuleMapsFaultCountsToErrorClass(long attemptTimeouts, long voting, String expected) {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());

        m.recordTransientRetryCapExhaustion(attemptTimeouts, voting);

        assertThat(m.transientRetryCapErrorClass()).isEqualTo(expected);
    }

    @Test
    void retryCapClassDefaultsToUnknownAndIsLastWriterWins() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        assertThat(m.transientRetryCapErrorClass()).isEqualTo("stuck_unknown");

        m.recordTransientRetryCapExhaustion(8L, 0L);
        assertThat(m.transientRetryCapErrorClass()).isEqualTo("stuck_api_timeouts");
        m.recordTransientRetryCapExhaustion(0L, 8L);
        assertThat(m.transientRetryCapErrorClass()).isEqualTo("stuck_throttle");
    }

    @Test
    void runWideClassifierReadsTheWindowSinceTheLastRealProgress() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_unknown");

        for (int i = 0; i < 8; i++) {
            m.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_api_timeouts");

        // Real progress re-arms the window, so the accumulated storm no longer classifies.
        m.recordPage();
        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_unknown");

        for (int i = 0; i < 8; i++) {
            m.recordThrottleEvent(ThrottleType.SLOWDOWN);
        }
        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_throttle");
    }

    @Test
    void everyRealProgressInputReArmsTheClassifierWindow() {
        assertReArms(m -> m.recordPage());
        assertReArms(m -> m.markProgress());
        assertReArms(m -> m.recordEntriesEmitted(1L));
        assertReArms(m -> m.recordProgress(1L));
        assertReArms(m -> m.recordSortSegment(1L, 1));
    }

    /** Throttle EVENTS alone never re-arm the window — only committed progress does. */
    @Test
    void throttleEventsAloneDoNotReArmTheWindow() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 8; i++) {
            m.recordThrottleEvent(ThrottleType.NETWORK);
            m.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_api_timeouts");
    }

    @Test
    void stuckErrorClassRoutesTheRetryCapSourceToTheLocalClassAndEverythingElseToTheWindow() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        m.recordTransientRetryCapExhaustion(0L, 8L);            // local: stuck_throttle
        for (int i = 0; i < 8; i++) {                            // run-wide window: stuck_api_timeouts
            m.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }

        assertThat(m.stuckErrorClass("transient_retry_cap")).isEqualTo("stuck_throttle");
        assertThat(m.stuckErrorClass("liveness_watchdog")).isEqualTo("stuck_api_timeouts");
        assertThat(m.stuckErrorClass(null)).isEqualTo("stuck_api_timeouts");
        assertThat(m.stuckErrorClass("")).isEqualTo("stuck_api_timeouts");
    }

    /** Drives a storm, applies {@code progress}, and asserts the window re-armed to unknown. */
    private static void assertReArms(Consumer<RunMetrics> progress) {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 8; i++) {
            m.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_api_timeouts");

        progress.accept(m);

        assertThat(m.classifyStuckErrorClass()).isEqualTo("stuck_unknown");
    }
}
