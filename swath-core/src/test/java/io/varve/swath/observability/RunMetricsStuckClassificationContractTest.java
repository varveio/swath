/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleType;
import org.junit.jupiter.api.Test;

/**
 * STUCK/TRANSIENT_RETRY_CAP/CRASH {@code error_class} classification — the {@link
 * StuckErrorClassifier} seam (extracted from {@code RunMetrics}; split from {@code
 * RunMetricsContractTest}): the run-wide windowed classifier ({@code classifyStuckErrorClass}),
 * the local one-fetch TRANSIENT_RETRY_CAP disposition, and the source-routed {@code
 * stuckErrorClass(String)} helper. Moved verbatim, no assertion changes.
 */
final class RunMetricsStuckClassificationContractTest {

    // ---- stuck error_class classification -----------------------------------------

    @Test
    void classifyStuckErrorClassIsApiTimeoutsWhenTimeoutsDominateAndRealProgressIsStarved() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // An attempt-timeout storm above the noise floor, with ZERO committed real progress.
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(metrics.realProgressSignal()).isZero();
        assertThat(metrics.classifyStuckErrorClass()).isEqualTo("stuck_api_timeouts");
    }

    @Test
    void classifyStuckErrorClassIsThrottleWhenRealFiveOhThreesDominate() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);   // voting (real S3 backpressure)
        }
        assertThat(metrics.classifyStuckErrorClass()).isEqualTo("stuck_throttle");
    }

    @Test
    void classifyStuckErrorClassIsUnknownBelowTheNoiseFloor() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 3; i++) {   // < STUCK_MIN_EVENTS (8): one wedged fetch, not a storm
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(metrics.classifyStuckErrorClass()).isEqualTo("stuck_unknown");
    }

    @Test
    void classifyStuckErrorClassIsNotApiTimeoutsWhenRealProgressKeptUp() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 10; i++) {
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        // A tail of timeouts on a run that still committed MORE real progress than timeouts is not
        // starved — this guards against mislabeling a healthy-progress run as a timeout wedge.
        for (int i = 0; i < 20; i++) {
            metrics.recordPage();
        }
        assertThat(metrics.classifyStuckErrorClass()).isNotEqualTo("stuck_api_timeouts");
    }

    // ---- proximate (since-last-real-progress) classification --------------------

    /**
     * The classifier must not sum timeout/real-progress activity across the WHOLE run: a run that
     * commits ~2 billion units of real progress (a healthy listing run), THEN wedges on a pure
     * attempt-timeout tail with no further real progress, would have its billions of committed
     * real-progress units dominate the timeout total under a cumulative count, misreading {@code
     * stuck_unknown}. The windowed classifier isolates activity since the last real-progress
     * advance, so the timeout tail (the actual proximate cause of the wedge) dominates that
     * window and correctly reads {@code stuck_api_timeouts}.
     */
    @Test
    void classifyStuckErrorClassIsApiTimeoutsOnLateWedgeAfterBillionsOfRealProgress() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordPage();
        metrics.recordEntriesEmitted(2_000_000_000L);   // billions of real-progress units, healthy tail
        assertThat(metrics.realProgressSignal()).isGreaterThan(1_000_000_000L);
        for (int i = 0; i < 20; i++) {                  // the late, pure attempt-timeout tail
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(metrics.classifyStuckErrorClass()).isEqualTo("stuck_api_timeouts");
    }

    /**
     * Regression companion to the acceptance test above: the same late-wedge-after-billions-of-progress
     * shape, but a real-503/5xx storm dominates the tail instead of attempt-timeouts — must still read
     * {@code stuck_throttle}, proving the windowed classifier didn't just special-case timeouts.
     */
    @Test
    void classifyStuckErrorClassIsThrottleOnLateWedgeAfterBillionsOfRealProgress() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordPage();
        metrics.recordEntriesEmitted(2_000_000_000L);
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        }
        assertThat(metrics.classifyStuckErrorClass()).isEqualTo("stuck_throttle");
    }

    /**
     * Isolates the windowing behavior directly: an early attempt-timeout blip (above the noise floor
     * on its own) is absorbed by a real-progress advance right after it, then only a handful of
     * timeouts (below the noise floor) occur since that advance. A cumulative classifier would sum
     * all 23 timeouts (>= floor, real progress of 1 < 23) and misclassify this as
     * {@code stuck_api_timeouts}; the windowed classifier only counts the 3 since the last real
     * progress, which is below the noise floor.
     */
    @Test
    void classifyStuckErrorClassOnlyCountsActivitySinceTheLastRealProgress() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        metrics.recordPage();   // real progress advances -- snapshots the tallies, resets the window
        for (int i = 0; i < 3; i++) {   // below STUCK_MIN_EVENTS since the reset
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        assertThat(metrics.classifyStuckErrorClass()).isEqualTo("stuck_unknown");
    }

    // ---- TRANSIENT_RETRY_CAP is a LOCAL, one-fetch disposition ------------------------------------

    /**
     * Under 64-worker concurrency, ONE range's fetch exhausts its own local attempt-timeout cap
     * while 63 OTHER workers keep committing
     * real progress. Interleaving {@code recordThrottleEvent(ATTEMPT_TIMEOUT)} (the wedged range's
     * consecutive retries) with UNRELATED {@code recordPage()} calls (the healthy workers) would make
     * {@link RunMetrics#classifyStuckErrorClass()} read {@code stuck_unknown} (each unrelated
     * {@code recordPage} re-snapshots the run-wide window and erases the wedged fetch's own timeout
     * history) — but the {@code CancelSource.TRANSIENT_RETRY_CAP} terminal must NOT use that run-wide
     * signal at all. It must read {@link RunMetrics#transientRetryCapErrorClass()}, populated by the
     * cap-tripping fetch itself (via {@link RunMetrics#recordTransientRetryCapExhaustion}) from ITS OWN
     * local fault tally — which this interleaving cannot touch.
     */
    @Test
    void transientRetryCapErrorClassIsApiTimeoutsDespiteInterleavedUnrelatedProgress() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // The wedged range's own 9 consecutive attempt-timeout faults, interleaved with 9 UNRELATED
        // healthy-worker page completions.
        for (int i = 0; i < 9; i++) {
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
            metrics.recordPage();
        }
        // Confirm the run-wide window IS in fact corrupted by the interleaving (the bug this test
        // guards against) -- classifyStuckErrorClass() must NOT be what the TRANSIENT_RETRY_CAP path
        // reads.
        assertThat(metrics.classifyStuckErrorClass()).isNotEqualTo("stuck_api_timeouts");
        // What the cap-tripping fetch itself records at its own cancel site: 9 attempt-timeout faults,
        // 0 voting faults, from ITS OWN local history.
        metrics.recordTransientRetryCapExhaustion(9, 0);
        assertThat(metrics.transientRetryCapErrorClass()).isEqualTo("stuck_api_timeouts");
    }

    /**
     * Companion to the above: the cap counts EVERY {@code ThrottleException} kind (not just
     * attempt-timeouts), so a cap exhausted by a SLOWDOWN storm must classify as {@code stuck_throttle},
     * not {@code stuck_api_timeouts} — same interleaved-unrelated-progress shape.
     */
    @Test
    void transientRetryCapErrorClassIsThrottleWhenVotingFaultsExhaustedTheCap() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 9; i++) {
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
            metrics.recordPage();
        }
        metrics.recordTransientRetryCapExhaustion(0, 9);
        assertThat(metrics.transientRetryCapErrorClass()).isEqualTo("stuck_throttle");
    }

    @Test
    void transientRetryCapErrorClassBelowNoiseFloorIsUnknown() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordTransientRetryCapExhaustion(3, 0);   // < STUCK_MIN_EVENTS (8)
        assertThat(metrics.transientRetryCapErrorClass()).isEqualTo("stuck_unknown");
    }

    @Test
    void transientRetryCapErrorClassDefaultsToUnknownWhenNeverRecorded() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        assertThat(metrics.transientRetryCapErrorClass()).isEqualTo("stuck_unknown");
    }

    // ---- stuckErrorClass(String) — the ONE source-routed helper shared by ListCommand's
    // list_stuck_stop marker and the JSON run-summary sidecar ---------------------

    @Test
    void stuckErrorClassRoutesToTheFetchLocalClassForTheTransientRetryCapTag() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // The run-wide windowed signal disagrees (unrelated real 503 storm), proving this reads
        // the LOCAL cap tally, not classifyStuckErrorClass() — same shape as
        // transientRetryCapErrorClassIsApiTimeoutsDespiteInterleavedUnrelatedProgress above.
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        }
        metrics.recordTransientRetryCapExhaustion(9, 0);
        assertThat(metrics.stuckErrorClass("transient_retry_cap")).isEqualTo("stuck_api_timeouts");
    }

    @Test
    void stuckErrorClassRoutesToTheWindowedClassForTheLivenessWatchdogTag() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        }
        assertThat(metrics.stuckErrorClass("liveness_watchdog")).isEqualTo("stuck_throttle");
    }

    @Test
    void stuckErrorClassRoutesToTheWindowedClassForANullOrUnrecognizedTag() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        for (int i = 0; i < 20; i++) {
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        // An unattributed STUCK (source == null, e.g. seed_interrupt/timebox — should not happen on
        // a production STUCK cancel site, but defensive) routes the same as every non-cap source.
        assertThat(metrics.stuckErrorClass(null)).isEqualTo("stuck_api_timeouts");
        assertThat(metrics.stuckErrorClass("seed_interrupt")).isEqualTo("stuck_api_timeouts");
    }
}
