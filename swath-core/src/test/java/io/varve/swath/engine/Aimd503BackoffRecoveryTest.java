/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.testkit.LatencyModels;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A sustained window of real {@code 503 SlowDown} backpressure (the AIMD-voting kind, unlike a
 * client-side attempt-timeout) followed by relief.
 *
 * <p><b>The controller invariant.</b> The AIMD controller must back off under sustained store
 * backpressure and then RECOVER when it clears, never livelock pinned at the concurrency floor
 * ({@code T = 1}). A controller that decreases correctly but cannot regrow is as broken as one that
 * never decreases.
 *
 * <p><b>Invariant this guards.</b> Over a sustained-503-then-relief timeline the AIMD gauge (1) casts
 * genuine down-votes and multiplicatively reduces {@code T} during the throttle window, then (2) once
 * the store heals, elapses the compressed clean window and grows {@code T} back OFF the floor
 * with stealing re-enabled — and the run completes byte-exact. The {@link GaugeClock} is what makes
 * the recovery half observable offline: production's 10&nbsp;s clean window outlives a fast run, so an
 * uncompressed offline run could only ever show the back-off, never the recovery.
 *
 * <p>Deep tier (real ms-scale page latency so the throttle window and recovery span wall-clock time).
 * Deterministic (fixed seeds, compressed clock); asserts robust thresholds, never exact counts.
 */
@Tag("deep")
final class Aimd503BackoffRecoveryTest {

    private static final int OBJECTS = 24_000;   // ~24 bulk pages at MAX_KEYS
    private static final int WORKERS = 16;
    private static final int MAX_KEYS = 1000;

    private static final long LAT_SEED = 0x5031_A1DL;
    private static final Duration PAGE_MIN = Duration.ofMillis(2);
    private static final Duration PAGE_MAX = Duration.ofMillis(3);

    // Sustained 503 window over the fetcher's call-index sequence: long enough to force several AIMD
    // down-votes, with a healthy tail of calls after it for the recovery half.
    private static final int SLOWDOWN_WINDOW_START = 4;
    private static final int SLOWDOWN_WINDOW_LENGTH = 16;

    /**
     * Compressed gauge clock: 2000× real time turns the 10&nbsp;s clean/recovery window into
     * ~5&nbsp;ms and the +1 growth pace into ~0.5&nbsp;ms, so the post-relief page tail (each ~2&nbsp;ms)
     * spans many clean windows and the recovery ramp is observable in a fast run. The AIMD down-vote is
     * immediate (not windowed), so the scale only governs how eagerly recovery ramps.
     */
    private static final long CLOCK_SCALE = 2000L;

    @Test
    @Timeout(120)
    void sustainedThrottleThenRelief_aimdBacksOffAndRecovers_noFloorLivelock(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = keyspace();
        MockPageFetcher engineFetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .latencyModel(LatencyModels.uniformFast(LAT_SEED, PAGE_MIN, PAGE_MAX))
                .interceptor(LatencyModels.stormFaultWindow(
                        SLOWDOWN_WINDOW_START, SLOWDOWN_WINDOW_LENGTH,
                        () -> ThrottleException.slowDown("503 SlowDown storm")))
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AdaptiveControlHarness.Result r = AdaptiveControlHarness.run(
                "r503", keyspace, engineFetcher, EngineToggles.DEFAULT, WORKERS, MAX_KEYS,
                /* root range */ null, GaugeClock.scaled(CLOCK_SCALE, ConcurrencyGauge.SHED_WINDOW_BASE_NANOS),
                new RetryConfig(RetryPolicy.BOUNDED, ms -> { }), new CancellationToken(), metrics, dir);

        // Byte-exact throughout — a 503 throttle window is a pacing event, never a correctness one.
        AdaptiveControlHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(r.completed()).as("the throttled run drains to quiescence").isTrue();

        RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);

        // BACK-OFF: genuine AIMD down-votes fired and multiplicatively reduced the target.
        assertThat(diag.aimdVotes())
                .as("a genuine 503 storm casts real AIMD down-votes").isGreaterThan(0L);
        assertThat(diag.aimdTargetReductions())
                .as("the down-votes multiplicatively reduced the concurrency target").isGreaterThan(0L);

        // RECOVER: once the store healed, the compressed clean window elapsed, stealing re-enabled, and
        // T grew back OFF the floor — no livelock pinned at T = 1.
        assertThat(r.gauge().isStealingAllowed())
                .as("stealing re-enabled after the throttle cleared").isTrue();
        assertThat(r.effectiveT())
                .as("AIMD recovered T off the floor after relief (no floor livelock)")
                .isGreaterThan(1);
    }

    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>(OBJECTS);
        for (int i = 0; i < OBJECTS; i++) {
            keys.add(("data/obj-%06d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }
}
