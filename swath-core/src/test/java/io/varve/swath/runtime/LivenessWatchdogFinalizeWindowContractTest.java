/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression guard: a finalize window (footer/fsync + full-file MD5 in the manifest pass) that spans
 * far longer than {@code --stall-timeout} must never trip {@link LivenessWatchdog}'s total-freeze
 * tripwire so long as real bytes are still moving, while a genuinely frozen finalize (no bytes moving)
 * still must. {@link ListRunner#md5HexWithLivenessProgress} emits a BYTE-KEYED
 * {@link RunMetrics#markProgress()} tick once per byte stride consumed, so real work moving keeps the
 * watchdog alive, while a true byte-freeze (no stride crossed) emits nothing and STILL trips the
 * watchdog. This class drives the REAL {@link LivenessWatchdog#tick()} escalation ladder over an
 * injected fake clock and a REAL {@link RunMetrics#progressSignal()} supplier, and proves BOTH
 * directions.
 *
 * <p>It complements — does not duplicate — {@code ListRunnerFinalizeProgressTest} (which pins the
 * byte-keyed emission mechanism in isolation) and {@code LivenessWatchdogPhaseAwareContractTest}
 * (phase-aware trip/no-trip in general): this class runs the REAL finalize digest path through the
 * watchdog's trip logic.
 *
 * <p><b>The two arms are the critical comparison.</b> {@link #slowButProgressingFinalizeWindowNeverHalts()} and
 * {@link #frozenFinalizeWindowWithZeroTicksStillHalts()} share the same driver, the same step count,
 * and the same per-step wall-clock advance; the ONLY difference is whether byte-keyed ticks arrive.
 * With ticks the ladder stays HEALTHY across a window many stall-timeouts long; without ticks (a
 * genuine freeze) the SAME window escalates to halt — proving the mechanism is NOT a disguised timer:
 * remove the real byte progress and the watchdog still kills a wedged JVM.
 */
final class LivenessWatchdogFinalizeWindowContractTest {

    private static final int STUCK_CODE = 75;

    /**
     * One "byte stride" of finalize wall time: just under the stall window. So a finalize that emits a
     * byte-keyed tick every stride advances the clock by ~a full window between ticks — the sharpest
     * slow-but-progressing shape — while a finalize that emits NOTHING accumulates elapsed time toward a
     * trip at exactly the same rate.
     */
    private static final long STEP_NANOS = Duration.ofSeconds(119).toNanos();

    /** Enough strides that the simulated finalize spans many stall windows (~119 min at 119s/step). */
    private static final int STEPS = 60;

    private static long secs(long s) {
        return Duration.ofSeconds(s).toNanos();
    }

    // ---- Arm 1 (the fix): a slow-but-progressing finalize NEVER halts -----------------------------

    /**
     * Model the manifest-finalize window as a sequence of byte strides: before each poll a byte-keyed
     * {@link RunMetrics#markProgress()} tick fires (exactly what {@link
     * ListRunner#md5HexWithLivenessProgress} does per stride) and the clock jumps forward by nearly a
     * full stall window. The window spans MANY stall-timeouts of wall time, far longer than
     * {@code --stall-timeout} itself — yet because real bytes keep moving the ladder must stay HEALTHY
     * the whole way and never cancel/interrupt/halt.
     *
     * <p>The sibling halt arm below is the identical harness with the ticks removed: with no
     * byte-keyed progress, the same clock advance freezes the signal and the ladder escalates instead.
     */
    @Test
    void slowButProgressingFinalizeWindowNeverHalts() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        for (int i = 1; i <= STEPS; i++) {
            h.metrics.markProgress();          // one byte-keyed finalize tick (bytes crossed a stride)
            h.now.addAndGet(STEP_NANOS);
            h.watchdog.tick();
            assertThat(h.watchdog.state())
                    .as("byte-keyed finalize progress keeps the ladder HEALTHY (stride %d of %d)", i, STEPS)
                    .isEqualTo(LivenessWatchdog.State.HEALTHY);
        }
        assertThat(h.token.isCancelled())
                .as("a progressing finalize window must never be cooperatively cancelled").isFalse();
        assertThat(h.interrupted).isFalse();
        assertThat(h.dumped).isFalse();
        assertThat(h.haltCode).as("halt() must NEVER be invoked on a live, progressing finalize").hasValue(-1);
    }

    // ---- Arm 2 (the honesty guard): a truly frozen finalize STILL halts ---------------------------

    /**
     * The SAME driver and the SAME per-step wall-clock advance as the arm above, but ZERO byte-keyed
     * ticks — i.e. bytes stopped moving (a genuine freeze). The ladder must still escalate
     * HEALTHY -> CANCELLED(STUCK) -> INTERRUPTED -> HALTED and invoke halt with the stuck exit code. If
     * this arm could not fail, the mechanism would be a timer in disguise (an unconditional keep-alive)
     * rather than honest byte-keyed progress; because it DOES halt when no bytes move, the liveness
     * tick is proven byte-keyed.
     */
    @Test
    void frozenFinalizeWindowWithZeroTicksStillHalts() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        for (int i = 1; i <= STEPS; i++) {
            // No markProgress(): the finalize is truly wedged (bytes are not moving).
            h.now.addAndGet(STEP_NANOS);
            h.watchdog.tick();
            if (h.watchdog.state() == LivenessWatchdog.State.HALTED) {
                break;
            }
        }
        assertThat(h.watchdog.state())
                .as("a finalize window with no byte progress must escalate all the way to halt")
                .isEqualTo(LivenessWatchdog.State.HALTED);
        assertThat(h.token.isCancelled()).isTrue();
        assertThat(h.token.stopReason()).isEqualTo(StopReason.STUCK);
        assertThat(h.interrupted).isTrue();
        assertThat(h.dumped).isTrue();
        assertThat(h.haltCode).as("halt() must fire with the stuck exit code when bytes truly stop")
                .hasValue(STUCK_CODE);
    }

    // ---- Arm 3 (real path): the REAL finalize digest drives the watchdog through its trip logic ----

    // 16 MiB / 256 KiB stride, with the reader's 1 MiB buffer, yields ~16 byte-keyed ticks — enough
    // that the replayed window spans many stall timeouts. Small enough to be a cheap temp fixture.
    private static final int FILE_BYTES = 16 * 1024 * 1024;
    private static final long SMALL_STRIDE = 256 * 1024;
    private static final long WIDE_STRIDE = 1L << 30;   // wider than the whole file => zero strides crossed

    /**
     * End-to-end: the REAL {@link ListRunner#md5HexWithLivenessProgress} over a multi-MiB temp file
     * advances {@link RunMetrics#progressSignal()} monotonically, proportional to bytes/stride; and that
     * SAME count of real byte-keyed ticks, replayed through the watchdog's {@link LivenessWatchdog#tick()}
     * ladder with a per-tick clock advance of nearly a full stall window, keeps the ladder HEALTHY across
     * a window many stall-timeouts long. The tick count that sizes the window comes from the real digest
     * path, so a finalize with no byte-keyed emission would drive zero progressing polls and fail the
     * {@code ticks >= } floor below.
     */
    @Test
    void realFinalizeDigestKeepsWatchdogAliveAcrossAWindowLongerThanStall(@TempDir Path dir)
            throws IOException {
        RunMetrics measure = new RunMetrics(new SimpleMeterRegistry());
        Path part = writeRandomFile(dir.resolve("part-0.parquet"), FILE_BYTES);

        long before = measure.progressSignal();
        ListRunner.md5HexWithLivenessProgress(part, measure, SMALL_STRIDE);
        long ticks = measure.progressSignal() - before;

        assertThat(ticks)
                .as("the real finalize digest must emit byte-keyed ticks proportional to bytes/stride")
                .isGreaterThanOrEqualTo((long) (FILE_BYTES / (2 * 1024 * 1024)));   // >= ~8 for 16 MiB

        // Replay exactly `ticks` real byte-keyed markProgress() ticks (the very call the digest makes per
        // stride) through the REAL watchdog ladder; each tick advances the fake clock by ~a full window.
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        for (long i = 1; i <= ticks; i++) {
            h.metrics.markProgress();
            h.now.addAndGet(STEP_NANOS);
            h.watchdog.tick();
            assertThat(h.watchdog.state())
                    .as("real finalize tick %d/%d keeps the watchdog HEALTHY", i, ticks)
                    .isEqualTo(LivenessWatchdog.State.HEALTHY);
        }
        assertThat(h.haltCode)
                .as("a finalize spanning %d stall windows of real byte progress must never halt", ticks)
                .hasValue(-1);
    }

    /**
     * Honesty guard on the real path: a digest whose byte stride is WIDER than the whole file crosses no
     * stride, so {@link ListRunner#md5HexWithLivenessProgress} emits ZERO ticks — the same signal a truly
     * stalled read (no bytes moving) presents. Fed that frozen signal, the watchdog must still escalate to
     * halt. Proves the finalize tick is genuinely byte-keyed (no free/unconditional tick) end-to-end.
     */
    @Test
    void realFinalizeWithNoByteStrideCrossedLeavesWatchdogFreeToHalt(@TempDir Path dir)
            throws IOException {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        Path part = writeRandomFile(dir.resolve("part-0.parquet"), FILE_BYTES);

        long before = h.metrics.progressSignal();
        ListRunner.md5HexWithLivenessProgress(part, h.metrics, WIDE_STRIDE);
        assertThat(h.metrics.progressSignal() - before)
                .as("no byte stride crossed => no free liveness tick (byte-keyed, not a timer)").isZero();

        // The signal is frozen (no ticks), so the real watchdog ladder must trip and halt.
        for (int i = 1; i <= STEPS; i++) {
            h.now.addAndGet(STEP_NANOS);
            h.watchdog.tick();
            if (h.watchdog.state() == LivenessWatchdog.State.HALTED) {
                break;
            }
        }
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
        assertThat(h.haltCode).hasValue(STUCK_CODE);
        assertThat(h.token.stopReason()).isEqualTo(StopReason.STUCK);
    }

    private static Path writeRandomFile(Path path, int bytes) throws IOException {
        byte[] data = new byte[bytes];
        new Random(254).nextBytes(data);
        Files.write(path, data);
        return path;
    }
}
