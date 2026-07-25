/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Adversarial CONC guard for the shed-window rollover under concurrency. Many
 * {@code GaugedFetcher}-style worker threads call {@link ConcurrencyGauge#onTransientTimeout()}
 * simultaneously across a shed-window boundary; the invariant is that the {@code swath.aimd.timeout_shed}
 * meter increases by AT MOST 1 per real window — the "at most one shed per jittered ~30 s window"
 * contract the regrowth pacing depends on.
 *
 * <p>Do not reset the counters and the boolean one-shed latch UNCONDITIONALLY once the window has
 * elapsed: two threads can both pass the elapsed check at a boundary — one fires a shed and latches
 * {@code shedFiredThisWindow=true}, the other then re-runs the reset and clears the latch back to
 * {@code false} inside the SAME real window, letting a later timeout fire a SECOND shed. Electing a
 * single roller via {@code compareAndSet} on {@code shedWindowStartNs} (mirroring the latency-baseline
 * decay-window roller) is necessary but not sufficient on its own.
 *
 * <p>Do not have the single-roller CAS publish the new {@code shedWindowStartNs} before resetting the
 * counters/latch, or clear the latch last: a losing (non-rolling) thread can see the NEW window start
 * (fast-path returns without rolling) while the timeout/success counters are still the OLD window's
 * stale values, tip a near-gate stale count over the gate, and fire a shed — which the CAS winner's
 * subsequent (delayed) latch-clear then clobbers, again allowing a second shed in the same real window.
 * The reachable case: a window sits at exactly {@code gate - 1} timeouts and never sheds, then a
 * concurrent burst crosses the boundary.
 *
 * <p>The correct design replaces the clearable boolean latch with a GENERATION-STAMPED latch
 * ({@code shedFiredWindowNs}, holding the {@code shedWindowStartNs} value of the window that last shed).
 * A shed fires only if the CURRENT window's start differs from the stamp, CAS-elected on the stamp
 * itself. This is immune to counter-reset ordering entirely — the ≤1-shed-per-window invariant holds no
 * matter how stale the counters are when a losing thread reads them. Reverting to a boolean latch makes
 * {@link #gateMinusOneWindow_thenConcurrentBurst_shedsAtMostOnce()} go RED.
 *
 * <p>Uses the injected-clock + fixed-shed-window seam (no real sleep). {@code @Tag("deep")} because it
 * is a stress/timing test run in the deep tier, not the per-commit tier.
 */
@Tag("deep")
final class ConcurrencyGaugeShedRolloverConcTest {

    /** A fixed shed-window length so window rolls are driven purely by the injected clock. */
    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;

    private static final int THREADS = 96;
    private static final int WINDOWS = 500;
    /** Enough per-thread timeouts that the storm gate is met even after a (buggy) mid-window counter reset. */
    private static final int CALLS_PER_THREAD_PER_WINDOW = 4;

    /** Null-safe read: the counter does not exist until the first shed fires. */
    private static double timeoutSheds(RunMetrics metrics) {
        Counter c = metrics.registry().find("swath.aimd.timeout_shed").counter();
        return c == null ? 0.0 : c.count();
    }

    @Test
    @Timeout(120)
    void concurrentBoundaryCrossings_shedAtMostOncePerWindow()
            throws InterruptedException, BrokenBarrierException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)
        // Small Tmax → small timeout gate (max(3, ceil(0.3*T))): a shed fires after only a few timeouts,
        // while most of the many worker threads are still executing their (racing) first roll of the
        // window — the exact interleaving in which the buggy unconditional reset re-clears the latch.
        ConcurrencyGauge gauge = new ConcurrencyGauge(8, metrics, now::get, () -> WINDOW, ConcurrencyGauge.defaultInitialT(8));

        // A shared barrier of workers + this coordinator thread. Between windows the workers park at
        // `start` while the coordinator advances the clock exactly ONE window boundary; then all
        // workers are released together to slam onTransientTimeout at that boundary.
        CyclicBarrier start = new CyclicBarrier(THREADS + 1);
        CyclicBarrier end = new CyclicBarrier(THREADS + 1);
        AtomicReference<Throwable> workerError = new AtomicReference<>();

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            Thread t = new Thread(() -> {
                try {
                    for (int w = 0; w < WINDOWS; w++) {
                        start.await();
                        for (int k = 0; k < CALLS_PER_THREAD_PER_WINDOW; k++) {
                            gauge.onTransientTimeout();   // zero successes → starvation gate always met
                        }
                        end.await();
                    }
                } catch (Throwable th) {
                    workerError.compareAndSet(null, th);
                }
            }, "shed-storm-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }

        try {
            for (int w = 0; w < WINDOWS; w++) {
                double before = timeoutSheds(metrics);
                now.addAndGet(WINDOW + 1);   // cross exactly this window's boundary while workers park
                start.await();               // release all workers into the window simultaneously
                end.await();                 // wait for every worker to finish this window's storm
                double delta = timeoutSheds(metrics) - before;
                assertThat(delta)
                        .as("window %d: at most one shed per real window", w)
                        .isLessThanOrEqualTo(1.0);
            }
        } finally {
            // Unblock any workers still parked at a barrier (they are if the assertion above threw
            // mid-run) so cleanup does not hang past the @Timeout and mask the real failure.
            for (Thread t : workers) {
                t.interrupt();
            }
            for (Thread t : workers) {
                t.join(2_000);
            }
        }

        // On the clean (all-windows-completed) path workers exit their loop normally, so no worker
        // observes the cleanup interrupt; a genuine worker fault would surface here.
        Throwable err = workerError.get();
        assertThat(err == null || err instanceof InterruptedException || err instanceof BrokenBarrierException)
                .as("no unexpected worker fault: %s", err)
                .isTrue();
        // Sanity: the storm did shed (the guard above is meaningful, not vacuously satisfied by no sheds).
        assertThat(timeoutSheds(metrics)).as("the sustained storm sheds").isGreaterThan(0.0);
    }

    /**
     * A window that sits at exactly {@code gate - 1}
     * timeouts (never shedding on its own — the boolean latch is still {@code false}), followed by a
     * concurrent burst that crosses INTO the next window. A losing (non-rolling) burst thread can observe
     * the just-published new {@code shedWindowStartNs} (fast-path returns without rolling) while
     * {@code shedWindowTimeouts} is still the OLD window's stale gate-1 count; incrementing that stale
     * count to the gate lets it fire a shed for the new window using carried-over data, which the CAS
     * winner's later (unconditional, in the old boolean-latch code) latch clear then clobbers — allowing
     * a SECOND shed in that same real window. Each iteration below deterministically (single-threaded)
     * settles a fresh window at gate-1, then races a burst across the NEXT boundary, so this exact
     * reachable state recurs every window.
     */
    @Test
    @Timeout(120)
    void gateMinusOneWindow_thenConcurrentBurst_shedsAtMostOnce()
            throws InterruptedException, BrokenBarrierException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)
        // tMax=8 -> timeoutGate = max(3, ceil(0.3*8)) = 3, so gate-1 = 2 timeouts (below gate, no shed).
        ConcurrencyGauge gauge = new ConcurrencyGauge(8, metrics, now::get, () -> WINDOW, ConcurrencyGauge.defaultInitialT(8));

        int burstThreads = 64;
        int windows = 400;

        CyclicBarrier start = new CyclicBarrier(burstThreads + 1);
        CyclicBarrier end = new CyclicBarrier(burstThreads + 1);
        AtomicReference<Throwable> workerError = new AtomicReference<>();

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < burstThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    for (int w = 0; w < windows; w++) {
                        start.await();
                        gauge.onTransientTimeout();   // exactly one timeout per thread, right at the crossing
                        end.await();
                    }
                } catch (Throwable th) {
                    workerError.compareAndSet(null, th);
                }
            }, "gate-minus-one-burst-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }

        try {
            for (int w = 0; w < windows; w++) {
                // Step 1 (single-threaded, deterministic): force a fresh roll, then settle the new window
                // at exactly gate-1 timeouts — the "never sheds on its own" state this scenario needs.
                now.addAndGet(WINDOW + 1);
                gauge.onTransientTimeout();
                gauge.onTransientTimeout();

                // Step 2: cross the NEXT boundary and release the burst simultaneously into the race —
                // the window that was just settled at gate-1 is now the "old, stale" window a losing
                // thread may still read from.
                double before = timeoutSheds(metrics);
                now.addAndGet(WINDOW + 1);
                start.await();
                end.await();
                double delta = timeoutSheds(metrics) - before;
                assertThat(delta)
                        .as("window %d: gate-1 settle + concurrent boundary burst sheds at most once", w)
                        .isLessThanOrEqualTo(1.0);
            }
        } finally {
            for (Thread t : workers) {
                t.interrupt();
            }
            for (Thread t : workers) {
                t.join(2_000);
            }
        }

        Throwable err = workerError.get();
        assertThat(err == null || err instanceof InterruptedException || err instanceof BrokenBarrierException)
                .as("no unexpected worker fault: %s", err)
                .isTrue();
        // Sanity: the scenario does shed at least once across all windows (the guard is meaningful).
        assertThat(timeoutSheds(metrics)).as("the gate-1 + burst scenario sheds").isGreaterThan(0.0);
    }
}
