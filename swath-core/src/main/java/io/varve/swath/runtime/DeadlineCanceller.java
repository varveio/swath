/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.observability.StopReason;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Arms a single {@code --max-duration} deadline: once {@code maxDuration} elapses it
 * calls {@link CancellationToken#cancel(StopReason)} with {@link StopReason#MAX_DURATION},
 * so the scan loop observes cancellation, unwinds through the existing {@code try/finally}
 * (leaving the checkpoint resumable and writing a {@code stop_reason=max_duration} partial
 * summary), and the process exits cleanly. A {@code null} duration disarms it (a no-op
 * closer). The deadline thread is a daemon and is torn down on {@link #close()} — armed
 * with a try-with-resources around the run, so normal completion cancels the pending task
 * and leaks no threads.
 *
 * <p>Cancellation is cooperative (the engine polls the token between pages and does its own
 * {@code shutdownNow()} on unwind), so setting the flag is sufficient — this class does not
 * reach into the worker pool.
 */
public final class DeadlineCanceller implements AutoCloseable {

    private final ScheduledExecutorService scheduler;

    private DeadlineCanceller(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    /** Arm the deadline, or return a disarmed no-op canceller when {@code maxDuration} is null. */
    public static DeadlineCanceller arm(CancellationToken token, Duration maxDuration) {
        if (maxDuration == null) {
            return new DeadlineCanceller(null);
        }
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofPlatform().daemon(true).name("swath-max-duration").unstarted(r));
        // Attribute the source alongside the reason (first-writer-wins, in lockstep) so the
        // provenance substrate is complete for every cancel site, though the MAX_DURATION disposition is
        // exit-0 and list_stuck_stop (the only stop_source printer) never fires on this path.
        scheduler.schedule(() -> token.cancel(StopReason.MAX_DURATION, CancelSource.TIMEBOX),
                Math.max(0L, maxDuration.toMillis()), TimeUnit.MILLISECONDS);
        return new DeadlineCanceller(scheduler);
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
