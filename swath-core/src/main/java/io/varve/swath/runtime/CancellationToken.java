/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.error.CancelledException;
import io.varve.swath.observability.StopReason;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cooperative cancellation for polling loops: a plain {@link AtomicBoolean} flag.
 * Do not add a {@code Set<Thread> waiters} register/deregister structure — waking
 * blocked threads is the executor's job ({@code shutdownNow()} interrupts every
 * task); the SIGINT hook does {@code token.cancel()} + {@code executor.shutdownNow()}.
 *
 * <p>A cancel may carry a {@link StopReason} ({@link #cancel(StopReason)}) so the
 * terminal JSON run-summary can name <i>why</i> the run stopped (timebox vs.
 * signal) and the CLI can map a {@code --max-duration} stop to a clean exit while
 * a signal stays exit 130. The no-arg {@link #cancel()} (engine-internal
 * completion/receiver-gone teardown) leaves the reason unset.
 */
public final class CancellationToken {
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    /**
     * The one attributed {@code (reason, source)} pair, published atomically by a SINGLE CAS so a
     * mixed-source race can never pair one caller's reason with another's source. The reason
     * and source are inseparable: they win or lose the first-writer-wins CAS together, so the pair
     * read back from {@link #stopReason()}/{@link #source()} always comes from the same caller.
     */
    private final AtomicReference<Attribution> attribution = new AtomicReference<>();

    /** The inseparable {@code (reason, source)} unit published by one CAS (source is {@code null} for the legacy {@link #cancel(StopReason)} form). */
    private record Attribution(StopReason reason, CancelSource source) {
    }

    public void cancel() {
        cancelled.set(true);
    }

    /**
     * Cancel with an attributed reason. Attribution is <b>first-writer-wins and
     * deterministic</b>: the FIRST attributed reason to fire sticks, and no later
     * cancel — attributed or the no-arg {@link #cancel()} — ever overwrites it. The
     * reason is CAS-published <i>before</i> the cancelled flag so any thread that
     * observes cancellation also sees the winning reason.
     *
     * <p>This closes the exit-code race: a SIGTERM/SIGINT arriving
     * just after the {@code --max-duration} deadline already fired keeps
     * {@code stop_reason=max_duration} (exit 0), and a deadline arriving just after a
     * signal keeps {@code stop_reason=signal} (exit 130) — so the terminal summary's
     * {@code stop_reason} and the process exit code never disagree. {@code crash} is
     * only ever <i>inferred</i> by a consumer (absence of a terminal record), never
     * stored here.
     */
    public void cancel(StopReason reason) {
        cancel(reason, null);   // legacy form: same single-CAS attribution, with no source tag
    }

    /**
     * Cancel with an attributed reason AND a {@link CancelSource provenance tag}. Both are
     * first-writer-wins and published <i>before</i> the cancelled flag, exactly like {@link
     * #cancel(StopReason)}.
     *
     * @return {@code true} iff THIS call's reason won the first-writer-wins CAS (i.e. this call's
     * {@code source}/{@code reason} is the pair that will actually be read back from {@link
     * #stopReason()}/{@link #source()}). Callers that also record OTHER
     * source-attributed state (e.g. {@code RunMetrics#recordTransientRetryCapExhaustion}) must gate
     * that recording on this return value — otherwise a losing caller's local state could overwrite a
     * winning caller's, leaving the terminal marker internally inconsistent (a {@code stop_source} from
     * the winner paired with state recorded by the loser). Existing callers that don't need this
     * (the single-attribution watchdog/timebox/seed sites) may simply discard it.
     */
    public boolean cancel(StopReason reason, CancelSource source) {
        // A null reason claims nothing (matches the legacy no-arg teardown), so a later
        // attributed cancel still wins.
        boolean won = reason != null && attribution.compareAndSet(null, new Attribution(reason, source));
        cancelled.set(true);
        return won;
    }

    /** The attributed stop reason, or {@code null} for an unattributed/never cancel. */
    public StopReason stopReason() {
        Attribution a = attribution.get();
        return a == null ? null : a.reason();
    }

    /** The attributed cancel provenance, or {@code null} if the cancel carried no source tag. */
    public CancelSource source() {
        Attribution a = attribution.get();
        return a == null ? null : a.source();
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    /** Throw if cancellation has been requested. Cheap to call in tight loops. */
    public void throwIfCancelled() throws CancelledException {
        if (cancelled.get()) {
            throw new CancelledException("operation cancelled");
        }
    }
}
