/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

/**
 * Why a run stopped — the {@code stop_reason} field of the JSON run-summary
 * (schema v2). It <b>refines</b> the {@code completed} boolean rather
 * than duplicating it: {@code completed:true} ⇔ {@link #COMPLETED}; every other
 * value is a {@code completed:false} terminal record whose category tells a
 * consumer <i>why</i> the run is partial (a timebox stop and an expected
 * resumable partial are not a crash, and a batch/orchestrator must be able to
 * tell them apart).
 *
 * <p>A mid-run heartbeat flush (still running) carries no {@code stop_reason} at
 * all ({@code null}) — the run has not stopped yet; only a terminal record names
 * a reason.
 */
public enum StopReason {
    /** Normal, clean completion ({@code completed:true}, {@code exit_code:0}). */
    COMPLETED("completed"),
    /** {@code --max-duration} timebox elapsed — a graceful, resumable partial (process exit 124). */
    MAX_DURATION("max_duration"),
    /**
     * A {@code --max-duration} timebox elapsed with ZERO objects emitted — the deterministic
     * "burned the whole timebox making no progress" case (a pathological/slow bucket, or a node
     * that only ever attempt-times-out/gets throttled), distinct from a legit large timeboxed
     * {@link #MAX_DURATION} partial that actually made headway. Exit code is unchanged (still 124,
     * still a resumable partial, {@code ListCommand#timeboxExitOrRethrow} keys off the
     * {@link io.varve.swath.runtime.CancellationToken}'s own {@code MAX_DURATION} attribution, not this
     * refined summary value) — this only sharpens post-hoc triage, already partly visible via
     * {@code THROTTLE.*}/api counters.
     */
    MAX_DURATION_NO_PROGRESS("max_duration_no_progress"),
    /** SIGTERM/SIGINT graceful stop — a resumable partial (process exit 143 for SIGTERM, 130 for SIGINT). */
    SIGNAL("signal"),
    /** An exception unwound the run. */
    CRASH("crash"),
    /** A fresh run's seed probe failed before any listing began (process exit 1). */
    SEED_FAILURE("seed_failure"),
    /**
     * A run was aborted for lack of forward progress rather than left to hang forever. Three
     * sources can trip it — recorded via {@code CancelSource}: the in-JVM liveness watchdog's
     * stall-window escalation (run-wide zero progress), the engine's bounded transient-retry cap
     * exhausting on a single wedged fetch (other ranges may still be progressing), and a bare
     * seed-time interrupt with no prior attributed reason. Distinct from a
     * {@link #SIGNAL} (user Ctrl-C) and from a graceful {@link #MAX_DURATION}
     * timebox: it is a NON-zero abort mapped by {@code ListCommand#timeboxExitOrRethrow} to
     * {@code ExitCodes.STUCK} (75, {@code EX_TEMPFAIL}; never the signal exit 130), and an external
     * runner's classifier maps the {@code stop_reason=stuck} marker to an {@code error_class} of
     * {@code stuck_api_timeouts}, {@code stuck_throttle}, or {@code stuck_unknown}.
     *
     * <p><b>RESUMABLE</b> (aligns with {@code ExitCodes.STUCK}'s {@code EX_TEMPFAIL} "safe to retry
     * later"): a stuck-abort is an ENVIRONMENTAL/transient condition, not a deterministic in-process
     * fault — the checkpoint is valid, so it leaves the run {@code RUNNING} and is never the
     * {@code fatal_error}-refused {@code FAILED}; a later {@code --resume} continues the bucket. The
     * watchdog escalates cooperative-cancel → forensic dump → thread-interrupt → {@code Runtime.halt}
     * (emitting a {@code stop_reason=stuck} marker just before halt, since halt bypasses the normal
     * summary finalizer) if the cooperative cancel cannot unwind a wedged worker.
     */
    STUCK("stuck"),
    /** {@code --resume} refused: the filter/format changed since the checkpoint (process exit 2). */
    RESUME_REFUSED("resume_refused");

    private final String wireName;

    StopReason(String wireName) {
        this.wireName = wireName;
    }

    /** The lowercase snake_case token written into the JSON {@code stop_reason} field. */
    public String wireName() {
        return wireName;
    }
}
