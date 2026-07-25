/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Classifies the PROXIMATE cause of a terminal disposition (STUCK / TRANSIENT_RETRY_CAP / CRASH)
 * into a distinct {@code error_class} token, so an external stderr classifier can name the failure
 * instead of {@code unknown}. Read-only over the throttle-event tallies it is fed (as DELTAS since
 * the last real progress) — no gauge/control logic. See metrics-and-observability.md §3 for the
 * full {@code error_class}/{@code stop_source} JSON contract this feeds.
 *
 * <p>The facade ({@code RunMetrics}) feeds it two booleans per throttle event ({@link
 * #recordThrottleEvent}) and a snapshot on every real-progress advance ({@link
 * #snapshotAtProgress}); it never imports {@code ThrottleType}/{@code ThrottleException}, keeping
 * the {@code observability → error} dependency in the facade.
 *
 * <p><b>Backed by plain AtomicLongs, never a Micrometer {@code Counter}.</b> Under a DELTA/step
 * OTLP registry {@code Counter.count()} resets per step, which would make the classification
 * non-monotonic in production.
 */
final class StuckErrorClassifier {

    /**
     * {@link #classifyStuck()} noise floor: below this many throttle events of a kind we can't call it
     * a "storm", so the disposition stays {@code stuck_unknown}. Chosen to match the per-fetch
     * transient-retry cap ({@code TransientRetryFetcher.MAX_TRANSIENT_RETRIES = 8}) — a single wedged
     * fetch's worth of timeouts is not a run-level storm. Kept in sync MANUALLY (the literal is
     * duplicated deliberately to avoid an observability→engine layering dependency); if the retry cap
     * changes, update this too.
     */
    private static final double STUCK_MIN_EVENTS = 8;

    /**
     * {@link #stuckErrorClass(String)}'s routing tag for {@code CancelSource.TRANSIENT_RETRY_CAP}.
     * Kept in sync MANUALLY (duplicated deliberately, same as {@link #STUCK_MIN_EVENTS} above) —
     * {@code observability} must not import {@code runtime}'s {@code CancelSource}; if that enum's
     * tag ever changes, update this too.
     */
    private static final String TRANSIENT_RETRY_CAP_TAG = "transient_retry_cap";

    // Plain monotonic mirrors of the two throttle-event inputs classifyStuck() reads. Incremented
    // alongside the typed throttleEvents Counters in the facade's recordThrottleEvent; the classifier
    // reads these tallies, never the Counters.
    private final AtomicLong attemptTimeoutEventsTally = new AtomicLong();
    private final AtomicLong votingThrottleEventsTally = new AtomicLong();
    // Snapshots of the two tallies above, taken at the instant realProgressSignal() last advanced —
    // see snapshotAtProgress()/classifyStuck().
    private final AtomicLong attemptTimeoutTallyAtLastProgress = new AtomicLong();
    private final AtomicLong votingThrottleTallyAtLastProgress = new AtomicLong();
    // Pre-derived error_class for the RetryPolicy.BOUNDED retry-cap STUCK path
    // (CancelSource.TRANSIENT_RETRY_CAP), recorded directly by the cap-tripping fetch from its OWN
    // local fault history (see recordTransientRetryCapExhaustion). The windowed run-wide signal above
    // is wrong for it: an unrelated healthy worker's recordPage() re-snapshots the tallies and erases
    // the wedged fetch's history.
    private final AtomicReference<String> transientRetryCapErrorClass = new AtomicReference<>("stuck_unknown");
    // The error_class of a CLASSIFIED FATAL in-process failure (StopReason.CRASH), as opposed to the
    // STUCK classifications above — recorded first-writer-wins by the failing subsystem
    // (recordFatalErrorClass) and read by ListRunner's CRASH terminal status.
    private final AtomicReference<String> fatalErrorClass = new AtomicReference<>();

    /**
     * Mirror one classified throttle/transient event into the two windowed tallies: {@code
     * attemptTimeout} is {@code type == ATTEMPT_TIMEOUT}; {@code votesAimdDown} is the behavioral
     * {@code ThrottleException.Kind#votesAimdDown()} the facade evaluated. Called once per event from
     * the facade's {@code recordThrottleEvent}.
     */
    void recordThrottleEvent(boolean attemptTimeout, boolean votesAimdDown) {
        if (attemptTimeout) {
            attemptTimeoutEventsTally.incrementAndGet();
        }
        if (votesAimdDown) {
            votingThrottleEventsTally.incrementAndGet();
        }
    }

    /**
     * Snapshot the two {@link #classifyStuck()} throttle-event tallies at the instant {@code
     * realProgressSignal()} advances — called from every facade site that bumps one of that signal's
     * inputs (recordPage, markProgress, recordEntriesEmitted, recordProgress, recordSortSegment).
     * Cheap: two plain {@link AtomicLong#set} writes, no allocation, no lock; safe to call on every
     * page/entries-batch. The read-then-write against the tallies is intentionally not atomic-as-a-pair
     * with a concurrent {@link #recordThrottleEvent} — the classifier is a best-effort post-hoc
     * heuristic, not a strict invariant, so a benign few-event race is acceptable.
     */
    void snapshotAtProgress() {
        attemptTimeoutTallyAtLastProgress.set(attemptTimeoutEventsTally.get());
        votingThrottleTallyAtLastProgress.set(votingThrottleEventsTally.get());
    }

    /**
     * Classify a run-wide STUCK freeze over the SINCE-LAST-REAL-PROGRESS window into {@code
     * stuck_api_timeouts} / {@code stuck_throttle} / {@code stuck_unknown} — see
     * metrics-and-observability.md §3 for what each value means and how a caller routes between
     * this signal and {@link #transientRetryCapErrorClass()}. Only valid for the {@code
     * CancelSource.LIVENESS_WATCHDOG} terminal, whose own trip condition is a global freeze; a
     * {@code TRANSIENT_RETRY_CAP} terminal must read {@link #transientRetryCapErrorClass()}
     * instead.
     *
     * <p><b>Windowed, not cumulative.</b> Subtracting the last-real-progress snapshot from the
     * current tally isolates exactly the activity SINCE the last real progress, matching what the
     * watchdog's no-real-progress backstop actually tripped on. A wedge starved from the very start
     * (the snapshot never advances past its zero default) still classifies correctly — the delta
     * degenerates to the full cumulative total.
     */
    String classifyStuck() {
        double attemptTimeouts = attemptTimeoutEventsTally.get() - attemptTimeoutTallyAtLastProgress.get();
        double voting = votingThrottleEventsTally.get() - votingThrottleTallyAtLastProgress.get();
        return classify(attemptTimeouts, voting);
    }

    /**
     * The shared dominance rule behind both {@link #classifyStuck()} (the run-wide windowed signal) and
     * {@link #recordTransientRetryCapExhaustion} (one fetch's local cap-exhaustion tally) — kept as ONE
     * rule so the two classification paths can never silently diverge on what counts as a "storm" (the
     * {@value #STUCK_MIN_EVENTS} noise floor and the dominance comparison).
     */
    private static String classify(double attemptTimeouts, double voting) {
        if (attemptTimeouts >= STUCK_MIN_EVENTS && attemptTimeouts > voting) {
            return "stuck_api_timeouts";
        }
        if (voting >= STUCK_MIN_EVENTS && voting >= attemptTimeouts) {
            return "stuck_throttle";
        }
        return "stuck_unknown";
    }

    /**
     * Called by a transient-retry loop AT THE INSTANT its own local cap is exhausted under {@code
     * RetryPolicy.BOUNDED} (the {@code CancelSource.TRANSIENT_RETRY_CAP} terminal), with the counts of
     * {@code ATTEMPT_TIMEOUT} vs voting (SLOWDOWN/SERVER_5XX) faults it personally saw within its OWN
     * cap-counting window. Classified with the identical {@link #classify} dominance rule {@link
     * #classifyStuck()} uses. Deliberately independent of any global/run-wide signal — this fetch's own
     * local history is the ONLY thing that can honestly answer "what exhausted THIS cap".
     *
     * <p><b>A plain {@code .set()} here is NOT benign on its own.</b> Every production call site is
     * required to call {@code CancellationToken.cancel(StopReason, CancelSource)} FIRST and only invoke
     * this method when that call returns {@code true} (i.e. THIS fetch's attribution actually won the
     * token's first-writer-wins {@code stop_source}). Under that discipline this plain {@code .set()}
     * only ever fires from the one fetch whose provenance the terminal marker will read, so no CAS is
     * needed here.
     */
    void recordTransientRetryCapExhaustion(long attemptTimeoutFaults, long votingFaults) {
        transientRetryCapErrorClass.set(classify(attemptTimeoutFaults, votingFaults));
    }

    /**
     * The pre-derived {@code error_class} for a {@code CancelSource.TRANSIENT_RETRY_CAP} terminal, as
     * recorded by the WINNING cap-tripping fetch via {@link #recordTransientRetryCapExhaustion}.
     * Defaults to {@code stuck_unknown} if the cap somehow trips without recording (defensive).
     */
    String transientRetryCapErrorClass() {
        return transientRetryCapErrorClass.get();
    }

    /**
     * The ONE source-routed {@code error_class} derivation for a {@code StopReason.STUCK} terminal.
     * Takes the raw {@code stop_source} TAG STRING (e.g. {@code "transient_retry_cap"}), not {@code
     * CancelSource} itself — {@code observability} must not depend on {@code runtime}. {@code null} or
     * any tag other than the retry-cap one routes to the run-wide windowed signal.
     */
    String stuckErrorClass(String stopSourceTag) {
        return TRANSIENT_RETRY_CAP_TAG.equals(stopSourceTag)
                ? transientRetryCapErrorClass()
                : classifyStuck();
    }

    /**
     * Record the {@code error_class} of a classified FATAL failure that is unwinding the run.
     * <b>First-writer-wins</b> (CAS from {@code null}): the FIRST classified cause to unwind is the one
     * that killed the run. Idempotent, and a {@code null} class is ignored, so a caller can pass the
     * result of a cause-chain walk unconditionally.
     */
    void recordFatalErrorClass(String errorClass) {
        if (errorClass != null) {
            fatalErrorClass.compareAndSet(null, errorClass);
        }
    }

    /**
     * The {@code error_class} of the classified fatal failure unwinding this run, or {@code null} if
     * the crash was never classified — which keeps the pre-existing {@code error_class:null} shape for
     * a generic crash.
     */
    String fatalErrorClass() {
        return fatalErrorClass.get();
    }
}
