/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.store.PageRequest;
import java.time.Duration;

/**
 * Pluggable per-call latency injection for {@link MockPageFetcher}.
 *
 * <p>{@code MockPageFetcher} is zero-latency by default, which makes timing-dependent engine
 * behavior — reactive-split races (e.g. {@code cursor_passed_pivot}), attempt-timeout escalation
 * under a slow tail, shed storms, demand-gate starvation — untestable offline: those only ever
 * showed up against a real store, where a fetch genuinely takes wall-clock time and lets other
 * workers/thieves interleave while it is in flight. A {@link LatencyModel} gives the mock a
 * deterministic, seedable notion of "how long this call took" so those races become reproducible
 * in CI.
 *
 * <p>The model is a pure function of a {@link Context} built from what {@code MockPageFetcher}
 * itself knows about a call — the {@link PageRequest} and two cheap classification signals the
 * fetcher derives from its own call history (see {@link Context}). It never sees engine internals,
 * wall-clock time, or other in-flight calls, so a canned profile behaves the same whether it is
 * driven directly (unit tests) or through the full concurrent engine.
 *
 * <p>Latency injected this way is <b>additive</b> with the existing {@code
 * MockPageFetcher.Builder#latency}/{@code #pageDelay}/{@code #slowFirstPage} knobs, and is applied
 * the same way they are: a real (interruptible) {@code Thread.sleep} on the calling thread before
 * the page is returned — i.e. it blocks like real I/O rather than busy-waiting, so cancellation/
 * shutdown paths that rely on interrupting an in-flight fetch are unaffected. A {@link
 * LatencyModel} never injects a fault (a 503, a timeout exception, a stuck token) by itself —
 * that stays the {@code PageInterceptor}'s job; the two seams compose (see {@link LatencyModels}
 * for how a probe-timeout-shaped fault can be layered on top of a cold delay).
 */
@FunctionalInterface
public interface LatencyModel {

    /** Zero latency for every call — {@code MockPageFetcher}'s default, exactly. */
    LatencyModel NONE = ctx -> Duration.ZERO;

    /** The (non-negative) delay to inject before this call's page is returned. */
    Duration delayFor(Context ctx);

    /**
     * Everything a {@link LatencyModel} sees about one {@code fetchPage} call.
     *
     * @param request the request as issued, unaltered.
     * @param callIndex this fetcher instance's 0-based call sequence number (the same counter a
     *     {@code PageInterceptor}'s {@code callIndex} argument uses) — useful for a scripted
     *     window (e.g. {@link LatencyModels#stormBurst}) but inherently order-DEPENDENT under
     *     concurrency: two runs can assign different indices to the "same" logical call if
     *     workers race differently. Prefer {@link #warm}/{@link #probe}/{@link #delimiterVisible}
     *     (all order-independent, request-coordinate-keyed classifications) when a profile can.
     * @param warm true iff {@code request.startAfter()} is byte-exact-equal to a boundary key this
     *     fetcher served as the last row of one of its most recently served pages (a bounded
     *     recency window — see {@code MockPageFetcher.RECENCY_WINDOW}). Operationally: "this call
     *     is the immediate next page of a scan the fetcher just served" — a warm sequential
     *     continuation. Everything else is COLD ({@link #cold()}): a fresh/root call
     *     ({@code startAfter() == null}), a jump to a position never recently served as a page
     *     boundary, or a stale continuation that has aged out of the recency window. This is
     *     exactly the shape of a pivot/structure probe or a thief steal into unexplored range,
     *     neither of which ever resumes another call's exact page boundary — it is a real (if
     *     approximate) proxy for "far from any recently-served position", not a byte-distance
     *     metric.
     * @param probe true iff {@code request.maxKeys() <= 1} — the engine's own convention for a
     *     single-key structural/pivot probe (see {@code Thief}, {@code SeedStep}).
     * @param delimiterVisible true iff the request carries a non-empty {@code delimiter} — a
     *     folder/common-prefix structure probe as opposed to a flat (recursive) listing call.
     */
    record Context(PageRequest request, int callIndex, boolean warm, boolean probe, boolean delimiterVisible) {

        /** The complement of {@link #warm} — see {@link #warm} for the operational definition. */
        public boolean cold() {
            return !warm;
        }
    }
}
