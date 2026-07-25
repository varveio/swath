/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

/**
 * Provenance tag naming <b>which mechanism</b> requested a cooperative cancel, stored
 * first-writer-wins alongside the {@link io.varve.swath.observability.StopReason} on
 * {@link CancellationToken}. A signal is such a mechanism, and so is each site that can trip a
 * {@code StopReason.STUCK} cancel — the liveness watchdog, either transient-retry cap, or a bare
 * seed interrupt: the tag lets the terminal {@code list_stuck_stop} marker print an honest
 * {@code stop_source=<tag>} instead of attributing every cap-driven stop to the watchdog, and lets
 * the CLI map a {@code StopReason.SIGNAL} cancel to a distinct exit code (SIGTERM vs. SIGINT).
 */
public enum CancelSource {
    /** A SIGINT (Ctrl-C) tripped the cooperative cancel. */
    SIGINT("sigint"),
    /** A SIGTERM (the default {@code kill}, a supervisor stop) tripped the cooperative cancel. */
    SIGTERM("sigterm"),
    /** The in-JVM {@code LivenessWatchdog} escalation ladder tripped the cooperative cancel. */
    LIVENESS_WATCHDOG("liveness_watchdog"),
    /** A transient-retry loop exhausted its cap under the {@code BOUNDED} policy (no watchdog armed). */
    TRANSIENT_RETRY_CAP("transient_retry_cap"),
    /** A seed-time interrupt with no prior attributed reason was converted to a resumable STUCK. */
    SEED_INTERRUPT("seed_interrupt"),
    /** The {@code --max-duration} deadline canceller fired. */
    TIMEBOX("timebox");

    private final String tag;

    CancelSource(String tag) {
        this.tag = tag;
    }

    /** The stable snake_case token emitted in the {@code stop_source=} marker field. */
    public String tag() {
        return tag;
    }
}
