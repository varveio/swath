/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.observability.RunProgressReporter;
import io.varve.swath.runtime.LivenessWatchdog;
import io.varve.swath.runtime.RunContext;
import java.time.Duration;

/**
 * Progress-reporting cadence and the run's liveness/timebox windows: the {@code --progress-interval}
 * reporting tick, the {@code --max-duration} timebox, and the {@code --idle-timeout}/
 * {@code --no-progress-timeout} watchdog windows.
 */
final class LivenessOptions {

    /** The default liveness-watchdog stall window when {@code --idle-timeout} is unset. */
    static final Duration DEFAULT_STALL_TIMEOUT = Duration.ofSeconds(120);

    /** The default zero-real-progress backstop window when {@code --no-progress-timeout} is unset. */
    static final Duration DEFAULT_NO_PROGRESS_TIMEOUT = LivenessWatchdog.DEFAULT_NO_PROGRESS_TIMEOUT;

    String progressInterval;

    String maxDuration;

    String stallTimeout;

    String noProgressTimeout;

    Duration resolveProgressInterval() throws InvalidConfigException {
        return progressInterval == null ? null : DurationParser.progressInterval(progressInterval);
    }

    /** The progress cadence actually in effect: {@code --progress-interval}, else the default. */
    Duration resolveEffectiveProgressInterval() throws InvalidConfigException {
        Duration explicit = resolveProgressInterval();
        return explicit != null ? explicit : RunProgressReporter.nonTtyInterval();
    }

    /** Resolve {@code --max-duration}: a strictly-positive duration, or {@code null} when unset. */
    Duration resolveMaxDuration() throws InvalidConfigException {
        return maxDuration == null ? null : DurationParser.parse(maxDuration, "max-duration", false);
    }

    /**
     * Resolve {@code --idle-timeout}: the liveness-watchdog stall window. Unset ⇒ the 120s default
     * (the watchdog is ON by default); {@code 0}/{@code none}/{@code off} ⇒
     * {@code Duration.ZERO}, which {@code LivenessWatchdog.arm} treats as disabled.
     */
    Duration resolveStallTimeout() throws InvalidConfigException {
        return stallTimeout == null ? DEFAULT_STALL_TIMEOUT
                : DurationParser.parse(stallTimeout, "idle-timeout", true);
    }

    /**
     * Resolve {@code --no-progress-timeout}: the liveness-watchdog zero-real-progress backstop window.
     * Unset ⇒ the 10m default (the backstop is ON by default); {@code 0}/{@code none}/{@code
     * off} ⇒ {@code Duration.ZERO}, which {@code LivenessWatchdog.arm} treats as disabled.
     */
    Duration resolveNoProgressTimeout() throws InvalidConfigException {
        return noProgressTimeout == null ? DEFAULT_NO_PROGRESS_TIMEOUT
                : DurationParser.parse(noProgressTimeout, "no-progress-timeout", true);
    }

    /**
     * Starts a {@link RunProgressReporter} at the same {@code --progress-interval} cadence
     * {@code ListRunner} uses for the engine phase (mirrors its private {@code startProgress} helper)
     * — the seam the seed-phase zero-progress heartbeat reuses, since {@code ListRunner}'s own
     * reporter does not exist yet during seeding.
     *
     * @param stdoutIsTerminal the CLI's own per-fd {@code TerminalCapabilities} probe --
     *                         threaded in explicitly so swath-core never calls {@code
     *                         System.console()} itself.
     */
    RunProgressReporter startProgressReporter(RunContext ctx, boolean stdoutIsTerminal) throws InvalidConfigException {
        Duration interval = resolveProgressInterval();
        return interval == null ? RunProgressReporter.start(ctx.metrics(), stdoutIsTerminal)
                : RunProgressReporter.start(ctx.metrics(), interval);
    }
}
