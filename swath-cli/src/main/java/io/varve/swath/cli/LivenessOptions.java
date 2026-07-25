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

    /**
     * The supported floor for {@code --progress-interval}. The grammar accepts {@code 1ms}, which
     * on a ten-hour run asks for some 36 million progress records — a rate no human reads and no
     * captured log wants. One record a second is already faster than a person can follow, so
     * anything under it is a mistake rather than a preference: REJECTED, not silently clamped, the
     * same way {@code --part-rotation-interval} treats its own floor and {@code --tune
     * parquet.writers} treats its range. A flag that means something other than what it says is
     * worse than an error message.
     */
    static final Duration MIN_PROGRESS_INTERVAL = Duration.ofSeconds(1);

    String progressInterval;

    String maxDuration;

    String stallTimeout;

    String noProgressTimeout;

    Duration resolveProgressInterval() throws InvalidConfigException {
        return progressInterval == null ? null : parseProgressInterval(progressInterval);
    }

    /** Parse {@code --progress-interval}, enforcing {@link #MIN_PROGRESS_INTERVAL}. */
    static Duration parseProgressInterval(String raw) throws InvalidConfigException {
        Duration parsed = DurationParser.progressInterval(raw);
        if (parsed.compareTo(MIN_PROGRESS_INTERVAL) < 0) {
            throw new InvalidConfigException("--progress-interval must be >= "
                    + MIN_PROGRESS_INTERVAL + " (got " + raw + "); a faster cadence outruns both a "
                    + "reader and a captured log");
        }
        return parsed;
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
     * Starts the run's SESSION progress reporter at the resolved {@code --progress-interval}
     * cadence. The CLI owns this one because it is the only layer whose scope covers the whole run
     * — the seed step included, which is where a stalled run is least visible and where every
     * listing-shaped counter reads zero. {@code ListRunner}'s own start then joins this reporter
     * rather than running a second one (see {@link RunProgressReporter}).
     */
    RunProgressReporter startProgressReporter(RunContext ctx) throws InvalidConfigException {
        return RunProgressReporter.start(ctx.metrics(), resolveEffectiveProgressInterval());
    }
}
