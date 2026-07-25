/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.error.SwathException;
import io.varve.swath.output.BrokenPipe;
import java.io.IOException;

/**
 * Maps a terminal throwable to a process exit code. The {@link SwathException}
 * hierarchy is sealed, so the mapping is exhaustive (UNIT-exit). A broken pipe (stdout
 * closed, e.g. piping to {@code head}) is a <i>clean</i> exit (0), not an
 * error — see {@link #isBrokenPipe}.
 */
public final class ExitCodes {

    private ExitCodes() {
    }

    public static final int SUCCESS = 0;
    public static final int UNEXPECTED = 1;

    /**
     * A usage / configuration error or a deliberate refusal — a bad flag combination, an invalid
     * URI or config, or a guarded refusal (an unfinished/foreign output dir, an extension/format
     * mismatch, a resume whose identity changed). The {@code error} hierarchy already returns this
     * from {@code InvalidArgsException}/{@code InvalidUriException}/{@code InvalidConfigException};
     * this names the same value at the CLI boundary so the published table has one source.
     */
    public static final int USAGE = 2;

    /**
     * A cooperative {@code stop_reason=stuck} cancel unwound to a resumable partial. Three sources
     * trip it — the liveness watchdog's escalation ladder, a transient-retry cap exhausting, and a
     * bare seed-time interrupt ({@code CancelSource}) — and the terminal {@code list_stuck_stop}
     * marker records which ({@code stop_source=}) alongside an {@code error_class=} of
     * {@code stuck_api_timeouts}, {@code stuck_throttle}, or {@code stuck_unknown}. A dedicated,
     * non-zero code — deliberately NOT 130 (a signal / user Ctrl-C) and NOT 1 (a generic
     * unexpected error) — so an external runner can tell a resumable stuck abort from an interrupt
     * or a crash on the exit code alone. {@code 75} is {@code EX_TEMPFAIL} (sysexits.h): a
     * "transient failure, safe to retry the bucket later" signal, which is exactly a stuck
     * partial's disposition. The watchdog's {@code Runtime.halt} backstop — for a cancel whose own
     * unwind wedges — exits with this code too.
     */
    public static final int STUCK = 75;

    /**
     * A {@code --max-duration} timebox elapsed and the run stopped gracefully with a resumable
     * partial — distinct from a clean, complete exit 0 so a resume-until-done supervisor can tell
     * "ran out of time, keep going" apart from "finished". 124 is the conventional timeout code
     * (GNU {@code timeout(1)} uses it), which is exactly this disposition. The terminal summary's
     * {@code stop_reason=max_duration} records the same fact for a machine parser.
     */
    public static final int TIMEBOX = 124;

    /**
     * The run was cancelled by SIGINT (Ctrl-C). 128 + signal number (SIGINT is 2) — the shell
     * convention for "terminated by signal N", so an external runner reads the disposition from the
     * code alone. A resumable partial, like the other cooperative-cancel codes.
     */
    public static final int SIGINT = 130;

    /**
     * The run was cancelled by SIGTERM (the default {@code kill} signal, a supervisor/orchestrator
     * stop). 128 + signal number (SIGTERM is 15), the shell convention — kept distinct from
     * {@link #SIGINT} so a caller can tell a supervisor-driven stop from an interactive Ctrl-C.
     * A resumable partial.
     */
    public static final int SIGTERM = 143;

    /**
     * How deep {@link #protocolViolation} follows a chain. A real one is a handful of links; the
     * bound only keeps a self-referential or absurdly nested chain from spinning.
     */
    private static final int MAX_CHAIN_DEPTH = 32;

    /** The documented exit code for any terminal throwable. */
    public static int forThrowable(Throwable t) {
        // A protocol violation is never retryable, so it outranks the primary failure's own code —
        // a cancellation's 130/75/124 (or a broken pipe's clean 0) would tell an external runner the
        // run stopped in a resumable partial it should come back to.
        ProtocolViolationException violation = protocolViolation(t);
        if (violation != null) {
            return violation.exitCode();
        }
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof SwathException swath) {
                return swath.exitCode();
            }
            if (cause instanceof IOException io && isBrokenPipe(io)) {
                return SUCCESS;
            }
            cause = cause.getCause();
        }
        return UNEXPECTED;
    }

    /**
     * The {@link ProtocolViolationException} carried anywhere in {@code t} — the throwable itself,
     * its cause chain, or a suppressed exception hanging off any link of it — or {@code null}.
     * A violation discovered while a scan is already unwinding is attached as a suppressed exception
     * so that whatever ended the scan stays primary, so reading only the primary and its causes would
     * miss it and classify the run as the plain, resumable stop the primary describes.
     */
    static ProtocolViolationException protocolViolation(Throwable t) {
        return protocolViolation(t, MAX_CHAIN_DEPTH);
    }

    private static ProtocolViolationException protocolViolation(Throwable t, int depthLeft) {
        if (t == null || depthLeft == 0) {
            return null;
        }
        if (t instanceof ProtocolViolationException violation) {
            return violation;
        }
        for (Throwable suppressed : t.getSuppressed()) {
            ProtocolViolationException violation = protocolViolation(suppressed, depthLeft - 1);
            if (violation != null) {
                return violation;
            }
        }
        return protocolViolation(t.getCause(), depthLeft - 1);
    }

    /**
     * Heuristic broken-pipe detection (downstream reader closed the pipe). The
     * JDK surfaces this as an {@link IOException} whose message is platform
     * dependent ("Broken pipe", "Stream closed").
     */
    public static boolean isBrokenPipe(IOException e) {
        return BrokenPipe.is(e);
    }
}
