/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.io.IOException;

/**
 * Out-of-space detection (the sink's filesystem has no room left). The JDK surfaces it as an
 * {@link IOException} whose message is the platform's {@code strerror} text, so — as with
 * {@link BrokenPipe} — the message is the only portable signal.
 *
 * <p>This exists so a full disk is <b>distinguishable from every other output failure</b>. An
 * external runner sizing the workspace has exactly one useful remedy for it (give the run more
 * space) and that remedy is wrong for the rest of {@code OutputException}'s territory, so the two
 * must not share an exit code — see {@link io.varve.swath.error.OutputException#exitCode()}.
 *
 * <p>The real exception is thrown deep in the Parquet/Arrow writer and reaches the surface wrapped,
 * so callers must search the whole cause chain rather than testing one throwable.
 */
public final class DiskFull {

    /**
     * How deep {@link #isIn} follows a chain. A real one is a handful of links; the bound only
     * keeps a self-referential or absurdly nested chain from spinning — same rationale as the
     * protocol-violation walk at the CLI boundary.
     */
    private static final int MAX_CHAIN_DEPTH = 32;

    private DiskFull() {
    }

    /** Whether {@code t} or anything in its cause chain is an out-of-space {@link IOException}. */
    public static boolean isIn(Throwable t) {
        return isIn(t, MAX_CHAIN_DEPTH);
    }

    private static boolean isIn(Throwable t, int depthLeft) {
        if (t == null || depthLeft == 0) {
            return false;
        }
        if (t instanceof IOException io && is(io)) {
            return true;
        }
        return isIn(t.getCause(), depthLeft - 1);
    }

    /**
     * Heuristic out-of-space detection. {@code ENOSPC} is the case that matters; a quota that has
     * been hit ({@code EDQUOT}) is included because it is the same condition from the run's point
     * of view and has the same remedy, and the JDK reports both only as text.
     */
    public static boolean is(IOException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String m = msg.toLowerCase();
        return m.contains("no space left on device")
            || m.contains("enospc")
            || m.contains("disk quota exceeded");
    }
}
