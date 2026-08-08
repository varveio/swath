/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Out-of-space detection (the sink's filesystem has no room left). The JDK surfaces it as an
 * {@link IOException} whose message is the platform's {@code strerror} text, so — as with
 * {@link BrokenPipe} — the message is the only portable signal.
 *
 * <p>The real exception is thrown deep in the Parquet/Arrow writer and reaches the surface wrapped,
 * so callers must search the whole cause chain rather than testing one throwable.
 */
public final class DiskFull {

    private static final Pattern SYMBOLIC_NAME =
            Pattern.compile("(?<![a-z0-9_])(?:enospc|edquot)(?![a-z0-9_])");

    /**
     * How deep {@link #isIn} follows a chain. The bound keeps a self-referential or absurdly nested
     * chain from spinning.
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
     *
     * <p>Each condition is matched by BOTH its {@code strerror} wording and its symbolic name,
     * because which one surfaces depends on the platform and on how far the exception travelled
     * before something re-wrapped it.
     */
    public static boolean is(IOException e) {
        String msg = e instanceof FileSystemException fileSystemException
                ? fileSystemException.getReason()
                : e.getMessage();
        if (msg == null) {
            return false;
        }
        String m = msg.toLowerCase(Locale.ROOT);
        return m.contains("no space left on device")
            || m.contains("disk quota exceeded")
            || SYMBOLIC_NAME.matcher(m).find();
    }
}
