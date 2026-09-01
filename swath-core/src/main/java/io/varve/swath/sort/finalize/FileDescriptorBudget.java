/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The file-descriptor budget for a single-pass merge. Raising the {@code fan-in} default to
 * 10&nbsp;000 (so a billion-scale run stays single-pass) means the final merge could try to open one
 * open reader per surviving segment at once — which would blow past the process's open-file limit and
 * fail with {@code EMFILE} hours into a merge. This computes a fan-in ceiling from the process's SOFT
 * open-file limit, reserving {@link #FD_HEADROOM} descriptors for the checkpoint SQLite handle, the
 * output part writers, log files, and the JVM's own fds. When the ceiling forces fan-in below the
 * segment count the merge degrades to the {@link CascadeReducer} cascade backstop (multi-pass) rather than
 * crashing — a misconfigured launcher becomes slower, never fatal.
 *
 * <p>The arithmetic is a pure, injectable function ({@link #clampedFanIn}) so it is testable without
 * depending on the real process {@code ulimit}; only {@link #softOpenFileLimit()} touches the OS.
 */
public final class FileDescriptorBudget {

    private static final Logger log = LoggerFactory.getLogger(FileDescriptorBudget.class);

    /** Descriptors reserved for checkpoint SQLite, output part writers, logs, and JVM internals. */
    static final int FD_HEADROOM = 128;

    /** Conservative soft-limit fallback when {@code /proc/self/limits} is unreadable (non-Linux, etc.). */
    static final int FALLBACK_SOFT_FD_LIMIT = 1024;

    private FileDescriptorBudget() {
    }

    /**
     * The fd-bounded fan-in ceiling: {@code softFdLimit - headroom}, never below 2. Pure/injectable —
     * an unlimited ({@code < 0} sentinel, e.g. {@code RLIM_INFINITY}) soft limit yields
     * {@link Integer#MAX_VALUE} (no fd constraint).
     */
    static int fdBoundedFanIn(int softFdLimit, int headroom) {
        if (softFdLimit < 0) {
            return Integer.MAX_VALUE;   // unlimited (RLIM_INFINITY) — no fd constraint
        }
        return Math.max(2, softFdLimit - headroom);
    }

    /**
     * Compose the runtime-clamped fan-in:
     * {@code max(2, min(staticFanIn, fdBound, recordSizedFanIn))}.
     * {@code recordSizedFanIn} is {@link Integer#MAX_VALUE} when no page-run record-size refinement
     * was computed, so it drops out of the {@code min} and the configured estimate governs.
     * Pure/injectable.
     */
    static int clampedFanIn(int staticFanIn, int softFdLimit, int headroom, int recordSizedFanIn) {
        int fdBound = fdBoundedFanIn(softFdLimit, headroom);
        return Math.max(2, Math.min(staticFanIn, Math.min(fdBound, recordSizedFanIn)));
    }

    /**
     * This process's SOFT open-file limit, parsed from {@code /proc/self/limits} (line
     * {@code "Max open files"}, first numeric column). Returns {@code -1} for an {@code unlimited} soft
     * limit, and {@link #FALLBACK_SOFT_FD_LIMIT} (with a debug log) when the file is absent or
     * unparseable (non-Linux, a locked-down container) so the clamp still applies a conservative
     * ceiling.
     */
    public static int softOpenFileLimit() {
        Path limits = Path.of("/proc/self/limits");
        try {
            List<String> lines = Files.readAllLines(limits);
            for (String line : lines) {
                if (!line.startsWith("Max open files")) {
                    continue;
                }
                // "Max open files    <soft>    <hard>    files" — the soft limit is the first column.
                String rest = line.substring("Max open files".length()).trim();
                String[] cols = rest.split("\\s+");
                if (cols.length == 0 || cols[0].isEmpty()) {
                    break;
                }
                if (cols[0].equalsIgnoreCase("unlimited")) {
                    return -1;
                }
                return Integer.parseInt(cols[0]);
            }
        } catch (IOException | RuntimeException e) {
            log.debug("could not read soft open-file limit from {} — falling back to a conservative {}; "
                    + "a single-pass merge fan-in will be clamped to {} - {} to avoid EMFILE",
                    limits, FALLBACK_SOFT_FD_LIMIT, FALLBACK_SOFT_FD_LIMIT, FD_HEADROOM, e);
            return FALLBACK_SOFT_FD_LIMIT;
        }
        log.debug("no 'Max open files' line in {} — falling back to a conservative soft fd limit of {}",
                limits, FALLBACK_SOFT_FD_LIMIT);
        return FALLBACK_SOFT_FD_LIMIT;
    }
}
