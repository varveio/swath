/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.observability.RunMetrics;
import java.time.Duration;
import java.util.Locale;

/**
 * The shared text idiom of swath's two operator-facing stderr surfaces — the live {@link
 * ProgressDisplay} record and the end-of-run {@link SummaryRenderer} block. One home for the
 * indent, the separator and every number format, so a figure cannot be spelled one way while a run
 * is in flight and another way when it ends.
 *
 * <p>{@link #cost} carries the most weight: the live line and the final block must never disagree
 * about what a run cost or about the rate that estimate assumed.
 */
final class OperatorText {

    private OperatorText() {
    }

    /** Two-space indent, so each block reads as one unit distinct from any log or echo line. */
    static final String INDENT = "  ";

    /** The field separator, matching the progress/summary idiom of modern CLIs. */
    static final String SEP = " · ";

    static String count(long value) {
        return String.format(Locale.ROOT, "%,d", value);
    }

    static String rate(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    /** {@code 4.2s} under a minute, then {@code 4m12s}, then {@code 1h04m}. */
    static String elapsed(Duration duration) {
        long seconds = duration.toSeconds();
        if (seconds < 60) {
            return String.format(Locale.ROOT, "%.1fs", duration.toMillis() / 1000.0);
        }
        if (seconds < 3600) {
            return String.format(Locale.ROOT, "%dm%02ds", seconds / 60, seconds % 60);
        }
        return String.format(Locale.ROOT, "%dh%02dm", seconds / 3600, (seconds % 3600) / 60);
    }

    /** Decimal (1000-based) units, the same convention S3 itself bills and reports in. */
    static String bytes(long value) {
        String[] units = {"B", "KB", "MB", "GB", "TB", "PB"};
        double scaled = value;
        int unit = 0;
        while (scaled >= 1000.0 && unit < units.length - 1) {
            scaled /= 1000.0;
            unit++;
        }
        return unit == 0
                ? count(value) + " B"
                : String.format(Locale.ROOT, "%.1f %s", scaled, units[unit]);
    }

    /**
     * The estimated spend WITH the rate it assumed. Stating the assumption is the honesty: LIST
     * pricing varies by region, so a labeled reference rate lets a reader rescale, where a bare
     * figure would over-claim (and a region→rate table in an OSS repo would silently go stale).
     */
    static String cost(double usd) {
        String amount = usd < 0.001
                ? "<$0.001"
                : String.format(Locale.ROOT, "~$%.3f", usd);
        return amount + " (est. @ $" + RunMetrics.LIST_COST_PER_1K_USD + "/1k LIST)";
    }
}
