/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

/**
 * Process-wide resource sampling for the end-of-run summary and the live
 * {@code swath.process.*} Micrometer meters (current + peak RSS, current +
 * peak heap, CPU time). Every probe is a cheap on-demand read — no background
 * sampler thread — and degrades gracefully: an unavailable source (non-Linux
 * {@code /proc}, no {@code com.sun} OS bean) returns {@code -1}, which
 * summary/log callers render as {@code unknown} and meter suppliers map to
 * {@code NaN}.
 */
final class ResourceMetrics {

    static final long UNKNOWN = -1L;

    private static final Path PROC_SELF_STATUS = Path.of("/proc/self/status");

    private ResourceMetrics() {
    }

    /** Peak resident set size in bytes from {@code /proc/self/status} {@code VmHWM}, or {@code -1}. */
    static long peakRssBytes() {
        return readStatusKilobytesField("VmHWM:");
    }

    /** Current resident set size in bytes from {@code /proc/self/status} {@code VmRSS}, or {@code -1}. */
    static long currentRssBytes() {
        return readStatusKilobytesField("VmRSS:");
    }

    private static long readStatusKilobytesField(String prefix) {
        if (!Files.isReadable(PROC_SELF_STATUS)) {
            return UNKNOWN;
        }
        try {
            for (String line : Files.readAllLines(PROC_SELF_STATUS)) {
                if (line.startsWith(prefix)) {
                    return parseStatusKilobytes(line);
                }
            }
        } catch (RuntimeException | IOException e) {
            return UNKNOWN;
        }
        return UNKNOWN;
    }

    /** Peak heap usage in bytes, summed across HEAP memory pools, or {@code -1} if unavailable. */
    static long peakHeapBytes() {
        return sumHeapPools(MemoryPoolMXBean::getPeakUsage);
    }

    /** Current heap usage in bytes, summed across HEAP memory pools, or {@code -1} if unavailable. */
    static long currentHeapBytes() {
        return sumHeapPools(MemoryPoolMXBean::getUsage);
    }

    private static long sumHeapPools(Function<MemoryPoolMXBean, MemoryUsage> usageOf) {
        try {
            long total = UNKNOWN;
            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                if (pool.getType() != MemoryType.HEAP) {
                    continue;
                }
                MemoryUsage usage = usageOf.apply(pool);
                if (usage == null) {
                    continue;
                }
                long used = usage.getUsed();
                if (used >= 0) {
                    total = (total == UNKNOWN ? 0L : total) + used;
                }
            }
            return total;
        } catch (RuntimeException e) {
            return UNKNOWN;
        }
    }

    /** Cumulative process CPU time in nanoseconds, or {@code -1} if the platform bean is absent. */
    static long processCpuTimeNanos() {
        try {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
                long cpu = sun.getProcessCpuTime();
                return cpu >= 0 ? cpu : UNKNOWN;
            }
            return UNKNOWN;
        } catch (RuntimeException e) {
            return UNKNOWN;
        }
    }

    /** Parses a {@code "VmHWM:   12345 kB"} status line into bytes, or {@code -1}. */
    private static long parseStatusKilobytes(String line) {
        String[] parts = line.split("\\s+");
        for (int i = 1; i < parts.length; i++) {
            if (!parts[i].isBlank() && Character.isDigit(parts[i].charAt(0))) {
                try {
                    return Long.parseLong(parts[i]) * 1024L;
                } catch (NumberFormatException e) {
                    return UNKNOWN;
                }
            }
        }
        return UNKNOWN;
    }
}
