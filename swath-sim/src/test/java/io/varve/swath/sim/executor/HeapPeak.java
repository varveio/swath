/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;

/**
 * The heap high-water mark <b>of one leg</b>, for the harnesses that report what a run cost as well as
 * what it found.
 *
 * <p>Per-leg is the whole point, and it is why this reads the memory pools rather than {@link Runtime}:
 * a pool's peak-usage mark is resettable, so a figure taken between {@link #reset()} and {@link #peakMb()}
 * describes the leg between them. A total read once after every leg has finished is the same number
 * every time and says nothing about any of them.
 *
 * <p>It is an upper bound on what a leg held, not its live set — it counts heap allocated and not yet
 * collected, so it moves with GC timing as well as with the run, and it necessarily includes whatever
 * the shared store handle and the previous leg left on the heap.
 */
final class HeapPeak {

    private HeapPeak() {
    }

    /** Drops every heap pool's peak mark, so the next {@link #peakMb()} measures one leg and not the run. */
    static void reset() {
        heapPools().forEach(MemoryPoolMXBean::resetPeakUsage);
    }

    /** The heap high-water mark since the last {@link #reset()}, in MB — see the class note. */
    static double peakMb() {
        long peak = 0;
        for (MemoryPoolMXBean pool : heapPools()) {
            MemoryUsage usage = pool.getPeakUsage();
            if (usage != null) {
                peak += usage.getUsed();
            }
        }
        return peak / (double) (1 << 20);
    }

    /**
     * Live heap after a collection — the on-heap half of "what did the fixture cost to hold". For the
     * one place a forced collection buys a figure worth having, an open; the per-leg column uses
     * {@link #peakMb()} instead, which needs no collection at all.
     */
    static double liveMbAfterCollection() {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (double) (1 << 20);
    }

    private static List<MemoryPoolMXBean> heapPools() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                .filter(pool -> pool.getType() == MemoryType.HEAP)
                .toList();
    }
}
