/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Allocation-free source-run classifier; counts saturate at two because only 0/1/many matters. */
final class MergeRunTracker {

    private final int[] counts;
    private int previous = -1;

    MergeRunTracker(int sources) {
        counts = new int[sources];
    }

    void emittedFrom(int source) {
        if (source != previous) {
            if (counts[source] < 2) {
                counts[source]++;
            }
            previous = source;
        }
    }

    int count(int source) {
        return counts[source];
    }

    void seedCountForTesting(int source, int count) {
        counts[source] = count;
    }
}
