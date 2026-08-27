/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.Objects;

/**
 * The shared staging-segment fullness policy. The byte threshold has precedence when both limits
 * are reached by the same entry, preserving the listing lane's trigger classification.
 */
record SegmentGate(SortConfig config) {

    SegmentGate {
        Objects.requireNonNull(config, "config");
    }

    boolean full(long estimatedBytes, long entries) {
        return trigger(estimatedBytes, entries) != SealTrigger.DRAIN;
    }

    SealTrigger trigger(long estimatedBytes, long entries) {
        if (estimatedBytes >= config.segmentBytes()) {
            return SealTrigger.BYTE_GATE;
        }
        if (entries >= config.segmentEntries()) {
            return SealTrigger.ENTRY_CAP;
        }
        return SealTrigger.DRAIN;
    }
}
