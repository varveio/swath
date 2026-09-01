/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.stage;

import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.spill.SealTrigger;
import java.util.Objects;

/**
 * The shared staging-segment fullness policy. The byte threshold has precedence when both limits
 * are reached by the same entry, preserving the listing lane's trigger classification.
 */
public record SegmentGate(SortConfig config) {

    public SegmentGate {
        Objects.requireNonNull(config, "config");
    }

    public boolean full(long estimatedBytes, long entries) {
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
