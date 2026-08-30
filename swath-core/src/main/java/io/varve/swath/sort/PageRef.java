/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Immutable location and routing metadata for one framed page-run record. */
record PageRef(int segmentId, long ordinal, long offset, int framedLen,
               byte[] minKey, byte[] maxKey, int count, int rawPayloadLength) {
    PageRef {
        if (segmentId < 0 || ordinal < 0 || offset < 0 || framedLen <= 8
                || count < 1 || rawPayloadLength < 1) {
            throw new IllegalArgumentException("invalid page reference");
        }
    }
}
