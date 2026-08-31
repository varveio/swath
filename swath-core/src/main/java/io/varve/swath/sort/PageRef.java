/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Immutable location and routing metadata for one framed page-run record. */
record PageRef(int segmentId, long ordinal, long offset, int framedLen,
               byte[] minKey, byte[] maxKey, int count, int rawPayloadLength) {
    /**
     * Conservative retained heap excluding the two key payloads: the record, both byte-array
     * headers/alignment, and worst-case uncompressed object references. Keeping the key bytes
     * separate lets admission use the post-cascade catalog's observed bound instead of an average
     * magic price that becomes false on long-key corpora.
     */
    private static final int FIXED_RETAINED_BYTES = 112;

    PageRef {
        if (segmentId < 0 || ordinal < 0 || offset < 0 || framedLen <= 8
                || count < 1 || rawPayloadLength < 1) {
            throw new IllegalArgumentException("invalid page reference");
        }
    }

    /** Upper bound for one reference whose two routing keys are at most {@code maxKeyBytes}. */
    static int retainedBytes(int maxKeyBytes) {
        if (maxKeyBytes < 0) {
            throw new IllegalArgumentException("maximum key length must not be negative");
        }
        return Math.addExact(FIXED_RETAINED_BYTES, Math.multiplyExact(2, maxKeyBytes));
    }
}
