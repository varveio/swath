/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import io.varve.swath.replay.protocol.ByteKey;

/**
 * A half-open span of the keyspace, {@code [fromInclusive, toExclusive)} in unsigned byte order — the
 * unit of work a simulated worker claims and lists to exhaustion.
 *
 * @param fromInclusive lower bound, {@code null} for open (start of the keyspace)
 * @param toExclusive   upper bound, {@code null} for open (end of the keyspace)
 */
public record KeyRange(ByteKey fromInclusive, ByteKey toExclusive) {

    public KeyRange {
        if (fromInclusive != null && toExclusive != null && fromInclusive.compareTo(toExclusive) >= 0) {
            throw new IllegalArgumentException("an empty or inverted range lists nothing: " + fromInclusive
                    + " .. " + toExclusive);
        }
    }

    /** The whole keyspace, both bounds open. */
    public static KeyRange wholeKeyspace() {
        return new KeyRange(null, null);
    }
}
