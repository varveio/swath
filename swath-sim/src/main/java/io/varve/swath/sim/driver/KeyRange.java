/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import io.varve.swath.replay.protocol.ByteKey;

/** A half-open unsigned-byte key range, {@code [fromInclusive, toExclusive)}; null bounds are open.
 * Empty and inverted ranges are rejected.
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

    /** Returns the whole keyspace. */
    public static KeyRange wholeKeyspace() {
        return new KeyRange(null, null);
    }
}
