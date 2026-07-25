/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/**
 * The exclusive upper bound of a prefix range, as computed by {@link ByteKeys#prefixUpper(byte[])}.
 *
 * <p>This is the <b>bound role</b> of the 0xFF-carry computation: an all-{@code 0xFF} prefix has no
 * finite exclusive upper bound, which here means the range is {@link Open} — scan to the end of the
 * store. This is a distinct type from {@link Successor} precisely so the "open upper bound" reading
 * cannot be confused with {@link Successor}'s "end of listing" reading of the very same bytes
 * (see {@link ByteKeys}).
 */
public sealed interface UpperBound {

    /** A finite exclusive upper bound: the range is {@code [from, exclusiveUpper)}. */
    record Bounded(ByteKey exclusiveUpper) implements UpperBound {
    }

    /** No finite upper bound (the prefix is empty or all {@code 0xFF}): the range is open-ended. */
    record Open() implements UpperBound {
    }
}
