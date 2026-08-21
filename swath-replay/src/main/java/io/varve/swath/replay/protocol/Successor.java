/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/**
 * The least key strictly greater than every key sharing a given prefix, as computed by
 * {@link ByteKeys#successor(byte[])} — the <b>resume-lower-bound role</b> of the 0xFF-carry
 * computation.
 *
 * <p>It is used to seek past a rolled-up common prefix in delimiter mode: after emitting
 * {@code CommonPrefix P}, the next scan resumes at {@code successor(P)} <em>inclusive</em>
 * (S3 §9.2: {@code successor("photos/") == "photos0"} can be a real key).
 *
 * <p>An all-{@code 0xFF} prefix has no successor: nothing sorts after it, so this means
 * {@link EndOfKeyspace} — the listing is finished, <b>not</b> "re-open from the start". This is a
 * distinct type from {@link UpperBound} so that this "end of listing" reading of the bytes can
 * never be mistaken for {@link UpperBound.Open}'s "open upper bound" reading (see {@link ByteKeys}).
 */
public sealed interface Successor {

    /** A concrete resume key: scanning resumes at {@code key}, inclusive. */
    record Key(ByteKey key) implements Successor {
    }

    /** No key sorts after the prefix (it is all {@code 0xFF}): the listing has ended. */
    record EndOfKeyspace() implements Successor {
    }
}
