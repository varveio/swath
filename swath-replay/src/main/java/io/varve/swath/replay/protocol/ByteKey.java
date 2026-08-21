/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.util.Arrays;

/**
 * Immutable, defensively-copied wrapper around an object key's raw bytes, used at the
 * {@code ListingStore} range seam (bounds are {@code ByteKey}s, never bare {@code byte[]}).
 *
 * <p>Keys are compared with unsigned byte ordering (S3's total order, {@code KeyBytes.compareUnsigned}
 * semantics). This is deliberately a narrow seam type: it centralises the defensive copy so a
 * store implementation can never mutate a caller's array and vice-versa. Row payloads still carry
 * {@code byte[]} (no big-bang migration) — only the store bounds cross through {@code ByteKey}.
 */
public final class ByteKey implements Comparable<ByteKey> {

    private final byte[] bytes;

    private ByteKey(byte[] bytes) {
        this.bytes = bytes;
    }

    /** Wraps a defensive copy of {@code raw}. */
    public static ByteKey copyOf(byte[] raw) {
        return new ByteKey(raw.clone());
    }

    /** Returns a defensive copy of the underlying bytes. */
    public byte[] toByteArray() {
        return bytes.clone();
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public int compareTo(ByteKey other) {
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ByteKey other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "ByteKey[" + ByteKeys.utf8(bytes) + "]";
    }
}
