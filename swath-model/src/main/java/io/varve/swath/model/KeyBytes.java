/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * An object key as <b>raw bytes</b>, with S3's ordering (unsigned lexicographic
 * over the bytes, exactly as {@code start-after} compares). Contract §1.1, I10.
 *
 * <p><b>Never</b> compare keys via {@code String.compareTo}: UTF-16 order
 * diverges from UTF-8 byte order for code points ≥ U+0800 and across
 * supplementary planes, which is a silent correctness bug for boundaries. All
 * ordering/midpoint/stop-checks go through {@link #compareUnsigned}.
 *
 * <p>The raw array is treated as immutable internally: {@link #of(byte[])} does
 * <b>not</b> defensive-copy (hot path). Public boundary access via {@link #raw()}
 * returns a defensive copy; engine/checkpoint/output internals use
 * {@link #rawUnsafe()} and must not mutate the returned array.
 */
public final class KeyBytes implements Comparable<KeyBytes> {

    private final byte[] raw;
    private String cachedString;   // lazy; benign race (idempotent decode)

    private KeyBytes(byte[] raw) {
        this.raw = raw;
    }

    /** Wrap raw key bytes (no defensive copy — treat as immutable). */
    public static KeyBytes of(byte[] raw) {
        return new KeyBytes(raw);
    }

    /** Convenience for tests/text input: UTF-8 encode. */
    public static KeyBytes ofUtf8(String s) {
        return new KeyBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    /** S3's order: unsigned lexicographic over the raw bytes. */
    public static int compareUnsigned(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    @Override
    public int compareTo(KeyBytes o) {
        return Arrays.compareUnsigned(this.raw, o.raw);
    }

    /** Public boundary copy: callers may mutate the returned array without corrupting this key. */
    public byte[] raw() {
        return raw.clone();
    }

    /** Internal no-copy view for hot paths; treat the returned array as immutable. */
    public byte[] rawUnsafe() {
        return raw;
    }

    /** Lazy UTF-8 decode — output/filter boundary ONLY. */
    public String asString() {
        String s = cachedString;
        if (s == null) {
            s = new String(raw, StandardCharsets.UTF_8);
            cachedString = s;
        }
        return s;
    }

    /** Length in bytes (≤ 1024 for S3). */
    public int length() {
        return raw.length;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof KeyBytes other && Arrays.equals(this.raw, other.raw);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(raw);
    }

    /** Lossy for binary keys; use {@link #asString()} / {@link #raw()} when exactness matters. */
    @Override
    public String toString() {
        return asString();
    }
}
