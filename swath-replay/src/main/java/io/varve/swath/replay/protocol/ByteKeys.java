/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Byte-level key helpers for S3 replay: unsigned comparison, prefix tests, S3 URL/percent codec,
 * and the two <b>role-distinct</b> derivations of a prefix's 0xFF-carry upper bound.
 *
 * <p>The carry (increment the last non-{@code 0xFF} byte, truncate, drop trailing {@code 0xFF}s)
 * yields the same bytes for two different questions, and confusing them silently drops keys or
 * re-opens a finished listing. The API therefore exposes them as two methods with two distinct
 * return types:
 * <ul>
 *   <li>{@link #prefixUpper(byte[])} → {@link UpperBound}: the <b>exclusive upper bound</b> of the
 *       prefix range. All-{@code 0xFF} ⇒ {@link UpperBound.Open} = "no finite upper bound".</li>
 *   <li>{@link #successor(byte[])} → {@link Successor}: the <b>inclusive resume lower bound</b> just
 *       past a rolled-up prefix. All-{@code 0xFF} ⇒ {@link Successor.EndOfKeyspace} = "end of
 *       listing".</li>
 * </ul>
 * Because the types are disjoint, an {@link UpperBound} can never be passed where a {@link Successor}
 * is expected, so the "open upper" and "end of listing" meanings cannot be transposed.
 */
public final class ByteKeys {

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private ByteKeys() {
    }

    public static int compareUnsigned(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    public static boolean startsWith(byte[] value, byte[] prefix) {
        if (prefix == null || prefix.length == 0) {
            return true;
        }
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * The exclusive upper bound of every key that starts with {@code prefix}. An empty prefix or an
     * all-{@code 0xFF} prefix has no finite bound and yields {@link UpperBound.Open}.
     */
    public static UpperBound prefixUpper(byte[] prefix) {
        if (prefix == null || prefix.length == 0) {
            return new UpperBound.Open();
        }
        byte[] carried = carry(prefix);
        return carried == null ? new UpperBound.Open() : new UpperBound.Bounded(ByteKey.copyOf(carried));
    }

    /**
     * The least key strictly greater than every key that starts with {@code prefix} — the inclusive
     * lower bound for resuming a scan past a rolled-up prefix. An all-{@code 0xFF} prefix has no such
     * key and yields {@link Successor.EndOfKeyspace}.
     */
    public static Successor successor(byte[] prefix) {
        byte[] carried = prefix == null ? null : carry(prefix);
        return carried == null ? new Successor.EndOfKeyspace() : new Successor.Key(ByteKey.copyOf(carried));
    }

    /**
     * Shared 0xFF-carry: increment the last byte below {@code 0xFF}, truncate there, and return the
     * result; {@code null} when every byte is {@code 0xFF} (or the input is empty).
     */
    private static byte[] carry(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            int v = prefix[i] & 0xff;
            if (v != 0xff) {
                byte[] upper = Arrays.copyOf(prefix, i + 1);
                upper[i] = (byte) (v + 1);
                return upper;
            }
        }
        return null;
    }

    public static String utf8(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    public static String percentEncode(byte[] raw) {
        StringBuilder out = new StringBuilder(raw.length);
        for (byte b : raw) {
            int v = b & 0xff;
            if ((v >= 'A' && v <= 'Z') || (v >= 'a' && v <= 'z') || (v >= '0' && v <= '9')
                    || v == '-' || v == '_' || v == '.' || v == '/') {
                out.append((char) v);
            } else if (v == ' ') {
                out.append('+');
            } else {
                out.append('%').append(HEX[v >>> 4]).append(HEX[v & 0x0f]);
            }
        }
        return out.toString();
    }

    public static byte[] percentDecode(String encoded) {
        byte[] in = encoded.getBytes(StandardCharsets.US_ASCII);
        ByteArrayOutputStream out = new ByteArrayOutputStream(in.length);
        for (int i = 0; i < in.length; i++) {
            int c = in[i] & 0xff;
            if (c == '%' && i + 2 < in.length) {
                int hi = hexVal(in[i + 1]);
                int lo = hexVal(in[i + 2]);
                if (hi >= 0 && lo >= 0) {
                    out.write((hi << 4) | lo);
                    i += 2;
                    continue;
                }
            }
            out.write(c);
        }
        return out.toByteArray();
    }

    public static byte[] fromHex(String hex) {
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException("odd hex length");
        }
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int hi = hexVal((byte) hex.charAt(i * 2));
            int lo = hexVal((byte) hex.charAt(i * 2 + 1));
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("invalid hex");
            }
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
    }

    private static int hexVal(byte b) {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        if (b >= 'a' && b <= 'f') {
            return b - 'a' + 10;
        }
        if (b >= 'A' && b <= 'F') {
            return b - 'A' + 10;
        }
        return -1;
    }
}
