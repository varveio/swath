/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Request-owned byte sink; actual array capacity stays charged through socket completion. */
public final class BudgetedOutput extends OutputStream {
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    private static final boolean[] URL_SAFE = urlSafeTable();

    private final ResponseByteBudget budget;
    private final int cap;
    private byte[] bytes;
    private int length;
    private boolean closed;

    BudgetedOutput(ResponseByteBudget budget, int initial, int cap) {
        if (cap <= 0 || initial <= 0 || initial > cap) {
            throw new IllegalArgumentException("invalid response output bounds");
        }
        this.budget = budget;
        this.cap = cap;
        if (!budget.tryCharge(initial)) {
            throw new ReplayOutputException(false);
        }
        try {
            bytes = new byte[initial];
        } catch (Throwable e) {
            budget.release(initial);
            throw e;
        }
    }

    public static BudgetedOutput standalone(int initial) {
        return new BudgetedOutput(new ResponseByteBudget(Long.MAX_VALUE), initial, Integer.MAX_VALUE);
    }

    @Override
    public void write(int value) {
        ensure(1);
        bytes[length++] = (byte) value;
    }

    @Override
    public void write(byte[] source, int offset, int count) {
        java.util.Objects.checkFromIndexSize(offset, count, source.length);
        ensure(count);
        System.arraycopy(source, offset, bytes, length, count);
        length += count;
    }

    public ByteBuffer buffer() {
        if (closed) {
            throw new IllegalStateException("response output is closed");
        }
        return ByteBuffer.wrap(bytes, 0, length);
    }

    public int size() { return length; }
    public int capacity() { return bytes == null ? 0 : bytes.length; }

    boolean owns(ByteBuffer body) {
        return !closed && body.hasArray() && body.array() == bytes
                && body.arrayOffset() >= 0 && body.limit() <= length;
    }

        public void appendByte(int value) {
            ensure(1);
            bytes[length++] = (byte) value;
        }

        @SuppressWarnings("deprecation")
        public void appendAscii(String value) {
            ensure(value.length());
            // This overload copies the low byte of each UTF-16 code unit straight into the target
            // array. Every caller is intentionally ASCII (tags, fixed values, or the proven-safe
            // fast path in appendEscaped), so truncation is exactly the desired encoding and avoids
            // both a temporary byte[] and a Java-level character-copy loop.
            value.getBytes(0, value.length(), bytes, length);
            length += value.length();
        }

        public void appendPercentEncoded(byte[] value) {
            long encodedLength = value.length;
            for (byte b : value) {
                if (!URL_SAFE[b & 0xff]) {
                    encodedLength += 2;
                }
            }
            if (encodedLength > Integer.MAX_VALUE) {
                throw new ReplayOutputException(true);
            }
            ensure((int) encodedLength);
            for (byte b : value) {
                int v = b & 0xff;
                if (URL_SAFE[v]) {
                    bytes[length++] = b;
                } else {
                    bytes[length++] = '%';
                    bytes[length++] = HEX[v >>> 4];
                    bytes[length++] = HEX[v & 0x0f];
                }
            }
        }

        public void appendEscaped(String value) {
            int safeLength = 0;
            while (safeLength < value.length()) {
                char c = value.charAt(safeLength);
                if (c > 0x7f || c == '&' || c == '<' || c == '>' || c == '"' || c == '\'') {
                    break;
                }
                safeLength++;
            }
            if (safeLength == value.length()) {
                appendAscii(value);
                return;
            }
            // One byte per code unit is enough for the overwhelmingly common ASCII case. Safe ASCII
            // is copied directly below; only metacharacters and non-ASCII take the general encoder.
            for (int offset = 0; offset < value.length();) {
                char c = value.charAt(offset);
                switch (c) {
                    case '&' -> appendAscii("&amp;");
                    case '<' -> appendAscii("&lt;");
                    case '>' -> appendAscii("&gt;");
                    case '"' -> appendAscii("&quot;");
                    case '\'' -> appendAscii("&apos;");
                    default -> {
                        if (c <= 0x7f) {
                            appendByte(c);
                            offset++;
                            continue;
                        }
                        int codePoint;
                        if (Character.isHighSurrogate(c) && offset + 1 < value.length()
                                && Character.isLowSurrogate(value.charAt(offset + 1))) {
                            codePoint = Character.toCodePoint(c, value.charAt(offset + 1));
                            offset++;
                        } else if (Character.isSurrogate(c)) {
                            // String.getBytes(UTF_8), used by the old renderer, replaces malformed
                            // UTF-16 with the encoder's one-byte default replacement, '?'.
                            codePoint = '?';
                        } else {
                            codePoint = c;
                        }
                        appendCodePoint(codePoint);
                    }
                }
                offset++;
            }
        }

        public void appendLong(long value) {
            if (value == Long.MIN_VALUE) {
                appendAscii("-9223372036854775808");
                return;
            }
            boolean negative = value < 0;
            long magnitude = negative ? -value : value;
            int digits = 1;
            for (long remaining = magnitude; remaining >= 10; remaining /= 10) {
                digits++;
            }
            int width = digits + (negative ? 1 : 0);
            ensure(width);
            int end = length + width;
            int cursor = end;
            do {
                bytes[--cursor] = (byte) ('0' + magnitude % 10);
                magnitude /= 10;
            } while (magnitude != 0);
            if (negative) {
                bytes[length] = '-';
            }
            length = end;
        }

        public void appendThreeDigits(int value) {
            ensure(3);
            bytes[length++] = (byte) ('0' + value / 100);
            bytes[length++] = (byte) ('0' + value / 10 % 10);
            bytes[length++] = (byte) ('0' + value % 10);
        }

        private void appendCodePoint(int codePoint) {
            if (codePoint <= 0x7f) {
                appendByte(codePoint);
            } else if (codePoint <= 0x7ff) {
                ensure(2);
                bytes[length++] = (byte) (0xc0 | codePoint >>> 6);
                bytes[length++] = (byte) (0x80 | codePoint & 0x3f);
            } else if (codePoint <= 0xffff) {
                ensure(3);
                bytes[length++] = (byte) (0xe0 | codePoint >>> 12);
                bytes[length++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                bytes[length++] = (byte) (0x80 | codePoint & 0x3f);
            } else {
                ensure(4);
                bytes[length++] = (byte) (0xf0 | codePoint >>> 18);
                bytes[length++] = (byte) (0x80 | codePoint >>> 12 & 0x3f);
                bytes[length++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                bytes[length++] = (byte) (0x80 | codePoint & 0x3f);
            }
        }


        private static boolean[] urlSafeTable() {
            boolean[] safe = new boolean[256];
            for (int value = 'A'; value <= 'Z'; value++) {
                safe[value] = true;
            }
            for (int value = 'a'; value <= 'z'; value++) {
                safe[value] = true;
            }
            for (int value = '0'; value <= '9'; value++) {
                safe[value] = true;
            }
            safe['-'] = true;
            safe['_'] = true;
            safe['.'] = true;
            safe['/'] = true;
            return safe;
        }
    private void ensure(int additional) {
        if (closed) {
            throw new IllegalStateException("response output is closed");
        }
        long needed = (long) length + additional;
        if (needed > cap) {
            throw new ReplayOutputException(true);
        }
        if (needed <= bytes.length) {
            return;
        }
        int oldCapacity = bytes.length;
        int grown = oldCapacity + (oldCapacity >>> 1);
        int next = (int) Math.min(cap, Math.max(needed, grown));
        // The old and new arrays coexist while copying, so both are charged.
        if (!budget.tryCharge(next)) {
            throw new ReplayOutputException(false);
        }
        byte[] replacement;
        try {
            replacement = Arrays.copyOf(bytes, next);
        } catch (Throwable e) {
            budget.release(next);
            throw e;
        }
        bytes = replacement;
        budget.release(oldCapacity);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            byte[] released = bytes;
            bytes = null;
            budget.release(released.length);
        }
    }
}
