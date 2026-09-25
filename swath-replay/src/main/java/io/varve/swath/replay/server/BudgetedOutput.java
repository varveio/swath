/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.metrics.ChunkAllocationReason;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Request-owned chunked UTF-8 sink; charged arrays live through socket completion. */
public final class BudgetedOutput extends OutputStream {
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    private static final boolean[] URL_SAFE = urlSafeTable();
    private static final int DEFAULT_CHUNK_BYTES = 256 * 1024;

    private final ResponseByteBudget budget;
    private final int cap;
    private final int chunkBytes;
    private final boolean standalone;
    private final ChunkAllocator allocator;
    private final List<Chunk> chunks = new ArrayList<>();
    private final byte[] decimalScratch = new byte[20];
    private int reservedCredit;
    private int allocatedCapacity;
    private int firstChunkHint;
    private int length;
    private int percentOnePassValues;
    private int percentExactFallbackValues;
    private boolean closed;
    private OwnedBody body;

    BudgetedOutput(ResponseByteBudget budget, int initial, int cap) {
        this(budget, initial, cap, configuredChunkBytes(), false, byte[]::new);
    }

    BudgetedOutput(ResponseByteBudget budget, int initial, int cap, int chunkBytes) {
        this(budget, initial, cap, chunkBytes, false, byte[]::new);
    }

    BudgetedOutput(ResponseByteBudget budget, int initial, int cap, int chunkBytes,
                   ChunkAllocator allocator) {
        this(budget, initial, cap, chunkBytes, false, allocator);
    }

    private BudgetedOutput(ResponseByteBudget budget, int initial, int cap, int chunkBytes,
                           boolean standalone, ChunkAllocator allocator) {
        if (cap <= 0 || initial <= 0 || initial > cap || chunkBytes <= 0) {
            throw new IllegalArgumentException("invalid response output bounds");
        }
        this.budget = budget;
        this.cap = cap;
        this.chunkBytes = chunkBytes;
        this.standalone = standalone;
        this.allocator = allocator;
        firstChunkHint = initial;
        if (!budget.tryCharge(initial)) {
            throw new ReplayOutputException(false);
        }
        // One nonblocking pre-page reservation. Chunks consume this credit only as needed.
        reservedCredit = initial;
    }

    public static BudgetedOutput standalone(int initial) {
        return new BudgetedOutput(new ResponseByteBudget(Long.MAX_VALUE), initial,
                Integer.MAX_VALUE, DEFAULT_CHUNK_BYTES, true, byte[]::new);
    }

    static int configuredChunkBytes() {
        return DEFAULT_CHUNK_BYTES;
    }

    @Override
    public void write(int value) {
        appendByte(value);
    }

    @Override
    public void write(byte[] source, int offset, int count) {
        java.util.Objects.checkFromIndexSize(offset, count, source.length);
        ensureTotal(count);
        while (count > 0) {
            Chunk chunk = writableChunk();
            int copied = Math.min(count, chunk.bytes.length - chunk.used);
            System.arraycopy(source, offset, chunk.bytes, chunk.used, copied);
            chunk.used += copied;
            length += copied;
            offset += copied;
            count -= copied;
        }
    }

    /** Freeze the response and return unused admission credit before network writes. */
    public OwnedBody body() {
        if (closed) throw new IllegalStateException("response output is closed");
        if (body != null) return body;
        List<ByteBuffer> views = new ArrayList<>(chunks.size());
        for (Chunk chunk : chunks) {
            views.add(ByteBuffer.wrap(chunk.bytes, 0, chunk.used).asReadOnlyBuffer());
        }
        body = OwnedBody.budgeted(this, length, views);
        if (reservedCredit > 0) {
            budget.release(reservedCredit);
            reservedCredit = 0;
        }
        return body;
    }

    /** Compatibility flattening is permitted only for standalone, non-serving callers. */
    public ByteBuffer buffer() {
        if (!standalone) {
            throw new IllegalStateException("serving responses must use chunked body()");
        }
        OwnedBody rendered = body();
        byte[] flattened = new byte[rendered.length()];
        int offset = 0;
        for (ByteBuffer view : rendered.views()) {
            ByteBuffer copy = view.duplicate();
            int count = copy.remaining();
            copy.get(flattened, offset, count);
            offset += count;
        }
        return ByteBuffer.wrap(flattened);
    }

    public int size() { return length; }
    public int capacity() { return allocatedCapacity; }
    int chargedCapacityAndCredit() { return allocatedCapacity + reservedCredit; }
    int chunkBytes() { return chunkBytes; }
    int percentOnePassValues() { return percentOnePassValues; }
    int percentExactFallbackValues() { return percentExactFallbackValues; }

    public void setFirstChunkHint(int hint) {
        if (hint <= 0) return;
        if (closed || body != null || !chunks.isEmpty()) {
            throw new IllegalStateException("first chunk hint must precede rendering");
        }
        firstChunkHint = hint;
    }

    boolean owns(OwnedBody candidate) {
        return !closed && body != null && body == candidate && candidate.owner() == this
                && candidate.length() == length;
    }

    public void appendByte(int value) {
        ensureTotal(1);
        Chunk chunk = writableChunk();
        chunk.bytes[chunk.used++] = (byte) value;
        length++;
    }

    @SuppressWarnings("deprecation")
    public void appendAscii(String value) {
        ensureTotal(value.length());
        int offset = 0;
        while (offset < value.length()) {
            Chunk chunk = writableChunk();
            int copied = Math.min(value.length() - offset, chunk.bytes.length - chunk.used);
            // All callers are known ASCII tags, fixed values, or the validated fast path below.
            value.getBytes(offset, offset + copied, chunk.bytes, chunk.used);
            chunk.used += copied;
            length += copied;
            offset += copied;
        }
    }

    public void appendPercentEncoded(byte[] value) {
        int inputLength = value.length;
        if (closed) throw new IllegalStateException("response output is closed");
        if (body != null) throw new IllegalStateException("response output is frozen");
        if (!chunks.isEmpty()) {
            Chunk last = chunks.getLast();
            int room = last.bytes.length - last.used;
            // This chunk is already charged, and length <= allocatedCapacity <= cap.
            // A three-byte upper bound avoids another reservation, allocation, or scan.
            if (inputLength <= room / 3) {
                int cursor = last.used;
                for (byte b : value) {
                    int v = b & 0xff;
                    if (URL_SAFE[v]) {
                        last.bytes[cursor++] = b;
                    } else {
                        last.bytes[cursor++] = '%';
                        last.bytes[cursor++] = HEX[v >>> 4];
                        last.bytes[cursor++] = HEX[v & 0x0f];
                    }
                }
                length += cursor - last.used;
                last.used = cursor;
                percentOnePassValues++;
                return;
            }
        }
        long encodedLength = inputLength;
        for (byte b : value) {
            if (!URL_SAFE[b & 0xff]) encodedLength += 2;
        }
        if (encodedLength > Integer.MAX_VALUE) throw new ReplayOutputException(true);
        ensureTotal((int) encodedLength);
        Chunk chunk = null;
        for (byte b : value) {
            int v = b & 0xff;
            if (URL_SAFE[v]) {
                if (chunk == null || chunk.used == chunk.bytes.length) chunk = writableChunk();
                chunk.bytes[chunk.used++] = b;
                length++;
            } else {
                if (chunk == null || chunk.used == chunk.bytes.length) chunk = writableChunk();
                if (chunk.bytes.length - chunk.used >= 3) {
                    chunk.bytes[chunk.used++] = '%';
                    chunk.bytes[chunk.used++] = HEX[v >>> 4];
                    chunk.bytes[chunk.used++] = HEX[v & 0x0f];
                    length += 3;
                } else {
                    // A three-byte escape can straddle a chunk boundary without padding a gap.
                    appendByte('%');
                    appendByte(HEX[v >>> 4]);
                    appendByte(HEX[v & 0x0f]);
                    chunk = chunks.getLast();
                }
            }
        }
        percentExactFallbackValues++;
    }

    public void appendEscaped(String value) {
        int safeLength = 0;
        while (safeLength < value.length()) {
            char c = value.charAt(safeLength);
            if (c > 0x7f || c == '&' || c == '<' || c == '>' || c == '"' || c == '\'') break;
            safeLength++;
        }
        if (safeLength == value.length()) {
            appendAscii(value);
            return;
        }
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
        int cursor = decimalScratch.length;
        do {
            decimalScratch[--cursor] = (byte) ('0' + magnitude % 10);
            magnitude /= 10;
        } while (magnitude != 0);
        if (negative) decimalScratch[--cursor] = '-';
        write(decimalScratch, cursor, decimalScratch.length - cursor);
    }

    public void appendThreeDigits(int value) {
        appendByte('0' + value / 100);
        appendByte('0' + value / 10 % 10);
        appendByte('0' + value % 10);
    }

    private void appendCodePoint(int codePoint) {
        if (codePoint <= 0x7f) {
            appendByte(codePoint);
        } else if (codePoint <= 0x7ff) {
            appendByte(0xc0 | codePoint >>> 6);
            appendByte(0x80 | codePoint & 0x3f);
        } else if (codePoint <= 0xffff) {
            appendByte(0xe0 | codePoint >>> 12);
            appendByte(0x80 | codePoint >>> 6 & 0x3f);
            appendByte(0x80 | codePoint & 0x3f);
        } else {
            appendByte(0xf0 | codePoint >>> 18);
            appendByte(0x80 | codePoint >>> 12 & 0x3f);
            appendByte(0x80 | codePoint >>> 6 & 0x3f);
            appendByte(0x80 | codePoint & 0x3f);
        }
    }

    private void ensureTotal(int additional) {
        if (closed) throw new IllegalStateException("response output is closed");
        if (body != null) throw new IllegalStateException("response output is frozen");
        if ((long) length + additional > cap) throw new ReplayOutputException(true);
    }

    private Chunk writableChunk() {
        if (!chunks.isEmpty()) {
            Chunk last = chunks.getLast();
            if (last.used < last.bytes.length) return last;
        }
        boolean first = chunks.isEmpty();
        int preferred = first ? Math.min(chunkBytes, Math.max(4096, firstChunkHint))
                : chunkBytes;
        int bytes = Math.min(preferred, cap - allocatedCapacity);
        boolean capPartial = bytes < preferred;
        if (bytes <= 0) throw new ReplayOutputException(true);
        int credit = Math.min(reservedCredit, bytes);
        int extra = bytes - credit;
        if (extra > 0 && !budget.tryCharge(extra)) throw new ReplayOutputException(false);
        Chunk chunk;
        try {
            byte[] allocated = allocator.allocate(bytes);
            if (allocated.length != bytes) {
                throw new IllegalStateException("chunk allocator returned unexpected capacity");
            }
            chunk = new Chunk(allocated);
            chunks.add(chunk);
            reservedCredit -= credit;
            allocatedCapacity += bytes;
        } catch (Throwable failure) {
            if (extra > 0) budget.release(extra);
            throw failure;
        }
        ChunkAllocationReason reason = first
                ? capPartial ? ChunkAllocationReason.INITIAL_CAP_PARTIAL : ChunkAllocationReason.INITIAL
                : capPartial ? ChunkAllocationReason.CAP_PARTIAL : ChunkAllocationReason.CONTINUATION;
        budget.chunkAllocated(reason, bytes);
        return chunk;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        body = null;
        chunks.clear();
        budget.release((long) allocatedCapacity + reservedCredit);
        allocatedCapacity = 0;
        reservedCredit = 0;
    }

    private static boolean[] urlSafeTable() {
        boolean[] safe = new boolean[256];
        for (int value = 'A'; value <= 'Z'; value++) safe[value] = true;
        for (int value = 'a'; value <= 'z'; value++) safe[value] = true;
        for (int value = '0'; value <= '9'; value++) safe[value] = true;
        safe['-'] = true;
        safe['_'] = true;
        safe['.'] = true;
        safe['/'] = true;
        return safe;
    }

    private static final class Chunk {
        private final byte[] bytes;
        private int used;

        private Chunk(byte[] bytes) { this.bytes = bytes; }
    }

    @FunctionalInterface
    interface ChunkAllocator {
        byte[] allocate(int bytes);
    }
}
