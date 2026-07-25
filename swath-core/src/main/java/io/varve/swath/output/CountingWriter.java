/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Wraps a text sink {@link Writer}, tracking the UTF-8 encoded byte count of everything written
 * through it: the JSONL/TSV/TABLE formatters write whole lines via {@code
 * Writer.write(String)} (see {@link JsonlFormatter}/{@link TsvFormatter}/{@link AlignedFormatter}),
 * so {@link #bytesWritten()} after the run flushes/closes is the real {@code swath.output.bytes}/
 * {@code compressed_size_bytes} figure.
 *
 * <p>No allocation on the hot path: bytes are derived arithmetically from each char's/surrogate
 * pair's UTF-8 encoded length, never by re-encoding into a {@code byte[]}. {@link FilterWriter}
 * forwards {@code write(int)}, {@code write(char[], int, int)}, and {@code write(String, int,
 * int)} directly to the delegate WITHOUT cross-calling each other, so all three are overridden
 * here (every other {@link Writer} method funnels into one of these three via {@code
 * java.io.Writer}'s default implementations).
 *
 * <p>Correctness of the astral-plane (4-byte) count depends on a surrogate pair never being split
 * across two {@code write} calls — true today because every formatter emits one line per {@code
 * write(String)}. A future streaming formatter that chunked a pair across calls would under-count
 * it (3+3 bytes instead of 4); keep pairs within a single write.
 *
 * <p>An unpaired (lone) surrogate is counted as 3 bytes; this is safe because a production key can
 * never contain one — {@link io.varve.swath.KeyBytes#asString()} decodes the raw key bytes with UTF-8
 * replacement (invalid bytes become U+FFFD, itself a valid 3-byte char), so a lone surrogate never
 * reaches this writer.
 */
public final class CountingWriter extends FilterWriter {

    // volatile: mutated only by the output-consumer thread, but read concurrently by the
    // JSON-summary scheduler thread via bytesWritten(); a single writer means a volatile
    // write is sufficient (no read-modify-write across threads) and avoids a torn 64-bit read.
    private volatile long bytes;

    public CountingWriter(Writer out) {
        super(out);
    }

    /** Total UTF-8 encoded bytes written so far (reflects whatever has been {@code write()}n, flushed or not). */
    public long bytesWritten() {
        return bytes;
    }

    @Override
    public void write(int c) throws IOException {
        out.write(c);
        bytes += utf8Length((char) c);
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        out.write(cbuf, off, len);
        bytes += utf8Length(cbuf, off, len);
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
        out.write(str, off, len);
        bytes += utf8Length(str, off, len);
    }

    /** UTF-8 byte length of a char-array range, treating an unpaired surrogate as one 3-byte code point. */
    private static long utf8Length(char[] cbuf, int off, int len) {
        long total = 0;
        int end = off + len;
        int i = off;
        while (i < end) {
            char c = cbuf[i];
            if (Character.isHighSurrogate(c) && i + 1 < end && Character.isLowSurrogate(cbuf[i + 1])) {
                total += 4;
                i += 2;
            } else {
                total += utf8Length(c);
                i++;
            }
        }
        return total;
    }

    /** UTF-8 byte length of a {@code String} range, treating an unpaired surrogate as one 3-byte code point. */
    private static long utf8Length(String s, int off, int len) {
        long total = 0;
        int end = off + len;
        int i = off;
        while (i < end) {
            char c = s.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < end && Character.isLowSurrogate(s.charAt(i + 1))) {
                total += 4;
                i += 2;
            } else {
                total += utf8Length(c);
                i++;
            }
        }
        return total;
    }

    /** UTF-8 byte length of a single BMP char that is not half of a surrogate pair. */
    private static long utf8Length(char c) {
        if (c < 0x80) {
            return 1;
        } else if (c < 0x800) {
            return 2;
        }
        return 3;
    }
}
