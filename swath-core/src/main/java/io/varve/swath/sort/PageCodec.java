/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import java.util.Arrays;

/**
 * Compress-at-pack codec for a {@link PageBlock}'s front-coded PAYLOAD — the record HEADER
 * (min/max keys, dict tables, {@code useDict}, count, ordered-bit) is never
 * touched by this: it stays plain so {@link PageFrontierReader} parses it without decompressing.
 * Centralizes the on-disk codec byte mapping ({@link #code()} / {@link #fromCode(byte)}) and every
 * codec's compress/decompress implementation in one place.
 *
 * <p><b>Thread-safety.</b> {@link #compress} and slice-aware {@link #decompress} allocate a fresh codec
 * instance (LZ4) or call stateless static methods (NONE, ZSTD1) on every invocation — no shared
 * mutable state, so concurrent calls from different threads (e.g. a future pack-on-fetch-thread
 * caller) never interfere with each other. Each call is otherwise a simple, one-shot,
 * whole-buffer compress/decompress — no streaming state to reuse across calls.
 */
enum PageCodec {

    /** Payload stored verbatim — {@link #compress}/{@link #decompress} are no-ops. */
    NONE((byte) 0) {
        @Override
        byte[] compress(byte[] raw) {
            return raw;
        }

        @Override
        byte[] decompress(byte[] stored, int offset, int length, int rawLen) {
            if (length != rawLen) {
                throw new IllegalStateException(
                        "PageBlock NONE codec length mismatch: expected " + rawLen + " but stored is "
                                + length);
            }
            return offset == 0 && length == stored.length
                    ? stored : Arrays.copyOfRange(stored, offset, offset + length);
        }
    },

    /** Pure-Java LZ4 (aircompressor) — default codec: fast compress/decompress. */
    LZ4((byte) 1) {
        @Override
        byte[] compress(byte[] raw) {
            Lz4Compressor compressor = new Lz4Compressor();
            byte[] out = new byte[compressor.maxCompressedLength(raw.length)];
            int n = compressor.compress(raw, 0, raw.length, out, 0, out.length);
            return Arrays.copyOf(out, n);
        }

        @Override
        byte[] decompress(byte[] stored, int offset, int length, int rawLen) {
            byte[] out = new byte[rawLen];
            int n;
            try {
                n = new Lz4Decompressor().decompress(stored, offset, length, out, 0, rawLen);
            } catch (MalformedInputException e) {
                throw new IllegalStateException("corrupt LZ4 PageBlock payload", e);
            }
            if (n != rawLen) {
                throw new IllegalStateException(
                        "LZ4 decompressed length mismatch: expected " + rawLen + " but got " + n);
            }
            return out;
        }
    },

    /** ZSTD (zstd-jni) at compression level 1 — a higher-ratio alternative to {@link #LZ4}. */
    ZSTD1((byte) 2) {
        @Override
        byte[] compress(byte[] raw) {
            int bound = (int) Zstd.compressBound(raw.length);
            byte[] out = new byte[bound];
            long n = Zstd.compressByteArray(out, 0, out.length, raw, 0, raw.length, ZSTD_LEVEL);
            if (Zstd.isError(n)) {
                throw new IllegalStateException("ZSTD PageBlock compress failed: " + Zstd.getErrorName(n));
            }
            return Arrays.copyOf(out, (int) n);
        }

        @Override
        byte[] decompress(byte[] stored, int offset, int length, int rawLen) {
            byte[] out = new byte[rawLen];
            long n = Zstd.decompressByteArray(out, 0, rawLen, stored, offset, length);
            if (Zstd.isError(n)) {
                throw new IllegalStateException("ZSTD PageBlock decompress failed: " + Zstd.getErrorName(n));
            }
            if (n != rawLen) {
                throw new IllegalStateException(
                        "ZSTD decompressed length mismatch: expected " + rawLen + " but got " + n);
            }
            return out;
        }
    };

    /** {@code swath.sort.segment-codec} default: compress-at-pack on by default. */
    static final PageCodec DEFAULT = LZ4;

    private static final int ZSTD_LEVEL = 1;

    private final byte code;

    PageCodec(byte code) {
        this.code = code;
    }

    /** The {@link PageBlock#serialize()} record's codec byte for this codec. */
    byte code() {
        return code;
    }

    /** Compress {@code raw} (a page's front-coded payload) per this codec. */
    abstract byte[] compress(byte[] raw);

    /**
     * Decompress the {@code [offset, offset + length)} slice of {@code stored} (this codec's {@link
     * #compress} output) back to the original {@code rawLen} bytes. Fails fast ({@link
     * IllegalStateException}) on any length mismatch or corrupt input, rather than silently
     * returning a truncated/garbled buffer.
     */
    abstract byte[] decompress(byte[] stored, int offset, int length, int rawLen);

    /** The codec whose {@link #code()} is {@code code}, or throws if none matches (corrupt record). */
    static PageCodec fromCode(byte code) {
        for (PageCodec c : values()) {
            if (c.code == code) {
                return c;
            }
        }
        throw new IllegalStateException("unsupported PageBlock codec: " + code);
    }

    /** Case-insensitive parse of a config value ({@code NONE}/{@code LZ4}/{@code ZSTD1}). */
    static PageCodec fromConfigValue(String raw) {
        String v = raw.trim();
        for (PageCodec c : values()) {
            if (c.name().equalsIgnoreCase(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(
                "unknown page codec: " + raw + " (expected NONE, LZ4, or ZSTD1)");
    }
}
