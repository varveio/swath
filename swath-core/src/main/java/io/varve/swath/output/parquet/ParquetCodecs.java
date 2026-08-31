/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/** Hadoop-independent codecs for swath's supported Parquet file contract. */
final class ParquetCodecs implements CompressionCodecFactory {

    private final Uncompressed uncompressed = new Uncompressed();
    private final Map<Integer, ZstandardCompressor> compressors = new HashMap<>();
    private final Map<CompressionCodecName, BytesInputDecompressor> decompressors =
            new EnumMap<>(CompressionCodecName.class);

    @Override
    public synchronized BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        return switch (codecName) {
            case UNCOMPRESSED -> uncompressed;
            case ZSTD -> getCompressor(codecName, ListEntryParquetWriters.ZSTD_LEVEL);
            default -> throw unsupported(codecName);
        };
    }

    @Override
    public synchronized BytesInputCompressor getCompressor(CompressionCodecName codecName, int level) {
        if (codecName != CompressionCodecName.ZSTD) {
            return getCompressor(codecName);
        }
        return compressors.computeIfAbsent(level, ZstandardCompressor::new);
    }

    @Override
    public synchronized BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        return decompressors.computeIfAbsent(codecName, name -> switch (name) {
            case UNCOMPRESSED -> uncompressed;
            case ZSTD -> new ZstandardDecompressor();
            default -> throw unsupported(name);
        });
    }

    @Override
    public synchronized void release() {
        compressors.values().forEach(ZstandardCompressor::release);
        decompressors.values().forEach(codec -> ((Releasable) codec).release());
        compressors.clear();
        decompressors.clear();
    }

    private static UnsupportedOperationException unsupported(CompressionCodecName codecName) {
        return new UnsupportedOperationException("unsupported swath Parquet codec: " + codecName);
    }

    private interface Releasable {
        void release();
    }

    private static final class Uncompressed
            implements BytesInputCompressor, BytesInputDecompressor, Releasable {

        @Override
        public BytesInput compress(BytesInput bytes) {
            return bytes;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public BytesInput decompress(BytesInput bytes, int uncompressedSize) {
            return bytes;
        }

        @Override
        public void decompress(
                ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) {
            ByteBuffer source = input.slice();
            source.limit(compressedSize);
            output.put(source);
            input.position(input.position() + compressedSize);
        }

        @Override
        public void release() {
        }
    }

    private static final class ZstandardCompressor implements BytesInputCompressor, Releasable {
        private final ZstdCompressCtx context;

        private ZstandardCompressor(int level) {
            context = new ZstdCompressCtx().setLevel(level).setWorkers(0);
        }

        @Override
        public synchronized BytesInput compress(BytesInput bytes) throws IOException {
            try {
                return BytesInput.from(context.compress(bytes.toByteArray()));
            } catch (ZstdException e) {
                throw new IOException("ZSTD compression failed", e);
            }
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.ZSTD;
        }

        @Override
        public void release() {
            context.close();
        }
    }

    private static final class ZstandardDecompressor implements BytesInputDecompressor, Releasable {
        private final ZstdDecompressCtx context = new ZstdDecompressCtx();

        @Override
        public synchronized BytesInput decompress(BytesInput bytes, int uncompressedSize)
                throws IOException {
            try {
                return BytesInput.from(context.decompress(bytes.toByteArray(), uncompressedSize));
            } catch (ZstdException e) {
                throw new IOException("ZSTD decompression failed", e);
            }
        }

        @Override
        public synchronized void decompress(
                ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
                throws IOException {
            byte[] encoded = new byte[compressedSize];
            input.get(encoded);
            try {
                output.put(context.decompress(encoded, uncompressedSize));
            } catch (ZstdException e) {
                throw new IOException("ZSTD decompression failed", e);
            }
        }

        @Override
        public void release() {
            context.close();
        }
    }
}
