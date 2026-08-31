/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.parquet;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;

/**
 * Test-only Phase 2 bridge to the package-private read-options constructor. It exists solely to
 * reveal the linkage edge behind the public builder; production code must not depend on it.
 */
public final class HadoopFreeReadOptionsBridge {

    private HadoopFreeReadOptionsBridge() {
    }

    public static Result read(Path path) throws IOException {
        CompressionCodecFactory codecs = new ReadCodecFactory();
        ParquetReadOptions options = new ParquetReadOptions(
                false,
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                FilterCompat.NOOP,
                ParquetMetadataConverter.NO_FILTER,
                codecs,
                new HeapByteBufferAllocator(),
                Integer.MAX_VALUE,
                Map.of(),
                null,
                null,
                new PlainParquetConfiguration());
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path), options)) {
            var footer = reader.getFooter();
            reader.setRequestedSchema(footer.getFileMetaData().getSchema());
            boolean indexes = reader.getColumnIndexStore(0).getColumnIndex(
                    org.apache.parquet.hadoop.metadata.ColumnPath.get("key")) != null;
            String firstKey;
            try (PageReadStore pages = reader.readRowGroup(0)) {
                var schema = footer.getFileMetaData().getSchema();
                var columnIo = new ColumnIOFactory().getColumnIO(schema);
                Group first = columnIo.getRecordReader(pages, new GroupRecordConverter(schema)).read();
                firstKey = new String(first.getBinary("key", 0).getBytes(), StandardCharsets.UTF_8);
            }
            return new Result(reader.getRecordCount(), footer.getBlocks().size(), indexes, firstKey);
        } finally {
            codecs.release();
        }
    }

    public record Result(long rows, int rowGroups, boolean indexes, String firstKey) {
        @Override
        public String toString() {
            return "rows=" + rows + ",row_groups=" + rowGroups + ",indexes=" + indexes
                    + ",first_key=" + firstKey;
        }
    }

    private static final class ReadCodecFactory implements CompressionCodecFactory {
        @Override
        public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
            throw new UnsupportedOperationException("reader-only laboratory bridge");
        }

        @Override
        public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
            return switch (codecName) {
                case UNCOMPRESSED -> new Uncompressed();
                case ZSTD -> new Zstandard();
                default -> throw new UnsupportedOperationException("laboratory codec: " + codecName);
            };
        }

        @Override
        public void release() {
        }
    }

    private static final class Uncompressed implements CompressionCodecFactory.BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int uncompressedSize) {
            return bytes;
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) {
            ByteBuffer source = input.slice();
            source.limit(compressedSize);
            output.put(source);
        }

        @Override
        public void release() {
        }
    }

    private static final class Zstandard implements CompressionCodecFactory.BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
            byte[] decoded = Zstd.decompress(bytes.toInputStream().readAllBytes(), uncompressedSize);
            if (decoded.length != uncompressedSize) {
                throw new IOException("ZSTD decoded " + decoded.length + " bytes, expected " + uncompressedSize);
            }
            return BytesInput.from(decoded);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
                throws IOException {
            byte[] encoded = new byte[compressedSize];
            input.get(encoded);
            byte[] decoded = Zstd.decompress(encoded, uncompressedSize);
            if (decoded.length != uncompressedSize) {
                throw new IOException("ZSTD decoded " + decoded.length + " bytes, expected " + uncompressedSize);
            }
            output.put(decoded);
        }

        @Override
        public void release() {
        }
    }
}
