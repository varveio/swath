/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

class ParquetCodecsTest {

    @Test
    void reusesNativeContextsAndRoundTripsZstandard() throws Exception {
        byte[] original = "swath-zstd-context-reuse".repeat(200).getBytes(StandardCharsets.UTF_8);
        ParquetCodecs codecs = new ParquetCodecs();
        try {
            var compressor = codecs.getCompressor(CompressionCodecName.ZSTD);
            var decompressor = codecs.getDecompressor(CompressionCodecName.ZSTD);

            assertThat(codecs.getCompressor(CompressionCodecName.ZSTD)).isSameAs(compressor);
            assertThat(codecs.getCompressor(
                    CompressionCodecName.ZSTD, ListEntryParquetWriters.ZSTD_LEVEL))
                    .isSameAs(compressor);
            assertThat(codecs.getDecompressor(CompressionCodecName.ZSTD)).isSameAs(decompressor);

            BytesInput encoded = compressor.compress(BytesInput.from(original));
            BytesInput decoded = decompressor.decompress(encoded, original.length);
            assertThat(decoded.toInputStream().readAllBytes()).isEqualTo(original);
        } finally {
            codecs.release();
        }

        assertThatCode(codecs::release).doesNotThrowAnyException();
    }
}
