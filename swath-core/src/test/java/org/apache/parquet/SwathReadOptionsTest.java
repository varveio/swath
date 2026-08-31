/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

class SwathReadOptionsTest {

    @Test
    void spikeBridgeMatchesEveryObservableStockBuilderDefault() {
        var configuration = new PlainParquetConfiguration();
        ParquetReadOptions expected = ParquetReadOptions.builder(configuration).build();
        ParquetReadOptions actual = SwathReadOptions.create(configuration, new NoCodecFactory());
        try {
            assertThat(actual.useSignedStringMinMax()).isEqualTo(expected.useSignedStringMinMax());
            assertThat(actual.useStatsFilter()).isEqualTo(expected.useStatsFilter());
            assertThat(actual.useDictionaryFilter()).isEqualTo(expected.useDictionaryFilter());
            assertThat(actual.useRecordFilter()).isEqualTo(expected.useRecordFilter());
            assertThat(actual.useColumnIndexFilter()).isEqualTo(expected.useColumnIndexFilter());
            assertThat(actual.usePageChecksumVerification())
                    .isEqualTo(expected.usePageChecksumVerification());
            assertThat(actual.useBloomFilter()).isEqualTo(expected.useBloomFilter());
            assertThat(actual.useOffHeapDecryptBuffer())
                    .isEqualTo(expected.useOffHeapDecryptBuffer());
            assertThat(actual.useHadoopVectoredIo()).isEqualTo(expected.useHadoopVectoredIo());
            assertThat(actual.getRecordFilter()).isSameAs(expected.getRecordFilter());
            assertThat(actual.getMetadataFilter()).isSameAs(expected.getMetadataFilter());
            assertThat(actual.getAllocator()).isInstanceOf(expected.getAllocator().getClass());
            assertThat(actual.getMaxAllocationSize()).isEqualTo(expected.getMaxAllocationSize());
            assertThat(actual.getPropertyNames()).isEqualTo(expected.getPropertyNames());
            assertThat(actual.getDecryptionProperties()).isEqualTo(expected.getDecryptionProperties());
            assertThat(actual.getMetricsCallback()).isEqualTo(expected.getMetricsCallback());
            assertThat(actual.getConfiguration()).isSameAs(configuration);
        } finally {
            expected.getCodecFactory().release();
            actual.getCodecFactory().release();
        }
    }

    private static final class NoCodecFactory implements CompressionCodecFactory {
        @Override
        public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release() {
        }
    }
}
