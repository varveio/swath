/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.parquet;

import java.util.Map;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

/**
 * Feasibility-spike bridge to Parquet 1.18's package-private read-options constructor.
 *
 * <p>The public generic-configuration builder eagerly creates Hadoop codecs and parses a Hadoop
 * input-format filter before callers can replace either one. Keeping this bridge in Parquet's
 * package lets the spike supply the same defaults plus swath's explicit codec factory without
 * loading Hadoop. This is intentionally version-coupled internal API, not a proposed permanent
 * swath abstraction.
 */
public final class SwathReadOptions {

    private SwathReadOptions() {
    }

    public static ParquetReadOptions create(
            ParquetConfiguration configuration, CompressionCodecFactory codecs) {
        return new ParquetReadOptions(
                false, // signed string min/max
                true,  // statistics filter
                true,  // dictionary filter
                true,  // record filter
                true,  // column-index filter
                false, // page-checksum verification
                true,  // bloom filter
                false, // off-heap decrypt buffer
                true,  // Hadoop-vectored-I/O flag; 1.18 default, also used by generic streams
                FilterCompat.NOOP,
                ParquetMetadataConverter.NO_FILTER,
                codecs,
                new HeapByteBufferAllocator(),
                8 * 1024 * 1024, // Parquet 1.18 builder default
                Map.of(),
                null,
                null,
                configuration);
    }
}
