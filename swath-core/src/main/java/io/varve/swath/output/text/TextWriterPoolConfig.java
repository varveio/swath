/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import io.varve.swath.output.OutputFormat;
import java.nio.file.Path;

/** Complete, validated construction input for a partitioned text dataset sink. */
public record TextWriterPoolConfig(
        Path directory, OutputFormat format, TextCompression compression, boolean escape,
        String argsHash, String bucket, int writers, long targetBytes, int queueCapacity,
        long rotationIntervalNanos, long rotationMaxRows) {
    public TextWriterPoolConfig {
        if (directory == null) throw new IllegalArgumentException("directory is required");
        if (format != OutputFormat.TSV && format != OutputFormat.JSONL) {
            throw new IllegalArgumentException("format must be tsv or jsonl");
        }
        if (compression == null) throw new IllegalArgumentException("compression is required");
        if (writers < 2 || writers > 4) throw new IllegalArgumentException("writers must be 2..4");
        if (targetBytes <= 0) throw new IllegalArgumentException("targetBytes must be positive");
        if (queueCapacity <= 0) throw new IllegalArgumentException("queueCapacity must be positive");
        if (rotationIntervalNanos < 0 || rotationMaxRows < 0) {
            throw new IllegalArgumentException("rotation values must be non-negative");
        }
    }
}
