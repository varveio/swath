/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.OutputFormat;
import java.nio.file.Path;

/** Complete, validated construction input for a partitioned text dataset sink. */
public record TextWriterPoolConfig(
        Path directory, OutputFormat format, TextCompression compression, boolean escape,
        String argsHash, String bucket, int writers, long targetBytes, int queueCapacity,
        long rotationIntervalNanos, long rotationMaxRows, RunMetrics metrics) {
    public static final int MIN_WRITERS = 2;
    public static final int MAX_WRITERS = 4;

    public TextWriterPoolConfig(
            Path directory, OutputFormat format, TextCompression compression, boolean escape,
            String argsHash, String bucket, int writers, long targetBytes, int queueCapacity,
            long rotationIntervalNanos, long rotationMaxRows) {
        this(directory, format, compression, escape, argsHash, bucket, writers, targetBytes,
                queueCapacity, rotationIntervalNanos, rotationMaxRows, null);
    }

    public TextWriterPoolConfig {
        if (directory == null) throw new IllegalArgumentException("directory is required");
        if (format != OutputFormat.TSV && format != OutputFormat.JSONL) {
            throw new IllegalArgumentException("format must be tsv or jsonl");
        }
        if (compression == null) throw new IllegalArgumentException("compression is required");
        if (writers < MIN_WRITERS || writers > MAX_WRITERS) {
            throw new IllegalArgumentException(
                    "writers must be " + MIN_WRITERS + ".." + MAX_WRITERS);
        }
        if (targetBytes <= 0) throw new IllegalArgumentException("targetBytes must be positive");
        if (queueCapacity <= 0) throw new IllegalArgumentException("queueCapacity must be positive");
        if (rotationIntervalNanos < 0 || rotationMaxRows < 0) {
            throw new IllegalArgumentException("rotation values must be non-negative");
        }
    }

    public TextWriterPoolConfig withMetrics(RunMetrics metrics) {
        return new TextWriterPoolConfig(directory, format, compression, escape, argsHash, bucket,
                writers, targetBytes, queueCapacity, rotationIntervalNanos, rotationMaxRows, metrics);
    }
}
