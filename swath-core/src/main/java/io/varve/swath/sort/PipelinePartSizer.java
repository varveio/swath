/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Router-owned part geometry with an internal fixed-row benchmark control. */
final class PipelinePartSizer {
    static final String POLICY_PROPERTY = "swath.sort.pipeline-part-policy";
    static final String FIXED_ROWS_PROPERTY = "swath.sort.pipeline-fixed-rows";
    static final double INITIAL_ENCODED_TO_LOGICAL_RATIO = 0.25;
    static final long DEFAULT_FIXED_ROWS = 1_000_000L;

    enum Policy {
        CALIBRATED_BYTES,
        FIXED_ROWS;

        static Policy fromSystemProperties() {
            String raw = System.getProperty(POLICY_PROPERTY, "calibrated").trim();
            return switch (raw) {
                case "calibrated" -> CALIBRATED_BYTES;
                case "fixed-rows" -> FIXED_ROWS;
                default -> throw new IllegalArgumentException(POLICY_PROPERTY
                        + ": expected calibrated or fixed-rows, got '" + raw + "'");
            };
        }
    }

    private final Policy policy;
    private final long finalFileBytes;
    private final long fixedRows;
    private long encodedBytes;
    private long logicalBytes;

    PipelinePartSizer(long finalFileBytes) {
        this(Policy.fromSystemProperties(), finalFileBytes,
                Long.getLong(FIXED_ROWS_PROPERTY, DEFAULT_FIXED_ROWS));
    }

    PipelinePartSizer(Policy policy, long finalFileBytes, long fixedRows) {
        if (finalFileBytes <= 0 || fixedRows <= 0) {
            throw new IllegalArgumentException("pipeline part targets must be positive");
        }
        this.policy = policy;
        this.finalFileBytes = finalFileBytes;
        this.fixedRows = fixedRows;
    }

    synchronized void completed(long actualBytes, long partLogicalBytes) {
        encodedBytes = Math.addExact(encodedBytes, actualBytes);
        logicalBytes = Math.addExact(logicalBytes, partLogicalBytes);
    }

    synchronized boolean shouldClose(long currentLogicalBytes, long currentRows) {
        if (policy == Policy.FIXED_ROWS) {
            return currentRows >= fixedRows;
        }
        if (finalFileBytes == Long.MAX_VALUE) {
            return false;
        }
        return currentLogicalBytes >= calibratedLogicalTarget();
    }

    synchronized long calibratedLogicalTarget() {
        double ratio = encodedBytes > 0 && logicalBytes > 0
                ? (double) encodedBytes / logicalBytes
                : INITIAL_ENCODED_TO_LOGICAL_RATIO;
        double target = finalFileBytes / Math.max(Double.MIN_NORMAL, ratio);
        return target >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(1L, (long) Math.ceil(target));
    }

    synchronized double encodedToLogicalRatio() {
        return encodedBytes > 0 && logicalBytes > 0
                ? (double) encodedBytes / logicalBytes
                : INITIAL_ENCODED_TO_LOGICAL_RATIO;
    }
}
