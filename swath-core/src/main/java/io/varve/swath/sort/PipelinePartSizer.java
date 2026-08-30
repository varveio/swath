/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Router-owned part geometry with an internal fixed-row benchmark control. */
final class PipelinePartSizer {
    static final double INITIAL_ENCODED_TO_LOGICAL_RATIO = 0.25;
    static final long DEFAULT_FIXED_ROWS = 1_000_000L;

    enum Policy {
        CALIBRATED_BYTES,
        FIXED_ROWS
    }

    /** Immutable benchmark-selected policy; production always supplies {@link #calibrated()}. */
    record Target(Policy policy, long fixedRows) {
        Target {
            if (fixedRows <= 0) {
                throw new IllegalArgumentException("pipeline fixed-row target must be positive");
            }
        }

        static Target calibrated() {
            return new Target(Policy.CALIBRATED_BYTES, DEFAULT_FIXED_ROWS);
        }

        static Target fixedRows(long rows) {
            return new Target(Policy.FIXED_ROWS, rows);
        }
    }

    private final Policy policy;
    private final long finalFileBytes;
    private final long fixedRows;
    private long encodedBytes;
    private long logicalBytes;

    PipelinePartSizer(long finalFileBytes) {
        this(Target.calibrated(), finalFileBytes);
    }

    PipelinePartSizer(Policy policy, long finalFileBytes, long fixedRows) {
        this(new Target(policy, fixedRows), finalFileBytes);
    }

    PipelinePartSizer(Target target, long finalFileBytes) {
        if (finalFileBytes <= 0) {
            throw new IllegalArgumentException("pipeline byte target must be positive");
        }
        this.policy = target.policy();
        this.finalFileBytes = finalFileBytes;
        this.fixedRows = target.fixedRows();
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
