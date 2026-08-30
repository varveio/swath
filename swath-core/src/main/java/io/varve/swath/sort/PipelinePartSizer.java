/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Thread-safe encoded-part estimator shared by the router and encoders. The router is the sole
 * boundary owner; encoders contribute only completed-part observations. The first byte-sized part
 * deliberately assumes no compression, then routing pauses for that result before using measured
 * geometry. A small first part costs one footer, while an oversized first wave can leave most
 * encoders idle.
 */
final class PipelinePartSizer {
    static final double INITIAL_ENCODED_TO_LOGICAL_RATIO = 1.0;

    /** Production byte calibration and a benchmark-only row-count control. */
    enum Policy {
        CALIBRATED_BYTES,
        FIXED_ROWS
    }

    /** Immutable benchmark-selected policy; production always supplies {@link #calibrated()}. */
    record Target(Policy policy, long fixedRows) {
        Target {
            if (policy == Policy.FIXED_ROWS && fixedRows <= 0) {
                throw new IllegalArgumentException("pipeline fixed-row target must be positive");
            }
            if (policy == Policy.CALIBRATED_BYTES && fixedRows != 0) {
                throw new IllegalArgumentException("calibrated target does not take a row count");
            }
        }

        static Target calibrated() {
            return new Target(Policy.CALIBRATED_BYTES, 0);
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

    /**
     * Freeze one sizing policy for the merge. Policy is not mutable because changing it after plans
     * are queued would make adjacent parts obey incomparable boundary rules.
     */
    PipelinePartSizer(Target target, long finalFileBytes) {
        if (finalFileBytes <= 0) {
            throw new IllegalArgumentException("pipeline byte target must be positive");
        }
        this.policy = target.policy();
        this.finalFileBytes = finalFileBytes;
        this.fixedRows = target.fixedRows();
    }

    /**
     * Add one footer-closed observation. Actual bytes intentionally include Parquet footer overhead;
     * that makes very small targets noisy, while cumulative calibration damps the fixed cost.
     */
    synchronized void completed(long actualBytes, long partLogicalBytes) {
        encodedBytes = Math.addExact(encodedBytes, actualBytes);
        logicalBytes = Math.addExact(logicalBytes, partLogicalBytes);
    }

    /**
     * Decide whether the next distinct-key item belongs in another part. This is a soft threshold;
     * the router, not this estimator, owns equal-key atomicity and the hard reference cap.
     */
    synchronized boolean shouldClose(long currentLogicalBytes, long currentRows) {
        if (policy == Policy.FIXED_ROWS) {
            return currentRows >= fixedRows;
        }
        if (finalFileBytes == Long.MAX_VALUE) {
            return false;
        }
        return currentLogicalBytes >= calibratedLogicalTarget();
    }

    /** Require one durable sample only when byte calibration has a finite target. */
    boolean needsCalibrationWarmup() {
        return policy == Policy.CALIBRATED_BYTES && finalFileBytes != Long.MAX_VALUE;
    }

    /**
     * Choose the deliberately undershooting no-compression first target. A small warm-up costs one
     * footer; an oversized warm-up can starve every encoder behind a single plan.
     */
    static long initialLogicalTarget(long finalFileBytes) {
        if (finalFileBytes == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        double target = finalFileBytes / INITIAL_ENCODED_TO_LOGICAL_RATIO;
        return Math.max(1L, (long) Math.ceil(target));
    }

    /** Convert the encoded-byte target through the cumulative observed compression ratio. */
    synchronized long calibratedLogicalTarget() {
        double ratio = encodedBytes > 0 && logicalBytes > 0
                ? (double) encodedBytes / logicalBytes
                : INITIAL_ENCODED_TO_LOGICAL_RATIO;
        double target = finalFileBytes / Math.max(Double.MIN_NORMAL, ratio);
        return target >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(1L, (long) Math.ceil(target));
    }

    /** Expose the same ratio used by boundary decisions for focused convergence tests. */
    synchronized double encodedToLogicalRatio() {
        return encodedBytes > 0 && logicalBytes > 0
                ? (double) encodedBytes / logicalBytes
                : INITIAL_ENCODED_TO_LOGICAL_RATIO;
    }
}
