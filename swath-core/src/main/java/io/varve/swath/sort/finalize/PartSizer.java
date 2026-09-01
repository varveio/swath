/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

/**
 * Thread-safe encoded-part estimator shared by the router and encoders. The router is the sole
 * boundary owner; encoders contribute only the one calibration observation. The first byte-sized
 * part deliberately assumes no compression, then routing pauses for that result before using
 * measured geometry. A small first part costs one footer, while an oversized first wave can leave
 * most encoders idle. The measured ratio is frozen at that part so identical input and configuration
 * produce identical parts at any encoder count; later parts drift from the target exactly as far as
 * the warm-up part misrepresents the corpus.
 */
public final class PartSizer {
    static final double INITIAL_ENCODED_TO_LOGICAL_RATIO = 1.0;

    /** Production byte calibration and a test-selected row-count control. */
    enum Policy {
        CALIBRATED_BYTES,
        FIXED_ROWS
    }

    /** Immutable production- or test-selected policy; production supplies {@link #calibrated()}. */
    public record Target(Policy policy, long fixedRows) {
        public Target {
            if (policy == Policy.FIXED_ROWS && fixedRows <= 0) {
                throw new IllegalArgumentException("pipeline fixed-row target must be positive");
            }
            if (policy == Policy.CALIBRATED_BYTES && fixedRows != 0) {
                throw new IllegalArgumentException("calibrated target does not take a row count");
            }
        }

        public static Target calibrated() {
            return new Target(Policy.CALIBRATED_BYTES, 0);
        }

        static Target fixedRows(long rows) {
            return new Target(Policy.FIXED_ROWS, rows);
        }
    }

    private final Policy policy;
    private final long finalFileBytes;
    private final long fixedRows;
    private boolean calibrated;
    private long encodedBytes;
    private long logicalBytes;

    /**
     * Freeze one sizing policy for the merge. Policy is not mutable because changing it after plans
     * are queued would make adjacent parts obey incomparable boundary rules.
     */
    PartSizer(Target target, long finalFileBytes) {
        if (finalFileBytes <= 0) {
            throw new IllegalArgumentException("pipeline byte target must be positive");
        }
        this.policy = target.policy();
        this.finalFileBytes = finalFileBytes;
        this.fixedRows = target.fixedRows();
    }

    /**
     * Keep the first footer-closed observation and ignore every later one. Routing waits for the
     * warm-up part before it emits another plan, so the retained sample is always part 0's; folding
     * in later parts would tie each boundary to whichever encoders happened to have finished by
     * then. Actual bytes intentionally include Parquet footer overhead, which makes a very small
     * target calibrate pessimistically rather than overshoot.
     */
    synchronized void completed(long actualBytes, long partLogicalBytes) {
        if (calibrated) {
            return;
        }
        calibrated = true;
        encodedBytes = actualBytes;
        logicalBytes = partLogicalBytes;
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

    /**
     * Convert the encoded-byte target through the compression ratio frozen from the first completed
     * part, or the no-compression assumption until one completes.
     */
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
