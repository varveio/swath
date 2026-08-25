/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import java.io.IOException;

/**
 * Format-neutral accounting for periodically forcing bytes from an open output file.
 *
 * <p>This shapes kernel writeback only. It never finalizes or publishes a part, advances a
 * checkpoint, or changes the crash-recovery boundary: a part remains durable iff its format's
 * final close barrier succeeds.
 */
public final class PeriodicDataSync {
    public static final long MIN_INTERVAL_BYTES = 4L * 1024 * 1024;

    private final long intervalBytes;
    private long syncedAtBytes;
    private IOException failure;

    public PeriodicDataSync(long intervalBytes) {
        requireValidInterval(intervalBytes);
        this.intervalBytes = intervalBytes;
    }

    /** Validate the shared disabled state and minimum enabled cadence. */
    public static void requireValidInterval(long intervalBytes) {
        if (intervalBytes != 0 && intervalBytes < MIN_INTERVAL_BYTES) {
            throw new IllegalArgumentException("data-sync interval must be zero or at least "
                    + MIN_INTERVAL_BYTES + " bytes");
        }
    }

    public boolean enabled() {
        return intervalBytes > 0;
    }

    /**
     * Force when newly emitted physical bytes reach the configured interval. Returns the newly
     * covered bytes, or zero when disabled/not due. The first force failure is sticky.
     */
    public long maybeSync(long physicalBytes, Forcer forcer) throws IOException {
        if (!enabled()) {
            return 0L;
        }
        requirePublishable();
        if (physicalBytes < syncedAtBytes) {
            throw new IllegalArgumentException("physical byte progress regressed from "
                    + syncedAtBytes + " to " + physicalBytes);
        }
        long newlyEmitted = physicalBytes - syncedAtBytes;
        if (newlyEmitted < intervalBytes) {
            return 0L;
        }
        try {
            forcer.force();
        } catch (IOException e) {
            failure = e;
            throw e;
        }
        syncedAtBytes = physicalBytes;
        return newlyEmitted;
    }

    /** Refuse publication after any failed periodic force. */
    public void requirePublishable() throws IOException {
        if (failure != null) {
            throw new IOException("refusing to publish after periodic data-sync failure", failure);
        }
    }

    /** Physical tail left for the final close barrier. Valid after final physical size is known. */
    public long residualBytes(long finalPhysicalBytes) {
        if (!enabled()) {
            throw new IllegalStateException("periodic data sync is disabled");
        }
        if (finalPhysicalBytes < syncedAtBytes) {
            throw new IllegalArgumentException("final physical bytes precede the last sync");
        }
        return finalPhysicalBytes - syncedAtBytes;
    }

    @FunctionalInterface
    public interface Forcer {
        void force() throws IOException;
    }
}
