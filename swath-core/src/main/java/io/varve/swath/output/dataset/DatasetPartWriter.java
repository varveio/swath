/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;

/** Format-specific state for one open dataset part. */
public interface DatasetPartWriter {
    Path path();
    long rows();
    long bufferedDataSize();
    void write(ListEntry entry) throws IOException;

    /**
     * Optionally force physical bytes already accepted by the file without closing, rotating, or
     * publishing the part. Returns the number of newly forced bytes, or zero when disabled/not due.
     * The default deliberately leaves Parquet and other formats unchanged.
     */
    default long maybeSyncData() throws IOException { return 0L; }

    /** Whether {@link #maybeSyncData()} can engage; lets the shared hot loop avoid timing no-ops. */
    default boolean periodicSyncEnabled() { return false; }

    void close() throws IOException;
    void discard() throws IOException;

    /** Byte-exact MD5 of the physical part, available only after {@link #close()} returns. */
    String md5();

    /** CPU time spent maintaining {@link #md5()} on the physical-write path. */
    long digestNanos();

    /**
     * Physical bytes covered only by the final close barrier after the last periodic data sync.
     * Valid only after a successful close; empty when periodic syncing is not enabled.
     */
    default OptionalLong periodicSyncResidualBytes() { return OptionalLong.empty(); }
}
