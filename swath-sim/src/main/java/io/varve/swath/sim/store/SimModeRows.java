/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.Projection;

/**
 * Rows for keys-only simulator tiers. All metadata is stubbed and {@link Projection} is ignored:
 * simulation needs keys, pagination, and truncation; callers needing metadata use Parquet.
 */
final class SimModeRows {

    /** Stubbed object size. */
    static final long STUB_SIZE = 0L;

    /** Stubbed last-modified timestamp. */
    static final long STUB_LAST_MODIFIED_EPOCH_MICROS = 0L;

    private SimModeRows() {
    }

    /** Returns {@code key} with stubbed metadata. */
    static ListedObject stub(byte[] key) {
        return new ListedObject(key, STUB_SIZE, STUB_LAST_MODIFIED_EPOCH_MICROS,
                null, null, null, null, null, null);
    }
}
