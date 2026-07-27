/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.Projection;

/**
 * The <b>sim-mode projection</b>: keys are ground truth, metadata is not. Both keys-only tiers
 * ({@link ArenaListingStore}, {@link StreamingListingStore}) return their rows through here, so the
 * contract is stated and stubbed in exactly one place.
 *
 * <p>Every metadata column of a returned row is a stub — {@link #STUB_SIZE},
 * {@link #STUB_LAST_MODIFIED_EPOCH_MICROS}, and {@code null} etag / storage class / owner / checksum
 * fields — and the {@link Projection} a caller passes is deliberately ignored, because nothing was
 * loaded for it to select from.
 *
 * <p>This is <b>not</b> {@link Projection#KEYS_ONLY}, despite the name: that projection only drops
 * the two owner fields, and a store honouring it still materialises size, etag and dates because the
 * replay server's XML renderer needs them. A simulator renders no XML; it decides splits, steals and
 * pagination from keys. Skipping metadata entirely is what makes a whole bucket's key set affordable
 * in memory — so responses from these tiers are ground truth for <b>keys, pagination and truncation,
 * and nothing else</b>. A caller that needs real metadata must use a Parquet-backed backend.
 */
final class SimModeRows {

    /** The stubbed object size every sim-mode row reports. */
    static final long STUB_SIZE = 0L;

    /** The stubbed last-modified every sim-mode row reports. */
    static final long STUB_LAST_MODIFIED_EPOCH_MICROS = 0L;

    private SimModeRows() {
    }

    /** {@code key} as a listing row with every metadata column stubbed. */
    static ListedObject stub(byte[] key) {
        return new ListedObject(key, STUB_SIZE, STUB_LAST_MODIFIED_EPOCH_MICROS,
                null, null, null, null, null, null);
    }
}
