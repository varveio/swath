/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Keys-only {@link ListingStore} backed by a {@link KeyArena}. Rows use {@link SimModeRows}, so
 * metadata is stubbed and {@link Projection} is ignored. Range bounds use binary search; delimiter
 * rollup stays in the pager, where no store round trip needs amortising.
 */
public final class ArenaListingStore implements ListingStore {

    /** Rows fetched per source read while loading. */
    static final int LOAD_BATCH_ROWS = 65_536;

    private final KeyArena arena;

    private ArenaListingStore(KeyArena arena) {
        this.arena = arena;
    }

    /**
     * Loads source keys in exclusive-resume batches, returning empty if their exact encoded size
     * exceeds {@code maxEncodedBytes}. This store borrows and never closes {@code source}.
     */
    public static Optional<ArenaListingStore> loadWithin(ListingStore source, long maxEncodedBytes) {
        return loadWithin(source, maxEncodedBytes, KeyArena.SEGMENT_BYTES);
    }

    // Test seam; production always uses KeyArena.SEGMENT_BYTES.
    static Optional<ArenaListingStore> loadWithin(ListingStore source, long maxEncodedBytes, int segmentBytes) {
        KeyArena.Builder builder = KeyArena.builder(maxEncodedBytes, segmentBytes);
        ByteKey cursor = null;
        while (true) {
            List<ListedObject> batch = source.rows(cursor, false, null, LOAD_BATCH_ROWS, Projection.KEYS_ONLY);
            if (batch.isEmpty()) {
                return Optional.of(new ArenaListingStore(builder.build()));
            }
            for (ListedObject row : batch) {
                if (!builder.append(row.key())) {
                    return Optional.empty();
                }
            }
            cursor = ByteKey.copyOf(batch.getLast().key());
        }
    }

    /** The number of keys held. */
    public int keyCount() {
        return arena.size();
    }

    /** This arena's encoded footprint in the capacity-budget unit. */
    public long encodedBytes() {
        return arena.encodedBytes();
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        if (limit <= 0) {
            return List.of();
        }
        int start = start(from, fromInclusive);
        // Resolve the lower and exclusive upper indices before materialising keys.
        int end = toExclusive == null ? arena.size() : arena.lowerBound(toExclusive.toByteArray());
        if (end <= start) {
            return List.of();
        }
        List<ListedObject> rows = new ArrayList<>(Math.min(limit, end - start));
        for (int i = start; i < end && rows.size() < limit; i++) {
            rows.add(SimModeRows.stub(arena.keyAt(i)));
        }
        return rows;
    }

    @Override
    public void close() {
        // No owned external resource.
    }

    private int start(ByteKey from, boolean fromInclusive) {
        if (from == null) {
            return 0;
        }
        byte[] bound = from.toByteArray();
        return fromInclusive ? arena.lowerBound(bound) : arena.upperBound(bound);
    }
}
