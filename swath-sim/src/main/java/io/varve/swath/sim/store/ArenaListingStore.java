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
 * A {@link ListingStore} that answers every range read from a {@link KeyArena} held in memory:
 * one binary search for the lower bound, then a walk. No query engine, no I/O, no decode — the
 * per-call cost a simulator needs when it issues millions of list requests per sweep, where a
 * bounded Parquet query's tens of milliseconds would dominate the whole run.
 *
 * <h2>The sim-mode projection: keys are ground truth, metadata is not</h2>
 * The arena loads <b>keys only</b> and answers through {@link SimModeRows}, which states that
 * contract for every keys-only tier in this module.
 *
 * <h2>No {@code delimitedRollup} override</h2>
 * {@link ListingStore#delimitedRollup} exists because a store with a high fixed per-call cost
 * turns a wide {@code delimiter=/} probe into hundreds of round trips. This store has no fixed
 * per-call cost worth amortising: the pager's range walk is a binary search and a scan over an
 * in-memory array. Declining keeps every delimiter rule in the pager, where the differential
 * suite can attribute a disagreement to a store by construction.
 */
public final class ArenaListingStore implements ListingStore {

    /** Rows pulled per source read while loading. Large enough that load is a scan, not a ping-pong. */
    static final int LOAD_BATCH_ROWS = 65_536;

    private final KeyArena arena;

    private ArenaListingStore(KeyArena arena) {
        this.arena = arena;
    }

    /**
     * Streams every key out of {@code source} — any {@link ListingStore}, so an arena can be built
     * over whichever backend can read the fixture on disk — and packs it into an arena, returning
     * empty when the result would exceed {@code maxEncodedBytes}.
     *
     * <p>The budget is checked as keys arrive rather than predicted up front: an exact answer,
     * whose wasted work is bounded by the budget itself, beats estimating a mean key length. The
     * caller is left to decide what an over-budget fixture means — a hard failure for an
     * explicitly requested arena, a fall back to another backend for an automatic choice.
     *
     * <p>{@code source} is read, not adopted: this store does not close it and does not reference
     * it afterwards.
     */
    public static Optional<ArenaListingStore> loadWithin(ListingStore source, long maxEncodedBytes) {
        return loadWithin(source, maxEncodedBytes, KeyArena.SEGMENT_BYTES);
    }

    // segmentBytes is a seam, not a knob: production always uses KeyArena.SEGMENT_BYTES, and a test
    // pins it small so cross-segment keys are exercised end-to-end through the pager rather than
    // only in the arena's own unit tests.
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

    /** This arena's encoded footprint in bytes — the quantity the capacity budget is measured in. */
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
        // The exclusive upper bound is one more binary search, not a comparison per row: both ends
        // of the half-open range are resolved to indices before a single key is materialised.
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
        // Nothing to release: the arena is plain heap, reclaimed with this store.
    }

    private int start(ByteKey from, boolean fromInclusive) {
        if (from == null) {
            return 0;
        }
        byte[] bound = from.toByteArray();
        return fromInclusive ? arena.lowerBound(bound) : arena.upperBound(bound);
    }
}
