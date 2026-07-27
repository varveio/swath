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
 * The arena loads <b>keys only</b>. Every metadata column of a returned row is a stub —
 * {@link #STUB_SIZE}, {@link #STUB_LAST_MODIFIED_EPOCH_MICROS}, and {@code null} etag / storage
 * class / owner / checksum fields — and the {@link Projection} argument is deliberately ignored,
 * because there is nothing loaded for it to select from.
 *
 * <p>This is <b>not</b> {@link Projection#KEYS_ONLY}, despite the name: that projection only
 * drops the two owner fields, and a store honouring it still materialises size, etag and dates
 * because the replay server's XML renderer needs them. A simulator renders no XML; it decides
 * splits, steals and pagination from keys. Skipping metadata entirely is what makes an arena of a
 * bucket's whole key set affordable — so responses from this store are ground truth for
 * <b>keys, pagination and truncation, and nothing else</b>. A caller that needs real metadata
 * must use a Parquet-backed backend.
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

    /** The stubbed object size every arena row reports; see the sim-mode projection above. */
    static final long STUB_SIZE = 0L;

    /** The stubbed last-modified every arena row reports; see the sim-mode projection above. */
    static final long STUB_LAST_MODIFIED_EPOCH_MICROS = 0L;

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
        KeyArena.Builder builder = KeyArena.builder(maxEncodedBytes);
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
        int size = arena.size();
        byte[] upper = toExclusive == null ? null : toExclusive.toByteArray();
        List<ListedObject> rows = new ArrayList<>(Math.min(limit, size - start));
        for (int i = start; i < size && rows.size() < limit; i++) {
            if (upper != null && arena.compareKeyAt(i, upper) >= 0) {
                break;
            }
            rows.add(stub(arena.keyAt(i)));
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

    private static ListedObject stub(byte[] key) {
        return new ListedObject(key, STUB_SIZE, STUB_LAST_MODIFIED_EPOCH_MICROS,
                null, null, null, null, null, null);
    }
}
