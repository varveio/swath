/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.util.ArrayList;
import java.util.List;

/**
 * Shapes the simulator's worker-page, pivot, structure, and floor listing calls. ListObjectsV2
 * semantics, including exact-range truncation and delimiter resume, are delegated to
 * {@link ListObjectsV2Pager}; this view only builds requests and distills responses.
 *
 * <p>The store is borrowed, and this view consumes no virtual time. Fixture reads affect simulator
 * runtime only, not modelled call durations.
 */
final class SimListingView {

    /** The engine's structure-probe delimiter. */
    private static final byte[] DELIMITER = {'/'};

    /** Required by the request shape but never sent to a service. */
    private static final String BUCKET = "sim";

    /**
     * A worker response before the client applies its upper bound.
     *
     * <p>Because {@code keys} contains byte arrays, generated equality and hashing compare array
     * references.
     */
    record Page(List<byte[]> keys, boolean truncated) {
    }

    /**
     * A distilled {@code delimiter=/} structure probe. {@code capped} reports that the request
     * truncated; callers decide what that fact means.
     *
     * <p>Its byte arrays make generated equality and hashing reference-based.
     */
    record Rollup(List<byte[]> commonPrefixes, int objectCount, byte[] lastKey, boolean capped) {
    }

    private final CountingStore store;
    private final ListObjectsV2Pager pager;
    private final byte[] scanPrefix;

    SimListingView(ListingStore store, byte[] scanPrefix) {
        this.store = new CountingStore(store);
        // Simulation counters and timelines are recorded elsewhere.
        this.pager = new ListObjectsV2Pager(this.store, new ReplayMetrics());
        this.scanPrefix = scanPrefix == null ? new byte[0] : scanPrefix;
    }

    /**
     * Returns up to {@code maxKeys} keys after {@code startAfter}, scoped to the scan prefix. The
     * response intentionally ignores the client's upper bound; the client trims it afterward.
     *
     * @param startAfter the exclusive resume cursor, or {@code null} to start at the prefix
     */
    Page page(byte[] startAfter, int maxKeys) {
        S3ListResult result = list(scanPrefix, null, startAfter, maxKeys);
        List<byte[]> keys = new ArrayList<>(result.entries().size());
        for (S3ResultEntry entry : result.entries()) {
            keys.add(entry.key());
        }
        return new Page(keys, result.truncated());
    }

    /** Tests whether the one-key pivot probe finds a key after {@code startAfter} and within {@code hi}. */
    boolean probeNonEmpty(byte[] startAfter, byte[] hi) {
        List<S3ResultEntry> entries = list(scanPrefix, null, startAfter, 1).entries();
        if (entries.isEmpty()) {
            return false;
        }
        return hi == null || KeyBytes.compareUnsigned(entries.getFirst().key(), hi) <= 0;
    }

    /** Returns the flat-leaf floor probe's first key under {@code prefix}, or {@code null}. */
    byte[] firstKeyUnder(byte[] prefix) {
        List<S3ResultEntry> entries = list(prefix, null, null, 1).entries();
        return entries.isEmpty() ? null : entries.getFirst().key();
    }

    /**
     * Runs a {@code maxKeys}-bounded structure probe under {@code probePrefix}, or the scan prefix
     * when it is {@code null}, resumed after {@code startAfter}.
     */
    Rollup rollup(byte[] probePrefix, byte[] startAfter, int maxKeys) {
        S3ListResult result = list(probePrefix == null ? scanPrefix : probePrefix, DELIMITER, startAfter, maxKeys);
        List<byte[]> commonPrefixes = new ArrayList<>();
        int objects = 0;
        byte[] lastKey = null;
        for (S3ResultEntry entry : result.entries()) {
            if (entry instanceof S3ResultEntry.CommonPrefixResult) {
                commonPrefixes.add(entry.key());
            } else {
                objects++;
            }
            lastKey = entry.key();
        }
        return new Rollup(commonPrefixes, objects, lastKey, result.truncated());
    }

    /** Fixture reads performed by the simulator, separate from modelled listing calls. */
    long storeReads() {
        return store.reads();
    }

    private S3ListResult list(byte[] prefix, byte[] delimiter, byte[] startAfter, int maxKeys) {
        return pager.list(new S3ListRequest(BUCKET, prefix, delimiter, startAfter, null, maxKeys, false, false));
    }

    /** Counts fixture work performed while the pager answers modelled calls. */
    private static final class CountingStore implements ListingStore {

        private final ListingStore delegate;
        private long reads;

        private CountingStore(ListingStore delegate) {
            this.delegate = delegate;
        }

        private long reads() {
            return reads;
        }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                       Projection projection) {
            reads++;
            return delegate.rows(from, fromInclusive, toExclusive, limit, projection);
        }

        /** A declined rollup performs no fixture read and therefore does not increment the counter. */
        @Override
        public List<DelimitedEntry> delimitedRollup(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                                    byte[] prefix, byte[] delimiter, int limit,
                                                    Projection projection) {
            List<DelimitedEntry> rollup =
                    delegate.delimitedRollup(from, fromInclusive, toExclusive, prefix, delimiter, limit, projection);
            if (rollup != null) {
                reads++;
            }
            return rollup;
        }

        /** The fixture handle is borrowed from the caller. */
        @Override
        public void close() {
        }
    }
}
