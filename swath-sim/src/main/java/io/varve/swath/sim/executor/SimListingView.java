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
 * The protocol half of a simulated store call: turns the four listing shapes the engine issues — a
 * worker's page, a one-key pivot probe, a bounded {@code delimiter=/} structure probe, and a leaf
 * floor probe — into ListObjectsV2 requests against a ground-truth {@link ListingStore}, which speaks
 * ranges and nothing else.
 *
 * <p><b>Every protocol rule comes from the pager.</b> Max-keys accounting, truncation, delimiter
 * rollup and common-prefix resume all live once, in {@link ListObjectsV2Pager}, which is also what
 * every replay-server store is differentialled through. This class only shapes requests and reads
 * answers. That matters more here than anywhere else in the module: the policies driven against this
 * view are the production ones, written against S3's protocol, so a second implementation of it —
 * however careful — mismodels the engine rather than the workload, and does so invisibly. Two rules
 * are worth naming because a hand-rolled walk gets both wrong: a resumed delimiter probe must
 * <b>not</b> roll up the directory it resumed at (every key beneath {@code d/} sorts after
 * {@code d/}, so a resume that only lifts the range floor emits {@code d/} twice and pushes a real
 * directory off the end of a page that stays capped), and a page that exactly consumes a range is
 * <b>not</b> truncated (S3 looks one key past what it returns, which is why the pager reads
 * {@code maxKeys + 1} rows). {@code SimListingViewProtocolTest} pins both against the pager.
 *
 * <p><b>Costing.</b> Nothing here consumes virtual time. A call's modelled duration is drawn when the
 * request is issued and the store is consulted only when the response arrives, so the number of range
 * reads a rollup happens to need is a property of the fixture's layout, not of the simulated world —
 * it shows up in how long the simulator takes to run, never in what it reports.
 *
 * <p>The handle is the caller's: opened once outside, used here, never closed here.
 */
final class SimListingView {

    /** The one delimiter the engine ever probes with. */
    private static final byte[] DELIMITER = {'/'};

    /** Any bucket name: nothing below the pager reads it, and no request here crosses a wire. */
    private static final String BUCKET = "sim";

    /**
     * One page as the store answered it, before any range bound is applied.
     *
     * <p>{@code keys} holds raw arrays, so this record's generated {@code equals}/{@code hashCode}
     * compare array <em>references</em>: read the fields, never use a {@code Page} as a map key or in
     * a set.
     */
    record Page(List<byte[]> keys, boolean truncated) {
    }

    /**
     * One {@code delimiter=/} probe's distilled result.
     *
     * <p>Same caveat as {@link Page}: the {@code byte[]} components make the generated
     * {@code equals}/{@code hashCode} reference comparisons, so this is a carrier to read, not a key
     * to look up by.
     */
    record Rollup(List<byte[]> commonPrefixes, int objectCount, byte[] lastKey, boolean capped) {
    }

    private final CountingStore store;
    private final ListObjectsV2Pager pager;
    private final byte[] scanPrefix;

    SimListingView(ListingStore store, byte[] scanPrefix) {
        this.store = new CountingStore(store);
        // The pager insists on a metrics sink; a run's own instrument is its counters and its
        // timeline, so this one is a bit bucket that nothing reads and nothing publishes.
        this.pager = new ListObjectsV2Pager(this.store, new ReplayMetrics());
        this.scanPrefix = scanPrefix == null ? new byte[0] : scanPrefix;
    }

    /**
     * One worker page: up to {@code maxKeys} keys after {@code startAfter}, scoped to the scan prefix
     * and <b>not</b> to the caller's own upper bound. That omission is the protocol being faithful
     * rather than helpful — a real listing call knows the prefix, not the range the client has since
     * narrowed itself to, so the last page of a bounded range comes back full and the client pays for
     * keys it then trims. A simulator that filtered here would quietly make bounded ranges cheaper than
     * they are, which is the exact cost the owner-side split's floor exists to avoid incurring.
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

    /**
     * The one-key speculative probe: is there a key after {@code startAfter} that is still at or below
     * {@code hi}? An open {@code hi} accepts any key the probe finds.
     */
    boolean probeNonEmpty(byte[] startAfter, byte[] hi) {
        List<S3ResultEntry> entries = list(scanPrefix, null, startAfter, 1).entries();
        if (entries.isEmpty()) {
            return false;
        }
        return hi == null || KeyBytes.compareUnsigned(entries.getFirst().key(), hi) <= 0;
    }

    /** The flat-leaf floor probe: the first key at or after {@code prefix}, or {@code null}. */
    byte[] firstKeyUnder(byte[] prefix) {
        List<S3ResultEntry> entries = list(prefix, null, null, 1).entries();
        return entries.isEmpty() ? null : entries.getFirst().key();
    }

    /**
     * One bounded {@code delimiter=/} probe of {@code probePrefix}, resuming after {@code startAfter}:
     * the directories directly beneath it and the objects directly in it, interleaved in key order,
     * stopping at {@code maxKeys} entries.
     *
     * <p>{@code capped} means the probe stopped at its own limit rather than at the end of the
     * directory — the sample of children is a strict prefix of the truth. Both callers read that bit,
     * and they read it differently (one as "the fan-out is at least this wide", the other as "this
     * child holds at least a page of mass"), which is why it is reported as the fact rather than as
     * either interpretation.
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

    /** Range reads issued against the fixture — the sim's own cost, never the modelled system's. */
    long storeReads() {
        return store.reads();
    }

    private S3ListResult list(byte[] prefix, byte[] delimiter, byte[] startAfter, int maxKeys) {
        return pager.list(new S3ListRequest(BUCKET, prefix, delimiter, startAfter, null, maxKeys, false, false));
    }

    /**
     * Counts what the pager asks of the fixture. The pager may need several reads to answer one
     * modelled call (a rollup hops once per directory), and that ratio is the simulator's own runtime
     * cost — hence a count of store work, kept separate from the modelled call counters.
     */
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

        /** The fixture handle belongs to whoever opened it; a run never closes it. */
        @Override
        public void close() {
        }
    }
}
