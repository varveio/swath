/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.StealMath;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The protocol half of a simulated store call: turns the four listing shapes the engine issues — a
 * worker's page, a one-key pivot probe, a bounded {@code delimiter=/} structure probe, and a leaf
 * floor probe — into range reads against a ground-truth {@link ListingStore}, which speaks ranges and
 * nothing else.
 *
 * <p><b>Why the rollup lives here.</b> The store seam deliberately excludes delimiter semantics: a
 * store answers "the next rows in this range" and the caller decides what a directory is. So a
 * delimiter probe is a skip-scan over that seam — read forward, and every time a key turns out to sit
 * under a directory, emit that directory once and jump the cursor past everything beneath it. That is
 * the same walk the real client's pager performs, and doing it here rather than inside a store keeps
 * every backend answering the identical, narrow contract.
 *
 * <p><b>Costing.</b> Nothing here consumes virtual time. A call's modelled duration is drawn when the
 * request is issued and the store is consulted only when the response arrives, so the number of range
 * reads a rollup happens to need is a property of the fixture's layout, not of the simulated world —
 * it shows up in how long the simulator takes to run, never in what it reports.
 *
 * <p>The handle is the caller's: opened once outside, used here, never closed here.
 */
final class SimListingView {

    /** Rows read per range hop while rolling up a directory listing. */
    private static final int ROLLUP_BATCH = 256;

    /** One page as the store answered it, before any range bound is applied. */
    record Page(List<byte[]> keys, boolean truncated) {
    }

    /** One {@code delimiter=/} probe's distilled result. */
    record Rollup(List<byte[]> commonPrefixes, int objectCount, byte[] lastKey, boolean capped) {
    }

    private final ListingStore store;
    private final byte[] scanPrefix;
    private final byte[] scanCeiling;
    private long storeReads;

    SimListingView(ListingStore store, byte[] scanPrefix) {
        this.store = store;
        this.scanPrefix = scanPrefix == null ? new byte[0] : scanPrefix;
        this.scanCeiling = StealMath.prefixCeil(this.scanPrefix);
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
        List<byte[]> keys = read(startAfter, false, scanCeiling, maxKeys);
        return new Page(keys, keys.size() == maxKeys);
    }

    /**
     * The one-key speculative probe: is there a key after {@code startAfter} that is still at or below
     * {@code hi}? An open {@code hi} accepts any key the probe finds.
     */
    boolean probeNonEmpty(byte[] startAfter, byte[] hi) {
        List<byte[]> keys = read(startAfter, false, scanCeiling, 1);
        if (keys.isEmpty()) {
            return false;
        }
        return hi == null || KeyBytes.compareUnsigned(keys.getFirst(), hi) <= 0;
    }

    /** The flat-leaf floor probe: the first key at or after {@code prefix}, or {@code null}. */
    byte[] firstKeyUnder(byte[] prefix) {
        byte[] ceiling = StealMath.prefixCeil(prefix);
        List<byte[]> keys = read(prefix, true, ceiling, 1);
        return keys.isEmpty() ? null : keys.getFirst();
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
        byte[] prefix = probePrefix == null ? scanPrefix : probePrefix;
        byte[] ceiling = StealMath.prefixCeil(prefix);
        // Entries in key order, each flagged as a rolled-up directory or a bare object. One entry past
        // the limit is collected on purpose: finding it is the only way to tell a directory that ends
        // exactly at the limit from one that goes on, and the two are read very differently upstream.
        List<byte[]> entries = new ArrayList<>();
        List<Boolean> isDirectory = new ArrayList<>();
        boolean fromInclusive = startAfter == null;
        byte[] from = startAfter == null ? prefix : startAfter;
        boolean exhausted = false;
        while (entries.size() <= maxKeys && !exhausted) {
            List<byte[]> batch = read(from, fromInclusive, ceiling, ROLLUP_BATCH);
            if (batch.isEmpty()) {
                exhausted = true;
                break;
            }
            boolean jumped = false;
            for (byte[] key : batch) {
                if (entries.size() > maxKeys) {
                    break;
                }
                int slash = indexOfDelimiter(key, prefix.length);
                if (slash < 0) {
                    entries.add(key);
                    isDirectory.add(false);
                    from = key;
                    fromInclusive = false;
                    continue;
                }
                byte[] child = Arrays.copyOf(key, slash + 1);
                entries.add(child);
                isDirectory.add(true);
                byte[] childCeiling = StealMath.prefixCeil(child);
                if (childCeiling == null) {
                    // Every byte of the child prefix is 0xFF: nothing sorts after it, so the walk is
                    // finished rather than merely past this directory.
                    exhausted = true;
                    break;
                }
                from = childCeiling;
                fromInclusive = true;
                jumped = true;
                break;
            }
            if (!jumped && batch.size() < ROLLUP_BATCH) {
                exhausted = true;   // the directory ended inside this batch
            }
        }
        boolean capped = entries.size() > maxKeys;
        int kept = Math.min(entries.size(), maxKeys);
        List<byte[]> commonPrefixes = new ArrayList<>();
        int objects = 0;
        byte[] lastKey = null;
        for (int i = 0; i < kept; i++) {
            if (isDirectory.get(i)) {
                commonPrefixes.add(entries.get(i));
            } else {
                objects++;
            }
            lastKey = entries.get(i);
        }
        return new Rollup(commonPrefixes, objects, lastKey, capped);
    }

    /** Range reads issued against the fixture — the sim's own cost, never the modelled system's. */
    long storeReads() {
        return storeReads;
    }

    private int indexOfDelimiter(byte[] key, int from) {
        for (int i = from; i < key.length; i++) {
            if (key[i] == '/') {
                return i;
            }
        }
        return -1;
    }

    private List<byte[]> read(byte[] from, boolean fromInclusive, byte[] toExclusive, int limit) {
        storeReads++;
        List<ListedObject> rows = store.rows(from == null ? null : ByteKey.copyOf(from), fromInclusive,
                toExclusive == null ? null : ByteKey.copyOf(toExclusive), limit, Projection.KEYS_ONLY);
        List<byte[]> keys = new ArrayList<>(rows.size());
        for (ListedObject row : rows) {
            keys.add(row.key());
        }
        return keys;
    }
}
