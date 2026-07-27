/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.fixture;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * A minimal in-memory {@link ListingStore} over a fixed, sorted key list — the range seam and nothing
 * else.
 *
 * <p>The invariants a simulated run is checked against depend only on which keys exist and how many,
 * so they are stated against this store rather than a Parquet-backed one: it makes those tests fast
 * enough to run on every change and leaves no doubt that a failure is the simulator's arithmetic
 * rather than a fixture's contents. That the same code also works against a real ground-truth store is
 * a separate claim, asserted separately.
 *
 * <p>Binary search, not a scan: a keyspace-shape fixture runs to hundreds of thousands of keys and is
 * read once per modelled call, so a linear walk per read would make the fixture's own cost dominate
 * every measurement taken through it.
 */
public class ListingFixtureStore implements ListingStore {

    private final List<byte[]> keys;
    private int reads;
    private int closes;

    /** A store over an already-sorted, ascending, duplicate-free key list. */
    public ListingFixtureStore(List<byte[]> keys) {
        for (int i = 1; i < keys.size(); i++) {
            if (Arrays.compareUnsigned(keys.get(i - 1), keys.get(i)) >= 0) {
                throw new IllegalArgumentException("fixture keys must ascend in unsigned byte order; "
                        + "entry " + i + " does not");
            }
        }
        this.keys = List.copyOf(keys);
    }

    /** A store over {@code count} keys named {@code key-00000}, {@code key-00001}, ... */
    public static ListingFixtureStore ofGeneratedKeys(int count) {
        List<byte[]> generated = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            generated.add(key(i));   // fixed-width decimal, so generation order IS unsigned byte order
        }
        return new ListingFixtureStore(generated);
    }

    /**
     * The {@code i}-th generated key, so a test can name a range bound without guessing the format.
     *
     * <p>The width covers every non-negative {@code int}, so generation order stays byte order no
     * matter how large a fixture some later test asks for — a narrower field would silently start
     * sorting {@code key-100000} ahead of {@code key-99999} and quietly truncate range scans. The
     * locale is pinned for the same reason: a default locale with non-ASCII digits would reorder the
     * whole fixture.
     */
    public static byte[] key(int i) {
        return String.format(Locale.ROOT, "key-%010d", i).getBytes(StandardCharsets.UTF_8);
    }

    /** Keys in this fixture. */
    public int size() {
        return keys.size();
    }

    /** Range reads served since construction — the count a lifecycle assertion reads. */
    public int reads() {
        return reads;
    }

    /** How many times a caller closed this handle; a run must never be one of them. */
    public int closes() {
        return closes;
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        reads++;
        int start = from == null ? 0 : lowerBound(from.toByteArray(), fromInclusive);
        List<ListedObject> out = new ArrayList<>();
        byte[] end = toExclusive == null ? null : toExclusive.toByteArray();
        for (int i = start; i < keys.size() && out.size() < limit; i++) {
            byte[] key = keys.get(i);
            if (end != null && Arrays.compareUnsigned(key, end) >= 0) {
                break;
            }
            out.add(new ListedObject(key, 0L, 0L, null, null, null, null, null, null));
        }
        return out;
    }

    @Override
    public void close() {
        closes++;
    }

    /** The index of the first key at (or, when exclusive, strictly after) {@code bound}. */
    private int lowerBound(byte[] bound, boolean inclusive) {
        int low = 0;
        int high = keys.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            int cmp = Arrays.compareUnsigned(keys.get(mid), bound);
            if (cmp < 0 || (cmp == 0 && !inclusive)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }
}
