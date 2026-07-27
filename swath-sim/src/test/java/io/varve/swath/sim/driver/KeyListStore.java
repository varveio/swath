/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

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
 * <p>The kernel's closed-form invariants depend only on how many keys each range holds, so they are
 * stated against this store rather than a Parquet-backed one: it makes those tests fast enough to run
 * on every change and leaves no doubt that a failure is the kernel's arithmetic rather than a
 * fixture's contents. That the driver also works against a real ground-truth store is a separate
 * claim, asserted separately in {@link SequentialListingDriverStoreTest}.
 */
final class KeyListStore implements ListingStore {

    private final List<byte[]> keys;
    private int calls;
    private int closes;

    private KeyListStore(List<byte[]> keys) {
        this.keys = keys;
    }

    /** A store over {@code count} keys named {@code key-00000}, {@code key-00001}, ... */
    static KeyListStore ofGeneratedKeys(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(key(i));   // fixed-width decimal, so generation order IS unsigned byte order
        }
        return new KeyListStore(keys);
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
    static byte[] key(int i) {
        return String.format(Locale.ROOT, "key-%010d", i).getBytes(StandardCharsets.UTF_8);
    }

    /** Store calls served since construction — the count a lifecycle assertion reads. */
    int calls() {
        return calls;
    }

    /** How many times a caller closed this handle; the driver must never be one of them. */
    int closes() {
        return closes;
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        calls++;
        List<ListedObject> out = new ArrayList<>();
        for (byte[] k : keys) {
            if (from != null) {
                int cmp = Arrays.compareUnsigned(k, from.toByteArray());
                if (cmp < 0 || (cmp == 0 && !fromInclusive)) {
                    continue;
                }
            }
            if (toExclusive != null && Arrays.compareUnsigned(k, toExclusive.toByteArray()) >= 0) {
                break;
            }
            out.add(new ListedObject(k, 0L, 0L, null, null, null, null, null, null));
            if (out.size() == limit) {
                break;
            }
        }
        return out;
    }

    @Override
    public void close() {
        closes++;
    }
}
