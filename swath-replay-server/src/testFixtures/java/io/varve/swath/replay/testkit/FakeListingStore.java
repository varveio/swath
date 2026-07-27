/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A faithful, range-only {@link ListingStore} over an in-memory sorted list, counting {@code rows()}
 * calls and recording the last requested {@code limit}. Consolidates three near-identical fakes
 * that had drifted apart — the call counter is atomic so it stays safe under the concurrent-walk test.
 */
public final class FakeListingStore implements ListingStore {

    private final List<ListedObject> sorted;
    private final AtomicInteger calls = new AtomicInteger();
    private volatile int lastLimit;

    public FakeListingStore(List<ListedObject> rows) {
        List<ListedObject> copy = new ArrayList<>(rows);
        copy.sort(Comparator.comparing(ListedObject::key, Arrays::compareUnsigned));
        this.sorted = copy;
    }

    /** Rows over UTF-8 string keys, all sharing the same fixed etag/storage-class/owner/checksum fields. */
    public static FakeListingStore ofKeys(String... keys) {
        List<ListedObject> rows = new ArrayList<>(keys.length);
        for (String key : keys) {
            rows.add(defaultRow(key.getBytes(StandardCharsets.UTF_8)));
        }
        return new FakeListingStore(rows);
    }

    /** Rows over raw byte keys, all sharing the same fixed etag/storage-class/owner/checksum fields. */
    public static FakeListingStore ofKeys(byte[]... keys) {
        List<ListedObject> rows = new ArrayList<>(keys.length);
        for (byte[] key : keys) {
            rows.add(defaultRow(key.clone()));
        }
        return new FakeListingStore(rows);
    }

    private static ListedObject defaultRow(byte[] key) {
        return new ListedObject(key, 1, 0, "etag", "STANDARD", "owner", "display", "CRC32", "FULL_OBJECT");
    }

    public int calls() {
        return calls.get();
    }

    public int lastLimit() {
        return lastLimit;
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        calls.incrementAndGet();
        lastLimit = limit;
        byte[] lower = from == null ? null : from.toByteArray();
        byte[] upper = toExclusive == null ? null : toExclusive.toByteArray();
        List<ListedObject> out = new ArrayList<>();
        for (ListedObject row : sorted) {
            byte[] key = row.key();
            if (lower != null) {
                int cmp = Arrays.compareUnsigned(key, lower);
                if (fromInclusive ? cmp < 0 : cmp <= 0) {
                    continue;
                }
            }
            if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) {
                break;
            }
            out.add(row);
            if (out.size() == limit) {
                break;
            }
        }
        return out;
    }

    @Override
    public void close() {
    }
}
