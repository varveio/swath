/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.PaginationTestProfile;
import io.varve.swath.replay.protocol.Successor;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Azure XML listing over one shared range-only store, in Azure's UTF-16 code-unit order. The fixture
 * stays byte-sorted; {@link AzureOrderedCursor} reads it in provider order.
 */
public final class AzureListPager {
    private final ListingStore store;
    private final String fixtureIdentity;
    private final PaginationTestProfile profile;
    private final ReplayMetrics metrics;

    public AzureListPager(ListingStore store, String fixtureIdentity) {
        this(store, fixtureIdentity, PaginationTestProfile.NONE, null);
    }

    public AzureListPager(ListingStore store, String fixtureIdentity, PaginationTestProfile profile,
                          ReplayMetrics metrics) {
        this.store = store;
        this.fixtureIdentity = fixtureIdentity;
        this.profile = profile;
        this.metrics = metrics;
    }

    public AzureListResult list(AzureListRequest request) {
        byte[] prefix = request.prefix() == null ? new byte[0] : bytes(request.prefix());
        byte[] delimiter = request.delimiter() == null ? new byte[0] : bytes(request.delimiter());
        byte[] binding = AzureToken.binding(request, fixtureIdentity, profile.bindingName());
        if (request.maxResultsSupplied() && request.requestedMaxResults() != null
                && Long.parseLong(request.requestedMaxResults()) > 5000) {
            record("page_limit_clamped", "requested_above_5000");
        }
        if (request.marker() != null && request.startFrom() != null) {
            record("marker_with_start_from", "matching_scope");
        }
        // Bounds compare in provider order (AzureOrder), not in the fixture's byte order.
        Boundary lower = new Boundary(prefix, true);
        lower = max(lower, new Boundary(bytes(request.startFrom()), true));
        int progress = 0;
        if (request.marker() != null) {
            AzureToken.Cursor marker = AzureToken.decode(request.marker(), binding);
            lower = max(lower, new Boundary(marker.boundary(), marker.inclusive()));
            progress = marker.injectionProgress();
        }
        if (profile.emptyFirstPage() && progress == 0) {
            record("injected_empty", "test_profile");
            return new AzureListResult(request, List.of(),
                    AzureToken.encode(lower.inclusive(), lower.key(), binding, 1));
        }
        int pageSize = profile.effectivePageSize(request.pageSize());
        if (pageSize < request.pageSize()) record("short_page", "test_profile");
        if (!ByteKeys.startsWith(lower.key(), prefix)) {
            // At or after the prefix in provider order but outside it: past the whole window.
            return new AzureListResult(request, List.of(), null);
        }
        return delimiter.length == 0
                ? flat(request, prefix, lower, binding, pageSize, progress)
                : delimited(request, prefix, delimiter, lower, binding, pageSize, progress);
    }

    /** Validate an opaque replay marker before shared admission and page reads. */
    public void validateMarker(AzureListRequest request) {
        if (request.marker() != null) {
            AzureToken.decode(request.marker(), AzureToken.binding(request, fixtureIdentity, profile.bindingName()));
        }
    }

    private AzureListResult flat(AzureListRequest request, byte[] prefix, Boundary lower, byte[] binding,
                                 int pageSize, int progress) {
        record("flat", request.startFrom() == null ? "prefix_window" : "start_from");
        AzureOrderedCursor cursor = new AzureOrderedCursor(this::rowItems,
                AzureOrder.rangesFrom(lower.key(), lower.inclusive(), prefix));
        return page(request, List.of(), cursor, binding, pageSize, progress);
    }

    /**
     * A lower bound inside a rolled prefix (a {@code startFrom} below a subtree) lists that prefix
     * only when one of its names is at or after the bound, then continues after the subtree. Every
     * byte range read later then holds whole rolled subtrees, so no prefix is listed twice.
     */
    private AzureListResult delimited(AzureListRequest request, byte[] prefix, byte[] delimiter,
                                      Boundary lower, byte[] binding, int pageSize, int progress) {
        List<AzureOrderedCursor.Item> leading = new ArrayList<>(1);
        byte[] from = lower.key();
        boolean inclusive = lower.inclusive();
        for (byte[] holder; from != null && (holder = rollup(from, prefix, delimiter)) != null;) {
            AzureOrderedCursor names = new AzureOrderedCursor(this::rowItems,
                    AzureOrder.rangesFrom(from, inclusive, holder));
            boolean listed = names.next(1) != null;
            record("delimiter_lower_in_prefix", listed ? "prefix_listed" : "prefix_skipped");
            if (listed) leading.add(AzureOrderedCursor.Item.prefix(holder));
            from = AzureOrder.upperOf(holder);
            inclusive = true;
            if (from != null && !ByteKeys.startsWith(from, prefix)) from = null;
        }
        AzureOrderedCursor cursor = from == null ? null : new AzureOrderedCursor(rollups(prefix, delimiter),
                AzureOrder.rangesFrom(from, inclusive, prefix));
        return page(request, leading, cursor, binding, pageSize, progress);
    }

    /**
     * Takes one page: {@code leading} entries first, then the cursor's. Continuation resumes after a
     * Blob's name, or at the provider-order upper bound of a BlobPrefix so its subtree is skipped.
     */
    private AzureListResult page(AzureListRequest request, List<AzureOrderedCursor.Item> leading,
                                 AzureOrderedCursor cursor, byte[] binding, int pageSize, int progress) {
        List<AzureListResult.Entry> entries = new ArrayList<>(Math.min(pageSize, 1024));
        AzureOrderedCursor.Item last = null;
        boolean more = false;
        int index = 0;
        while (true) {
            AzureOrderedCursor.Item item = index < leading.size() ? leading.get(index++)
                    : cursor == null ? null : cursor.next(pageSize + 1 - entries.size());
            if (item == null) break;
            if (entries.size() == pageSize) {
                more = true;
                break;
            }
            entries.add(item.isPrefix() ? new AzureListResult.Entry.BlobPrefix(item.name())
                    : new AzureListResult.Entry.Blob(item.object()));
            last = item;
        }
        if (cursor != null && cursor.splits() > 0) record("utf16_order", "branch_swapped");
        String next = null;
        if (more) {
            next = last.isPrefix()
                    ? AzureToken.encode(true, AzureOrder.upperOf(last.name()), binding, progress)
                    : AzureToken.encode(false, last.name(), binding, progress);
        }
        return new AzureListResult(request, entries, next);
    }

    /**
     * Byte-ordered Blob and BlobPrefix entries of one range: the store's native rollup when it has
     * one, else a walk of its rows that seeks past each rolled prefix. A rolled prefix is returned
     * when any name of the range rolls up to it, so the range bounds decide membership, not the
     * prefix spelling.
     */
    private AzureOrderedCursor.Source rollups(byte[] prefix, byte[] delimiter) {
        // 0: not yet asked, 1: the store answers rollups, 2: it declined and rows are walked.
        int[] path = {0};
        return (from, inclusive, upper, limit) -> {
            if (path[0] != 2) {
                List<ListingStore.DelimitedEntry> indexed = store.delimitedRollup(key(from, inclusive), inclusive,
                        upper == null ? null : ByteKey.copyOf(upper), prefix, delimiter, limit - 1,
                        Projection.of(false), false);
                if (indexed != null) {
                    if (path[0] == 0) record("delimiter_indexed", "native_skip_scan");
                    path[0] = 1;
                    List<AzureOrderedCursor.Item> items = new ArrayList<>(indexed.size());
                    for (ListingStore.DelimitedEntry entry : indexed) {
                        items.add(entry.isCommonPrefix() ? AzureOrderedCursor.Item.prefix(entry.commonPrefix())
                                : AzureOrderedCursor.Item.blob(entry.object()));
                    }
                    return items;
                }
                path[0] = 2;
                record("delimiter_walk", "store_declined");
            }
            return walk(from, inclusive, upper, limit, prefix, delimiter);
        };
    }

    private List<AzureOrderedCursor.Item> walk(byte[] from, boolean inclusive, byte[] upper, int limit,
                                               byte[] prefix, byte[] delimiter) {
        List<AzureOrderedCursor.Item> items = new ArrayList<>();
        byte[] cursor = from;
        boolean cursorInclusive = inclusive;
        while (items.size() < limit) {
            int wanted = Math.min(4096, limit - items.size() + 1);
            List<ListedObject> batch = rows(cursor, cursorInclusive, upper, wanted);
            int i = 0;
            while (i < batch.size() && items.size() < limit) {
                ListedObject object = batch.get(i);
                byte[] rolled = rollup(object.key(), prefix, delimiter);
                if (rolled == null) {
                    items.add(AzureOrderedCursor.Item.blob(object));
                    cursor = object.key();
                    cursorInclusive = false;
                    i++;
                    continue;
                }
                items.add(AzureOrderedCursor.Item.prefix(rolled));
                cursor = byteSuccessor(rolled);
                if (cursor == null) return items;
                cursorInclusive = true;
                // Stay within this batch for a short subtree; a longer one seeks to its successor.
                do {
                    i++;
                } while (i < batch.size() && ByteKeys.startsWith(batch.get(i).key(), rolled));
            }
            if (batch.size() < wanted) return items;
        }
        return items;
    }

    private List<AzureOrderedCursor.Item> rowItems(byte[] from, boolean inclusive, byte[] upper, int limit) {
        List<ListedObject> rows = rows(from, inclusive, upper, limit);
        List<AzureOrderedCursor.Item> items = new ArrayList<>(rows.size());
        for (ListedObject row : rows) items.add(AzureOrderedCursor.Item.blob(row));
        return items;
    }

    private List<ListedObject> rows(byte[] from, boolean inclusive, byte[] upper, int limit) {
        return store.rows(key(from, inclusive), inclusive, upper == null ? null : ByteKey.copyOf(upper), limit,
                Projection.of(false));
    }

    /** An inclusive empty lower bound is the open start of the keyspace. */
    private static ByteKey key(byte[] from, boolean inclusive) {
        return from.length == 0 && inclusive ? null : ByteKey.copyOf(from);
    }

    /** The byte-order resume point after every name under a rolled prefix, or {@code null} at the end. */
    static byte[] byteSuccessor(byte[] rolled) {
        return switch (ByteKeys.successor(rolled)) {
            case Successor.Key(ByteKey successor) -> successor.toByteArray();
            case Successor.EndOfKeyspace() -> null;
        };
    }

    private static byte[] rollup(byte[] key, byte[] prefix, byte[] delimiter) {
        for (int i = prefix.length; i <= key.length - delimiter.length; i++) {
            boolean matches = true;
            for (int j = 0; j < delimiter.length; j++) {
                if (key[i + j] != delimiter[j]) { matches = false; break; }
            }
            if (matches) return Arrays.copyOf(key, i + delimiter.length);
        }
        return null;
    }

    private static Boundary max(Boundary a, Boundary b) {
        if (b.key() == null) return a;
        int cmp = AzureOrder.compare(a.key(), b.key());
        if (cmp > 0) return a;
        if (cmp < 0) return b;
        return new Boundary(a.key(), a.inclusive() && b.inclusive());
    }

    private static byte[] bytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    private void record(String path, String reason) {
        if (metrics != null) metrics.recordProviderPath("azure", path, reason);
    }

    private record Boundary(byte[] key, boolean inclusive) { }
}
