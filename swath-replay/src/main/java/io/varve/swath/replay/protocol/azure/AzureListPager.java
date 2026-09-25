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
import io.varve.swath.replay.protocol.UpperBound;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Flat Azure XML listing over one shared range-only store. */
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
        if (request.startFrom() != null && request.delimiter() != null) {
            throw new IllegalArgumentException("startFrom with delimiter is unmeasured");
        }
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
        Boundary lower = new Boundary(prefix.length == 0 ? null : prefix, true);
        lower = max(lower, new Boundary(bytes(request.startFrom()), true));
        int progress = 0;
        if (request.marker() != null) {
            AzureToken.Cursor marker = AzureToken.decode(request.marker(), binding);
            lower = max(lower, new Boundary(marker.boundary(), marker.inclusive()));
            progress = marker.injectionProgress();
        }
        byte[] upper = switch (ByteKeys.prefixUpper(prefix)) {
            case UpperBound.Bounded(ByteKey key) -> key.toByteArray();
            case UpperBound.Open() -> null;
        };
        if (profile.emptyFirstPage() && progress == 0) {
            record("injected_empty", "test_profile");
            byte[] key = lower.key() == null ? new byte[0] : lower.key();
            return new AzureListResult(request, List.of(), AzureToken.encode(lower.inclusive(), key, binding, 1));
        }
        int pageSize = profile.effectivePageSize(request.pageSize());
        if (pageSize < request.pageSize()) record("short_page", "test_profile");
        return delimiter.length == 0
                ? flat(request, lower, upper, binding, pageSize, progress)
                : delimited(request, prefix, delimiter, lower, upper, binding, pageSize, progress);
    }

    /** Validate an opaque replay marker before shared admission and page reads. */
    public void validateMarker(AzureListRequest request) {
        if (request.marker() != null) {
            AzureToken.decode(request.marker(), AzureToken.binding(request, fixtureIdentity, profile.bindingName()));
        }
    }

    private AzureListResult flat(AzureListRequest request, Boundary lower, byte[] upper, byte[] binding,
                                 int pageSize, int progress) {
        record("flat", request.startFrom() == null ? "prefix_window" : "start_from");
        List<ListedObject> rows = rows(lower, upper, pageSize + 1);
        int count = Math.min(pageSize, rows.size());
        List<AzureListResult.Entry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) entries.add(new AzureListResult.Entry.Blob(rows.get(i)));
        String next = rows.size() > count
                ? AzureToken.encode(false, rows.get(count - 1).key(), binding, progress) : null;
        return new AzureListResult(request, entries, next);
    }

    private AzureListResult delimited(AzureListRequest request, byte[] prefix, byte[] delimiter,
                                      Boundary lower, byte[] upper, byte[] binding, int pageSize, int progress) {
        List<ListingStore.DelimitedEntry> indexed = store.delimitedRollup(
                lower.key() == null ? null : ByteKey.copyOf(lower.key()), lower.inclusive(),
                upper == null ? null : ByteKey.copyOf(upper), prefix, delimiter, pageSize, Projection.of(false));
        if (indexed != null) {
            record("delimiter_indexed", "native_skip_scan");
            return indexedPage(request, indexed, binding, pageSize, progress);
        }
        record("delimiter_walk", "store_declined");
        List<AzureListResult.Entry> entries = new ArrayList<>();
        Boundary cursor = lower;
        Boundary last = null;
        while (true) {
            List<ListedObject> batch = rows(cursor, upper,
                    Math.min(4096, pageSize - entries.size() + 1));
            if (batch.isEmpty()) return new AzureListResult(request, entries, null);
            for (int i = 0; i < batch.size();) {
                ListedObject object = batch.get(i);
                byte[] rolled = rollup(object.key(), prefix, delimiter);
                if (entries.size() == pageSize) {
                    return new AzureListResult(request, entries,
                            AzureToken.encode(last.inclusive(), last.key(), binding, progress));
                }
                if (rolled == null) {
                    entries.add(new AzureListResult.Entry.Blob(object));
                    cursor = new Boundary(object.key(), false);
                    i++;
                } else {
                    entries.add(new AzureListResult.Entry.BlobPrefix(rolled));
                    cursor = switch (ByteKeys.successor(rolled)) {
                        case Successor.Key(ByteKey successor) -> new Boundary(successor.toByteArray(), true);
                        case Successor.EndOfKeyspace() -> null;
                    };
                    do {
                        i++;
                    } while (i < batch.size() && ByteKeys.startsWith(batch.get(i).key(), rolled));
                }
                last = cursor;
                if (cursor == null) return new AzureListResult(request, entries, null);
            }
        }
    }

    private static AzureListResult indexedPage(AzureListRequest request, List<ListingStore.DelimitedEntry> indexed,
                                               byte[] binding, int pageSize, int progress) {
        List<AzureListResult.Entry> entries = new ArrayList<>();
        Boundary last = null;
        int count = Math.min(pageSize, indexed.size());
        for (int i = 0; i < count; i++) {
            ListingStore.DelimitedEntry entry = indexed.get(i);
            if (entry.isCommonPrefix()) {
                entries.add(new AzureListResult.Entry.BlobPrefix(entry.commonPrefix()));
                last = switch (ByteKeys.successor(entry.commonPrefix())) {
                    case Successor.Key(ByteKey successor) -> new Boundary(successor.toByteArray(), true);
                    case Successor.EndOfKeyspace() -> null;
                };
            } else {
                entries.add(new AzureListResult.Entry.Blob(entry.object()));
                last = new Boundary(entry.object().key(), false);
            }
        }
        String next = indexed.size() > count && last != null
                ? AzureToken.encode(last.inclusive(), last.key(), binding, progress) : null;
        return new AzureListResult(request, entries, next);
    }

    private List<ListedObject> rows(Boundary lower, byte[] upper, int limit) {
        return store.rows(lower.key() == null ? null : ByteKey.copyOf(lower.key()), lower.inclusive(),
                upper == null ? null : ByteKey.copyOf(upper), limit, Projection.of(false));
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
        if (a.key() == null) return b;
        int cmp = ByteKeys.compareUnsigned(a.key(), b.key());
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
