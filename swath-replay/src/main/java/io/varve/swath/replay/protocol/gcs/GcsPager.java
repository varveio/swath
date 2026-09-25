/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

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

/** GCS range and delimiter semantics over a range-only store; no S3 pager calls. */
public final class GcsPager {
    private final ListingStore store;
    private final String fixtureIdentity;
    private final PaginationTestProfile paginationTestProfile;
    private final ReplayMetrics metrics;

    public GcsPager(ListingStore store, String fixtureIdentity) {
        this(store, fixtureIdentity, PaginationTestProfile.NONE, null);
    }

    public GcsPager(ListingStore store, String fixtureIdentity, PaginationTestProfile paginationTestProfile) {
        this(store, fixtureIdentity, paginationTestProfile, null);
    }

    public GcsPager(ListingStore store, String fixtureIdentity, PaginationTestProfile paginationTestProfile,
                    ReplayMetrics metrics) {
        this.store = store;
        this.fixtureIdentity = fixtureIdentity;
        this.paginationTestProfile = paginationTestProfile;
        this.metrics = metrics;
    }

    public GcsPage list(GcsListRequest request) {
        if (request.pageSizeClamped()) {
            record("page_limit_clamped", "requested_above_1000");
        }
        byte[] prefix = request.prefix() == null ? new byte[0] : bytes(request.prefix());
        byte[] delimiter = request.delimiter() == null ? new byte[0] : bytes(request.delimiter());
        byte[] binding = GcsToken.binding(request, fixtureIdentity, paginationTestProfile.bindingName());
        Boundary lower = new Boundary(prefix.length == 0 ? null : prefix, true);
        lower = max(lower, new Boundary(bytes(request.startOffset()), true));
        int injectionProgress = 0;
        if (request.pageToken() != null) {
            GcsToken.Cursor cursor = GcsToken.decode(request.pageToken(), binding);
            lower = max(lower, new Boundary(cursor.boundary(), cursor.inclusive()));
            injectionProgress = cursor.injectionProgress();
        }
        byte[] upper = minUpper(prefix, bytes(request.endOffset()));
        if (lower.key() != null && upper != null && ByteKeys.compareUnsigned(lower.key(), upper) >= 0) {
            record("empty_intersection", "lower_at_or_after_upper");
            return new GcsPage(List.of(), List.of(), null);
        }
        if (paginationTestProfile.emptyFirstPage() && injectionProgress == 0) {
            record("injected_empty", "test_profile");
            byte[] cursorKey = lower.key() == null ? new byte[0] : lower.key();
            return new GcsPage(List.of(), List.of(), GcsToken.encode(lower.inclusive(), cursorKey, binding, 1));
        }
        int pageSize = paginationTestProfile.effectivePageSize(request.pageSize());
        if (pageSize < request.pageSize()) {
            record("short_page", "test_profile");
        }
        return delimiter.length == 0
                ? flat(lower, upper, binding, pageSize, injectionProgress)
                : delimited(prefix, delimiter, lower, upper, binding, pageSize, injectionProgress,
                        request.startOffset() != null);
    }

    /** Validate a replay token before acquiring shared serving resources. */
    public void validateToken(GcsListRequest request) {
        if (request.pageToken() != null) {
            byte[] binding = GcsToken.binding(request, fixtureIdentity, paginationTestProfile.bindingName());
            GcsToken.decode(request.pageToken(), binding);
        }
    }

    private GcsPage flat(Boundary lower, byte[] upper, byte[] binding, int pageSize, int injectionProgress) {
        record("flat", upper == null ? "open_upper" : "bounded_upper");
        List<ListedObject> rows = rows(lower, upper, pageSize + 1);
        int count = Math.min(pageSize, rows.size());
        String token = rows.size() > count
                ? GcsToken.encode(false, rows.get(count - 1).key(), binding, injectionProgress) : null;
        return new GcsPage(rows.subList(0, count), List.of(), token);
    }

    private GcsPage delimited(byte[] prefix, byte[] delimiter, Boundary lower, byte[] upper,
                              byte[] binding, int pageSize, int injectionProgress, boolean hasStartOffset) {
        List<ListingStore.DelimitedEntry> indexed = store.delimitedRollup(
                lower.key() == null ? null : ByteKey.copyOf(lower.key()), lower.inclusive(),
                upper == null ? null : ByteKey.copyOf(upper), prefix, delimiter, pageSize,
                Projection.of(false), false);
        if (indexed != null) {
            record("delimiter_indexed", hasStartOffset ? "start_offset_intersection" : "native_skip_scan");
            return indexedPage(pageSize, indexed, binding, injectionProgress);
        }
        record("delimiter_walk", hasStartOffset ? "start_offset_store_declined" : "store_declined");
        List<ListedObject> objects = new ArrayList<>();
        List<byte[]> prefixes = new ArrayList<>();
        Boundary cursor = lower;
        Boundary lastEmitted = null;
        int emitted = 0;
        while (true) {
            List<ListedObject> rows = rows(cursor, upper, 4096);
            if (rows.isEmpty()) {
                return new GcsPage(objects, prefixes, null);
            }
            int i = 0;
            while (i < rows.size()) {
                ListedObject object = rows.get(i);
                byte[] rolled = rollup(object.key(), prefix, delimiter);
                if (emitted == pageSize) {
                    return new GcsPage(objects, prefixes,
                            GcsToken.encode(lastEmitted.inclusive(), lastEmitted.key(), binding,
                                    injectionProgress));
                }
                if (rolled == null) {
                    objects.add(object);
                    cursor = new Boundary(object.key(), false);
                    i++;
                } else {
                    prefixes.add(rolled);
                    cursor = switch (ByteKeys.successor(rolled)) {
                        case Successor.Key(ByteKey successor) -> new Boundary(successor.toByteArray(), true);
                        case Successor.EndOfKeyspace() -> null;
                    };
                    // Stay within this already-read batch for a short subtree; if it spans the
                    // batch edge, the next outer iteration seeks directly to successor(prefix).
                    do {
                        i++;
                    } while (i < rows.size() && ByteKeys.startsWith(rows.get(i).key(), rolled));
                }
                lastEmitted = cursor;
                emitted++;
                if (cursor == null) {
                    return new GcsPage(objects, prefixes, null);
                }
            }
        }
    }

    private static GcsPage indexedPage(int pageSize, List<ListingStore.DelimitedEntry> indexed,
                                       byte[] binding, int injectionProgress) {
        List<ListedObject> objects = new ArrayList<>();
        List<byte[]> prefixes = new ArrayList<>();
        Boundary last = null;
        int count = Math.min(pageSize, indexed.size());
        for (int i = 0; i < count; i++) {
            ListingStore.DelimitedEntry entry = indexed.get(i);
            if (entry.isCommonPrefix()) {
                prefixes.add(entry.commonPrefix());
                last = switch (ByteKeys.successor(entry.commonPrefix())) {
                    case Successor.Key(ByteKey successor) -> new Boundary(successor.toByteArray(), true);
                    case Successor.EndOfKeyspace() -> null;
                };
            } else {
                objects.add(entry.object());
                last = new Boundary(entry.object().key(), false);
            }
        }
        String token = indexed.size() > count && last != null
                ? GcsToken.encode(last.inclusive(), last.key(), binding, injectionProgress) : null;
        return new GcsPage(objects, prefixes, token);
    }

    private List<ListedObject> rows(Boundary lower, byte[] upper, int limit) {
        return store.rows(lower.key() == null ? null : ByteKey.copyOf(lower.key()), lower.inclusive(),
                upper == null ? null : ByteKey.copyOf(upper), limit, Projection.of(false));
    }

    private static byte[] rollup(byte[] key, byte[] prefix, byte[] delimiter) {
        for (int i = prefix.length; i <= key.length - delimiter.length; i++) {
            boolean matches = true;
            for (int j = 0; j < delimiter.length; j++) {
                if (key[i + j] != delimiter[j]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return Arrays.copyOf(key, i + delimiter.length);
            }
        }
        return null;
    }

    private static Boundary max(Boundary a, Boundary b) {
        if (b.key() == null) {
            return a;
        }
        if (a.key() == null) {
            return b;
        }
        int cmp = ByteKeys.compareUnsigned(a.key(), b.key());
        if (cmp > 0) {
            return a;
        }
        if (cmp < 0) {
            return b;
        }
        return new Boundary(a.key(), a.inclusive() && b.inclusive());
    }

    private static byte[] minUpper(byte[] prefix, byte[] endOffset) {
        byte[] prefixEnd = switch (ByteKeys.prefixUpper(prefix)) {
            case UpperBound.Bounded(ByteKey upper) -> upper.toByteArray();
            case UpperBound.Open() -> null;
        };
        if (prefixEnd == null) {
            return endOffset;
        }
        if (endOffset == null) {
            return prefixEnd;
        }
        return ByteKeys.compareUnsigned(prefixEnd, endOffset) < 0 ? prefixEnd : endOffset;
    }

    private static byte[] bytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    private record Boundary(byte[] key, boolean inclusive) {
    }

    private void record(String path, String reason) {
        if (metrics != null) {
            metrics.recordProviderPath("gcs", path, reason);
        }
    }
}
