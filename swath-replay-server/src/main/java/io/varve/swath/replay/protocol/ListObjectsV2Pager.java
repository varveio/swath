/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Owns every S3 ListObjectsV2 protocol rule once, driving a range-only {@link ListingStore}:
 * max-keys accounting (objects and common prefixes counted together, emit-then-check;
 * {@code max-keys=0} ⇒ empty, non-truncated), truncation, delimiter rollup, common-prefix resume,
 * prefix/start-after interplay, and continuation-token encode/decode. Because both the DuckDB and
 * the sorted-Parquet stores are driven through this one pager, a differential test attributes any
 * disagreement to a store by construction.
 *
 * <p>The pager translates S3 semantics into byte ranges: the prefix window is
 * {@code [prefix, prefixUpper(prefix))} ({@link ByteKeys#prefixUpper}), a resume boundary is the
 * lower bound, and a rolled-up common prefix {@code P} is skipped by resuming at
 * {@code successor(P)} inclusive ({@link ByteKeys#successor}).
 */
public final class ListObjectsV2Pager implements ListingFixture {

    private static final int DELIMITER_BATCH = 4096;

    /** Default hybrid seek-scan threshold: scan this many keys of a common-prefix run in
     *  the already-fetched batch before issuing a fresh seek to {@code successor(P)}. Overridable via
     *  {@code -Dswath.replay.seek-scan-threshold} (the replay server sits outside swath's CLI config
     *  system, so a plain system property is fine). Tuning it does not change output, only how many
     *  bounded reads a deep hierarchy costs. */
    static final int DEFAULT_SEEK_SCAN_THRESHOLD = 32;

    private static final String SEEK_SCAN_THRESHOLD_PROPERTY = "swath.replay.seek-scan-threshold";

    private final ListingStore store;
    private final ReplayMetrics metrics;
    private final int seekScanThreshold;

    public ListObjectsV2Pager(ListingStore store, ReplayMetrics metrics) {
        this(store, metrics, seekScanThresholdFromSystemProperty());
    }

    // Package-private: tests pin the threshold to exercise both sides of the hybrid (pure scan vs seek).
    ListObjectsV2Pager(ListingStore store, ReplayMetrics metrics, int seekScanThreshold) {
        this.store = store;
        this.metrics = metrics;
        this.seekScanThreshold = seekScanThreshold;
    }

    private static int seekScanThresholdFromSystemProperty() {
        String value = System.getProperty(SEEK_SCAN_THRESHOLD_PROPERTY);
        if (value == null) {
            return DEFAULT_SEEK_SCAN_THRESHOLD;
        }
        int parsed = Integer.parseInt(value.trim());
        return parsed > 0 ? parsed : DEFAULT_SEEK_SCAN_THRESHOLD;
    }

    @Override
    public S3ListResult list(S3ListRequest request) {
        var sample = metrics.startTimer();
        try {
            Boundary boundary = resolveBoundary(request);
            int maxKeys = request.pageSize();
            if (maxKeys == 0) {
                return new S3ListResult(request, List.of(), false, null);
            }
            if (request.delimiter() == null || request.delimiter().length == 0) {
                return listObjects(request, boundary, maxKeys);
            }
            return listDelimited(request, boundary, maxKeys);
        } finally {
            metrics.recordFixtureList(sample);
        }
    }

    @Override
    public void close() {
        store.close();
    }

    private S3ListResult listObjects(S3ListRequest request, Boundary boundary, int maxKeys) {
        byte[] prefix = request.prefix();
        UpperBound upper = ByteKeys.prefixUpper(prefix);
        Lower lower = lowerBound(prefix, boundary);
        List<ListedObject> rows = fetch(lower, upper, maxKeys + 1, prefix, request.fetchOwner());

        boolean truncated = rows.size() > maxKeys;
        int n = Math.min(rows.size(), maxKeys);
        List<S3ResultEntry> entries = new ArrayList<>(n);
        byte[] last = boundary == null ? null : boundary.key();
        for (int i = 0; i < n; i++) {
            ListedObject object = rows.get(i);
            entries.add(new S3ResultEntry.ObjectResult(object));
            last = object.key();
        }
        String next = truncated && last != null ? ContinuationToken.encode(last) : null;
        return new S3ListResult(request, entries, truncated, next);
    }

    private S3ListResult listDelimited(S3ListRequest request, Boundary boundary, int maxKeys) {
        byte[] prefix = request.prefix();
        byte[] delimiter = request.delimiter();
        UpperBound upper = ByteKeys.prefixUpper(prefix);
        Lower cur = lowerBound(prefix, boundary);

        // Fast path: let the store answer the whole rollup in one set-oriented pass instead of a seek
        // per common prefix. Only when the prefix has a finite upper bound (the all-0xFF prefix's open
        // bound falls back); a store that declines returns null and the range walk below runs as before.
        if (upper instanceof UpperBound.Bounded(ByteKey upperKey)) {
            List<ListingStore.DelimitedEntry> rollup = store.delimitedRollup(
                    cur.key(), cur.inclusive(), upperKey, prefix, delimiter, maxKeys,
                    Projection.of(request.fetchOwner()));
            if (rollup != null) {
                return buildDelimitedPage(request, rollup, maxKeys);
            }
        }

        List<S3ResultEntry> entries = new ArrayList<>(maxKeys);

        // `cur` always names the store-agnostic resume boundary of everything not yet emitted/skipped:
        // an object O leaves cur = (O, exclusive); a rolled-up prefix P leaves cur = (successor(P),
        // inclusive) — the whole P subtree jumped in one hop, no re-scan-and-dedup needed. Do not
        // encode the token exclusive-at-P and rely on a `commonPrefix > boundary.key()` dedup guard to
        // swallow the re-emitted P on resume: a range-only store has no such guard, so the CP-resume
        // token must be inclusive-at-successor(P) directly (ContinuationToken.encode(cur.key, cur.inclusive) below).
        while (entries.size() < maxKeys) {
            List<ListedObject> rows = fetch(cur, upper, DELIMITER_BATCH, prefix, request.fetchOwner());
            if (rows.isEmpty()) {
                return new S3ListResult(request, entries, false, null);
            }
            int i = 0;
            while (i < rows.size() && entries.size() < maxKeys) {
                ListedObject object = rows.get(i);
                byte[] key = object.key();
                byte[] commonPrefix = commonPrefix(key, prefix, delimiter);
                if (commonPrefix == null) {
                    entries.add(new S3ResultEntry.ObjectResult(object));
                    cur = new Lower(ByteKey.copyOf(key), false);
                    i++;
                    continue;
                }
                // Rolled-up common prefix P: emit once (dedup guard vs the request boundary), then
                // advance past P's whole subtree.
                if (boundary == null || ByteKeys.compareUnsigned(commonPrefix, boundary.key()) > 0) {
                    entries.add(new S3ResultEntry.CommonPrefixResult(commonPrefix));
                }
                switch (ByteKeys.successor(commonPrefix)) {
                    case Successor.EndOfKeyspace() -> {
                        // Nothing sorts after an all-0xFF prefix: end of listing, never reopen.
                        return new S3ListResult(request, entries, false, null);
                    }
                    case Successor.Key(ByteKey successorKey) -> cur = new Lower(successorKey, true);
                }
                // Hybrid: scan up to `seekScanThreshold` keys of P's run already in this batch before
                // paying for a fresh seek. A short run ends inside the batch (keep scanning in place);
                // a long run (or a batch that ends still inside P) breaks out to re-fetch from
                // cur = successor(P) inclusive.
                int j = i + 1;
                int scanned = 0;
                while (j < rows.size() && scanned < seekScanThreshold
                        && ByteKeys.startsWith(rows.get(j).key(), commonPrefix)) {
                    j++;
                    scanned++;
                }
                if (j < rows.size() && !ByteKeys.startsWith(rows.get(j).key(), commonPrefix)) {
                    i = j;   // end of P's run found in-batch — continue scanning without a seek
                } else {
                    break;   // long run or batch-end under P — re-fetch (seek) from successor(P)
                }
            }
        }

        boolean truncated = cur.key() != null && !fetch(cur, upper, 1, prefix, request.fetchOwner()).isEmpty();
        String next = truncated ? ContinuationToken.encode(cur.key().toByteArray(), cur.inclusive()) : null;
        return new S3ListResult(request, entries, truncated, next);
    }

    /**
     * Builds a delimiter page from a store's one-shot {@link ListingStore#delimitedRollup} — the fast
     * path's equivalent of the range walk above, and byte-identical to it (the differential suite
     * proves this). The rollup arrives already boundary-guarded and interleaved in key order, with up
     * to {@code maxKeys + 1} entries: emitting {@code maxKeys} and finding one more is truncation,
     * exactly what the walk's trailing one-row probe detects. The resume boundary follows the same
     * rule — {@code (object, exclusive)} or {@code (successor(prefix), inclusive)} — and an all-{@code
     * 0xFF} common prefix ends the listing without reopening, as in the walk.
     */
    private S3ListResult buildDelimitedPage(S3ListRequest request, List<ListingStore.DelimitedEntry> rollup,
                                            int maxKeys) {
        List<S3ResultEntry> entries = new ArrayList<>(Math.min(maxKeys, rollup.size()));
        Lower cur = null;
        for (ListingStore.DelimitedEntry entry : rollup) {
            if (entries.size() == maxKeys) {
                break;                 // the extra entry only signals truncation; it is not emitted
            }
            if (entry.isCommonPrefix()) {
                byte[] commonPrefix = entry.commonPrefix();
                entries.add(new S3ResultEntry.CommonPrefixResult(commonPrefix));
                switch (ByteKeys.successor(commonPrefix)) {
                    case Successor.EndOfKeyspace() -> {
                        return new S3ListResult(request, entries, false, null);
                    }
                    case Successor.Key(ByteKey successorKey) -> cur = new Lower(successorKey, true);
                }
            } else {
                ListedObject object = entry.object();
                entries.add(new S3ResultEntry.ObjectResult(object));
                cur = new Lower(ByteKey.copyOf(object.key()), false);
            }
        }
        boolean truncated = rollup.size() > maxKeys && cur != null;
        String next = truncated ? ContinuationToken.encode(cur.key().toByteArray(), cur.inclusive()) : null;
        return new S3ListResult(request, entries, truncated, next);
    }

    /**
     * Reads a range from the store and applies the prefix filter (the store speaks only ranges).
     * With a correct {@code prefixUpper} the filter is a no-op in every case, including the
     * pathological all-{@code 0xFF} prefix: {@link UpperBound.Open} means no finite exclusive bound
     * was needed to exclude non-matching keys, not that the store might return some — the store's
     * lower bound is already {@code >= prefix}, so an open upper bound still only reaches keys
     * {@code >= prefix} in ascending order, and {@code startsWith} passes them all. The filter is
     * defensive belt-and-braces, not load-bearing, for any correctly-bounded call.
     */
    private List<ListedObject> fetch(Lower lower, UpperBound upper, int limit, byte[] prefix, boolean fetchOwner) {
        ByteKey to = switch (upper) {
            case UpperBound.Bounded(ByteKey exclusiveUpper) -> exclusiveUpper;
            case UpperBound.Open() -> null;
        };
        List<ListedObject> rows = store.rows(lower.key(), lower.inclusive(), to, limit, Projection.of(fetchOwner));
        if (prefix == null || prefix.length == 0) {
            return rows;
        }
        List<ListedObject> filtered = new ArrayList<>(rows.size());
        for (ListedObject row : rows) {
            if (ByteKeys.startsWith(row.key(), prefix)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    private Boundary resolveBoundary(S3ListRequest request) {
        if (request.continuationToken() != null && request.startAfter() != null) {
            throw new S3Error(400, "InvalidArgument", "continuation-token and start-after are mutually exclusive");
        }
        if (request.continuationToken() != null) {
            ContinuationToken token = ContinuationToken.decode(request.continuationToken());
            return token == null ? null : new Boundary(token.boundary(), token.inclusive());
        }
        if (request.startAfter() != null) {
            return new Boundary(request.startAfter(), false);
        }
        return null;
    }

    /** Combines the prefix window's inclusive floor with an exclusive/inclusive resume boundary. */
    private static Lower lowerBound(byte[] prefix, Boundary boundary) {
        boolean hasPrefix = prefix != null && prefix.length > 0;
        if (boundary != null) {
            byte[] key = boundary.key();
            if (hasPrefix && ByteKeys.compareUnsigned(prefix, key) > 0) {
                return new Lower(ByteKey.copyOf(prefix), true);
            }
            return new Lower(ByteKey.copyOf(key), boundary.inclusive());
        }
        if (hasPrefix) {
            return new Lower(ByteKey.copyOf(prefix), true);
        }
        return new Lower(null, true);
    }

    private static byte[] commonPrefix(byte[] key, byte[] prefix, byte[] delimiter) {
        int start = prefix == null ? 0 : prefix.length;
        for (int i = start; i <= key.length - delimiter.length; i++) {
            if (matchesAt(key, delimiter, i)) {
                return Arrays.copyOf(key, i + delimiter.length);
            }
        }
        return null;
    }

    private static boolean matchesAt(byte[] key, byte[] delimiter, int offset) {
        for (int i = 0; i < delimiter.length; i++) {
            if (key[offset + i] != delimiter[i]) {
                return false;
            }
        }
        return true;
    }

    /** A resolved lower bound: {@code key == null} means open. */
    private record Lower(ByteKey key, boolean inclusive) {
    }

    /** A resolved resume boundary (from continuation-token or start-after). */
    private record Boundary(byte[] key, boolean inclusive) {
    }
}
