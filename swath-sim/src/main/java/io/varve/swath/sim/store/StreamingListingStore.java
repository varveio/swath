/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.store.SortedRouting;
import io.varve.swath.sort.SortedRowGroupReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The giant-fixture tier: <b>decode each region of a sorted fixture once</b>, as simulated streams
 * advance over it, and serve every range read from the decoded keys in memory. A fixture too large
 * for {@link ArenaListingStore} — which loads the whole key set up front — is served here at the
 * same per-call cost, because a served call touches only decoded key blocks; the Parquet decode is
 * hoisted out of the serving loop entirely and paid once per region.
 *
 * <h2>Why not a query per window</h2>
 * The alternative already in the tree, {@code WindowedListingStore} over the sorted-Parquet store,
 * decodes <em>inside</em> the serving loop: every window refill is a bounded DuckDB range query, and
 * because the {@code key} column is a {@code BLOB} with no usable zonemaps that query costs a scan of
 * the whole key column — a cost proportional to the fixture, not to the window. Amortising it over a
 * window of rows divides that cost by a constant and leaves the fixture-size term intact, which is
 * why the windowed tier stays the memory-bounded conformance/fallback path rather than the tier a
 * simulated sweep runs on. Here, decode is proportional to the rows actually walked and nothing else.
 *
 * <h2>Segments, faults, and eviction</h2>
 * A <b>segment</b> is one physical row group of the fixture, decoded keys-only into an off-heap
 * {@link KeyBlock}. Segments are faulted in on demand and held in an access-ordered map bounded by
 * {@code maxResidentBytes}: the least recently used segment is dropped once the budget is exceeded,
 * which for N mostly-sequential cursors is exactly "evict behind the cursors" — each cursor keeps the
 * segment it is walking (and the next, across a boundary) at the hot end of the order, while the
 * segments it has walked off fall to the cold end. Eviction {@linkplain KeyBlock#close() releases} a
 * block's memory then and there rather than leaving it for a collector, so the budget bounds what the
 * process actually holds. The {@link ListingStore} seam carries no cursor identity for the store to
 * key a per-cursor window off, and inventing one would put session state below a seam whose whole
 * purpose is to be stateless, so residency is governed by one shared budget.
 *
 * <p><b>Mid-keyspace starts are a seek, not a scan.</b> A steal or a split starts a fresh cursor at
 * an arbitrary key; {@link SortedRouting#startRowGroup} locates the one row group that contains it
 * from the derived index alone (a binary search over ~10^3 entries), and decode begins there. Nothing
 * before it in the file is ever touched.
 *
 * <h2>Sizing</h2>
 * A segment's footprint is exactly its row group's key bytes plus an 8-byte offset per key. For a
 * fixture whose row groups hold ~300K keys of ~100 bytes that is ~33 MB per resident segment, so the
 * default budget ({@link SimStoreConfig#DEFAULT_STREAMING_MAX_RESIDENT_BYTES}) holds ~30 of them —
 * enough for a fleet of ~30 concurrent cursors, on a fixture of any size, since residency is a
 * function of the budget and never of the key count. It is off-heap, so it sits beside the JVM heap
 * rather than inside it. Raise it (or lower it) through
 * {@link SimStoreConfig#STREAMING_MAX_RESIDENT_BYTES_PROPERTY}; a budget too small to hold even one
 * row group's decoded keys fails fast at the first decode that hits it, naming the property to raise.
 *
 * <p>The budget bounds the <b>settled</b> residency. A fault transiently exceeds it, because the
 * block being decoded is built before the eviction it triggers frees anything: worst case, the
 * budget plus one segment's decoded bytes plus that decode's staging buffer. Sizing a run means
 * leaving room for one more row group than the budget names, not for a hidden multiple of it.
 *
 * <h2>What it serves, and what it declines</h2>
 * Keys only, through {@link SimModeRows} — the same sim-mode projection the arena tier answers with,
 * for the same reason (decoding nine columns per row group would reinstate the cost this tier exists
 * to remove). {@link #delimitedRollup} is declined, so every delimiter rule stays in the pager where
 * the differential suite can attribute a disagreement to a store by construction. A rollup's hop
 * pattern jumps whole subtrees and so faults segments it then reads few rows from; that shows up as
 * {@code seek} rather than {@code forward} in {@code swath.sim.store.streaming.segment.fault}, which
 * is precisely the signal that says a workload is not the sequential walk this tier is shaped for.
 *
 * <h2>Concurrency</h2>
 * One lock covers the resident map and the decoders (a {@link SortedRowGroupReader} is single-
 * threaded by contract), and a fault decodes while holding it. The caller this tier exists for is a
 * simulator's event loop, which is sequential by construction; a concurrent caller stays correct and
 * serialises behind a fault, which is rare by design — one per row group walked, hundreds of served
 * calls apart.
 */
public final class StreamingListingStore implements ListingStore {

    private final List<IndexEntry> index;
    private final SimStoreMetrics metrics;
    private final long maxResidentBytes;

    private final Object lock = new Object();
    private final Map<Path, SortedRowGroupReader> readers = new HashMap<>();
    private final LinkedHashMap<Integer, KeyBlock> resident = new LinkedHashMap<>(16, 0.75f, true);
    private long residentBytes;
    private long peakResidentBytes;

    /**
     * @param index            the derived {@code (file, rowGroup, firstKey, rowCount)} routing index of a
     *                         sorted-eligible fixture, globally ascending by first key
     * @param metrics          records this tier's segment hit/fault/decode/evict signals
     * @param maxResidentBytes the decoded-segment residency budget; see the sizing note above
     */
    public StreamingListingStore(List<IndexEntry> index, SimStoreMetrics metrics, long maxResidentBytes) {
        if (maxResidentBytes < 1) {
            throw new IllegalArgumentException("streaming max-resident-bytes must be at least 1, got "
                    + maxResidentBytes);
        }
        this.index = List.copyOf(index);
        this.metrics = metrics;
        this.maxResidentBytes = maxResidentBytes;
        metrics.registerStreamingResidentBytes(this::residentBytes);
    }

    /** The decoded segments' current footprint in bytes. */
    public long residentBytes() {
        synchronized (lock) {
            return residentBytes;
        }
    }

    /** The high-water mark of {@link #residentBytes()} over this store's lifetime. */
    public long peakResidentBytes() {
        synchronized (lock) {
            return peakResidentBytes;
        }
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        if (limit <= 0 || index.isEmpty()) {
            return List.of();
        }
        byte[] lower = from == null ? null : from.toByteArray();
        byte[] upper = toExclusive == null ? null : toExclusive.toByteArray();
        List<ListedObject> rows = new ArrayList<>(limit);
        synchronized (lock) {
            // Walk forward from the row group holding `from`, faulting each segment in as the read
            // reaches it: a page that straddles a boundary spans two segments, and one wide enough to
            // span more spans as many as it needs. `lower` is re-bounded per segment rather than only
            // in the first — every later segment starts wholly above it, so the search returns 0 there
            // and the uniform form cannot drift from the first-segment case.
            for (int group = SortedRouting.startRowGroup(index, from);
                 group < index.size() && rows.size() < limit;
                 group++) {
                // Every read out of `segment` finishes before the next iteration asks for another —
                // which matters, because faulting that next one can evict this one, and an evicted
                // block's memory is released immediately rather than at some later collection.
                KeyBlock segment = segment(group);
                int start = start(segment, lower, fromInclusive);
                int end = upper == null ? segment.size() : segment.lowerBound(upper);
                for (int i = start; i < end && rows.size() < limit; i++) {
                    rows.add(SimModeRows.stub(segment.keyAt(i)));
                }
                if (end < segment.size()) {
                    break;   // the upper bound falls inside this segment: no later key is in range
                }
            }
        }
        return rows;
    }

    @Override
    public void close() {
        synchronized (lock) {
            resident.values().forEach(KeyBlock::close);
            resident.clear();
            residentBytes = 0;
            RuntimeException failure = null;
            for (SortedRowGroupReader reader : readers.values()) {
                try {
                    reader.close();
                } catch (IOException | RuntimeException e) {
                    if (failure == null) {
                        failure = new IllegalStateException("failed to close the streaming store's readers", e);
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            readers.clear();
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static int start(KeyBlock segment, byte[] lower, boolean lowerInclusive) {
        if (lower == null) {
            return 0;
        }
        return lowerInclusive ? segment.lowerBound(lower) : segment.upperBound(lower);
    }

    /**
     * Row group {@code group}'s decoded keys, faulting them in when absent. A fault is classified
     * from the resident set alone: {@code forward} when the preceding group is resident (a cursor
     * walking off the end of the segment it was serving from) and {@code seek} otherwise (a cursor
     * started mid-keyspace — a steal or a split — or the first touch of a run). Called under
     * {@link #lock}.
     */
    private KeyBlock segment(int group) {
        KeyBlock cached = resident.get(group);
        if (cached != null) {
            metrics.recordStreamingSegmentHit();
            return cached;
        }
        metrics.recordStreamingSegmentFault(resident.containsKey(group - 1)
                ? SimStoreMetrics.FAULT_FORWARD : SimStoreMetrics.FAULT_SEEK);
        KeyBlock decoded = decode(group);
        resident.put(group, decoded);
        residentBytes += decoded.residentBytes();
        peakResidentBytes = Math.max(peakResidentBytes, residentBytes);
        evictBehind();
        return decoded;
    }

    /**
     * Decodes one row group's key column into a {@link KeyBlock}. The block's strictly-ascending
     * check is the fail-fast here: a row group whose keys are not ascending would corrupt every later
     * binary search over this segment, and the sorted-eligibility gate that let the fixture reach
     * this tier only proved the ascent of row-group <em>first</em> keys, not of the rows within one
     * group. A decode that fails part-way releases what it had already allocated rather than leaking
     * it off-heap, where no collector would come for it. Called under {@link #lock}.
     */
    private KeyBlock decode(int group) {
        IndexEntry entry = index.get(group);
        KeyBlock.Builder builder = KeyBlock.builder(entry.rowCount(), maxResidentBytes);
        Timer.Sample sample = metrics.startStreamingDecodeTimer();
        long rowCount;
        try {
            rowCount = reader(entry.file()).forEachKey(entry.rowGroup(), key -> {
                if (!builder.append(key)) {
                    throw new IllegalStateException("row group " + entry.rowGroup() + " of " + entry.file()
                            + " does not fit the streaming tier's " + maxResidentBytes
                            + "-byte residency budget (raise "
                            + SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY + ")");
                }
            });
        } catch (IOException e) {
            builder.discard();
            throw new UncheckedIOException("failed to decode row group " + entry.rowGroup()
                    + " of " + entry.file(), e);
        } catch (RuntimeException e) {
            builder.discard();
            throw e;
        }
        metrics.recordStreamingDecode(sample, rowCount);
        return builder.build();
    }

    /**
     * Drops least-recently-used segments until the residency budget is met, always keeping the one
     * just faulted in — a budget smaller than a single segment would otherwise evict it immediately
     * and re-decode it on the very next call. Called under {@link #lock}.
     */
    private void evictBehind() {
        while (residentBytes > maxResidentBytes && resident.size() > 1) {
            Map.Entry<Integer, KeyBlock> eldest = resident.entrySet().iterator().next();
            resident.remove(eldest.getKey());
            residentBytes -= eldest.getValue().residentBytes();
            eldest.getValue().close();
            metrics.recordStreamingSegmentEvict();
        }
    }

    /** The (lazily opened, then retained) decoder for {@code file}. Called under {@link #lock}. */
    private SortedRowGroupReader reader(Path file) {
        return readers.computeIfAbsent(file, f -> {
            try {
                return new SortedRowGroupReader(f);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to open " + f + " for streaming decode", e);
            }
        });
    }
}
