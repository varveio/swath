/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.output.parquet.sorted.RowGroupOrderException;
import io.varve.swath.output.parquet.sorted.SortedRowGroupReader;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.store.SortedRouting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keys-only, on-demand decoded-block store for a sorted-eligible fixture. Each off-heap
 * {@link KeyBlock} holds one row group; {@link SortedRouting#startRowGroup} seeks the index for a
 * mid-keyspace start, so it need not decode earlier groups. An evicted block is decoded again if
 * revisited.
 *
 * <p>A single lock protects both the access-ordered resident LRU and its single-threaded
 * {@link SortedRowGroupReader}s. A block is charged as key bytes plus {@code (rowCount + 1)}
 * eight-byte offsets and is evicted promptly on crossing {@code maxResidentBytes}. The budget
 * bounds settled residency; a fault can transiently add its decoded block and staging space before
 * eviction. A row group larger than the budget fails rather than being immediately evicted and
 * repeatedly decoded.
 *
 * <p>Range reads are half-open and can span blocks. Rows use {@link SimModeRows}; delimiter rollup
 * remains pager-owned. {@link #close()} owns all resident blocks and decoders.
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
     * @param index            routing index for a sorted-eligible fixture
     * @param metrics          records segment activity
     * @param maxResidentBytes settled decoded-block residency budget
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
            // A page can span multiple blocks; applying the lower bound uniformly keeps that path exact.
            for (int group = SortedRouting.startRowGroup(index, from);
                 group < index.size() && rows.size() < limit;
                 group++) {
                // Finish with this block before faulting another, which may evict and close it.
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
     * Gets or faults row group {@code group}; called under {@link #lock}. {@code forward} means its
     * predecessor was resident: usual forward-walk evidence, not proof of cursor causality.
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
     * Decodes one row group's keys under {@link #lock}. Eligibility validates first keys; the
     * {@link KeyBlock} validates every key before binary search can rely on it. Disorder is typed,
     * located, and counted before rethrow; every failed partial decode discards its off-heap state.
     */
    private KeyBlock decode(int group) {
        IndexEntry entry = index.get(group);
        KeyBlock.Builder builder = KeyBlock.builder(entry.rowCount(), maxResidentBytes);
        Timer.Sample sample = metrics.startStreamingDecodeTimer();
        long rowCount;
        try {
            rowCount = reader(entry.file()).forEachKey(entry.rowGroup(), key -> {
                boolean fits;
                try {
                    fits = builder.append(key);
                } catch (RowGroupOrderException disordered) {
                    // The aborted run must still leave a countable exclusion reason.
                    metrics.recordStreamingSegmentRefused(disordered.reason());
                    throw disordered.locatedIn(entry.file(), entry.rowGroup());
                } catch (IllegalStateException rejected) {
                    throw new IllegalStateException("row group " + entry.rowGroup() + " of " + entry.file()
                            + " cannot be served: " + rejected.getMessage(), rejected);
                }
                if (!fits) {
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
     * Evicts least-recently-used blocks to the budget, retaining the just-faulted block. Called
     * under {@link #lock}.
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

    /** Returns the retained decoder for {@code file}; called under {@link #lock}. */
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
