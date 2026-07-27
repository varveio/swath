/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link StreamingListingStore}'s own contract, below the pager: that a range read is answered from
 * decoded segments, that a segment is decoded <b>once</b> for a sequential walk, that a mid-keyspace
 * start decodes only the row group it lands in, and that residency stays inside its budget while the
 * answers stay exact. Equivalence with the other tiers through the whole S3 protocol is
 * {@link SimStoreDifferentialTest}'s job; what is pinned here is the machinery that differential
 * cannot see — how many segments were decoded, in what order, and how much memory was held.
 */
class StreamingListingStoreTest {

    /** Enough keys, at enough bytes each, to form several row groups under the small-group config. */
    private static final int KEY_COUNT = 600;

    /** Every key below is this many bytes, so a block's exact footprint is arithmetic, not a guess. */
    private static final int KEY_BYTES = 52;

    /** Comfortably more than the whole fixture's decoded keys: nothing is ever evicted under it. */
    private static final long GENEROUS_BUDGET = 1L << 20;

    @TempDir
    private Path dir;

    private MeterRegistry registry;
    private SimStoreMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new SimStoreMetrics(registry);
    }

    @Test
    void aSequentialWalkDecodesEveryRowGroupExactlyOnce() throws IOException {
        List<String> keys = keys(KEY_COUNT);
        List<IndexEntry> index = index(sortedFixture(keys));
        assertThat(index.size()).isGreaterThan(2);   // the multi-segment path is genuinely exercised

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET)) {
            assertThat(walk(store, 7)).isEqualTo(keys);
        }

        // Decode-once, stated as an equality rather than a bound: the walk read every key of the
        // fixture and the decoder read every key of the fixture, no key twice.
        assertThat(counter(SimStoreMetrics.SEGMENT_DECODE_ROWS_METRIC)).isEqualTo(KEY_COUNT);
        assertThat(faults(SimStoreMetrics.FAULT_SEEK)).isEqualTo(1);   // only the very first touch
        assertThat(faults(SimStoreMetrics.FAULT_FORWARD)).isEqualTo(index.size() - 1);
        assertThat(counter(SimStoreMetrics.SEGMENT_EVICT_METRIC)).isZero();
    }

    /**
     * The steal case: a fresh cursor started at an arbitrary key must seek to the row group holding
     * it off the routing index and decode from there — never from the head of the file. Measured, not
     * asserted by inspection: the decoded row count after one such read equals the landing group's
     * own row count, so decoding even one group before it would fail.
     */
    @Test
    void aMidKeyspaceStartDecodesOnlyTheRowGroupItLandsIn() throws IOException {
        List<String> keys = keys(KEY_COUNT);
        List<IndexEntry> index = index(sortedFixture(keys));
        int landing = index.size() - 1;
        IndexEntry landingGroup = index.get(landing);

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET)) {
            List<ListedObject> page = store.rows(landingGroup.firstKey(), true, null, 1, Projection.KEYS_ONLY);
            assertThat(utf8(page.getFirst().key())).isEqualTo(utf8(landingGroup.firstKey().toByteArray()));
        }

        assertThat(counter(SimStoreMetrics.SEGMENT_DECODE_ROWS_METRIC)).isEqualTo(landingGroup.rowCount());
        assertThat(faults(SimStoreMetrics.FAULT_SEEK)).isEqualTo(1);
        assertThat(faults(SimStoreMetrics.FAULT_FORWARD)).isZero();
    }

    /**
     * A page wide enough to span several row groups is one call, not one call per group: the read
     * walks forward through as many segments as {@code limit} needs, faulting each in turn, and the
     * keys come back in one ascending run with no boundary duplicated or skipped.
     */
    @Test
    void oneReadSpansAsManySegmentsAsItsLimitNeeds() throws IOException {
        List<String> keys = keys(KEY_COUNT);
        List<IndexEntry> index = index(sortedFixture(keys));
        long firstTwoGroups = index.get(0).rowCount() + index.get(1).rowCount();
        int limit = (int) firstTwoGroups + 1;   // one key past the second group's last

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET)) {
            List<ListedObject> page = store.rows(null, true, null, limit, Projection.KEYS_ONLY);
            assertThat(page.stream().map(row -> utf8(row.key())).toList())
                    .isEqualTo(keys.subList(0, limit));
        }
        assertThat(faults(SimStoreMetrics.FAULT_SEEK) + faults(SimStoreMetrics.FAULT_FORWARD)).isEqualTo(3);
    }

    @Test
    void rangeBoundsAreHonouredAtAndAcrossSegmentEdges() throws IOException {
        List<String> keys = keys(KEY_COUNT);
        List<IndexEntry> index = index(sortedFixture(keys));
        ByteKey secondGroupStart = index.get(1).firstKey();
        int boundary = keys.indexOf(utf8(secondGroupStart.toByteArray()));
        assertThat(boundary).isPositive();

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET)) {
            // An exclusive upper bound exactly at a segment's first key stops before it, and never
            // faults the segment that key starts.
            assertThat(pageKeys(store.rows(null, true, secondGroupStart, KEY_COUNT, Projection.KEYS_ONLY)))
                    .isEqualTo(keys.subList(0, boundary));
            // The same key as an inclusive lower bound starts exactly on it; exclusive skips it.
            assertThat(pageKeys(store.rows(secondGroupStart, true, null, 2, Projection.KEYS_ONLY)))
                    .isEqualTo(keys.subList(boundary, boundary + 2));
            assertThat(pageKeys(store.rows(secondGroupStart, false, null, 2, Projection.KEYS_ONLY)))
                    .isEqualTo(keys.subList(boundary + 1, boundary + 3));
            // A lower bound past every key returns nothing rather than wrapping or throwing.
            assertThat(store.rows(ByteKey.copyOf(utf8Bytes("zzzz")), true, null, 5, Projection.KEYS_ONLY))
                    .isEmpty();
            // A non-positive limit is answered without touching a segment at all.
            assertThat(store.rows(null, true, null, 0, Projection.KEYS_ONLY)).isEmpty();
        }
    }

    /**
     * The memory claim: residency is a function of the budget, never of the fixture. Under a budget
     * sized for two decoded row groups a walk of the whole fixture must stay inside it however far it
     * runs, must actually evict, and must still return every key — and, because a forward-only cursor
     * never revisits what it evicted, must still decode each row group exactly once.
     *
     * <p>The peak allows one segment over: a fault decodes the new block before the eviction it
     * triggers drops the old one, so the high-water mark is the settled budget plus the block that
     * crossed it. That is the bound the module docs state, and it is asserted here rather than left
     * as a caveat — a tier that quietly held two extra blocks would still pass a settled-state check.
     */
    @Test
    void residencyStaysInsideTheBudgetByEvictingBehindTheCursor() throws IOException {
        List<String> keys = keys(KEY_COUNT);
        List<IndexEntry> index = index(sortedFixture(keys));
        long largestBlock = largestBlockBytes(index);
        long budget = 2 * largestBlock;

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, budget)) {
            assertThat(walk(store, 7)).isEqualTo(keys);
            assertThat(store.peakResidentBytes()).isLessThanOrEqualTo(budget + largestBlock);
            assertThat(store.residentBytes()).isLessThanOrEqualTo(budget);
        }
        assertThat(counter(SimStoreMetrics.SEGMENT_EVICT_METRIC)).isPositive();
        assertThat(counter(SimStoreMetrics.SEGMENT_DECODE_ROWS_METRIC)).isEqualTo(KEY_COUNT);
    }

    @Test
    void aBudgetTooSmallForOneRowGroupFailsFastNamingTheProperty() throws IOException {
        List<IndexEntry> index = index(sortedFixture(keys(KEY_COUNT)));

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, 1L)) {
            assertThatThrownBy(() -> store.rows(null, true, null, 1, Projection.KEYS_ONLY))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY);
        }
    }

    @Test
    void anEmptyFixtureServesNothingAndDecodesNothing() throws IOException {
        List<IndexEntry> index = index(sortedFixture(List.of()));

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET)) {
            assertThat(store.rows(null, true, null, 10, Projection.KEYS_ONLY)).isEmpty();
            assertThat(store.residentBytes()).isZero();
        }
        assertThat(faults(SimStoreMetrics.FAULT_SEEK) + faults(SimStoreMetrics.FAULT_FORWARD)).isZero();
    }

    @Test
    void closeDropsTheResidentSegments() throws IOException {
        List<IndexEntry> index = index(sortedFixture(keys(KEY_COUNT)));

        StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET);
        store.rows(null, true, null, 10, Projection.KEYS_ONLY);
        assertThat(store.residentBytes()).isPositive();
        store.close();

        assertThat(store.residentBytes()).isZero();
    }

    @Test
    void theResidentBytesGaugeTracksTheStore() throws IOException {
        List<IndexEntry> index = index(sortedFixture(keys(KEY_COUNT)));

        try (StreamingListingStore store = new StreamingListingStore(index, metrics, GENEROUS_BUDGET)) {
            assertThat(registry.get(SimStoreMetrics.RESIDENT_BYTES_METRIC).gauge().value()).isZero();
            store.rows(null, true, null, 10, Projection.KEYS_ONLY);
            assertThat(registry.get(SimStoreMetrics.RESIDENT_BYTES_METRIC).gauge().value())
                    .isEqualTo(store.residentBytes());
        }
    }

    // --- helpers --------------------------------------------------------------

    /** Pages the whole fixture at {@code pageSize}, chaining each read from the last key served. */
    private static List<String> walk(StreamingListingStore store, int pageSize) {
        List<String> walked = new ArrayList<>();
        ByteKey from = null;
        while (true) {
            List<ListedObject> page = store.rows(from, false, null, pageSize, Projection.KEYS_ONLY);
            if (page.isEmpty()) {
                return walked;
            }
            page.forEach(row -> walked.add(utf8(row.key())));
            from = ByteKey.copyOf(page.getLast().key());
        }
    }

    /**
     * The exact off-heap footprint of the fattest row group in {@code index}: its keys (all
     * {@value #KEY_BYTES} bytes here) plus one {@code int64} offset per key and a terminator. The
     * residency budget is a byte budget, so a test about eviction has to be expressed in the same
     * bytes the store counts, not in a round number that happens to be close.
     */
    private static long largestBlockBytes(List<IndexEntry> index) {
        return index.stream()
                .mapToLong(entry -> entry.rowCount() * KEY_BYTES + (entry.rowCount() + 1) * Long.BYTES)
                .max().orElseThrow();
    }

    private static List<String> keys(int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = String.format("keys/%06d/", i) + "x".repeat(40);
            assertThat(key).hasSize(KEY_BYTES);   // largestBlockBytes' arithmetic depends on it
            keys.add(key);
        }
        return keys;
    }

    /** {@code keys} written as a capture, then run through the production sorter into many small
     *  row groups — the same shape the differential suite uses, so both see real segment boundaries. */
    private Path sortedFixture(List<String> keys) throws IOException {
        Path capture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String key : keys) {
                writer.write(ObjectEntries.withOwner(utf8Bytes(key), "etag-" + key));
            }
        }
        Path out = Files.createDirectory(dir.resolve("out"));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, out);
        return out;
    }

    private static List<IndexEntry> index(Path fixture) throws IOException {
        IndexLoadResult loaded = SortedFixtures.loadIndex(
                SortedFixtures.resolveFiles(fixture), new FixtureMetrics());
        return ((IndexLoadResult.Loaded) loaded).entries();
    }

    private double counter(String name) {
        var counter = registry.find(name).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private double faults(String kind) {
        var counter = registry.find(SimStoreMetrics.SEGMENT_FAULT_METRIC).tag("kind", kind).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static List<String> pageKeys(List<ListedObject> page) {
        return page.stream().map(row -> utf8(row.key())).toList();
    }

    private static String utf8(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    private static byte[] utf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
