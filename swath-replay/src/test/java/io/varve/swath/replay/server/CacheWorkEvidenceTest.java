/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import io.varve.swath.replay.testkit.FakeListingStore;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Bounded cache-work evidence; this is not a timed HTTP capacity measurement. */
class CacheWorkEvidenceTest {
    private static final Projection KEYS = Projection.KEYS_ONLY;

    @ParameterizedTest(name = "{0} keys")
    @ValueSource(ints = {80_000, 120_000})
    void realSortedOneKLeaderAndFiveKFollowerCharacterizeStartupAndSteadyState(
            int inventory, @TempDir Path dir) throws IOException {
        Fixture fixture = sortedFixture(dir, inventory);
        ReplayMetrics referenceMetrics = new ReplayMetrics();
        ReplayMetrics sharedMetrics = new ReplayMetrics();
        try (CountingStore referenceStore = new CountingStore(new SortedParquetStore(
                     fixture.files(), fixture.index(), referenceMetrics, 2));
             WindowedListingStore reference = new WindowedListingStore(referenceStore,
                     referenceMetrics, 12_500, 96);
             CountingStore sharedStore = new CountingStore(new SortedParquetStore(
                     fixture.files(), fixture.index(), sharedMetrics, 2));
             WindowedListingStore shared = new WindowedListingStore(sharedStore,
                     sharedMetrics, 12_500, 96)) {
            Walker referenceWalker = new Walker(1_000);
            while (!referenceWalker.done) referenceWalker.step(reference);
            assertThat(referenceWalker.keys).containsExactlyElementsOf(expectedKeys(inventory));
            Walker leader = new Walker(1_000);
            Walker follower = new Walker(5_000);
            for (int i = 0; i < 15; i++) leader.step(shared);
            while (!leader.done || !follower.done) {
                for (int i = 0; i < 5 && !leader.done; i++) leader.step(shared);
                if (!follower.done) follower.step(shared);
            }
            assertThat(leader.keys).isEqualTo(referenceWalker.keys);
            assertThat(follower.keys).isEqualTo(referenceWalker.keys);
            if (inventory == 80_000) {
                // A first 5k cold read adds one complete response-sized backing call; this
                // short-corpus startup case is characterization, not a passing <=1.1 gate.
                assertThat(referenceStore.calls).isEqualTo(8);
                assertThat(sharedStore.calls).isEqualTo(9);
                assertThat(referenceStore.returnedRows).isEqualTo(80_070);
                assertThat(sharedStore.returnedRows).isEqualTo(85_071);
                assertThat(sharedStore.returnedRows - referenceStore.returnedRows).isEqualTo(5_001);
                System.out.println("cache-80k-reference-reads " + referenceStore.events);
                System.out.println("cache-80k-shared-reads " + sharedStore.events);
            } else {
                assertThat((double) sharedStore.calls / referenceStore.calls).isLessThanOrEqualTo(1.1);
                assertThat((double) sharedStore.returnedRows / referenceStore.returnedRows)
                        .isLessThanOrEqualTo(1.1);
            }
            assertThat(sharedMetrics.registry().find("swath.replay.prefetch.window.join")
                    .tag("reason", "hit").counter().count()).isGreaterThan(0);
            assertThat(sharedMetrics.registry().find("swath.replay.parquet.query.rows")
                    .summary().totalAmount()).isEqualTo((double) sharedStore.returnedRows);
            assertThat(referenceMetrics.registry().find("swath.replay.parquet.query.rows")
                    .summary().totalAmount()).isEqualTo((double) referenceStore.returnedRows);
            System.out.printf("cache-real-work unique=%d oneK-calls=%d shared-calls=%d "
                            + "oneK-returned-rows=%d shared-returned-rows=%d join-hits=%.0f%n",
                    inventory, referenceStore.calls, sharedStore.calls,
                    referenceStore.returnedRows, sharedStore.returnedRows,
                    sharedMetrics.registry().find("swath.replay.prefetch.window.join")
                            .tag("reason", "hit").counter().count());
        } finally {
            referenceMetrics.registry().close();
            sharedMetrics.registry().close();
        }
    }

    @Test
    void ninetySixDeterministicallyInterleavedWalkersRespectScaledGlobalRowCap() {
        List<String> keys = expectedKeys(14_000);
        try (FakeListingStore backing = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            ReplayMetrics metrics = new ReplayMetrics();
            try (WindowedListingStore cache = new WindowedListingStore(backing, metrics, 100, 96)) {
                ByteKey[] cursors = new ByteKey[96];
                for (int step = 0; step < 11; step++) {
                    for (int walker = 0; walker < cursors.length; walker++) {
                        int first = walker * 140 + step * 10;
                        ByteKey from = cursors[walker] == null && walker != 0
                                ? key(walker * 140 - 1) : cursors[walker];
                        List<ListedObject> page = cache.rows(from, from == null, null, 11, KEYS);
                        assertThat(page).hasSize(11);
                        for (int i = 0; i < 10; i++) {
                            assertThat(new String(page.get(i).key(), StandardCharsets.UTF_8))
                                    .isEqualTo(keys.get(first + i));
                        }
                        cursors[walker] = ByteKey.copyOf(page.get(9).key());
                    }
                }
                double retained = metrics.registry().find("swath.replay.prefetch.rows.live")
                        .gauge().value();
                double windows = metrics.registry().find("swath.replay.prefetch.windows.live")
                        .gauge().value();
                assertThat(retained).isLessThanOrEqualTo(9_600);
                assertThat(windows).isLessThanOrEqualTo(96);
                assertThat(metrics.registry().find("swath.replay.prefetch.window.row_budget_eviction")
                        .counter().count()).isGreaterThan(0);
                System.out.printf("cache-96-walker-characterization windows=%.0f retained-rows=%.0f "
                        + "row-budget-evictions=%.0f%n", windows, retained,
                        metrics.registry().find("swath.replay.prefetch.window.row_budget_eviction")
                                .counter().count());
            } finally { metrics.registry().close(); }
        }
    }

    private static Fixture sortedFixture(Path dir, int count) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String key : expectedKeys(count)) {
                writer.write(ObjectEntries.withOwner(key.getBytes(StandardCharsets.UTF_8), "etag"));
            }
        }
        Path sorted = Files.createDirectories(dir.resolve("sorted"));
        new CaptureSorter(SortConfigs.base()).sort(capture, sorted);
        List<Path> files = SortedFixtures.resolveFiles(sorted);
        IndexLoadResult result = SortedFixtures.loadIndex(files, new FixtureMetrics());
        assertThat(result).isInstanceOf(IndexLoadResult.Loaded.class);
        return new Fixture(files, ((IndexLoadResult.Loaded) result).entries());
    }

    private static List<String> expectedKeys(int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) keys.add(String.format("key-%06d", i));
        return keys;
    }

    private static ByteKey key(int value) {
        return ByteKey.copyOf(String.format("key-%06d", value).getBytes(StandardCharsets.UTF_8));
    }

    private record Fixture(List<Path> files, List<IndexEntry> index) { }

    private static final class Walker {
        private final int emittedPageSize;
        private final List<String> keys = new ArrayList<>();
        private ByteKey cursor;
        private boolean done;

        private Walker(int emittedPageSize) { this.emittedPageSize = emittedPageSize; }

        void step(WindowedListingStore store) {
            if (done) return;
            int limit = emittedPageSize + 1;
            List<ListedObject> page = store.rows(cursor, cursor == null, null, limit, KEYS);
            int emitted = Math.min(emittedPageSize, page.size());
            for (int i = 0; i < emitted; i++) {
                keys.add(new String(page.get(i).key(), StandardCharsets.UTF_8));
            }
            done = page.size() < limit;
            if (!done) cursor = ByteKey.copyOf(page.get(emitted - 1).key());
        }
    }

    private static final class CountingStore implements ListingStore {
        private final SortedParquetStore delegate;
        private final List<ReadEvent> events = new ArrayList<>();
        private long calls;
        private long returnedRows;

        private CountingStore(SortedParquetStore delegate) { this.delegate = delegate; }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                       int limit, Projection projection) {
            List<ListedObject> rows = delegate.rows(from, fromInclusive, toExclusive, limit, projection);
            calls++;
            returnedRows += rows.size();
            events.add(new ReadEvent(from == null ? null : new String(from.toByteArray(), StandardCharsets.UTF_8),
                    fromInclusive, limit, rows.size(), rows.isEmpty() ? null
                            : new String(rows.getFirst().key(), StandardCharsets.UTF_8),
                    rows.isEmpty() ? null : new String(rows.getLast().key(), StandardCharsets.UTF_8)));
            return rows;
        }

        @Override public void close() { delegate.close(); }
    }

    private record ReadEvent(String from, boolean inclusive, int limit, int returned,
                             String first, String last) { }
}
