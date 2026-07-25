/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * UNIT coverage for the sequential-window prefetch decorator: hit byte-identity, the miss/refill and
 * end-of-listing hit conditions, {@code fromInclusive} boundaries, projection keying, LRU + eager
 * eviction, a concurrent-walk smoke, and system-property config parsing. Differential no-gap/no-overlap
 * coverage against the real store exercises the same window boundaries under the real implementation.
 */
class WindowedListingStoreTest {

    private static final Projection KEYS = Projection.KEYS_ONLY;

    @Test
    void hitServesByteIdenticalSliceVersusAFreshDelegateCall() {
        FakeListingStore delegate = store(100);
        FakeListingStore reference = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 8);

        // A COLD miss reads exactly `limit` — nothing is prefetched for a caller that has not yet
        // shown it is paginating, so this fill is fully consumed and never cached.
        windowed.rows(null, true, null, 10, KEYS);
        assertThat(delegate.calls()).isEqualTo(1);

        // Continuing from the tail of that page is the pagination signal: this miss ramps the fill
        // (10 → 40) and caches the surplus.
        windowed.rows(key(9), false, null, 10, KEYS);
        assertThat(delegate.calls()).isEqualTo(2);

        // Next continuation page: a hit that slices deeper into the ramped window.
        ByteKey from = key(19);
        List<ListedObject> hit = windowed.rows(from, false, null, 10, KEYS);
        assertThat(delegate.calls()).isEqualTo(2);   // no new delegate read — served from the window

        List<ListedObject> fresh = reference.rows(from, false, null, 10, KEYS);
        assertThat(keys(hit)).isEqualTo(keys(fresh));
        assertThat(rowsEqual(hit, fresh)).isTrue();
    }

    @Test
    void insufficientTailOfANonFinalWindowMissesAndRefills() {
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 10, 8);

        // windowRows == 10 and 100 rows exist ⇒ the fill returns exactly 10 rows ⇒ NON-final window.
        windowed.rows(null, true, null, 10, KEYS);
        assertThat(delegate.calls()).isEqualTo(1);
        assertThat(delegate.lastLimit()).isEqualTo(10);     // fill used windowRows, not the page limit

        // Past the window's last buffered row, non-final ⇒ must refill.
        List<ListedObject> page = windowed.rows(key(9), false, null, 5, KEYS);
        assertThat(delegate.calls()).isEqualTo(2);
        assertThat(keys(page)).containsExactly("key-010", "key-011", "key-012", "key-013", "key-014");
    }

    @Test
    void limitLargerThanWindowRowsIsHonoredNotSilentlyTruncated() {
        FakeListingStore delegate = store(100);
        FakeListingStore reference = store(100);
        // Deliberately configure window-rows (10) below a caller's limit (25) — the I1 hazard: a
        // small operator-configured window-rows must never make a single rows() call return fewer
        // rows than a bare delegate call would for the same (from, limit).
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 10, 8);

        List<ListedObject> page = windowed.rows(null, true, null, 25, KEYS);
        assertThat(delegate.lastLimit()).isEqualTo(25);     // fill requested max(windowRows, limit)
        assertThat(page).hasSize(25);
        assertThat(keys(page)).isEqualTo(keys(reference.rows(null, true, null, 25, KEYS)));

        // Truncation semantics hold: the pager asks for maxKeys+1 to detect truncation. With 100 rows
        // total, requesting 26 must still yield 26 (more exist past row 25), matching the bare store.
        WindowedListingStore windowedTruncation = new WindowedListingStore(store(100), metrics(), 10, 8);
        List<ListedObject> withLookahead = windowedTruncation.rows(null, true, null, 26, KEYS);
        assertThat(withLookahead).hasSize(26);
        assertThat(keys(withLookahead)).isEqualTo(keys(reference.rows(null, true, null, 26, KEYS)));
    }

    @Test
    void delegateFinalWindowServesAShortTailWithoutRefilling() {
        FakeListingStore delegate = store(100);
        FakeListingStore reference = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 8);

        // Walk in 10-row pages until a fill outruns the 100-row store. The fills ramp 10 → 40 →
        // window-rows, so the third one returns fewer rows than it asked for and is delegate-final.
        walkPages(windowed, 6, 10);
        assertThat(delegate.calls()).isEqualTo(3);

        // Ask for 10 starting near the end; only 5 remain, but the window is final ⇒ hit with a short tail.
        List<ListedObject> tail = windowed.rows(key(94), false, null, 10, KEYS);
        assertThat(delegate.calls()).isEqualTo(3);
        assertThat(keys(tail)).containsExactly("key-095", "key-096", "key-097", "key-098", "key-099");
        assertThat(keys(tail)).isEqualTo(keys(reference.rows(key(94), false, null, 10, KEYS)));
    }

    @Test
    void fromInclusiveTogglesTheInteriorBoundaryRow() {
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 8);
        // Walk until the ramped, delegate-final window spans key(50) and beyond.
        walkPages(windowed, 6, 10);
        int fills = delegate.calls();

        List<ListedObject> inclusive = windowed.rows(key(50), true, null, 3, KEYS);
        List<ListedObject> exclusive = windowed.rows(key(50), false, null, 3, KEYS);
        assertThat(delegate.calls()).isEqualTo(fills);   // both served from the same window

        assertThat(keys(inclusive)).containsExactly("key-050", "key-051", "key-052");
        assertThat(keys(exclusive)).containsExactly("key-051", "key-052", "key-053");
    }

    @Test
    void projectionMismatchMissesAndRefills() {
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 8);

        windowed.rows(null, true, null, 10, Projection.KEYS_ONLY);
        assertThat(delegate.calls()).isEqualTo(1);

        // Same bounds, different projection ⇒ a distinct window key ⇒ miss.
        windowed.rows(null, true, null, 10, Projection.WITH_OWNER);
        assertThat(delegate.calls()).isEqualTo(2);
    }

    @Test
    void lruEvictsTheLeastRecentlyUsedWindowAtCapacity() {
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 2);

        // Three distinct windows (distinct toExclusive keys). Each needs two reads to become cached:
        // the cold read fills exactly `limit` and is fully consumed, and only the continuation read
        // ramps and leaves a surplus worth retaining. With max-windows=2 the first (A) is the LRU.
        cacheWindow(windowed, key(30));                   // A
        cacheWindow(windowed, key(60));                   // B
        cacheWindow(windowed, key(90));                   // C ⇒ evicts A (the LRU); cache now {B, C}
        assertThat(delegate.calls()).isEqualTo(6);

        // B is still cached ⇒ a hit (no refill).
        windowed.rows(key(9), false, key(60), 5, KEYS);
        assertThat(delegate.calls()).isEqualTo(6);

        // A was evicted ⇒ re-requesting it misses and refills.
        windowed.rows(key(9), false, key(30), 5, KEYS);
        assertThat(delegate.calls()).isEqualTo(7);
    }

    @Test
    void eagerlyEvictsAFinalWindowServedThroughItsLastRow() {
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 8);

        // limit covers the whole final window ⇒ served through the last row ⇒ dead window, dropped.
        List<ListedObject> all = windowed.rows(null, true, null, 1000, KEYS);
        assertThat(all).hasSize(100);
        assertThat(delegate.calls()).isEqualTo(1);

        // The identical request must miss again — the window was eagerly evicted, not reused.
        windowed.rows(null, true, null, 1000, KEYS);
        assertThat(delegate.calls()).isEqualTo(2);
    }

    @Test
    void concurrentWalksAllSeeTheCompleteCorrectListing() throws InterruptedException {
        FakeListingStore delegate = store(500);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 64, 8);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            expected.add(String.format("key-%03d", i));
        }

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<List<String>> results = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            results.add(new ArrayList<>());
        }
        for (int t = 0; t < threads; t++) {
            List<String> sink = results.get(t);
            pool.submit(() -> {
                try {
                    start.await();
                    ByteKey from = null;
                    boolean inclusive = true;
                    while (true) {
                        List<ListedObject> page = windowed.rows(from, inclusive, null, 25, KEYS);
                        if (page.isEmpty()) {
                            break;
                        }
                        sink.addAll(keys(page));
                        from = ByteKey.copyOf(page.get(page.size() - 1).key());
                        inclusive = false;
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(failure.get()).isNull();
        for (List<String> sink : results) {
            assertThat(sink).isEqualTo(expected);
        }
    }

    /**
     * The regression guard for the cache-key defect: a real {@code ListObjectsV2} carries no upper
     * bound, so every page a work-stealing scan issues shares {@code (toExclusive=null, projection)}
     * and only its POSITION distinguishes it. Keyed on the bounds alone, N walkers hold one slot
     * between them and every read evicts the previous walker's window, so the hit rate is zero no
     * matter how large {@code max-windows} is. Each walker must keep its own window.
     */
    @Test
    void concurrentUnboundedWalksAtDistinctPositionsDoNotEvictEachOther() {
        FakeListingStore delegate = store(500);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 100, 8);

        // Four walkers, far apart in the keyspace, each ramped to a cached window of its own.
        int[] starts = {0, 100, 200, 300};
        ByteKey[] cursors = new ByteKey[starts.length];
        for (int i = 0; i < starts.length; i++) {
            windowed.rows(key(starts[i]), false, null, 5, KEYS);                  // cold
            List<ListedObject> page = windowed.rows(key(starts[i] + 5), false, null, 5, KEYS);   // ramps + caches
            cursors[i] = ByteKey.copyOf(page.get(page.size() - 1).key());
        }
        int fillsAfterWarmup = delegate.calls();

        // Interleave the walkers round-robin — the access order that makes a single-slot cache thrash.
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < starts.length; i++) {
                List<ListedObject> page = windowed.rows(cursors[i], false, null, 5, KEYS);
                assertThat(keys(page)).hasSize(5);
                cursors[i] = ByteKey.copyOf(page.get(page.size() - 1).key());
            }
        }

        // Every one of those 8 interleaved pages came out of its own walker's window.
        assertThat(delegate.calls()).isEqualTo(fillsAfterWarmup);
    }

    /**
     * A one-shot reader (the engine's single-row pivot probe, and each {@code successor(P)} seek in a
     * delimiter rollup) must not pay for a full prefetch window it will never read a second row from.
     */
    @Test
    void aColdReadFetchesOnlyWhatTheCallerAskedFor() {
        FakeListingStore delegate = store(500);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 50_000, 8);

        windowed.rows(key(42), false, null, 1, KEYS);
        assertThat(delegate.lastLimit()).isEqualTo(1);

        // ... while a caller that demonstrates it is paginating does get the window.
        windowed.rows(key(43), false, null, 1, KEYS);
        assertThat(delegate.lastLimit()).isGreaterThan(1);
    }

    @Test
    void configParsesSystemPropertiesAndDefaults() {
        WindowedListingStore.Config defaults = WindowedListingStore.Config.fromSystemProperties();
        assertThat(defaults.enabled()).isTrue();
        assertThat(defaults.windowRows()).isEqualTo(12_500);
        assertThat(defaults.maxWindows()).isEqualTo(96);

        String enabled = System.getProperty("swath.replay.prefetch.enabled");
        String windowRows = System.getProperty("swath.replay.prefetch.window-rows");
        String maxWindows = System.getProperty("swath.replay.prefetch.max-windows");
        try {
            System.setProperty("swath.replay.prefetch.enabled", "false");
            System.setProperty("swath.replay.prefetch.window-rows", "12345");
            System.setProperty("swath.replay.prefetch.max-windows", "3");
            WindowedListingStore.Config overridden = WindowedListingStore.Config.fromSystemProperties();
            assertThat(overridden.enabled()).isFalse();
            assertThat(overridden.windowRows()).isEqualTo(12345);
            assertThat(overridden.maxWindows()).isEqualTo(3);
        } finally {
            restore("swath.replay.prefetch.enabled", enabled);
            restore("swath.replay.prefetch.window-rows", windowRows);
            restore("swath.replay.prefetch.max-windows", maxWindows);
        }
    }

    @Test
    void hitAndMissCountersTrackServingPath() {
        ReplayMetrics metrics = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics, 1000, 8);

        windowed.rows(null, true, null, 10, KEYS);        // cold miss (fills 10, uncached)
        windowed.rows(key(9), false, null, 10, KEYS);     // continuation miss (ramps to 40, cached)
        windowed.rows(key(19), false, null, 10, KEYS);    // hit
        windowed.rows(key(29), false, null, 10, KEYS);    // hit

        assertThat(missCounter(metrics, "cold")).isEqualTo(1.0);
        assertThat(missCounter(metrics, "continuation")).isEqualTo(1.0);
        assertThat(counter(metrics, "swath.replay.prefetch.window.hit")).isEqualTo(2.0);
        assertThat(metrics.registry().find("swath.replay.prefetch.window.fill").timer().count()).isEqualTo(2L);
        // The outer per-page corridor timer fires once per rows() call (hit or miss).
        assertThat(metrics.registry().find("swath.replay.page.read.latency").timer().count()).isEqualTo(4L);
    }

    @Test
    void continuationMissIsTaggedByAnchorNotFillSize() {
        // window-rows below the page limit: a continuation anchor's ramped size saturates at
        // window-rows (5) and clamps back up to limit (10), so `requested == limit` even on a real
        // continuation. The miss reason must still read `continuation` (an anchor was consulted), not
        // `cold` — which the old `requested > limit` tag would have wrongly reported here.
        ReplayMetrics metrics = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics, 5, 8);  // window-rows < limit

        windowed.rows(null, true, null, 10, KEYS);        // cold miss
        windowed.rows(key(9), false, null, 10, KEYS);     // continuation miss (anchor fill 5 <= limit 10)

        assertThat(missCounter(metrics, "cold")).isEqualTo(1.0);
        assertThat(missCounter(metrics, "continuation")).isEqualTo(1.0);
    }

    /**
     * Walks {@code pages} sequential pages of {@code limit} rows from the start of the listing,
     * continuing from the last key served — the pagination pattern that ramps the fill size.
     */
    private static void walkPages(WindowedListingStore windowed, int pages, int limit) {
        ByteKey from = null;
        boolean inclusive = true;
        for (int i = 0; i < pages; i++) {
            List<ListedObject> page = windowed.rows(from, inclusive, null, limit, KEYS);
            if (page.isEmpty()) {
                return;
            }
            from = ByteKey.copyOf(page.get(page.size() - 1).key());
            inclusive = false;
        }
    }

    /** Drives one {@code toExclusive}-bounded walk far enough to leave a cached window behind. */
    private static void cacheWindow(WindowedListingStore windowed, ByteKey toExclusive) {
        windowed.rows(null, true, toExclusive, 5, KEYS);        // cold: fills 5, fully consumed
        windowed.rows(key(4), false, toExclusive, 5, KEYS);     // ramped: fills 20, caches the surplus
    }

    private static double counter(ReplayMetrics metrics, String name) {
        return metrics.registry().find(name).counter().count();
    }

    private static double missCounter(ReplayMetrics metrics, String reason) {
        return metrics.registry().find("swath.replay.prefetch.window.miss")
                .tag("reason", reason).counter().count();
    }

    private static void restore(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static ReplayMetrics metrics() {
        return new ReplayMetrics();
    }

    private static FakeListingStore store(int count) {
        List<ListedObject> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String k = String.format("key-%03d", i);
            rows.add(new ListedObject(k.getBytes(StandardCharsets.UTF_8), i, i, "etag-" + i, "STANDARD",
                    "owner-" + i, "display-" + i, "CRC32", "FULL_OBJECT"));
        }
        return new FakeListingStore(rows);
    }

    private static ByteKey key(int i) {
        return ByteKey.copyOf(String.format("key-%03d", i).getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> keys(List<ListedObject> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (ListedObject row : rows) {
            out.add(new String(row.key(), StandardCharsets.UTF_8));
        }
        return out;
    }

    private static boolean rowsEqual(List<ListedObject> a, List<ListedObject> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            ListedObject x = a.get(i);
            ListedObject y = b.get(i);
            if (!Arrays.equals(x.key(), y.key()) || x.size() != y.size()
                    || x.lastModifiedEpochMicros() != y.lastModifiedEpochMicros()
                    || !x.etag().equals(y.etag()) || !x.storageClass().equals(y.storageClass())) {
                return false;
            }
        }
        return true;
    }
}
