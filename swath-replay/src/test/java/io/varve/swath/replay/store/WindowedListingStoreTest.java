/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * UNIT coverage for the sequential-window prefetch decorator: hit byte-identity, the miss/refill and
 * end-of-listing hit conditions, {@code fromInclusive} boundaries, projection keying, bounded LRU
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

        // A was evicted, but B's wider upper bound safely covers the narrower A request.
        windowed.rows(key(9), false, key(30), 5, KEYS);
        assertThat(delegate.calls()).isEqualTo(6);
    }

    @Test
    void finalWindowRemainsAvailableToLaggingWalkers() {
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics(), 1000, 8);

        // The first caller consumes the entire final window, but another walker can reuse it.
        List<ListedObject> all = windowed.rows(null, true, null, 1000, KEYS);
        assertThat(all).hasSize(100);
        assertThat(delegate.calls()).isEqualTo(1);

        // The identical request is a hit until ordinary LRU eviction.
        windowed.rows(null, true, null, 1000, KEYS);
        assertThat(delegate.calls()).isEqualTo(1);
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
        assertThat(metrics.registry().find("swath.replay.prefetch.anchor")
                .tag("event", "claimed").counter().count()).isEqualTo(1.0);
        assertThat(metrics.registry().find("swath.replay.prefetch.windows.live").gauge().value())
                .isEqualTo(1.0);
        assertThat(metrics.registry().find("swath.replay.prefetch.anchors.live").gauge().value())
                .isGreaterThan(0.0);
        // The cache decorator never owns the backing-decode timer. This fake has no instrumented
        // reader, so neither its two fills nor its two hits fabricate a page.read sample.
        assertThat(metrics.registry().find("swath.replay.page.read.latency").timer().count()).isZero();
    }

    @Test
    void oversizedReadDoesNotConsumeOrRegisterContinuationAnchor() {
        // A caller above the nominal window size is served uncached at exactly its requested size.
        ReplayMetrics metrics = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore windowed = new WindowedListingStore(delegate, metrics, 5, 8);  // window-rows < limit

        windowed.rows(null, true, null, 10, KEYS);        // cold miss
        windowed.rows(key(9), false, null, 10, KEYS);

        assertThat(missCounter(metrics, "cold")).isEqualTo(2.0);
        assertThat(metrics.registry().find("swath.replay.prefetch.anchor")
                .tag("event", "claimed").counter().count()).isZero();
        assertThat(metrics.registry().find("swath.replay.prefetch.anchor")
                .tag("event", "registered").counter().count()).isZero();
        assertThat(metrics.registry().find("swath.replay.prefetch.anchors.live").gauge().value()).isZero();
        assertThat(metrics.registry().find("swath.replay.prefetch.window.uncached")
                .tag("reason", "request_exceeds_window").counter().count()).isEqualTo(2);
        assertThat(delegate.lastLimit()).isEqualTo(10);
    }

    @Test
    void continuationCeilingRoundsUpToWholeRequestedPages() {
        FakeListingStore oneK = store(30_000);
        CountingStore oneKWork = new CountingStore(oneK);
        ReplayMetrics oneKMeters = metrics();
        WindowedListingStore oneKCache = new WindowedListingStore(oneKWork, oneKMeters, 12_500, 8);
        ByteKey cursor = null;
        for (int i = 0; i < 6; i++) {
            List<ListedObject> page = oneKCache.rows(cursor, cursor == null, null, 1001, KEYS);
            // The pager emits 1,000 entries and uses the 1,001st only as lookahead.
            cursor = ByteKey.copyOf(page.get(999).key());
        }
        assertThat(oneK.lastLimit()).isEqualTo(13_013);
        assertThat(oneKWork.calls.get()).isEqualTo(3);
        assertThat(oneKWork.rows.get()).isEqualTo(1_001 + 4_004 + 13_013);
        assertThat(oneKMeters.registry().find("swath.replay.prefetch.window.ramp_ceiling_rows")
                .summary().max()).isEqualTo(13_013);

        FakeListingStore fiveK = store(30_000);
        CountingStore fiveKWork = new CountingStore(fiveK);
        ReplayMetrics fiveKMeters = metrics();
        WindowedListingStore fiveKCache = new WindowedListingStore(fiveKWork, fiveKMeters, 12_500, 8);
        List<ListedObject> first = fiveKCache.rows(null, true, null, 5001, KEYS);
        fiveKCache.rows(ByteKey.copyOf(first.get(4_999).key()), false, null, 5001, KEYS);
        assertThat(fiveK.lastLimit()).isEqualTo(15_003);
        assertThat(fiveKWork.calls.get()).isEqualTo(2);
        assertThat(fiveKWork.rows.get()).isEqualTo(5_001 + 15_003);
        assertThat(fiveKMeters.registry().find("swath.replay.prefetch.window.ramp_ceiling_rows")
                .summary().max()).isEqualTo(15_003);
    }

    @Test
    void narrowedUpperBoundUsesCoveredRowsButIncompleteTailRefills() {
        FakeListingStore delegate = store(100);
        WindowedListingStore cache = new WindowedListingStore(delegate, metrics(), 50, 8);
        cache.rows(null, true, null, 5, KEYS);
        cache.rows(key(4), false, null, 5, KEYS); // 5..24 cached under open upper bound
        int fills = delegate.calls();

        assertThat(keys(cache.rows(key(18), false, key(20), 10, KEYS))).containsExactly("key-019");
        assertThat(delegate.calls()).isEqualTo(fills);
        assertThat(keys(cache.rows(key(21), false, key(40), 10, KEYS)))
                .containsExactly("key-022", "key-023", "key-024", "key-025", "key-026",
                        "key-027", "key-028", "key-029", "key-030", "key-031");
        assertThat(delegate.calls()).isEqualTo(fills + 1);
    }

    @Test
    void twoWindowJoinBridgesLeadingSmallPageWalkWithoutBackingRead() {
        ReplayMetrics meters = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore cache = new WindowedListingStore(delegate, meters, 12, 8);
        ByteKey cursor = null;
        for (int i = 0; i < 6; i++) {
            List<ListedObject> page = cache.rows(cursor, cursor == null, null, 2, KEYS);
            cursor = ByteKey.copyOf(page.get(page.size() - 1).key());
        }
        assertThat(delegate.calls()).isEqualTo(3); // W2 is present before crossing probe
        int before = delegate.calls();
        assertThat(keys(cache.rows(key(6), false, null, 5, KEYS)))
                .containsExactly("key-007", "key-008", "key-009", "key-010", "key-011");
        assertThat(delegate.calls()).isEqualTo(before);
        assertThat(meters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "hit").counter().count()).isGreaterThan(0);
    }

    @Test
    void globalRowBudgetEvictsEvenBeforeEntryLimit() {
        ReplayMetrics meters = metrics();
        WindowedListingStore cache = new WindowedListingStore(store(100), meters, 10, 2);
        cache.rows(null, true, null, 6, KEYS);
        cache.rows(key(5), false, null, 6, KEYS); // rounded 12-row first window
        cache.rows(key(30), false, null, 6, KEYS);
        cache.rows(key(36), false, null, 6, KEYS); // second 12-row window; 24 > 20 budget
        assertThat(meters.registry().find("swath.replay.prefetch.rows.live").gauge().value())
                .isLessThanOrEqualTo(20.0);
        assertThat(meters.registry().find("swath.replay.prefetch.window.row_budget_eviction")
                .counter().count()).isGreaterThan(0);
    }

    @Test
    void uncacheableRoundedContinuationFallsBackToRequestedRows() {
        FakeListingStore delegate = store(100);
        ReplayMetrics meters = metrics();
        WindowedListingStore cache = new WindowedListingStore(delegate, meters, 40, 1);
        ByteKey cursor = null;
        for (int i = 0; i < 8; i++) {
            List<ListedObject> page = cache.rows(cursor, cursor == null, null, 9, KEYS);
            cursor = ByteKey.copyOf(page.get(page.size() - 1).key());
            assertThat(delegate.lastLimit()).isEqualTo(9);
        }
        assertThat(meters.registry().find("swath.replay.prefetch.window.uncached")
                .tag("reason", "rounded_exceeds_row_budget").counter().count()).isGreaterThan(0);
    }

    @Test
    void inclusiveSeekAtAnchorDoesNotClaimContinuationRamp() {
        FakeListingStore delegate = store(100);
        WindowedListingStore cache = new WindowedListingStore(delegate, metrics(), 50, 8);
        cache.rows(null, true, null, 10, KEYS);
        cache.rows(key(9), true, null, 10, KEYS);
        assertThat(delegate.lastLimit()).isEqualTo(10);
        cache.rows(key(9), false, null, 10, KEYS);
        assertThat(delegate.lastLimit()).isEqualTo(40);
    }

    @Test
    void olderSingleWindowWinsOverTighterInsufficientWindow() {
        ReplayMetrics meters = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore cache = new WindowedListingStore(delegate, meters, 50, 8);
        cache.rows(key(19), false, null, 1, KEYS); // cold row 20
        cache.rows(key(20), false, null, 1, KEYS); // tight window 21..24
        cache.rows(null, true, null, 10, KEYS);
        cache.rows(key(9), false, null, 10, KEYS); // broad window 10..49
        int before = delegate.calls();
        assertThat(keys(cache.rows(key(22), false, null, 5, KEYS)))
                .containsExactly("key-023", "key-024", "key-025", "key-026", "key-027");
        assertThat(delegate.calls()).isEqualTo(before);
        assertThat(meters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "hit").counter()).isNull();
    }

    @Test
    void joinDeclinesGapAndProjectionMismatch() {
        FakeListingStore gapDelegate = store(100);
        ReplayMetrics gapMeters = metrics();
        WindowedListingStore gap = new WindowedListingStore(gapDelegate, gapMeters, 12, 8);
        leadSmallWalkThroughFirstWindow(gap);
        gap.rows(key(11), false, null, 2, KEYS);
        gap.rows(key(13), false, null, 2, KEYS); // W2 lower 13 > W1 last 9
        int gapReads = gapDelegate.calls();
        assertThat(keys(gap.rows(key(6), false, null, 5, KEYS)))
                .containsExactly("key-007", "key-008", "key-009", "key-010", "key-011");
        assertThat(gapDelegate.calls()).isEqualTo(gapReads + 1);
        assertThat(gapMeters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "gap").counter().count()).isEqualTo(1);

        FakeListingStore projectionDelegate = store(100);
        ReplayMetrics projectionMeters = metrics();
        WindowedListingStore projection = new WindowedListingStore(projectionDelegate, projectionMeters, 12, 8);
        leadSmallWalkThroughFirstWindow(projection);
        projection.rows(key(9), false, null, 2, Projection.WITH_OWNER);
        int projectionReads = projectionDelegate.calls();
        assertThat(keys(projection.rows(key(6), false, null, 5, KEYS)))
                .containsExactly("key-007", "key-008", "key-009", "key-010", "key-011");
        assertThat(projectionDelegate.calls()).isEqualTo(projectionReads + 1);
        assertThat(projectionMeters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "projection").counter().count()).isEqualTo(1);
    }

    @Test
    void joinSkipsOverlapAndRejectsNarrowerSecondUpperBound() {
        ReplayMetrics meters = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore cache = new WindowedListingStore(delegate, meters, 12, 8);
        cache.rows(null, true, key(50), 2, KEYS);
        cache.rows(key(1), false, key(50), 2, KEYS); // W1: rows 2..9
        cache.rows(key(1), false, null, 5, KEYS); // cold because W1's upper is only 50
        cache.rows(key(6), false, null, 5, KEYS); // W2 lower 6 overlaps W1 through 9
        int before = delegate.calls();
        assertThat(keys(cache.rows(key(4), false, key(50), 8, KEYS)))
                .containsExactly("key-005", "key-006", "key-007", "key-008", "key-009",
                        "key-010", "key-011", "key-012");
        assertThat(delegate.calls()).isEqualTo(before);
        assertThat(meters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "hit").counter().count()).isGreaterThan(0);

        FakeListingStore narrowDelegate = store(100);
        ReplayMetrics narrowMeters = metrics();
        WindowedListingStore narrow = new WindowedListingStore(narrowDelegate, narrowMeters, 12, 8);
        narrow.rows(null, true, key(50), 2, KEYS);
        narrow.rows(key(1), false, key(50), 2, KEYS);
        narrow.rows(key(4), false, key(20), 2, KEYS); // hit seeds an exclusive anchor at key 6
        narrow.rows(key(6), false, key(20), 5, KEYS); // W2 now has surplus but ends at upper 20
        assertThat(narrowDelegate.calls()).isEqualTo(3);
        int narrowReads = narrowDelegate.calls();
        assertThat(keys(narrow.rows(key(4), false, key(50), 8, KEYS)))
                .containsExactly("key-005", "key-006", "key-007", "key-008", "key-009",
                        "key-010", "key-011", "key-012");
        assertThat(narrowDelegate.calls()).isEqualTo(narrowReads + 1);
        assertThat(narrowMeters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "upper_bound").counter().count()).isEqualTo(1);
    }

    @Test
    void joinDeclinesInsufficientNonFinalSecondWindow() {
        ReplayMetrics meters = metrics();
        FakeListingStore delegate = store(100);
        WindowedListingStore cache = new WindowedListingStore(delegate, meters, 12, 8);
        cache.rows(null, true, key(50), 2, KEYS);
        cache.rows(key(1), false, key(50), 2, KEYS); // W1 rows 2..9
        cache.rows(key(7), false, null, 1, KEYS); // cold key 8 under wider upper
        cache.rows(key(8), false, null, 1, KEYS); // W2 rows 9..12, non-final
        int before = delegate.calls();
        assertThat(keys(cache.rows(key(6), false, key(50), 10, KEYS))).hasSize(10);
        assertThat(delegate.calls()).isEqualTo(before + 1);
        assertThat(meters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "insufficient_second").counter().count()).isEqualTo(1);
    }

    @Test
    void joinedSecondWindowCanProveUpperBoundOrFinalShortTail() {
        FakeListingStore upperDelegate = store(100);
        WindowedListingStore upper = new WindowedListingStore(upperDelegate, metrics(), 12, 8);
        leadSmallWalkThroughSecondWindow(upper);
        int upperReads = upperDelegate.calls();
        assertThat(keys(upper.rows(key(6), false, key(12), 8, KEYS)))
                .containsExactly("key-007", "key-008", "key-009", "key-010", "key-011");
        assertThat(upperDelegate.calls()).isEqualTo(upperReads);

        FakeListingStore finalDelegate = store(13);
        WindowedListingStore finalCache = new WindowedListingStore(finalDelegate, metrics(), 12, 8);
        leadSmallWalkThroughSecondWindow(finalCache);
        int finalReads = finalDelegate.calls();
        assertThat(keys(finalCache.rows(key(6), false, null, 8, KEYS)))
                .containsExactly("key-007", "key-008", "key-009", "key-010", "key-011", "key-012");
        assertThat(finalDelegate.calls()).isEqualTo(finalReads);
    }

    @Test
    void joinedSecondWindowMayIncludeItsEqualLowerBoundary() {
        FakeListingStore delegate = store(19);
        WindowedListingStore cache = new WindowedListingStore(delegate, metrics(), 12, 8);
        cache.rows(null, true, key(50), 2, KEYS);
        cache.rows(key(1), false, key(50), 2, KEYS); // W1 rows 2..9
        cache.rows(key(9), true, null, 11, KEYS); // final W2 includes boundary 9
        int before = delegate.calls();
        assertThat(keys(cache.rows(key(6), false, key(50), 8, KEYS)))
                .containsExactly("key-007", "key-008", "key-009", "key-010", "key-011",
                        "key-012", "key-013", "key-014");
        assertThat(delegate.calls()).isEqualTo(before);
    }

    private static void leadSmallWalkThroughFirstWindow(WindowedListingStore cache) {
        ByteKey cursor = null;
        for (int i = 0; i < 5; i++) {
            List<ListedObject> page = cache.rows(cursor, cursor == null, null, 2, KEYS);
            cursor = ByteKey.copyOf(page.get(page.size() - 1).key());
        }
    }

    private static void leadSmallWalkThroughSecondWindow(WindowedListingStore cache) {
        ByteKey cursor = null;
        for (int i = 0; i < 6; i++) {
            List<ListedObject> page = cache.rows(cursor, cursor == null, null, 2, KEYS);
            cursor = ByteKey.copyOf(page.get(page.size() - 1).key());
        }
    }

    @Test
    void staggeredWalkersReuseBackingWorkAgainstUniqueInventory() {
        CountingStore aloneStore = new CountingStore(store(2400));
        WindowedListingStore alone = new WindowedListingStore(aloneStore, metrics(), 120, 32);
        List<String> unique = fullWalk(alone, 10);
        assertThat(unique).hasSize(2400).doesNotHaveDuplicates();

        for (int stagger = 1; stagger <= 3; stagger++) {
            CountingStore sharedStore = new CountingStore(store(2400));
            WindowedListingStore shared = new WindowedListingStore(sharedStore, metrics(), 120, 32);
            List<String> leader = new ArrayList<>();
            List<String> follower = new ArrayList<>();
            ByteKey leadCursor = null;
            ByteKey followCursor = null;
            // Establish a real lead, rather than starting both at the identical cold position.
            for (int i = 0; i < stagger; i++) {
                List<ListedObject> page = shared.rows(leadCursor, leadCursor == null, null, 10, KEYS);
                leader.addAll(keys(page));
                leadCursor = ByteKey.copyOf(page.get(page.size() - 1).key());
            }
            boolean leadDone = false;
            boolean followDone = false;
            while (!leadDone || !followDone) {
                if (!leadDone) {
                    List<ListedObject> page = shared.rows(leadCursor, false, null, 10, KEYS);
                    leadDone = page.isEmpty();
                    if (!leadDone) {
                        leader.addAll(keys(page));
                        leadCursor = ByteKey.copyOf(page.get(page.size() - 1).key());
                    }
                }
                if (!followDone) {
                    List<ListedObject> page = shared.rows(followCursor, followCursor == null, null, 10, KEYS);
                    followDone = page.isEmpty();
                    if (!followDone) {
                        follower.addAll(keys(page));
                        followCursor = ByteKey.copyOf(page.get(page.size() - 1).key());
                    }
                }
            }
            assertThat(leader).isEqualTo(unique);
            assertThat(follower).isEqualTo(unique);
            // Denominator is the one walker's unique inventory, never duplicated client output.
            assertThat(sharedStore.calls.get()).isLessThanOrEqualTo((long) Math.ceil(aloneStore.calls.get() * 1.1));
            assertThat(sharedStore.rows.get()).isLessThanOrEqualTo((long) Math.ceil(aloneStore.rows.get() * 1.1));
        }
    }

    @Test
    void leadingSmallPagesAndTrailingLargePagesShareUniqueInventoryWork() {
        CountingStore referenceStore = new CountingStore(store(2400));
        WindowedListingStore reference = new WindowedListingStore(referenceStore, metrics(), 125, 32);
        PagerWalker baseline = new PagerWalker(11);
        while (!baseline.done) baseline.step(reference);
        assertThat(baseline.keys).hasSize(2400).doesNotHaveDuplicates();

        ReplayMetrics meters = metrics();
        CountingStore sharedStore = new CountingStore(store(2400));
        WindowedListingStore shared = new WindowedListingStore(sharedStore, meters, 125, 32);
        PagerWalker leading = new PagerWalker(11);
        PagerWalker trailing = new PagerWalker(51);
        for (int i = 0; i < 25; i++) leading.step(shared);
        while (!leading.done || !trailing.done) {
            for (int i = 0; i < 5 && !leading.done; i++) leading.step(shared);
            if (!trailing.done) trailing.step(shared);
        }
        assertThat(leading.keys).isEqualTo(baseline.keys);
        assertThat(trailing.keys).isEqualTo(baseline.keys);
        assertThat(meters.registry().find("swath.replay.prefetch.window.join")
                .tag("reason", "hit").counter().count()).isGreaterThan(0);
        // Two clients each emit the inventory; denominator remains the ONE unique inventory walk.
        assertThat(sharedStore.calls.get()).isLessThanOrEqualTo((long) Math.ceil(referenceStore.calls.get() * 1.1));
        assertThat(sharedStore.rows.get()).isLessThanOrEqualTo((long) Math.ceil(referenceStore.rows.get() * 1.1));
    }

    private static final class PagerWalker {
        private final int lookaheadLimit;
        private final List<String> keys = new ArrayList<>();
        private ByteKey cursor;
        private boolean done;

        PagerWalker(int lookaheadLimit) { this.lookaheadLimit = lookaheadLimit; }

        void step(WindowedListingStore store) {
            if (done) return;
            List<ListedObject> rows = store.rows(cursor, cursor == null, null, lookaheadLimit, KEYS);
            int emitted = Math.min(rows.size(), lookaheadLimit - 1);
            keys.addAll(keys(rows.subList(0, emitted)));
            done = rows.size() < lookaheadLimit;
            if (!done) cursor = ByteKey.copyOf(rows.get(emitted - 1).key());
        }
    }

    private static List<String> fullWalk(WindowedListingStore store, int limit) {
        List<String> out = new ArrayList<>();
        ByteKey cursor = null;
        while (true) {
            List<ListedObject> page = store.rows(cursor, cursor == null, null, limit, KEYS);
            if (page.isEmpty()) return out;
            out.addAll(keys(page));
            cursor = ByteKey.copyOf(page.get(page.size() - 1).key());
        }
    }

    private static final class CountingStore implements ListingStore {
        private final FakeListingStore delegate;
        private final AtomicLong calls = new AtomicLong();
        private final AtomicLong rows = new AtomicLong();

        CountingStore(FakeListingStore delegate) { this.delegate = delegate; }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                       Projection projection) {
            List<ListedObject> result = delegate.rows(from, fromInclusive, toExclusive, limit, projection);
            calls.incrementAndGet();
            rows.addAndGet(result.size());
            return result;
        }

        @Override public void close() { delegate.close(); }
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
