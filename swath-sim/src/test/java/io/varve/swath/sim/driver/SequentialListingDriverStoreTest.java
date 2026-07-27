/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.kernel.SimStopReason;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.IidClientCost;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The kernel driven end to end against a real ground-truth store, and the store handle's lifecycle
 * across the runs of a sweep.
 *
 * <p>Two claims are made here that the in-memory tests cannot make. First, that the whole stack —
 * kernel, latency model, client-cost model, and a store resolved through the real factory — closes
 * the loop and lists a real fixture exactly once, in order, in virtual time. Second, and this is the
 * one with teeth for a sweep: <b>a store handle outlives a run</b>. Opening a store over a large
 * fixture is expensive enough to dominate a sweep if it were paid per run, so the run API takes an
 * open handle and does not close it — and running the same scenario twice against one handle must
 * neither reopen it nor produce a different answer the second time.
 */
class SequentialListingDriverStoreTest {

    private static final int KEY_COUNT = 40;
    private static final int PAGE_SIZE = 7;
    private static final int WORKERS = 3;
    private static final int RANGE_COUNT = 4;
    private static final long LATENCY_NANOS = 2_000_000L;

    /**
     * A store handle that records what the driver did with it: how many calls, whether it was closed,
     * and — for the key-coverage assertion — the keys each call returned, tagged by which range's
     * upper bound the call carried. Each range here has a distinct {@code toExclusive}, so that bound
     * identifies the range a call belongs to without the decorator needing to know about ranges.
     */
    private static final class RecordingStore implements ListingStore {
        private record Call(String rangeTag, List<String> keys) {
        }

        private final ListingStore delegate;
        private final List<Call> callLog = new ArrayList<>();
        private int closes;

        private RecordingStore(ListingStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                       Projection projection) {
            List<ListedObject> rows = delegate.rows(from, fromInclusive, toExclusive, limit, projection);
            callLog.add(new Call(toExclusive == null ? "*" : toExclusive.toString(),
                    rows.stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList()));
            return rows;
        }

        int calls() {
            return callLog.size();
        }

        /**
         * The keys this store served, per range, concatenated in the order the calls were made and
         * with the ranges themselves in ascending key order. Order is preserved throughout: an earlier
         * version of this test sorted the observations before comparing, which meant a driver that
         * walked a range backwards, or resumed one range's cursor inside another, still passed.
         */
        List<String> keysByRangeInCallOrder() {
            Map<String, List<String>> byRange = new LinkedHashMap<>();
            for (Call call : callLog) {
                byRange.computeIfAbsent(call.rangeTag(), tag -> new ArrayList<>()).addAll(call.keys());
            }
            return byRange.values().stream()
                    .sorted(Comparator.comparing(keys -> keys.isEmpty() ? "" : keys.getFirst()))
                    .flatMap(List::stream)
                    .toList();
        }

        @Override
        public void close() {
            closes++;
            delegate.close();
        }
    }

    /**
     * Every key of a real fixture, listed exactly once and in ascending order — asserted against the
     * keys themselves, not against page sizes. Page sizes alone cannot distinguish a correct run from
     * one that dropped key {@code j} and served key {@code k} twice, nor from one that swapped two
     * ranges' cursors; the key sequence can.
     */
    @Test
    void listsEveryKeyOfARealFixtureExactlyOnceInOrder(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture(dir), SimStoreBackend.ARENA);
        RecordingStore store = new RecordingStore(opened.store());
        try (store) {
            SimRunResult result = SequentialListingDriver.run(scenario(true), store);

            assertThat(opened.resolvedBackend()).isEqualTo(SimStoreBackend.ARENA);
            assertThat(result.stopReason()).isEqualTo(SimStopReason.QUIESCED);
            assertThat(store.keysByRangeInCallOrder())
                    .as("the fixture's whole key set, in ascending order, with nothing dropped, "
                            + "duplicated or reordered")
                    .isEqualTo(allFixtureKeys());
            assertThat(result.counter(SequentialListingDriver.KEYS_LISTED_COUNTER)).isEqualTo(KEY_COUNT);
            assertThat(result.counter(SequentialListingDriver.RANGES_CLAIMED_COUNTER)).isEqualTo(RANGE_COUNT);
            assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER))
                    .as("sum over ranges of floor(n_r / 7) + 1").isEqualTo(expectedCalls());
            assertThat(result.wallNanos())
                    .as("virtual time advanced, and only by modelled latency").isPositive();
            assertThat(pageSizesFromTrace(result)).as("and the page shape the closed form predicts")
                    .isEqualTo(expectedPageSizes());
        }
    }

    @Test
    void oneStoreHandleServesTwoRunsAndIsNeverClosedByTheDriver(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture(dir), SimStoreBackend.ARENA);
        RecordingStore handle = new RecordingStore(opened.store());
        try (handle) {
            SimRunResult first = SequentialListingDriver.run(scenario(true), handle);
            int callsAfterFirst = handle.calls();
            SimRunResult second = SequentialListingDriver.run(scenario(true), handle);

            assertThat(handle.closes).as("the run API borrows the handle; closing it is the caller's")
                    .isZero();
            assertThat(callsAfterFirst).isEqualTo((int) expectedCalls());
            assertThat(handle.calls()).as("the second run served from the same open handle")
                    .isEqualTo(2 * callsAfterFirst);
            assertThat(second.log().canonicalBytes())
                    .as("a reused handle changes nothing about the run's outcome")
                    .isEqualTo(first.log().canonicalBytes());

            // Still usable directly afterwards -- the handle was borrowed, not consumed.
            assertThat(handle.rows(null, true, null, 1, Projection.KEYS_ONLY)).hasSize(1);
        }
        assertThat(handle.closes).as("closing is the caller's, and it happened exactly once").isEqualTo(1);
    }

    /** Every key the fixture holds, in ascending order. */
    private static List<String> allFixtureKeys() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < KEY_COUNT; i++) {
            keys.add(new String(key(i), StandardCharsets.UTF_8));
        }
        return keys;
    }

    /** Page sizes in the order the pages were served, read out of the recorded trace. */
    private static List<Integer> pageSizesFromTrace(SimRunResult result) {
        List<Integer> pages = new ArrayList<>();
        for (var entry : result.log().entries()) {
            if (entry.kind().equals("list.rows")) {
                pages.add(Integer.parseInt(entry.detail().substring("rows=".length())));
            }
        }
        pages.sort(null);
        return pages;
    }

    /** The multiset of page sizes the four ranges must produce, sorted for comparison. */
    private static List<Integer> expectedPageSizes() {
        List<Integer> pages = new ArrayList<>();
        for (int size : rangeSizes()) {
            for (int remaining = size; remaining >= PAGE_SIZE; remaining -= PAGE_SIZE) {
                pages.add(PAGE_SIZE);
            }
            pages.add(size % PAGE_SIZE);
        }
        pages.sort(null);
        return pages;
    }

    private static long expectedCalls() {
        long calls = 0;
        for (int size : rangeSizes()) {
            calls += size / PAGE_SIZE + 1;
        }
        return calls;
    }

    /** {@link #RANGE_COUNT} contiguous ranges over {@link #KEY_COUNT} keys: 10, 10, 10, 10. */
    private static int[] rangeSizes() {
        int[] sizes = new int[RANGE_COUNT];
        for (int r = 0; r < RANGE_COUNT; r++) {
            sizes[r] = KEY_COUNT / RANGE_COUNT;
        }
        return sizes;
    }

    private static SimScenario scenario(boolean recordEventLog) {
        return new SimScenario(
                5L,
                WORKERS,
                PAGE_SIZE,
                ranges(),
                ConstantLatencyModel.uniform(LATENCY_NANOS),
                new IidClientCost(new ClientCostTerm(ClientCostTerm.Provenance.PROVISIONAL,
                        200_000L, 1_000L, "illustrative provisional term, this test only")),
                EngineTimeBudgets.engineDefaults(),
                recordEventLog,
                SimScenario.DEFAULT_MAX_EVENTS);
    }

    private static List<KeyRange> ranges() {
        int perRange = KEY_COUNT / RANGE_COUNT;
        List<KeyRange> ranges = new ArrayList<>();
        for (int r = 0; r < RANGE_COUNT; r++) {
            ByteKey from = r == 0 ? null : ByteKey.copyOf(key(r * perRange));
            ByteKey to = r == RANGE_COUNT - 1 ? null : ByteKey.copyOf(key((r + 1) * perRange));
            ranges.add(new KeyRange(from, to));
        }
        return ranges;
    }

    private static Path fixture(Path dir) throws IOException {
        Path capture = dir.resolve("part-0.parquet");
        try (var writer = ParquetFixtures.open(capture)) {
            for (int i = 0; i < KEY_COUNT; i++) {
                writer.write(ObjectEntries.withOwner(new String(key(i), StandardCharsets.UTF_8),
                        "etag-" + i));
            }
        }
        return capture;
    }

    private static byte[] key(int i) {
        return String.format("obj/%04d", i).getBytes(StandardCharsets.UTF_8);
    }
}
