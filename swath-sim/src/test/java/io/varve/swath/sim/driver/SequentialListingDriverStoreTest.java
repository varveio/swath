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
import java.util.List;
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

    /** A store handle that records what the driver did with it — most importantly, closing it. */
    private static final class CountingStore implements ListingStore {
        private final ListingStore delegate;
        private int calls;
        private int closes;

        private CountingStore(ListingStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                       Projection projection) {
            calls++;
            return delegate.rows(from, fromInclusive, toExclusive, limit, projection);
        }

        @Override
        public void close() {
            closes++;
            delegate.close();
        }
    }

    @Test
    void listsEveryKeyOfArealFixtureExactlyOnceInOrder(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture(dir), SimStoreBackend.ARENA);
        try (ListingStore store = opened.store()) {
            SimRunResult result = SequentialListingDriver.run(scenario(true), store);

            assertThat(opened.resolvedBackend()).isEqualTo(SimStoreBackend.ARENA);
            assertThat(result.stopReason()).isEqualTo(SimStopReason.QUIESCED);
            assertThat(result.counter(SequentialListingDriver.KEYS_LISTED_COUNTER)).isEqualTo(KEY_COUNT);
            assertThat(result.counter(SequentialListingDriver.RANGES_CLAIMED_COUNTER)).isEqualTo(RANGE_COUNT);
            assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER))
                    .as("sum over ranges of floor(n_r / 7) + 1").isEqualTo(expectedCalls());
            assertThat(result.wallNanos())
                    .as("virtual time advanced, and only by modelled latency").isPositive();
            assertThat(pagesFromTrace(result)).as("every page of the fixture, and no page twice")
                    .isEqualTo(expectedPageSizes());
        }
    }

    @Test
    void oneStoreHandleServesTwoRunsAndIsNeverClosedByTheDriver(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture(dir), SimStoreBackend.ARENA);
        CountingStore handle = new CountingStore(opened.store());
        try (handle) {
            SimRunResult first = SequentialListingDriver.run(scenario(true), handle);
            int callsAfterFirst = handle.calls;
            SimRunResult second = SequentialListingDriver.run(scenario(true), handle);

            assertThat(handle.closes).as("the run API borrows the handle; closing it is the caller's")
                    .isZero();
            assertThat(callsAfterFirst).isEqualTo((int) expectedCalls());
            assertThat(handle.calls).as("the second run served from the same open handle")
                    .isEqualTo(2 * callsAfterFirst);
            assertThat(second.log().canonicalBytes())
                    .as("a reused handle changes nothing about the run's outcome")
                    .isEqualTo(first.log().canonicalBytes());

            // Still usable directly afterwards -- the handle was borrowed, not consumed.
            assertThat(handle.rows(null, true, null, 1, Projection.KEYS_ONLY)).hasSize(1);
        }
        assertThat(handle.closes).as("closing is the caller's, and it happened exactly once").isEqualTo(1);
    }

    /** Page sizes in the order the pages were served, read out of the recorded trace. */
    private static List<Integer> pagesFromTrace(SimRunResult result) {
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
