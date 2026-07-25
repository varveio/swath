/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression guard for the transient-unsplittable serialization bug: idle workers
 * see the root seed while its slow first page is still in flight. The correct
 * behavior is to retry that unstarted frontier, then split once the seed commits
 * page 1; a regressed thief would cache it unsplittable and the scan would stay
 * serial.
 */
final class SlowFirstPageParallelizesTest {

    private static final int OBJECTS = 2048;
    private static final int WORKERS = 8;
    private static final int MAX_KEYS = 64;
    private static final Duration SLOW_FIRST_PAGE = Duration.ofMillis(75);
    private static final Duration OBSERVATION_DELAY = Duration.ofMillis(1);

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "slow-first-page-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(60)
    void slowFirstPageStillParallelizesAndEmitsEveryKeyOnce(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .slowFirstPage(SLOW_FIRST_PAGE)
                .build();
        InstrumentedFetcher fetcher = new InstrumentedFetcher(mock, OBSERVATION_DELAY);

        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(1);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(1000, engine, emitted);
        }

        EngineHarness.assertExactlyOnce(emitted, keyspace);

        int peak = fetcher.peakInFlight();
        Map<Long, Long> perWorker = fetcher.bulkFetchesByThread();
        long totalBulk = perWorker.values().stream().mapToLong(Long::longValue).sum();
        long maxOneWorker = perWorker.values().stream().mapToLong(Long::longValue).max().orElse(0);
        double maxShare = (double) maxOneWorker / totalBulk;

        assertThat(peak)
                .as("engine ran bulk fetches concurrently after a slow seed page; a serialized "
                        + "regression would peak at 1")
                .isGreaterThan(1);
        assertThat(perWorker.size())
                .as("work spread across multiple workers after the slow first page; workers=%s",
                        perWorker)
                .isGreaterThanOrEqualTo(3);
        assertThat(maxShare)
                .as("no single worker did most of the bulk listings (max share %.2f of %d fetches)",
                        maxShare, totalBulk)
                .isLessThan(0.5);
    }

    /**
     * Counts only worker bulk pages. Thief probes use maxKeys == 1, so they are
     * deliberately excluded from the peak and per-worker distribution.
     */
    private static final class InstrumentedFetcher implements PageFetcher {

        private final PageFetcher delegate;
        private final Duration observationDelay;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final ConcurrentHashMap<Long, LongAdder> byThread = new ConcurrentHashMap<>();

        private InstrumentedFetcher(PageFetcher delegate, Duration observationDelay) {
            this.delegate = delegate;
            this.observationDelay = observationDelay;
        }

        @Override
        public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
            boolean bulk = req.maxKeys() > 1;
            if (!bulk) {
                return delegate.fetchPage(req);
            }
            byThread.computeIfAbsent(Thread.currentThread().threadId(), k -> new LongAdder()).increment();
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                sleep(observationDelay);
                return delegate.fetchPage(req);
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public StoreCapabilities capabilities() {
            return delegate.capabilities();
        }

        int peakInFlight() {
            return peak.get();
        }

        Map<Long, Long> bulkFetchesByThread() {
            Map<Long, Long> out = new HashMap<>();
            byThread.forEach((k, v) -> out.put(k, v.sum()));
            return out;
        }

        private static void sleep(Duration delay) throws InterruptedException {
            if (delay.isZero()) {
                return;
            }
            long millis = delay.toMillis();
            int nanos = delay.minusMillis(millis).getNano();
            Thread.sleep(millis, nanos);
        }
    }
}
