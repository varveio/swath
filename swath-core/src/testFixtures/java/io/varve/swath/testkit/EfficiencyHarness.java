/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.SeedMode;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The efficiency half of the engine test matrix (the generality stress-test matrix), sibling
 * to the correctness-focused {@link EngineHarness}. Seeds the DEFAULT production path
 * (SHALLOW, no scatter-scout), drives the full {@link WorkStealingScan} with a fixed per-page
 * latency so fan-out is observable, and records peak/sustained in-flight + the over-fetch /
 * empty-probe / split / steal-reason signals from {@link RunMetrics} so post-hoc analysis can
 * classify each shape from the metrics alone.
 *
 * <p>Correctness of an efficiency run is asserted with {@link
 * EngineHarness#assertExactlyOnce(List, List)} — this harness owns only the efficiency signals.
 */
public final class EfficiencyHarness {

    private EfficiencyHarness() {
    }

    /** Everything the invariant battery needs for one shape run. */
    public record Result(
            List<byte[]> emitted,
            int workers,
            long keyCount,
            long pages,
            long totalApiCalls,
            long emptyLists,
            int peakInFlight,
            double sustainedFraction,   // fraction of body samples with in-flight >= T/2; -1 = N/A
            long timeToHalfMs,          // wall-ms to first reach T/2 in-flight; -1 = never reached
            long splits,
            long probeFetches,
            long emptyUpperBisections,
            Map<String, Long> stealReasons,
            RunSummary.ShapeSummary shape,
            RunSummary.TrajectorySummary trajectory) {
    }

    /**
     * Seed {@code keyspace} via {@code SHALLOW} then drive the full {@link WorkStealingScan}
     * with {@code workers} workers, {@code maxKeys}-page bulk listings, and a fixed
     * {@code pageLatency} on every worker (recursive, non-delimiter) fetch. Samples in-flight
     * concurrency over the run and reads the efficiency signals back from {@link RunMetrics}.
     */
    public static Result run(List<byte[]> keyspace, int workers, int maxKeys,
            Duration pageLatency, Path ckptDir) throws Exception {
        int halfT = Math.max(1, (workers + 1) / 2);
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                // Latency only on worker bulk listings (recursive, maxKeys>1, no delimiter). Seed/thief
                // probes stay instant so the exposed concurrency is genuine worker fan-out.
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? pageLatency : Duration.ZERO)
                .build();

        long distinct;
        {
            TreeSet<byte[]> d = new TreeSet<>(KeyBytes::compareUnsigned);
            d.addAll(keyspace);
            distinct = d.size();
        }
        long pages = Math.max(1L, (long) Math.ceil((double) distinct / 1000.0));

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>((int) Math.min(Integer.MAX_VALUE, distinct));
        InFlightFetcher fetcher;
        Sampler sampler;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(EngineHarness.harnessRunKey(), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, new byte[0], workers, metrics)
                    .seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            List<Node> seeds = store.loadResumable(run.id(), false);

            fetcher = new InFlightFetcher(mock, halfT);
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            sampler = new Sampler(fetcher);
            sampler.start();
            PipelineDrain.collectKeys(5000, engine, emitted);
            sampler.stopAndJoin();
        }

        double sustained = sampler.sustainedFraction(halfT, pages, workers);
        RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);
        RunSummary fullSummary = metrics.summary(Duration.ZERO, "WORK_STEALING", 0L, 0L);
        return new Result(emitted, workers, distinct, pages,
                fetcher.totalLists(), fetcher.emptyLists(), fetcher.peak(),
                sustained, fetcher.timeToHalfMs(), diag.splitsCommitted(),
                diag.probeFetches(), diag.emptyUpperBisections(), diag.stealReasons(),
                fullSummary.shape(), fullSummary.trajectory());
    }

    /**
     * Wraps the mock: tracks in-flight concurrency over worker BULK fetches (maxKeys&gt;1, no
     * delimiter) — the fan-out signal — and counts total LISTs plus zero-result LISTs (the
     * empty-probe signal). Thief probes (maxKeys==1) and structure probes (delimiter) count
     * toward totals but never toward the in-flight peak.
     */
    private static final class InFlightFetcher implements PageFetcher {
        private final PageFetcher delegate;
        private final int halfT;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicLong totalLists = new AtomicLong();
        private final AtomicLong emptyLists = new AtomicLong();
        private final AtomicLong firstHalfNanos = new AtomicLong(-1L);
        private final long startNanos = System.nanoTime();

        InFlightFetcher(PageFetcher delegate, int halfT) {
            this.delegate = delegate;
            this.halfT = halfT;
        }

        @Override
        public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
            totalLists.incrementAndGet();
            boolean bulk = req.maxKeys() > 1 && req.delimiter() == null;
            if (bulk) {
                int now = inFlight.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                if (now >= halfT) {
                    firstHalfNanos.compareAndSet(-1L, System.nanoTime() - startNanos);
                }
            }
            try {
                ListPage page = delegate.fetchPage(req);
                if (page.entries().isEmpty() && page.commonPrefixes().isEmpty()) {
                    emptyLists.incrementAndGet();
                }
                return page;
            } finally {
                if (bulk) {
                    inFlight.decrementAndGet();
                }
            }
        }

        @Override
        public StoreCapabilities capabilities() {
            return delegate.capabilities();
        }

        int currentInFlight() {
            return inFlight.get();
        }

        int peak() {
            return peak.get();
        }

        long totalLists() {
            return totalLists.get();
        }

        long emptyLists() {
            return emptyLists.get();
        }

        long timeToHalfMs() {
            long n = firstHalfNanos.get();
            return n < 0 ? -1L : Duration.ofNanos(n).toMillis();
        }
    }

    /** Background 1ms in-flight sampler: the occupancy time-series behind (ii) "sustained". */
    private static final class Sampler extends Thread {
        private final InFlightFetcher fetcher;
        private final List<Integer> samples = new ArrayList<>(4096);
        private volatile boolean running = true;

        Sampler(InFlightFetcher fetcher) {
            super("efficiency-sampler");
            setDaemon(true);
            this.fetcher = fetcher;
        }

        @Override
        public void run() {
            while (running) {
                samples.add(fetcher.currentInFlight());
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        void stopAndJoin() throws InterruptedException {
            running = false;
            join(5000);
        }

        /**
         * Fraction of BODY samples with in-flight &ge; {@code halfT}. The body drops the first
         * 10% (warmup ramp) and the last {@code 2T/pages} fraction (the legitimate drain tail,
         * where remaining mass &le; ~2T pages so fan-out is expected to shrink). Returns -1 when
         * the keyspace is too small for sustained fan-out to be meaningful (pages &le; 2T).
         */
        double sustainedFraction(int halfT, long pages, int workers) {
            int n = samples.size();
            if (n == 0 || pages <= 2L * workers) {
                return -1.0;
            }
            double tailFrac = Math.min(0.9, (2.0 * workers) / pages);
            int from = (int) (n * 0.10);
            int to = (int) (n * (1.0 - tailFrac));
            if (to <= from) {
                return -1.0;
            }
            int atOrAbove = 0;
            for (int i = from; i < to; i++) {
                if (samples.get(i) >= halfT) {
                    atOrAbove++;
                }
            }
            return (double) atOrAbove / (to - from);
        }
    }
}
