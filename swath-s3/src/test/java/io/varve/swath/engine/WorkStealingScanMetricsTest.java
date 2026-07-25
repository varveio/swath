/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.JsonlFormatter;
import io.varve.swath.output.OutputStage;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.s3.FakeS3Client;
import io.varve.swath.store.s3.S3ExceptionFixtures;
import io.varve.swath.store.s3.S3PageFetcher;
import io.varve.swath.store.s3.S3PageFetcherConfig;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

final class WorkStealingScanMetricsTest {

    private static final int SIBLINGS = 8;
    private static final int WORKERS = 16;
    private static final int MAX_KEYS = 4;

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "metrics-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(30)
    void registryCountersAndInflightGaugeMoveOnMockRun(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = wideKeyspace();
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (idx >= SIBLINGS && req.maxKeys() == MAX_KEYS) {
                        awaitSplit(ctx.meterRegistry());
                    }
                    Thread.sleep(25);
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("metrics.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            for (int s = 0; s < SIBLINGS; s++) {
                byte[] lo = (s == 0) ? null : cut(s);
                byte[] hi = (s == SIBLINGS - 1) ? null : cut(s + 1);
                store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            }
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            new Pipeline<PageBatch>(1000).run(ctx, engine, output);
            long emittedRows = out.toString().lines().count();

            MeterRegistry registry = ctx.meterRegistry();
            assertThat(emittedRows).isEqualTo(keyspace.size());
            assertThat(counterTotal(registry, "swath.entries.emitted")).isEqualTo(emittedRows);
            assertThat(registry.getMeters().stream()
                    .filter(m -> m.getId().getName().equals("swath.steals"))
                    .allMatch(m -> m.getId().getTag("result") != null)).isTrue();
            assertThat(registry.get("swath.queue.wait").timer().count()).isGreaterThan(0L);
            assertThat(registry.get("swath.rate_limit.wait").timer().count()).isGreaterThan(0L);
            assertThat(ctx.metrics().peakInFlight()).isGreaterThan(1L);
            // Slow-start ramps T up from SLOW_START_INITIAL_T by paced doubling (≤1 step/s) rather
            // than starting at Tmax, so a fast in-memory clean run need not have reached WORKERS by
            // the time it drains. A CLEAN run only ever GROWS T (never below the slow-start floor,
            // never above Tmax), with no permit leak (workers.active tracks effective T).
            assertThat(registry.get("swath.workers.active").gauge().value())
                    .as("clean run: T ramps up from the slow-start floor toward Tmax, never shrinks")
                    .isBetween((double) ConcurrencyGauge.SLOW_START_INITIAL_T, (double) WORKERS);
            assertThat(registry.find("swath.splits").meter()).isNull();
            assertThat(registry.find("swath.pivots.unsplittable").meter()).isNull();
            assertThat(registry.find("swath.pages").meter()).isNull();
            assertThat(registry.find("swath.keys.per.page").meter()).isNull();
            assertThat(registry.find("swath.worker.keys").meter()).isNull();
            assertThat(registry.find("swath.inflight").meter()).isNull();
            assertThat(registry.find("swath.outstanding").meter()).isNull();
            assertThat(registry.find("swath.s3.requests").meter()).isNull();
            assertThat(registry.find("swath.s3.page.latency").meter()).isNull();
            assertThat(registry.find("swath.s3.errors").meter()).isNull();
            assertThat(registry.find("swath.concurrency.target").meter()).isNull();
        }
    }

    @Test
    @Timeout(30)
    void realS3PageFetcherThrottleRetriesCountAttemptsAndOneErrorPerThrottle(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        S3PageFetcher fetcher = new S3PageFetcher(slowDownThenSucceed(2), "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(ctx.metrics()));

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("s3-throttle-metrics.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, 1, 1000, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = new ArrayList<>();
            PipelineDrain.collectKeys(1000, ctx, engine, emitted);

            MeterRegistry registry = ctx.meterRegistry();
            assertThat(emitted).containsExactly("a".getBytes(StandardCharsets.UTF_8),
                    "b".getBytes(StandardCharsets.UTF_8));
            assertThat(counterTotal(registry, "swath.api.calls", "strategy", "WORK_STEALING"))
                    .isEqualTo(3.0);
            assertThat(counterTotal(registry, "swath.errors", "type", "throttle"))
                    .isEqualTo(2.0);
            assertThat(registry.get("swath.api.latency").tag("op", "listObjectsV2").timer().count())
                    .isEqualTo(3L);
            assertThat(registry.get("swath.workers.active").gauge().value())
                    .as("workers.active is the AIMD target T, reduced by throttles")
                    .isEqualTo(1.0);
        }
    }

    @Test
    @Timeout(30)
    void thrownThrottleFromFetcherStillReducesWorkersActiveT(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 240; i++) {
            keyspace.add(("k%04d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor(new ThrottleThrowingInterceptor(10))
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("throttle-metrics.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, 6, 12, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = new ArrayList<>();
            PipelineDrain.collectKeys(1000, ctx, engine, emitted);

            MeterRegistry registry = ctx.meterRegistry();
            assertThat(emitted).hasSize(keyspace.size());
            assertThat(registry.get("swath.workers.active").gauge().value())
                    .as("workers.active is the AIMD target T, reduced by throttles")
                    .isLessThan(6.0);
        }
    }

    private static List<byte[]> wideKeyspace() {
        List<byte[]> keys = new ArrayList<>(127);
        for (int s = 0; s < SIBLINGS; s++) {
            int first = s * 16 + 1;
            int last = (s == SIBLINGS - 1) ? 127 : (s + 1) * 16;
            for (int k = first; k <= last; k++) {
                keys.add(new byte[]{(byte) k});
            }
        }
        return keys;
    }

    private static byte[] cut(int i) {
        return new byte[]{(byte) (i * 16)};
    }

    private static void awaitSplit(MeterRegistry registry) throws InterruptedException {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (counterTotal(registry, "swath.steals", "result", "CHILD_CREATED") == 0.0
                && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private static double counterTotal(MeterRegistry registry, String name) {
        double total = 0.0;
        for (Meter meter : registry.getMeters()) {
            if (!meter.getId().getName().equals(name)) {
                continue;
            }
            for (var measurement : meter.measure()) {
                if (measurement.getStatistic() == Statistic.COUNT) {
                    total += measurement.getValue();
                }
            }
        }
        return total;
    }

    private static double counterTotal(MeterRegistry registry, String name, String tagKey, String tagValue) {
        double total = 0.0;
        for (Meter meter : registry.getMeters()) {
            if (!meter.getId().getName().equals(name) || !tagValue.equals(meter.getId().getTag(tagKey))) {
                continue;
            }
            for (var measurement : meter.measure()) {
                if (measurement.getStatistic() == Statistic.COUNT) {
                    total += measurement.getValue();
                }
            }
        }
        return total;
    }

    private record ThrottleThrowingInterceptor(int throttleCalls) implements MockPageFetcher.PageInterceptor {
        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            if (callIndex < throttleCalls) {
                throw ThrottleException.slowDown("injected SlowDown (call " + callIndex + ")");
            }
            return computed;
        }
    }

    /** Throws {@code slowDowns} SlowDown faults, then always returns a fixed 2-object page. */
    private static FakeS3Client slowDownThenSucceed(int slowDowns) {
        ListObjectsV2Response page = ListObjectsV2Response.builder()
                .contents(
                        S3Object.builder().key("a").size(1L).lastModified(Instant.EPOCH).build(),
                        S3Object.builder().key("b").size(1L).lastModified(Instant.EPOCH).build())
                .isTruncated(false)
                .build();
        FakeS3Client.Builder builder = FakeS3Client.builder();
        for (int i = 0; i < slowDowns; i++) {
            builder.thenThrow(S3ExceptionFixtures.s3Exception(503, "SlowDown"));
        }
        return builder.then(page).build();
    }
}
