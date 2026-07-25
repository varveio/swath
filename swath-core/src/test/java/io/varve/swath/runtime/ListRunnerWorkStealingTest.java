/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CancelledException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Mechanics test for the engine-backed runner paths ({@link ListRunner#runWorkStealing}
 * and {@link ListRunner#runToParquetWorkStealing}): a small deterministic keyspace over
 * {@link MockPageFetcher}, T=4, verifies every key is emitted exactly once and the run
 * is marked COMPLETED.
 *
 * <p>NOT the PROP-1/RES-3/CONC interleavings — those are covered separately.
 */
final class ListRunnerWorkStealingTest {

    private static RunKey textKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "ws-text-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static RunKey parquetKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "ws-parquet-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    @Test
    @Timeout(30)
    void textSink_emitsEveryKeyOnce_runsCompleted(@TempDir Path dir) throws Exception {
        List<byte[]> keys = Keyspaces.exactly(1500);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true,
                    8000, 1000, FilterChain.EMPTY, null, null);
            StringWriter out = new StringWriter();
            RunContext ctx = RunContext.create();

            ListingStatistics stats = new ListRunner().runWorkStealing(
                    ctx, fetcher, out, spec, store, run.id(), 4, seeds);

            String text = out.toString();
            String[] lines = text.split("\n");
            assertThat(lines).hasSize(keys.size());                     // every key emitted
            assertThat(stats.objects()).isEqualTo(keys.size());

            // Run must be COMPLETED (markRunFinished was called)
            List<Node> resumable = store.loadResumable(run.id(), false);
            assertThat(resumable).as("run is complete — no resumable nodes").isEmpty();
        }
    }

    @Test
    @Timeout(10)
    void textSink_brokenPipeCancelsSlowInFlightWorkers_runNotCompleted(@TempDir Path dir) throws Exception {
        List<byte[]> keys = Keyspaces.exactly(8000);
        AtomicInteger slowCallsStarted = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys)
                .interceptor((req, idx, page) -> {
                    if (idx > 0) {
                        slowCallsStarted.incrementAndGet();
                        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                    }
                    return page;
                })
                .build();

        Path db = dir.resolve("c.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true,
                    8000, 1000, FilterChain.EMPTY, null, null);
            BrokenPipeAfterRowsWriter out = new BrokenPipeAfterRowsWriter(5, slowCallsStarted);
            RunContext ctx = RunContext.create();

            ListingStatistics stats = assertTimeoutPreemptively(Duration.ofSeconds(3),
                    () -> new ListRunner().runWorkStealing(ctx, fetcher, out, spec, store, run.id(), 4, seeds));

            assertThat(stats.objects()).as("stdout was truncated by the simulated pipe close").isLessThan(5);
            assertThat(stats.objects()).as("statistics count only accepted stdout rows")
                    .isEqualTo(out.successfulRows());
            assertThat(counterTotal(ctx.meterRegistry(), "swath.entries.emitted"))
                    .as("entries.emitted must not include producer-side batches past the pipe cutoff")
                    .isEqualTo(out.successfulRows());
            assertThat(readRunStatus(db, run.id())).isEqualTo(RunStatus.FAILED);
            assertThat(store.loadResumable(run.id(), false))
                    .as("a truncated text run remains resumable")
                    .isNotEmpty();
        }
    }

    @Test
    @Timeout(10)
    void textSink_preCancelledBeforeClaimEscapesAsCancelledException_runNotCompleted(@TempDir Path dir)
            throws Exception {
        RunContext ctx = RunContext.create();
        ctx.cancellation().cancel();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2500))
                .build();

        Path db = dir.resolve("c.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true,
                    8000, 1000, FilterChain.EMPTY, null, null);
            StringWriter out = new StringWriter();

            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    ctx, fetcher, out, spec, store, run.id(), 4, seeds))
                    .isInstanceOf(CancelledException.class);

            assertThat(fetcher.apiCalls()).as("cancelled before workers claimed a seed").isZero();
            assertThat(readRunStatus(db, run.id())).isNotEqualTo(RunStatus.COMPLETED);
            assertThat(store.loadResumable(run.id(), false))
                    .as("a pre-cancelled text run remains resumable")
                    .isNotEmpty();
        }
    }

    @Test
    @Timeout(10)
    void textSink_userCancellationStillEscapesAsCancelledException(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2500))
                .interceptor((req, idx, page) -> {
                    if (idx == 0) {
                        ctx.cancellation().cancel();
                    }
                    return page;
                })
                .build();

        Path db = dir.resolve("c.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true,
                    8000, 1000, FilterChain.EMPTY, null, null);
            StringWriter out = new StringWriter();

            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    ctx, fetcher, out, spec, store, run.id(), 4, seeds))
                    .isInstanceOf(CancelledException.class);

            assertThat(readRunStatus(db, run.id())).isNotEqualTo(RunStatus.COMPLETED);
            assertThat(store.loadResumable(run.id(), false))
                    .as("an after-fetch cancelled text run remains resumable")
                    .isNotEmpty();
        }
    }

    @Test
    @Timeout(30)
    void parquetSink_emitsEveryKeyOnce_runsCompleted(@TempDir Path tmp) throws Exception {
        List<byte[]> keys = Keyspaces.exactly(1500);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(parquetKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            // numWriters=2, small part target to exercise rotation; argsHash ties manifest filename
            ListRunner.ParquetSpec spec = new ListRunner.ParquetSpec(
                    new byte[0], 8000, 1000, FilterChain.EMPTY, 2, 256L * 1024, 64, "ws-parquet-hash",
                            null, null, 0L, 0L, "");
            RunContext ctx = RunContext.create();

            new ListRunner().runToParquetWorkStealing(
                    ctx, fetcher, dir, spec, store, run.id(), 4, seeds, List.of());

            // Collect all keys from every part file
            List<String> emitted = new ArrayList<>();
            for (Path part : DatasetLayout.of(dir).dataParts()) {
                emitted.addAll(ParquetReads.keys(part));
            }

            assertThat(emitted).as("no duplicates").doesNotHaveDuplicates();
            assertThat(emitted).as("every key emitted").hasSize(keys.size());

            // Run must be COMPLETED
            List<Node> resumable = store.loadResumable(run.id(), true);
            assertThat(resumable).as("run is complete — no resumable nodes").isEmpty();
        }
    }

    private static RunStatus readRunStatus(Path db, long runId) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement("SELECT status FROM run_meta WHERE id=?")) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return RunStatus.valueOf(rs.getString(1));
            }
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

    private static final class BrokenPipeAfterRowsWriter extends Writer {
        private final int failAt;
        private final AtomicInteger slowCallsStarted;
        private int rows;
        private int successfulRows;

        private BrokenPipeAfterRowsWriter(int failAt, AtomicInteger slowCallsStarted) {
            this.failAt = failAt;
            this.slowCallsStarted = slowCallsStarted;
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            rows++;
            if (rows == failAt) {
                awaitSlowInFlightCall();
                throw new IOException("Broken pipe");
            }
            successfulRows++;
        }

        private int successfulRows() {
            return successfulRows;
        }

        private void awaitSlowInFlightCall() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (slowCallsStarted.get() == 0 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
