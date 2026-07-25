/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.ParquetWriterPool;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Guards the idle-lane cadence at the CHECKPOINT level, distinct
 * from {@code ParquetWriterPool}'s own mechanism tests ({@code
 * ParquetRotationCadenceTest}, {@code IdleLaneRotationCadenceTest}): the point
 * of the fix is not merely that a stray part rotates, but that a lane's own
 * idle wake-up makes progress <b>durable in the checkpoint</b> — {@code
 * durable_cursor} advances mid-run — so a crash during the idle window would
 * re-list only a bounded tail rather than the whole node. This drives a real
 * {@link ListRunner#runToParquetCheckpointed} through a {@link
 * SqliteCheckpointStore}, blocks the page fetcher mid-run (page 1 never
 * returns until released) so the single writer lane goes idle with an open,
 * non-empty part while the run is neither fed nor closed, and asserts the
 * checkpoint's {@code durable_cursor}/{@code part_file} state DURING that
 * window — never via a fixed sleep-then-assert-true, and never via {@link
 * SqliteCheckpointStore#loadResumable} (which mutates node state and would
 * corrupt the concurrently-running node).
 */
final class IdleLaneDurableCursorCadenceTest {

    private static final int TOTAL_KEYS = 20;
    private static final int PAGE_SIZE = 5;
    private static final Duration ROTATION_INTERVAL = Duration.ofMillis(300);

    private static List<byte[]> keys(int n) {
        List<byte[]> ks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ks.add(String.format("k%04d", i).getBytes(StandardCharsets.UTF_8));
        }
        return ks;
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "h1",
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static PageBatch warmupBatch() {
        List<ListEntry> entries = List.of(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("warmup/key"),
                100, 1_700_000_000_000_000L, "etag", "STANDARD", null, true, null, null));
        return new PageBatch(999, 0, entries);
    }

    /**
     * Same rationale as {@code ParquetRotationCadenceTest}/{@code IdleLaneRotationCadenceTest}:
     * the first {@code PartWriter} construction in the JVM (parquet-mr/Hadoop classloading +
     * ZSTD codec init) is slow enough to blow past {@link #ROTATION_INTERVAL} on its own. Warm
     * it up here, off the timed critical section below.
     */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        var warm = new ParquetWriterPool(warmupDir, ParquetSchema.canonical(), "warmup", 1, Long.MAX_VALUE, 4);
        warm.submit(warmupBatch());
        warm.close();
    }

    /**
     * A fetcher whose page {@code blockAtIdx} does not return until {@code release} counts
     * down — reproducing "lane has an open, non-empty part; no further batches arrive; the
     * run is not closed" deterministically, without a race against wall-clock timing.
     */
    private static MockPageFetcher idleAfterPage(List<byte[]> all, int blockAtIdx, CountDownLatch release) {
        return MockPageFetcher.builder().keys(all).maxKeysCap(PAGE_SIZE)
                .interceptor((req, idx, page) -> {
                    if (idx == blockAtIdx) {
                        release.await(20, TimeUnit.SECONDS);
                    }
                    return page;
                }).build();
    }

    /**
     * Reads {@code durable_cursor} via a fresh ad hoc connection (like {@link
     * SqliteCheckpointStore#readLatestRun}) rather than through the store's public API — every
     * store method that surfaces node state ({@code loadResumable}) also REOPENS the node
     * (reverts IN_PROGRESS to PENDING, bumps generation), which would corrupt the node the
     * background run is actively working on. This is a pure, non-mutating read.
     */
    private static byte[] readDurableCursor(Path dbPath, long nodeId) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath())) {
            try (Statement st = c.createStatement()) {
                st.execute("PRAGMA busy_timeout=5000");
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT durable_cursor FROM listing_node WHERE id = ?")) {
                ps.setLong(1, nodeId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getBytes(1);
                }
            }
        }
    }

    /**
     * The guard: with the idle-timeout cadence enabled, the lane's own wake-up finalizes its
     * stale part and the checkpoint commit advances {@code durable_cursor} — mid-run, while the
     * run is still blocked fetching page 1 and has not been closed.
     */
    @Test
    void idleLaneAdvancesDurableCursorMidRunWithoutCloseWhenCadenceEnabled(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(TOTAL_KEYS);
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path dbPath = tmp.resolve("c.sqlite");

        CountDownLatch release = new CountDownLatch(1);
        MockPageFetcher fetcher = idleAfterPage(all, 1, release);
        RunContext ctx = RunContext.create();
        ListRunner.ParquetSpec spec = new ListRunner.ParquetSpec(new byte[0], 1000, PAGE_SIZE, FilterChain.EMPTY,
                1, Long.MAX_VALUE, 8, "h1", null, null, 0L, 0L, "")
                        .withRotationIntervalNanos(ROTATION_INTERVAL.toNanos());

        AtomicReference<Throwable> bgFailure = new AtomicReference<>();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dbPath)) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();
            long nodeId = node.id();

            Thread runner = new Thread(() -> {
                try {
                    new ListRunner().runToParquetCheckpointed(
                            ctx, fetcher, dir, spec, store, run.id(), node, List.of());
                } catch (Throwable t) {
                    bgFailure.set(t);
                }
            }, "idle-lane-cadence-enabled-runner");
            runner.start();
            try {
                // Page 0's batch opened + wrote lane 0's part; the run is now blocked
                // fetching page 1 — the idle window — NOT closed.
                await().atMost(Duration.ofSeconds(5))
                        .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));

                await().atMost(Duration.ofSeconds(5))
                        .pollInterval(Duration.ofMillis(20))
                        .until(() -> !store.finalizedParts(run.id()).isEmpty());
                List<PartRef> finalized = store.finalizedParts(run.id());
                assertThat(finalized).as("exactly one part finalized from the idle wake-up").hasSize(1);
                assertThat(finalized.getFirst().rows()).isEqualTo(PAGE_SIZE);

                byte[] expectedMaxKey = all.get(PAGE_SIZE - 1);
                await().atMost(Duration.ofSeconds(5))
                        .pollInterval(Duration.ofMillis(20))
                        .until(() -> readDurableCursor(dbPath, nodeId) != null);
                assertThat(readDurableCursor(dbPath, nodeId))
                        .as("durable_cursor advances to the idle-finalized part's max key")
                        .isEqualTo(expectedMaxKey);

                assertThat(runner.isAlive())
                        .as("the run is still in progress (blocked on page 1, not closed) right now")
                        .isTrue();
            } finally {
                release.countDown();
                runner.join(Duration.ofSeconds(20).toMillis());
            }
            assertThat(runner.isAlive()).as("runner thread finished").isFalse();
            assertThat(bgFailure.get()).as("background run must not fail").isNull();

            // Sanity: the run completed cleanly; the final durable_cursor covers the whole run.
            assertThat(readDurableCursor(dbPath, nodeId)).isEqualTo(all.getLast());
        }
    }

    /**
     * Guard-bites companion: the exact same idle window, but with the cadence trigger DISABLED
     * ({@code rotationIntervalNanos=0}, huge {@code targetBytes}, no row-count trigger). {@code
     * durable_cursor} stays {@code NULL} and nothing finalizes for as long as the lane is idle;
     * only the eventual {@code close()} (after release) finalizes the part. This is what proves
     * the enabled test above is a real guard, not a vacuous pass: remove the idle-timeout fix
     * and this test's "stays null while idle" assertions are exactly what the enabled test's
     * "advances while idle" assertions would start failing with.
     */
    @Test
    void idleLaneDurableCursorStaysNullUntilCloseWhenCadenceDisabled(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(TOTAL_KEYS);
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path dbPath = tmp.resolve("c.sqlite");

        CountDownLatch release = new CountDownLatch(1);
        MockPageFetcher fetcher = idleAfterPage(all, 1, release);
        RunContext ctx = RunContext.create();
        // Cadence OFF: rotationIntervalNanos=0 (the lane blocks on take(), never wakes on a
        // timeout), rotationMaxRows=0, targetBytes never reachable.
        ListRunner.ParquetSpec spec = new ListRunner.ParquetSpec(new byte[0], 1000, PAGE_SIZE, FilterChain.EMPTY,
                1, Long.MAX_VALUE, 8, "h1", null, null, 0L, 0L, "");

        AtomicReference<Throwable> bgFailure = new AtomicReference<>();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dbPath)) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();
            long nodeId = node.id();

            Thread runner = new Thread(() -> {
                try {
                    new ListRunner().runToParquetCheckpointed(
                            ctx, fetcher, dir, spec, store, run.id(), node, List.of());
                } catch (Throwable t) {
                    bgFailure.set(t);
                }
            }, "idle-lane-cadence-disabled-runner");
            runner.start();
            try {
                await().atMost(Duration.ofSeconds(5))
                        .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));

                // Idle for several multiples of the interval the enabled test uses — long enough
                // that the idle-wake fix, if active, would already have finalized the part.
                Thread.sleep(ROTATION_INTERVAL.multipliedBy(5).toMillis());

                assertThat(store.finalizedParts(run.id()))
                        .as("cadence disabled: nothing finalizes during the idle window")
                        .isEmpty();
                assertThat(readDurableCursor(dbPath, nodeId))
                        .as("durable_cursor never advances while idle without the cadence trigger")
                        .isNull();
                assertThat(runner.isAlive()).isTrue();
            } finally {
                release.countDown();
                runner.join(Duration.ofSeconds(20).toMillis());
            }
            assertThat(runner.isAlive()).as("runner thread finished").isFalse();
            assertThat(bgFailure.get()).as("background run must not fail").isNull();

            // Only the eventual close() (after release) finalizes the lane's part.
            assertThat(store.finalizedParts(run.id())).isNotEmpty();
            assertThat(readDurableCursor(dbPath, nodeId)).isEqualTo(all.getLast());
        }
    }
}
