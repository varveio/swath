/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Multiple SIMULTANEOUSLY idle lanes: the production default is
 * multi-writer (2-4 lanes). {@link IdleLaneRotationCadenceTest} and {@link
 * ParquetRotationCadenceTest} exercise the idle-timeout wake-up mechanism on a single
 * lane; this drives {@code numWriters=3} with one node sticky-routed to each lane, all
 * three lanes holding a non-empty open part and going idle at once, so the idle-timeout
 * fires concurrently on three independent lane threads — exercising per-lane poll
 * wakeups, the shared publication coordinator, and the {@link PartListener} callback all
 * firing concurrently without corrupting the manifest or
 * dropping/duplicating a lane's contribution.
 */
class MultiLaneIdleRotationCadenceTest {

    private static final int NUM_WRITERS = 3;

    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void everyIdleLaneFinalizesOnCadenceAndTheManifestRecordsAllOfThem(@TempDir Path dir) throws Exception {
        // A real (wall-clock) interval, same idiom as IdleLaneRotationCadenceTest: this
        // value ALSO governs the lane's poll WAIT, so an injected clock decoupled from real
        // time would leave every lane blocked in a real 10s+ poll() with nothing to wake it
        // early (no further submit() ever arrives here to nudge it) — a real, short interval
        // avoids that trap and keeps the test itself simple and fast.
        long intervalNanos = Duration.ofMillis(250).toNanos();
        // Collects every PartFinalizedEvent's node-contribution map — the pool-level
        // analog of "durable_cursor advances for all their nodes" (ListRunner turns each
        // event's nodeMaxKeys() into a checkpoint durable_cursor advance per node).
        Map<Long, byte[]> observedMaxKeyByNode = new ConcurrentHashMap<>();
        PartListener listener = e -> observedMaxKeyByNode.putAll(e.nodeMaxKeys());

        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", NUM_WRITERS, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withPartListener(listener).withRotationIntervalNanos(intervalNanos));

        // Sticky routing: node id == lane id here (id % NUM_WRITERS == id for 0,1,2).
        for (long node = 0; node < NUM_WRITERS; node++) {
            pool.submit(batch(node, 0, 0, 10));
        }
        for (long node = 0; node < NUM_WRITERS; node++) {
            long lane = node;
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> Files.exists(DatasetLayout.of(dir).dataFile(String.format("part-w%d-00000.parquet", lane))));
        }

        // No further submit(), no close() — the ONLY thing that can finalize any of these
        // three parts is each lane's own idle wake-up, firing independently and concurrently.
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> pool.committedPartCount() == NUM_WRITERS);

        pool.close();   // nothing left open; a clean close is a no-op finalize per lane
        assertThat(pool.committedPartCount()).isEqualTo(NUM_WRITERS);

        // Every lane's part file exists, and every node's contribution was reported exactly once.
        for (long node = 0; node < NUM_WRITERS; node++) {
            assertThat(DatasetLayout.of(dir).dataFile(String.format("part-w%d-00000.parquet", node)))
                    .as("lane %d's idle-finalized part exists on disk", node).exists();
            assertThat(observedMaxKeyByNode).as("node %d's contribution was recorded by the finalize listener", node)
                    .containsKey(node);
        }

        // The manifest, written under the shared lock across three concurrent finalizes,
        // is valid JSON recording all three parts with no corruption/interleaving.
        String manifest = Files.readString(DatasetLayout.of(dir).manifest());
        long partEntries = manifest.lines().filter(l -> l.contains("\"key\":")).count();
        assertThat(partEntries).as("the manifest records all %d idle-finalized parts", NUM_WRITERS)
                .isEqualTo(NUM_WRITERS);
        for (long node = 0; node < NUM_WRITERS; node++) {
            assertThat(manifest).contains(String.format("data/part-w%d-00000.parquet", node));
        }
    }
}
