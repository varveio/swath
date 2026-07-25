/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import static io.varve.swath.checkpoint.CheckpointStoreTestSupport.b;
import static io.varve.swath.checkpoint.CheckpointStoreTestSupport.key;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.MeterRegistry;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.runtime.RunContext;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Checkpoint/resume layer instrumentation: commit latency/batch-size, the writer
 * queue-depth gauge/queue-wait timer, and the {@code RESUME.*} resume-engagement counters
 * ({@code nodes_reopened}, {@code durable_cursor_lag}, {@code args_hash_refused}). A store
 * opened via the metrics-less {@link SqliteCheckpointStore#open(Path)} stays null-safe (no
 * meters registered, no NPE) — asserted separately below.
 */
final class SqliteCheckpointStoreMetricsTest {

    private static RunKey keyParquet(String argsHash) {
        return new RunKey("s3", null, "bucket", b("p/"), argsHash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    @Test
    void commitLatencyAndQueueMetersFireOnCommitPage(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store =
                     SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"), ctx.metrics())) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k1"), false));

            MeterRegistry registry = ctx.meterRegistry();
            assertThat(registry.get("swath.checkpoint.commit.latency").timer().count())
                    .isGreaterThan(0L);
            assertThat(registry.get("swath.checkpoint.commit_batch_size").summary().count())
                    .isGreaterThan(0L);
            assertThat(registry.get("swath.checkpoint.queue.wait").timer().count())
                    .isGreaterThan(0L);
            assertThat(registry.get("swath.checkpoint.queue.depth").gauge().value())
                    .as("no task in flight once commitPage() has returned")
                    .isEqualTo(0.0);
        }
    }

    @Test
    void noMetricsAttached_openIsNullSafe(@TempDir Path dir) throws Exception {
        // The metrics-less overload — every production test that doesn't care about
        // observability uses this; instrumentation must never NPE when metrics is absent.
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k1"), false));
            store.splitNode(new SplitSpec(run.id(), node, b("m"), null));
            store.loadResumable(run.id(), true);
        }
    }

    @Test
    void nodesReopened_firesOnlyOnGenuineResume_notOnFreshSeed(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        Path path = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(path, ctx.metrics())) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            // Fresh-seed-then-immediately-load pattern (ListCommand always does this): no crash,
            // no genuine resume — nodes_reopened must stay at 0.
            store.loadResumable(run.id(), false);
            assertThat(ctx.meterRegistry().find("swath.steal_reason")
                    .tags("outcome", "RESUME", "reason", "nodes_reopened").counter()).isNull();

            // Claim it (IN_PROGRESS, one committed page) to model a crash mid-listing.
            store.commitPage(new PageCommit(node, b("k1"), false));
        }

        // Reopen the same DB (a genuine resume): the IN_PROGRESS node is reverted to PENDING.
        RunContext resumeCtx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(path, resumeCtx.metrics())) {
            RunMeta resumed = store.openRun(key("h1"), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<Node> nodes = store.loadResumable(resumed.id(), false);
            assertThat(nodes).hasSize(1);

            assertThat(resumeCtx.meterRegistry().get("swath.steal_reason")
                    .tags("outcome", "RESUME", "reason", "nodes_reopened").counter().count())
                    .isEqualTo(1.0);
        }
    }

    @Test
    void durableCursorLag_firesOnFileSinkResumeWithNonDurableTail(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(path)) {
            RunMeta run = store.openRun(keyParquet("h1"), false, false);
            long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));
            // Committed but never finalized into a part: cursor advances past durable_cursor (null).
            store.commitPage(new PageCommit(nodeId, b("k1"), false));
        }

        RunContext resumeCtx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(path, resumeCtx.metrics())) {
            RunMeta resumed = store.openRun(keyParquet("h1"), true, false);
            List<Node> nodes = store.loadResumable(resumed.id(), true);   // file sink
            assertThat(nodes).hasSize(1);
            // File-sink reopen resets cursor := durable_cursor (COALESCE'd to range_start, i.e. null).
            assertThat(nodes.getFirst().cursor()).isNull();

            assertThat(resumeCtx.meterRegistry().get("swath.steal_reason")
                    .tags("outcome", "RESUME", "reason", "durable_cursor_lag").counter().count())
                    .isEqualTo(1.0);
            assertThat(resumeCtx.meterRegistry().get("swath.steal_reason")
                    .tags("outcome", "RESUME", "reason", "nodes_reopened").counter().count())
                    .isEqualTo(1.0);
        }
    }

    @Test
    void argsHashRefused_recordsCounterAndThrows(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(path)) {
            store.openRun(key("h1"), false, false);
        }

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(path, ctx.metrics())) {
            assertThatThrownBy(() -> store.openRun(key("h2-different"), true, false))
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("--resume refused");
            assertThat(ctx.meterRegistry().get("swath.steal_reason")
                    .tags("outcome", "RESUME", "reason", "args_hash_refused").counter().count())
                    .isEqualTo(1.0);
        }
    }
}
