/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.JsonlFormatter;
import io.varve.swath.output.OutputStage;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code IdleStealBackoff} engagement counters:
 * {@code swath.idle_backoff.{level,resets,slot_denied,park_time}}. A single seeded range with
 * far more workers than victims forces a burst of idle workers to race the shared backoff's
 * one-attempt-slot gate at start-up (deterministic {@code slot_denied}); the slow interceptor
 * keeps the range busy long enough for non-productive steal attempts (raising {@code level}) to
 * be followed by a productive split (a {@code reset}, since the level was {@code >0}); nothing
 * claimable in between means every idle worker also parks at least once ({@code park_time}).
 */
final class IdleBackoffMetricsTest {

    private static final int WORKERS = 8;
    private static final int MAX_KEYS = 2;

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "idle-backoff-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(30)
    void manyIdleWorkersAgainstOneVictimDriveBackoffMeters(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            keyspace.add(("k%04d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    Thread.sleep(5);
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("idle-backoff.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            new Pipeline<PageBatch>(1000).run(ctx, engine, output);
            long emittedRows = out.toString().lines().count();
            assertThat(emittedRows).isEqualTo(keyspace.size());

            MeterRegistry registry = ctx.meterRegistry();
            assertThat(registry.get("swath.idle_backoff.slot_denied").counter().count())
                    .as("a burst of idle workers should race the shared one-attempt-slot gate")
                    .isGreaterThan(0.0);
            assertThat(registry.get("swath.idle_backoff.resets").counter().count())
                    .as("a productive CHILD_CREATED after a non-productive streak resets the level")
                    .isGreaterThan(0.0);
            assertThat(registry.get("swath.idle_backoff.park_time").timer().count())
                    .as("an idle worker with nothing claimable and no attempt slot parks")
                    .isGreaterThan(0L);
            // Live gauge; by run end (quiescence) every worker has terminated, so 0 is expected —
            // just assert it is registered and never negative.
            assertThat(registry.get("swath.idle_backoff.level").gauge().value()).isGreaterThanOrEqualTo(0.0);
        }
    }
}
