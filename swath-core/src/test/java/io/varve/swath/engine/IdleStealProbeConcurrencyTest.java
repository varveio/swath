/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * CONC guard at engine scale: across a live work-stealing scan, the number of <b>concurrent</b>
 * speculative probe fetches never exceeds one — {@link IdleStealBackoff}'s fleet-wide bound as
 * observed at the store, not as claimed by the class javadoc.
 *
 * <p>This is the shape the leak needed and the existing suite could not see. {@code
 * NarrowTailProbeVsWorkerAttributionTest} asserts probe <i>pressure</i> under a storm where
 * {@code eligibleVictims()} is empty or probes fail fast; it never measures overlap, so a slot that
 * {@code reset()} handed away mid-attempt went unnoticed. Here every worker commits pages
 * continuously — each commit is a {@code reset()} from a worker that does not own the slot — while
 * probes are held open long enough that any handover shows up as two probes in flight at once.
 *
 * <p>Only the overlap method is {@code deep} — it is latency-injecting (probes sleep to open the
 * overlap window) and races real worker threads, so it is schedule-sensitive by construction; its
 * contract line is pinned per-commit by the deterministic {@link IdleStealSlotOwnershipTest}
 * (TESTING.md § tag convention). The release-on-throw method injects no latency and aborts at the
 * first probe, so it stays per-commit.
 */
final class IdleStealProbeConcurrencyTest {

    private static final int WORKERS = 16;
    private static final int MAX_KEYS = 4;
    private static final int KEYS = 4_000;
    /** Long enough that a handed-away slot produces an overlap, short enough to keep the run brisk. */
    private static final long PROBE_HOLD_MILLIS = 25L;

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "probe-concurrency-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** The thief's speculative calls: a {@code max_keys<=1} pivot probe or a delimited structure probe. */
    private static boolean isProbe(PageRequest req) {
        return req.maxKeys() <= 1 || (req.delimiter() != null && req.delimiter().length > 0);
    }

    @Test
    @Tag("deep")
    @Timeout(120)
    void stealProbeFetchesNeverOverlapUnderCommitDrivenResets(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            keyspace.add(("k%05d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        AtomicInteger inFlightProbes = new AtomicInteger();
        AtomicInteger maxConcurrentProbes = new AtomicInteger();
        AtomicInteger totalProbes = new AtomicInteger();

        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (!isProbe(req)) {
                        Thread.sleep(1);   // keep workers committing pages (each commit is a reset)
                        return page;
                    }
                    totalProbes.incrementAndGet();
                    int now = inFlightProbes.incrementAndGet();
                    maxConcurrentProbes.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(PROBE_HOLD_MILLIS);
                    } finally {
                        inFlightProbes.decrementAndGet();
                    }
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("probe-concurrency.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            new Pipeline<PageBatch>(1000).run(ctx, engine, output);

            assertThat(out.toString().lines().count())
                    .as("exactly-once emission is unaffected by the slot policy")
                    .isEqualTo(keyspace.size());
        }

        assertThat(totalProbes.get())
                .as("the fixture must actually exercise the steal path, or the bound is untested")
                .isGreaterThan(0);
        assertThat(maxConcurrentProbes.get())
                .as("at most one speculative steal attempt in flight fleet-wide (%d probes seen)",
                        totalProbes.get())
                .isEqualTo(1);
    }

    /**
     * The release is unconditional. An <b>unchecked</b> throw from inside the acquired region —
     * modelled here at the probe fetch, but equally reachable from the metrics, logging,
     * {@code eligibleVictims()} or the child enqueue that share that region — must still hand the
     * slot back, or stealing is dead for the rest of the run.
     *
     * <p>Fails against a release wired only to the productive {@code CHILD_CREATED} path (the shape
     * the prototype for #3 had): there the throw skips the release entirely and the slot stays
     * owned by a worker that no longer exists. Fast and deterministic, so it stays per-commit.
     */
    @Test
    @Timeout(60)
    void anUncheckedThrowInsideTheAcquiredRegionStillReleasesTheSlot(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            keyspace.add(("k%05d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        AtomicInteger probesThrown = new AtomicInteger();
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (isProbe(req)) {
                        probesThrown.incrementAndGet();
                        throw new IllegalStateException("injected unchecked fault inside the steal attempt");
                    }
                    Thread.sleep(1);   // keep workers committing, so someone reaches the steal path
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("steal-throw.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            assertThatThrownBy(() -> new Pipeline<PageBatch>(1000).run(ctx, engine, output))
                    .as("the injected fault is not swallowed — the scan aborts, as Scope requires")
                    .isInstanceOf(Throwable.class);

            assertThat(probesThrown.get())
                    .as("the fixture must reach the steal path, or nothing was injected into the region")
                    .isGreaterThan(0);
            // Every worker has exited by the time run() returns; a slot still held means a path out
            // of the acquired region skipped its finally.
            assertThat(engine.stealAttemptInFlight())
                    .as("the attempt slot is released even when the acquired region throws")
                    .isFalse();
        }
    }
}
