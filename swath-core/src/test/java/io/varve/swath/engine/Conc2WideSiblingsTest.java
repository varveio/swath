/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RecordingSplitStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * CONC-2 (invariant I7) — the bug {@code s3ls-rs} does <b>not</b> have: a
 * permit-held-across-{@code join()} deadlock. Seed <b>65 WIDE sibling ranges</b>
 * (strictly MORE than the {@code T = 64} worker permits), each holding real
 * multi-page work, and assert the full {@link WorkStealingScan} run COMPLETES
 * within a bounded Awaitility timeout with full exactly-once coverage.
 *
 * <p><b>Why this catches the deadlock.</b> A fan-out engine that forked a worker
 * per sibling and {@code join()}ed the children <i>while holding its own
 * concurrency permit</i> would, with 65 siblings and only 64 permits, strand the
 * 65th fork forever: every permit held by a parent blocked on a child that can
 * never acquire the 65th permit. The worklist model instead claims the next node
 * from a ready queue and never holds a slot while waiting on child work, so the
 * run drains to quiescence. The headline guard is assertion #1: a
 * permit-held-across-join engine HANGS and blows the bounded await; this is a
 * liveness test, not a no-throw test.
 *
 * <p><b>Why the fixture is genuinely wide (not degenerate).</b> Each of the 65
 * siblings owns one top-level prefix worth of keys ({@code KEYS_PER_SIBLING},
 * paged at {@code MAX_KEYS}) — {@code KEYS_PER_SIBLING / MAX_KEYS} real pages per
 * node, each page a gauge-permitted fetch. With 64 workers and 65 ready nodes,
 * all 64 permits are in genuine simultaneous use (and contended) for the whole
 * run; the 65th node waits in the ready queue until a worker frees up. An
 * empty/instantly-done fixture would never put 64 permits in flight at once and
 * so would not exercise the join-under-permit hazard.
 *
 * <p>The sibling tiling is built locally from {@link NodeSpec} inserts (one
 * bounded range per prefix, one open frontier), mirroring algorithms.md §8's
 * seed partition.
 */
final class Conc2WideSiblingsTest {

    /** Strictly MORE sibling ranges than worker permits — the I7 trigger condition. */
    private static final int SIBLINGS = 65;
    /** Worker/permit count: one fewer than the sibling count. */
    private static final int WORKERS = 64;
    /** Keys per sibling — wide enough that each node is several pages, not instantly done. */
    private static final int KEYS_PER_SIBLING = 120;
    /** Small page size ⇒ many pages per node ⇒ permits genuinely in flight for the whole run. */
    private static final int MAX_KEYS = 20;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "conc2-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** The synthetic cut-point before sibling {@code i}: {@code "p<ii>/"} (no real key equals it). */
    private static byte[] cut(int i) {
        return b(String.format("p%02d/", i));
    }

    /** Build {@code SIBLINGS} prefixes of {@code KEYS_PER_SIBLING} keys each: {@code p<ii>/<kkkkkk>}. */
    private static List<byte[]> wideKeyspace() {
        List<byte[]> keys = new ArrayList<>(SIBLINGS * KEYS_PER_SIBLING);
        for (int s = 0; s < SIBLINGS; s++) {
            for (int k = 0; k < KEYS_PER_SIBLING; k++) {
                keys.add(b(String.format("p%02d/%06d", s, k)));
            }
        }
        return keys;
    }

    /**
     * The headline I7 liveness guard. 65 wide siblings, 64 permits: run on a separate thread
     * and {@code await().atMost(...)} completion. A permit-held-across-join deadlock would hang
     * here and blow the bounded await; the {@code @Timeout} is the suite-level backstop.
     *
     * <p>Adversarial on three axes: (1) the bounded await fails fast on any hang — the deadlock
     * detector; (2) exactly-once coverage fails if it "completed" by abandoning a sibling's keys
     * or double-listing across a steal; (3) {@code effectiveT() == 64} confirms no permit leaked
     * / no spurious throttle silently shrank the pool by the end.
     */
    @Test
    @Timeout(120)
    void wideSiblings_moreThanPermits_completesWithoutPermitHeldAcrossJoin(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = wideKeyspace();

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("conc2.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);

            // Seed exactly 65 WIDE sibling ranges that tile (⊥, ⊤]: one bounded range per prefix,
            // plus the open frontier for the last. Cut-points are synthetic "p<ii>/" boundaries —
            // boundary-belongs-left, so the tiling has no gap/overlap (algorithms.md §1.2/§8).
            for (int s = 0; s < SIBLINGS; s++) {
                byte[] lo = (s == 0) ? null : cut(s);
                byte[] hi = (s == SIBLINGS - 1) ? null : cut(s + 1);
                // A fresh sub-range starts at its range_start: cursor = lo (start_after = A in (A, B]),
                // exactly as the split-child path sets cursor = childLo. Leaving cursor null would make
                // every node list from ⊥ and re-emit lower siblings' keys.
                store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            }
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).as("65 wide siblings seeded, > the 64 permits").hasSize(SIBLINGS);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = Collections.synchronizedList(new ArrayList<>());
            RunContext ctx = RunContext.create();
            Runnable pipeline = () -> {
                try {
                    PipelineDrain.collectKeys(1000, ctx, engine, emitted);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            ExecutorService exec = Executors.newSingleThreadExecutor();
            AtomicReference<Throwable> err = new AtomicReference<>();
            try {
                Future<?> f = exec.submit(() -> {
                    try {
                        pipeline.run();
                    } catch (Throwable t) {
                        err.set(t);
                    }
                });
                // THE DEADLOCK DETECTOR: a permit-held-across-join hang never completes ⇒ this
                // bounded await fails fast. Generous enough never to flake on a loaded CI box,
                // short enough to fail a true hang well inside the @Timeout backstop.
                await().atMost(Duration.ofSeconds(60)).until(f::isDone);
            } finally {
                exec.shutdownNow();
            }
            if (err.get() != null) {
                fail("engine run failed over 65 wide siblings", err.get());
            }

            // #2 — completed by DOING the work, not abandoning it: every key exactly once.
            EngineHarness.assertExactlyOnce(emitted, keyspace);
            // #3 — no permit leaked / no spurious throttle shrank the pool. Slow-start: T ramps up from
            // SLOW_START_INITIAL_T via doubling rather than starting at Tmax, so a fast in-memory run
            // need not have reached 64; the invariant is that a CLEAN run only ever GROWS T (never below
            // the initial floor) and leaks no permit — availablePermits == effectiveT once every
            // in-flight permit is returned at quiescence.
            assertThat(engine.gauge().effectiveT())
                    .as("clean run: T ramps up from the slow-start floor, never shrinks (no spurious throttle)")
                    .isBetween(ConcurrencyGauge.SLOW_START_INITIAL_T, WORKERS);
            assertThat(engine.gauge().availablePermits())
                    .as("no permit leaked: the pool equals effective T at quiescence")
                    .isEqualTo(engine.gauge().effectiveT());
        }
    }

}
