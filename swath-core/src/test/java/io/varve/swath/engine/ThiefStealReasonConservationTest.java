/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.policy.NoVictimReason;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Executor-protocol-contract audit, deliverable (c): a genuinely-contended
 * {@code thief.steal(pool)} run reconciling {@code swath.steal_reason} totals against the number
 * of calls made — <b>conservation, not a specific interleaving</b> (issue #18). {@code THREADS}
 * threads race {@link Thief#steal} against a small, shared, mutating pool of real {@link
 * WorkerState} victims (plus every child a winning split adds), forcing the same stale-snapshot /
 * CAS-loss races {@link Conc3TwoThievesTest} forces deterministically — except here nothing is
 * choreographed: whichever races the scheduler actually produces are what this test must survive.
 *
 * <p>The invariant asserted is interleaving-independent because every {@link Thief#steal} return
 * path funnels through exactly one terminal {@code record(Outcome, reason)} call (verified by
 * inspection of {@link Thief#steal}/{@link Thief#commit} — see the executor-protocol contract),
 * <b>with one documented exception</b>: a {@code NO_VICTIM} outcome fires a discriminator reason
 * (one of {@link NoVictimReason}'s five specific values) <i>and</i> the aggregate {@code
 * no_splittable_victim} reason in the same call — see {@link NoVictimReason}'s javadoc ("the
 * aggregate always fired alongside exactly one discriminator so the two counter series stay
 * reconcilable"). Summing only the aggregate reason for {@code NO_VICTIM} (never every {@code
 * NO_VICTIM.*} series) avoids double-counting that documented pair; the two are cross-checked
 * against each other below as well, so a divergence there — a second, unrelated
 * engagement-accounting bug — would also fail this test.
 */
final class ThiefStealReasonConservationTest {

    /** Sibling ranges the initial pool starts with — few enough that threads genuinely contend. */
    private static final int SIBLINGS = 6;
    /** Keys per sibling — enough for several generations of splits before a range goes unsplittable. */
    private static final int KEYS_PER_SIBLING = 64;
    private static final int THREADS = 8;
    private static final int ITERATIONS_PER_THREAD = 250;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "conservation-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** The synthetic cut-point before sibling {@code i}: {@code "p<ii>/"} (no real key equals it). */
    private static byte[] cut(int i) {
        return b(String.format("p%02d/", i));
    }

    /** {@code SIBLINGS} prefixes of {@code KEYS_PER_SIBLING} keys each: {@code p<ii>/<kkkkkk>}. */
    private static List<byte[]> wideKeyspace() {
        List<byte[]> keys = new ArrayList<>(SIBLINGS * KEYS_PER_SIBLING);
        for (int s = 0; s < SIBLINGS; s++) {
            for (int k = 0; k < KEYS_PER_SIBLING; k++) {
                keys.add(b(String.format("p%02d/%06d", s, k)));
            }
        }
        return keys;
    }

    /** Sums every {@code swath.steal_reason{outcome=<outcome>,reason=*}} counter, any reason. */
    private static long sumByOutcome(RunMetrics metrics, String outcome) {
        return metrics.registry().getMeters().stream()
                .filter(m -> m.getId().getName().equals("swath.steal_reason"))
                .filter(m -> outcome.equals(m.getId().getTag("outcome")))
                .filter(m -> m instanceof Counter)
                .mapToLong(m -> Math.round(((Counter) m).count()))
                .sum();
    }

    /** Reads exactly one {@code swath.steal_reason{outcome,reason}} counter (0 if never registered). */
    private static long countReason(RunMetrics metrics, String outcome, String reason) {
        return metrics.registry().getMeters().stream()
                .filter(m -> m.getId().getName().equals("swath.steal_reason"))
                .filter(m -> outcome.equals(m.getId().getTag("outcome")) && reason.equals(m.getId().getTag("reason")))
                .filter(m -> m instanceof Counter)
                .mapToLong(m -> Math.round(((Counter) m).count()))
                .sum();
    }

    @Test
    @Timeout(60)
    void contendedStealsConserveEveryStealReasonCount(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = wideKeyspace();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("conservation.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);

            // Seed SIBLINGS wide ranges tiling (⊥, ⊤] — the shared, mutating pool every thread races
            // over. cursor = lo (a fresh sub-range starts at its range_start), the same convention
            // Conc2WideSiblingsTest's seed uses.
            List<WorkerState> pool = new CopyOnWriteArrayList<>();
            for (int s = 0; s < SIBLINGS; s++) {
                byte[] lo = (s == 0) ? null : cut(s);
                byte[] hi = (s == SIBLINGS - 1) ? null : cut(s + 1);
                long nodeId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
                pool.add(WorkerStates.of(nodeId, lo, lo, hi));
            }

            // Every winning split's child joins the SAME live pool other threads are already racing
            // over — real, not scripted, contention keeps arriving as the run progresses.
            Thief.ChildSink childSink = (childId, childLo, childHi) ->
                    pool.add(WorkerStates.of(childId, childLo, childLo, childHi));
            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS, childSink, metrics);

            ExecutorService exec = Executors.newFixedThreadPool(THREADS);
            CountDownLatch start = new CountDownLatch(1);
            List<AtomicReference<Throwable>> errors = new ArrayList<>();
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int t = 0; t < THREADS; t++) {
                    AtomicReference<Throwable> err = new AtomicReference<>();
                    errors.add(err);
                    futures.add(exec.submit(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                                thief.steal(pool);
                            }
                        } catch (Throwable th) {
                            err.set(th);
                        }
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                exec.shutdownNow();
            }

            for (AtomicReference<Throwable> err : errors) {
                assertThat(err.get()).as("a racing thief thread threw").isNull();
            }

            long totalCalls = (long) THREADS * ITERATIONS_PER_THREAD;

            long noVictim = countReason(metrics, "NO_VICTIM", NoVictimReason.NO_SPLITTABLE_VICTIM.code());
            long retry = sumByOutcome(metrics, "RETRY");
            long unsplittable = sumByOutcome(metrics, "UNSPLITTABLE");
            long childCreated = sumByOutcome(metrics, "CHILD_CREATED");

            // The headline conservation invariant: every one of the totalCalls steal() invocations
            // landed in EXACTLY one of the four outcome buckets, regardless of which races the
            // scheduler actually produced.
            assertThat(noVictim + retry + unsplittable + childCreated)
                    .as("every steal() call lands in exactly one outcome bucket")
                    .isEqualTo(totalCalls);

            // The reconciliation NoVictimReason's own javadoc claims (discriminators sum to the
            // aggregate) -- checked here too, so a second, unrelated engagement-accounting bug in
            // that pairing would also fail this test rather than being silently absorbed above.
            long discriminatorSum = Arrays.stream(NoVictimReason.values())
                    .filter(r -> r != NoVictimReason.NO_SPLITTABLE_VICTIM)
                    .mapToLong(r -> countReason(metrics, "NO_VICTIM", r.code()))
                    .sum();
            assertThat(discriminatorSum)
                    .as("every NO_VICTIM discriminator sums to the aggregate no_splittable_victim count")
                    .isEqualTo(noVictim);

            // Sanity: this is a contention test, not a no-op loop -- real splits must have committed.
            assertThat(childCreated).as("at least some splits committed under contention").isGreaterThan(0);
        }
    }
}
