/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ListingMode;
import io.varve.swath.testkit.AbortingCheckpointStore;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PageGate;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * CONC-3 — two thieves contend for one victim, the stale-snapshot race (algorithms.md §3/§4.3).
 * Two idle workers pick the <b>same</b> victim {@code (lo, H]} with pivot {@code m}; the loser's
 * pre-lock snapshot of {@code H} is stale. The two loser paths differ by exactly one thing —
 * whether the loser touched {@code victim.hi} — and are asserted <b>separately</b>, plus the
 * durable {@code status <> COMPLETED} backstop:
 *
 * <ul>
 *   <li><b>(1) Early loser:</b> thief B observes {@code victim.hi != H} (thief A already
 *       narrowed it) <b>before</b> B narrows anything → B RETRYs and must <b>not</b> touch
 *       {@code victim.hi} (restoring {@code H} would clobber A's narrow and re-open the overlap).
 *       Asserted: B returns RETRY, {@code victim.hi} is exactly A's narrow {@code m} (never
 *       reverted to {@code H}), and exactly one split is durable.</li>
 *   <li><b>(2) Late loser:</b> thief B narrows {@code victim.hi = m}, then {@code splitNode}
 *       returns ABORT (B lost the durable CAS — modeled with {@link AbortingCheckpointStore}) →
 *       B <b>restores</b> {@code victim.hi = H} and RETRYs. Asserted: {@code victim.hi} is
 *       restored to {@code H} (not left at {@code m}), B returns RETRY, no split is durable.</li>
 *   <li><b>(3) COMPLETED victim rejects the split:</b> a victim that finished durably via an
 *       empty/terminal page (status COMPLETED, cursor unmoved) but still looks splittable
 *       in-memory must have its split rejected by the <b>real</b> {@code status <> COMPLETED}
 *       CAS clause — no child, no durable narrow; the thief restores {@code hi} and RETRYs.</li>
 * </ul>
 *
 * <p>Every interleaving is forced <b>deterministically</b> (a {@link PageGate} parks the loser's
 * speculative probe so it carries a stale {@code H} into the lock; an {@link AbortingCheckpointStore}
 * injects the durable CAS loss) — never wall-clock racing.
 */
final class Conc3TwoThievesTest {

    /** Single-byte keys (all valid 1-byte UTF-8, &lt; 0x80) spanning a wide range. */
    private static List<byte[]> singleByteKeys(int... vals) {
        List<byte[]> ks = new ArrayList<>();
        for (int v : vals) {
            ks.add(new byte[]{(byte) v});
        }
        return ks;
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "conc3-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static Thief noChildSinkThief(RecordingSplitStore store, MockPageFetcher fetcher, long runId) {
        return Thiefs.of(store, fetcher, runId, new byte[0], ListingMode.OBJECTS, (id, lo, hi) -> { });
    }

    // -------------------------------------------------------------------------
    // (1) EARLY loser: B's pre-lock snapshot of H is stale (A narrowed first).
    //     B must observe victim.hi != H and RETRY WITHOUT restoring H (no clobber of A's narrow).
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void earlyLoserObservesNarrowedHiAndRetriesWithoutClobbering(@TempDir Path dir) throws Exception {
        // Keys 0x10..0x70, bound H = 0x7e. byteMidpoint(⊥, 0x7e) = 0x3E (synthetic). Both thieves
        // snapshot (⊥, 0x7e) and compute the SAME pivot m = 0x3E; probe(0x3E, 0x7e) finds 0x40.
        List<byte[]> keyspace = singleByteKeys(0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70);
        byte[] H = {(byte) 0x7e};
        byte[] m = {(byte) 0x3E};

        // Park ONLY the loser B's speculative probe (maxKeys == 1) AFTER its snapshot of H but
        // BEFORE it takes the lock — so B carries the stale H into the lock. The gate fires once,
        // so the FIRST probe (B's) parks; A's later probe passes straight through.
        PageGate gate = new PageGate(req -> req.maxKeys() == 1);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace).interceptor(gate.interceptor()).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, H, null, null));
            WorkerState victim = WorkerStates.of(victimId, null, null, H);

            Thief thief = noChildSinkThief(store, fetcher, run.id());

            // Thief B on a background thread: selects the victim, snapshots (⊥, H), computes m,
            // then BLOCKS at its probe (stale H captured, no lock held).
            AtomicReference<Thief.Outcome> bOutcome = new AtomicReference<>();
            AtomicReference<Throwable> bError = new AtomicReference<>();
            Thread thiefB = new Thread(() -> {
                try {
                    bOutcome.set(thief.steal(List.of(victim)));
                } catch (Throwable t) {
                    bError.set(t);
                }
            }, "thief-B");
            thiefB.start();

            // B is parked at its probe with H still == 0x7e in its snapshot.
            gate.awaitFetched();

            // Thief A (main thread) runs to completion: snapshot (⊥, H), narrow victim.hi = m,
            // durable split commits child (m, H]. A's probe passes the (now disarmed) gate.
            assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
            assertThat(victim.hi()).as("A narrowed victim.hi to the pivot").isEqualTo(m);

            // Release B: it re-acquires the lock, observes victim.hi (== m) != H (== 0x7e) → RETRY.
            gate.release();
            thiefB.join(30_000);
            assertThat(bError.get()).as("thief B threw").isNull();

            // EARLY-loser contract: RETRY, and B did NOT touch victim.hi — it is still A's narrow m,
            // NOT reverted to H. Reverting to H here would clobber A's narrow and re-open (m, H].
            assertThat(bOutcome.get()).as("early loser RETRYs").isEqualTo(Thief.Outcome.RETRY);
            assertThat(victim.hi()).as("early loser must NOT clobber the winner's narrow").isEqualTo(m);
            assertThat(victim.hi()).as("early loser must NOT restore H").isNotEqualTo(H);

            // Exactly one split is durable: B never reached splitNode (no overlapping (m, H] child).
            assertThat(store.splits()).as("only the winner's split is durable").hasSize(1);
            assertThat(store.splits().getFirst().pivot()).isEqualTo(m);
            assertThat(store.splits().getFirst().oldHi()).isEqualTo(H);
        }
    }

    // -------------------------------------------------------------------------
    // (2) LATE loser: B narrows victim.hi = m, then the durable splitTxn ABORTs
    //     (lost the CAS to the winner) → B must RESTORE victim.hi = H and RETRY.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void lateLoserNarrowsThenAbortsAndRestoresHi(@TempDir Path dir) throws Exception {
        // Keys 0x10..0x70, bound H = 0x7a. byteMidpoint(⊥, 0x7a) = 0x3C; probe(0x3C, 0x7a) finds 0x40.
        // The injected ABORT models the durable CAS that the (conceptual) winning thief already took.
        List<byte[]> keyspace = singleByteKeys(0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70);
        byte[] H = {(byte) 0x7a};
        byte[] m = {(byte) 0x3C};
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            // Abort the first (only) split: the loser's durable CAS loses with no delegate touch.
            AbortingCheckpointStore aborting = AbortingCheckpointStore.abortFirst(sqlite);
            RecordingSplitStore store = new RecordingSplitStore(aborting);
            RunMeta run = store.openRun(key(), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, H, null, null));
            WorkerState victim = WorkerStates.of(victimId, null, null, H);

            Thief thief = noChildSinkThief(store, fetcher, run.id());

            // Loser B: snapshot (⊥, H), narrow victim.hi = m, splitNode → ABORT → restore hi = H.
            Thief.Outcome outcome = thief.steal(List.of(victim));

            assertThat(outcome).as("late loser RETRYs").isEqualTo(Thief.Outcome.RETRY);
            assertThat(aborting.aborts()).as("the durable split aborted exactly once").isEqualTo(1);
            // LATE-loser contract: hi restored to the bound validated under the lock (H), NOT left at m.
            assertThat(victim.hi()).as("late loser restores victim.hi = H after the abort").isEqualTo(H);
            assertThat(victim.hi()).as("late loser must NOT leave hi at the pivot m").isNotEqualTo(m);
            // No child is durable (a torn narrow with no child would be a gap).
            assertThat(store.splits()).as("no split is durable after the abort").isEmpty();
        }
    }

    // -------------------------------------------------------------------------
    // (3) COMPLETED victim rejects the split: a victim that finished durably via an
    //     empty/terminal page (status COMPLETED, cursor never advanced) but still looks
    //     splittable in-memory must be rejected by the REAL status<>COMPLETED CAS clause.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void completedVictimRejectsSplit(@TempDir Path dir) throws Exception {
        // Keys 0x10..0x70, bound H = 0x7e. byteMidpoint(⊥, 0x7e) = 0x3E; probe(0x3E, 0x7e) finds 0x40,
        // so the steal advances all the way to splitNode — where the REAL CAS must reject it.
        List<byte[]> keyspace = singleByteKeys(0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70);
        byte[] H = {(byte) 0x7e};
        byte[] m = {(byte) 0x3E};
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, H, null, null));

            // Durably COMPLETE the victim via an empty/terminal page: advanceTo == null leaves the
            // cursor at ⊥ (so cursor < m still HOLDS), completed == true sets status = COMPLETED.
            // This isolates the status<>COMPLETED clause: cursor IS NULL and range_end IS H both
            // pass, so ONLY the status clause can abort the split.
            store.commitPage(new PageCommit(victimId, null, true));

            // In-memory the worker still looks fresh & splittable (cursor ⊥, hi H): the race where
            // a victim's terminal page completed it durably while a thief still holds a live snapshot.
            WorkerState victim = WorkerStates.of(victimId, null, null, H);

            Thief thief = noChildSinkThief(store, fetcher, run.id());
            Thief.Outcome outcome = thief.steal(List.of(victim));

            // The real CAS rejects (status COMPLETED) → SPLIT_ABORTED → restore hi → RETRY.
            assertThat(outcome).as("a COMPLETED victim's split is rejected → RETRY").isEqualTo(Thief.Outcome.RETRY);
            assertThat(store.splits()).as("no child created against a COMPLETED victim").isEmpty();
            assertThat(victim.hi()).as("hi restored to H after the COMPLETED-victim abort").isEqualTo(H);
            assertThat(victim.hi()).as("hi must not be left narrowed at m").isNotEqualTo(m);
        }
    }
}
