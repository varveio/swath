/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.testkit.AbortingCheckpointStore;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PageGate;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RangeScanners;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two mandatory PROP-1 corner cases driven with a <b>seeded, explicit
 * step ordering</b> (latches / a one-shot abort), not random timing — the narrow
 * in-flight-page-vs-steal window and the split-guard ABORT path a random generator rarely
 * lands. Both use a <b>bounded</b> victim (finite {@code hi}), so the pivot is an exact
 * {@code byteMidpoint} and the interleaving is fully deterministic.
 *
 * <p>The stepping control is the reusable {@link PageGate} + {@link AbortingCheckpointStore}
 * (testkit), which RES-3 / CONC-3 reuse.
 */
final class Prop1InterleavingTest {

    private static final long RUN = 1L;

    /** Single-byte keys (all valid 1-byte UTF-8, < 0x80) spanning a wide range. */
    private static List<byte[]> singleByteKeys(int... vals) {
        List<byte[]> ks = new ArrayList<>();
        for (int v : vals) {
            ks.add(new byte[]{(byte) v});
        }
        return ks;
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "interleave-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    // -------------------------------------------------------------------------
    // (i) in-flight-page-vs-steal: a thief narrows victim.hi to a pivot m that falls
    //     INSIDE an in-flight page; the per-key volatile hi re-read must stop the victim
    //     at m (emit k <= m), leaving the keys > m to the child (m, H] — no double-emit.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void inFlightPageVsStealStopsVictimAtPivot(@TempDir Path dir) throws Exception {
        // 8 keys 0x10..0x70; bound H = 0x7e. byteMidpoint(⊥, 0x7e) = 0x3E (synthetic, not a key):
        // keys <= 0x3E = {0x10,0x20,0x30}; keys in (0x3E,0x7e] = {0x40,0x50,0x60,0x70}.
        List<byte[]> keyspace = singleByteKeys(0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70);
        byte[] H = {(byte) 0x7e};
        byte[] expectedPivot = {(byte) 0x3E};

        // Gate ONLY the victim's main page (startAfter == null, maxKeys > 1). The thief's
        // 1-key probe (startAfter = m) and the child re-list pass straight through.
        PageGate gate = new PageGate(req -> req.startAfter() == null && req.maxKeys() > 1);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace).interceptor(gate.interceptor()).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            // The victim node: (⊥, H], nothing emitted yet.
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, H, null, null));
            WorkerState victim = WorkerStates.of(victimId, null, null, H);

            // Worker thread: scan (⊥, H] with the per-key volatile hi re-read wired to the victim.
            List<byte[]> victimEmitted = new ArrayList<>();
            AtomicReference<Boolean> lastCompleted = new AtomicReference<>();
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            RangeScanner scanner = RangeScanners.of(fetcher, 16);
            Thread worker = new Thread(() -> {
                try {
                    scanner.runRange(new byte[0], ListingMode.OBJECTS, null, victim.hiSupplier(), null,
                            (batch, lastKey, completed) -> {
                                for (ListEntry e : batch) {
                                    victimEmitted.add(e.key().raw());
                                }
                                lastCompleted.set(completed);
                            });
                } catch (Throwable t) {
                    workerError.set(t);
                }
            }, "victim-worker");
            worker.start();

            // Step 1: wait until the victim's page is fetched (under the OLD wide bound) and parked.
            gate.awaitFetched();

            // Step 2: a thief steals — snapshots (⊥, H), computes m = 0x3E (inside the in-flight
            // page), probes it, narrows victim.hi = m under the lock, and durably splits.
            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, lo, hi) -> { });
            Thief.Outcome outcome = thief.steal(List.of(victim));
            assertThat(outcome).isEqualTo(Thief.Outcome.CHILD_CREATED);
            assertThat(victim.hi()).isEqualTo(expectedPivot);   // narrowed to m, mid-flight

            // Step 3: release the parked page — the victim now re-reads the narrowed hi PER KEY.
            gate.release();
            worker.join(30_000);
            assertThat(workerError.get()).as("victim worker threw").isNull();

            // The victim stopped at m: emitted exactly the keys <= m, completed the node.
            assertThat(lastCompleted.get()).isTrue();
            assertThat(victimEmitted).containsExactly(
                    new byte[]{0x10}, new byte[]{0x20}, new byte[]{0x30});
            for (byte[] k : victimEmitted) {
                assertThat(KeyBytes.compareUnsigned(k, expectedPivot))
                        .as("victim key <= m").isLessThanOrEqualTo(0);
            }

            // The child (m, H] covers exactly the keys the victim did NOT emit — re-list it.
            List<byte[]> childEmitted = new ArrayList<>();
            RangeScanners.of(fetcher, 16).runRange(new byte[0], ListingMode.OBJECTS, expectedPivot, H, null,
                    (batch, lastKey, completed) -> batch.forEach(e -> childEmitted.add(e.key().raw())));
            assertThat(childEmitted).containsExactly(
                    new byte[]{0x40}, new byte[]{0x50}, new byte[]{0x60}, new byte[]{0x70});

            // Tiling: victim ∪ child == keyspace, pairwise-disjoint — no gap, no double-emit.
            assertUnionTilesWithNoOverlap(victimEmitted, childEmitted, keyspace);

            // The split landed exactly where the volatile narrow did (pivot belongs LEFT, I3).
            List<RangePartition.Split> splits = store.splits();
            assertThat(splits).hasSize(1);
            assertThat(splits.get(0).pivot()).isEqualTo(expectedPivot);
            assertThat(splits.get(0).oldHi()).isEqualTo(H);
        }
    }

    // -------------------------------------------------------------------------
    // (ii) split-guard ABORT path: a split whose durable CAS returns rowcount 0
    //      (SPLIT_ABORTED) must make the thief RESTORE victim.hi = H and RETRY, leaving the
    //      range set still tiling (no gap) — algorithms.md §3/§4.3.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void splitGuardAbortRestoresHiAndStillTiles(@TempDir Path dir) throws Exception {
        // Keys 0x10..0x70, bound H = 0x7a. First split (m1 = 0x3C) commits; the SECOND split is
        // forced to ABORT, so the thief must restore victim.hi from m2 back to m1.
        List<byte[]> keyspace = singleByteKeys(0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70);
        byte[] H = {(byte) 0x7a};
        byte[] m1 = {(byte) 0x3C};
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            // Abort the SECOND split attempt (0-based index 1); record the first (committed) split.
            AbortingCheckpointStore aborting =
                    new AbortingCheckpointStore(sqlite, (idx, spec) -> idx == 1);
            RecordingSplitStore store = new RecordingSplitStore(aborting);
            RunMeta run = store.openRun(key(), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, H, null, null));
            WorkerState victim = WorkerStates.of(victimId, null, null, H);

            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, lo, hi) -> { });

            // Steal #1 commits: victim → (⊥, m1], child1 → (m1, H].
            assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
            assertThat(victim.hi()).isEqualTo(m1);

            // Steal #2 narrows hi to m2, then the durable CAS ABORTs → thief restores hi = m1.
            Thief.Outcome second = thief.steal(List.of(victim));
            assertThat(second).isEqualTo(Thief.Outcome.RETRY);
            assertThat(aborting.aborts()).isEqualTo(1);
            assertThat(victim.hi()).as("hi restored to the validated bound after ABORT").isEqualTo(m1);

            // Only the first split is durable. The range set {(⊥, m1], (m1, H]} still tiles (⊥, H]:
            // the aborted split created no child and left no gap.
            List<RangePartition.Split> splits = store.splits();
            assertThat(splits).hasSize(1);
            RangePartition.Split s1 = splits.get(0);
            assertThat(s1.pivot()).isEqualTo(m1);
            assertThat(s1.oldHi()).isEqualTo(H);
            // victim.hi == child1.lo (== m1): boundary-belongs-left, contiguous, covers (⊥, H].
            assertThat(victim.hi()).isEqualTo(s1.pivot());
        }
    }

    /** Assert {@code left ∪ right} equals {@code keyspace} byte-exactly with empty intersection. */
    private static void assertUnionTilesWithNoOverlap(List<byte[]> left, List<byte[]> right,
                                                      List<byte[]> keyspace) {
        TreeSet<byte[]> union = new TreeSet<>(Arrays::compareUnsigned);
        union.addAll(left);
        union.addAll(right);
        assertThat(union).as("no double-emit between victim and child")
                .hasSize(left.size() + right.size());
        TreeSet<byte[]> expected = new TreeSet<>(Arrays::compareUnsigned);
        expected.addAll(keyspace);
        assertThat(union.size()).isEqualTo(expected.size());
        var u = union.iterator();
        var e = expected.iterator();
        while (u.hasNext()) {
            assertThat(Arrays.equals(u.next(), e.next())).isTrue();
        }
    }
}
